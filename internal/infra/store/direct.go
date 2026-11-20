package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
)

// directPage is how many messages one direct get asks for. A retained
// replay or an offline queue rarely fills one, and a bigger one only makes
// the reply buffer bigger.
const directPage = 256

// errDirectGet reports a direct get the server answered with an error status.
var errDirectGet = errors.New("store: direct get failed")

// directReader issues batched direct gets: one request, many messages back,
// read from any replica and with no consumer behind it.
//
// nats.go has no client for the batched form, and the shape of the reply is
// why: it is a run of messages ending in a status, not one response, so the
// request/reply helper — which takes the first message — cannot read it.
//
// Replies come in on one wildcard subscription the reader holds for its
// whole life rather than an inbox per read. A subscription per read is a
// SUB and an UNSUB that cross the leaf to the core, and a retained replay
// runs on every SUBSCRIBE, so a reconnect storm would have been an interest
// storm too — the cost the session control plane was rebuilt to get rid of.
type directReader struct {
	nc *nats.Conn

	mu      sync.Mutex
	sub     *nats.Subscription
	inbox   string
	waiting map[string]chan *nats.Msg
	next    atomic.Uint64
}

func newDirectReader(nc *nats.Conn) *directReader {
	return &directReader{
		nc:      nc,
		mu:      sync.Mutex{},
		sub:     nil,
		inbox:   "",
		waiting: make(map[string]chan *nats.Msg),
		next:    atomic.Uint64{},
	}
}

// directRequest is the subset of JetStream's message get request a batched
// read uses.
type directRequest struct {
	Seq     uint64 `json:"seq,omitempty"`
	NextFor string `json:"next_by_subj"`
	Batch   int    `json:"batch"`

	// StartTime starts the read at the first message stored at or after
	// it, in place of Seq. The server refuses both at once.
	StartTime *time.Time `json:"start_time,omitempty"`
}

// each calls fn with every message on stream whose subject matches filter,
// in sequence order from sequence from, until fn returns false.
func (d *directReader) each(
	ctx context.Context,
	apiPrefix, stream, filter string,
	from uint64,
	fn func(m *nats.Msg, seq uint64) bool,
) error {
	return d.eachFrom(ctx, apiPrefix, stream, filter, from, nil, fn)
}

// eachSince is [directReader.each] from the first message stored at or
// after since.
func (d *directReader) eachSince(
	ctx context.Context,
	apiPrefix, stream, filter string,
	since time.Time,
	fn func(m *nats.Msg, seq uint64) bool,
) error {
	return d.eachFrom(ctx, apiPrefix, stream, filter, 0, &since, fn)
}

func (d *directReader) eachFrom(
	ctx context.Context,
	apiPrefix, stream, filter string,
	from uint64,
	since *time.Time,
	fn func(m *nats.Msg, seq uint64) bool,
) error {
	for {
		last, more, err := d.page(ctx, apiPrefix+"DIRECT.GET."+stream, directRequest{
			Seq: from, NextFor: filter, Batch: directPage, StartTime: since,
		}, fn)
		if err != nil || !more {
			return err
		}

		// Only the first page starts by time; the rest follow on.
		from, since = last+1, nil
	}
}

// page reads one batch. more reports that the server said there is more to
// read past last.
func (d *directReader) page(
	ctx context.Context,
	subject string,
	req directRequest,
	fn func(m *nats.Msg, seq uint64) bool,
) (uint64, bool, error) {
	reply, replies, err := d.register()
	if err != nil {
		return 0, false, err
	}
	defer d.unregister(reply)

	if err := d.request(subject, reply, req); err != nil {
		return 0, false, err
	}

	var last uint64

	for {
		m, err := next(ctx, subject, replies)
		if err != nil {
			return last, false, err
		}

		seq, end, more, err := readReply(m)
		if err != nil {
			return last, false, err
		}

		// Every reply names the sequence of the one before it, and the
		// status that ends the batch names the last. A reply missing in
		// between — dropped by a subscription that fell behind, which nats.go
		// reports only to an error handler — would otherwise read as a short
		// batch, and a drain would trim the message it never delivered.
		if err := continuous(m, last); err != nil {
			return last, false, err
		}

		if end {
			return last, more && last > 0, nil
		}

		last = seq

		if !fn(m, seq) {
			return last, false, nil
		}
	}
}

// next waits for one reply.
func next(ctx context.Context, subject string, replies <-chan *nats.Msg) (*nats.Msg, error) {
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("store: direct get on %s: %w", subject, ctx.Err())
	case m := <-replies:
		return m, nil
	}
}

func (d *directReader) request(subject, reply string, req directRequest) error {
	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("store: encoding direct get: %w", err)
	}

	if err := d.nc.PublishRequest(subject, reply, body); err != nil {
		return fmt.Errorf("store: direct get on %s: %w", subject, err)
	}

	return nil
}

// readReply classifies one message of a batched reply: a stored message
// with its sequence, or the status that ends the batch and whether the
// server has more past it.
func readReply(m *nats.Msg) (uint64, bool, bool, error) {
	switch status := m.Header.Get("Status"); status {
	case "":
		seq, err := strconv.ParseUint(m.Header.Get(nats.JSSequence), 10, 64)
		if err != nil {
			return 0, false, false, fmt.Errorf("%w: reply without a sequence", errDirectGet)
		}

		return seq, false, false, nil
	case "204": // end of batch
		pending, _ := strconv.ParseUint(m.Header.Get("Nats-Num-Pending"), 10, 64)

		return 0, true, pending > 0, nil
	case "404": // nothing matches
		return 0, true, false, nil
	default:
		return 0, false, false, fmt.Errorf("%w: %s %s", errDirectGet, status, m.Header.Get("Description"))
	}
}

// errDirectGap reports a batched read that lost a reply in transit.
var errDirectGap = errors.New("store: direct get lost a reply")

// continuous checks that m follows the reply whose sequence was last. A
// reply without the header — a 404, or a server too old to send it — is
// taken on trust.
func continuous(m *nats.Msg, last uint64) error {
	raw := m.Header.Get(nats.JSLastSequence)
	if raw == "" {
		return nil
	}

	prev, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || prev == last {
		return nil //nolint:nilerr // an unreadable header is not evidence of a gap
	}

	return fmt.Errorf("%w: after %d came one following %d", errDirectGap, last, prev)
}

// register opens a reply slot. Its channel holds a full page and the status
// that ends it, so the subscription's handler never has to block.
func (d *directReader) register() (string, chan *nats.Msg, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.sub == nil {
		inbox := nats.NewInbox() + "."

		sub, err := d.nc.Subscribe(inbox+"*", d.deliver)
		if err != nil {
			return "", nil, fmt.Errorf("store: direct get inbox: %w", err)
		}

		d.sub, d.inbox = sub, inbox
	}

	token := strconv.FormatUint(d.next.Add(1), 36)
	replies := make(chan *nats.Msg, directPage+1)
	d.waiting[token] = replies

	return d.inbox + token, replies, nil
}

func (d *directReader) unregister(reply string) {
	d.mu.Lock()
	delete(d.waiting, strings.TrimPrefix(reply, d.inbox))
	d.mu.Unlock()
}

// deliver routes one reply to the read waiting for it. A reply for a read
// that already gave up is dropped.
func (d *directReader) deliver(m *nats.Msg) {
	d.mu.Lock()
	replies, ok := d.waiting[strings.TrimPrefix(m.Subject, d.inbox)]
	d.mu.Unlock()

	if !ok {
		return
	}

	select {
	case replies <- m:
	default: // more than a page and its status: not something the server sends
	}
}

func (d *directReader) close() {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.sub != nil {
		_ = d.sub.Unsubscribe()
		d.sub = nil
	}
}
