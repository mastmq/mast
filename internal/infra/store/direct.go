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
}

// each calls fn with every message on stream whose subject matches filter,
// in sequence order from sequence from, until fn returns false.
func (d *directReader) each(
	ctx context.Context,
	apiPrefix, stream, filter string,
	from uint64,
	fn func(m *nats.Msg, seq uint64) bool,
) error {
	for {
		last, more, err := d.page(ctx, apiPrefix+"DIRECT.GET."+stream, filter, from, fn)
		if err != nil || !more {
			return err
		}

		from = last + 1
	}
}

// page reads one batch. more reports that the server said there is more to
// read past last.
func (d *directReader) page(
	ctx context.Context,
	subject, filter string,
	from uint64,
	fn func(m *nats.Msg, seq uint64) bool,
) (uint64, bool, error) {
	reply, replies, err := d.register()
	if err != nil {
		return 0, false, err
	}
	defer d.unregister(reply)

	if err := d.request(subject, reply, filter, from); err != nil {
		return 0, false, err
	}

	var last uint64

	for {
		var m *nats.Msg

		select {
		case <-ctx.Done():
			return last, false, fmt.Errorf("store: direct get on %s: %w", subject, ctx.Err())
		case m = <-replies:
		}

		seq, end, more, err := readReply(m)

		switch {
		case err != nil:
			return last, false, err
		case end:
			return last, more && last > 0, nil
		}

		last = seq

		if !fn(m, seq) {
			return last, false, nil
		}
	}
}

func (d *directReader) request(subject, reply, filter string, from uint64) error {
	body, err := json.Marshal(directRequest{Seq: from, NextFor: filter, Batch: directPage})
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
