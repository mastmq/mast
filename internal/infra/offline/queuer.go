// Package offline keeps the messages a device misses while it is away, for
// as long as its session lasts, whatever happens to the edge it last used.
//
// # Why a queuer
//
// The offline queue used to be filled by one node only: the one a device
// disconnected from, because that node kept the session's subscriptions,
// and mochi on that node was the thing that noticed a message for a client
// with no connection. That ties a session's whole backlog to the life of one
// edge pod, and edges are the tier that restarts on every deploy. Once the
// pod was gone nothing anywhere was subscribed on the device's behalf, and
// everything published until it woke was dropped.
//
// The queuer moves that job to the nodes that carry JetStream — the cores,
// or the single all-in-one node — which are the stable tier. It needs two
// things, and gets each without a consumer per device:
//
//   - who is away, and what they subscribed to: one watch on the sessions
//     bucket, held in a topic index in memory.
//   - every QoS 1 and 2 message: one consumer of its own on the durable
//     stream, the same per-node shape the bridge already uses.
//
// Each core runs one, and they all write the same message for the same
// session under the same id, which the queue stream stores once. The edge
// still writes too, at the moment mochi sees the client is gone, because the
// queuer only learns that once the record saying so reaches it.
//
// QoS 0 is never queued, here or anywhere: MQTT allows discarding it for an
// absent client, and the durable stream does not carry it.
package offline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mastmq/mast/internal/domain/topic"
	"github.com/mastmq/mast/internal/infra/obs"
	"github.com/mastmq/mast/internal/infra/store"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Headers the bridge sets on every message it puts on the fabric. They are
// the bridge's names; repeated here rather than exported from it, because an
// edge-side hook is not something a core-side package should import.
const (
	headerQoS   = "Mast-Qos"
	headerID    = "Mast-Id"
	headerProps = "Mast-Props"
)

// fabricPrefix is what the topic codec puts in front of every subject; the
// durable stream stores the same subject under [store.DurablePrefix].
const fabricPrefix = "t."

// enqueueTimeout bounds the writes for one message. A fan-out to many
// absent sessions is many writes, so it is longer than a single store call.
const enqueueTimeout = 30 * time.Second

// loadTimeout bounds reading the sessions bucket at startup.
const loadTimeout = 2 * time.Minute

// errLoad reports a sessions watch that ended before it was caught up.
var errLoad = errors.New("offline: sessions watch closed before it caught up")

// Queuer writes QoS 1 and 2 messages to the queues of the sessions that are
// away.
type Queuer struct {
	store   *store.Store
	log     *slog.Logger
	metrics *obs.Metrics

	// expiry is the sessions bucket's TTL. A record older than that has
	// been aged out of the bucket, and the watch never says so, so the
	// queuer has to notice by itself.
	expiry time.Duration

	mu sync.RWMutex
	// index matches a topic to the away sessions subscribed to it, keyed
	// by session key, merging overlapping filters of one session so a
	// message is queued once per session. It matches as mochi does but
	// costs an eighth of mochi's memory; see index.go.
	index  *index
	absent map[string]absentSession
	// reached is the stream time of the last message handled: how far the
	// queuer has caught up, which decides when a session that came back
	// can leave the index.
	reached time.Time

	watcher jetstream.KeyWatcher
	stop    func()
	done    chan struct{}
	once    sync.Once

	// inflight is the writes started for messages handled and not yet
	// acknowledged on the durable stream; flushMu keeps two flushes from
	// waiting on the same writes.
	inflightMu sync.Mutex
	inflight   []pendingWrites
	flushMu    sync.Mutex
}

// pendingWrites is one message's queue writes, in flight.
type pendingWrites struct {
	tenant string
	items  []store.Queued
	writes *store.Pending
}

// absentSession is what the index holds for one session that is away.
type absentSession struct {
	filters []string // mounted under the tenant, as the index has them
	since   time.Time

	// back is when the session's record said it was held again; zero while
	// it is away. See [Queuer.apply].
	back time.Time
}

// clockSlack widens the comparison of a message's stream time with a
// session's return time. The two are stamped by different cores, and a
// message kept that the device also got live is a duplicate, which
// at-least-once allows; one skipped is lost.
const clockSlack = 250 * time.Millisecond

// returnedTTL bounds how long a returned session stays in the index when
// the queuer hears of no message to show it has caught up: with no traffic
// there is nothing left to keep for it.
const returnedTTL = 5 * time.Minute

// pruneEvery is how often returned sessions the queuer has caught up with
// are let go.
const pruneEvery = 10 * time.Second

// Start loads every session record, then begins reading the durable stream.
// ctx must last as long as the queuer: the watch ends with it.
func Start(
	ctx context.Context,
	st *store.Store,
	expiry time.Duration,
	metrics *obs.Metrics,
	log *slog.Logger,
) (*Queuer, error) {
	q := newQueuer(st, expiry, metrics, log)

	watcher, err := st.WatchSessions(ctx)
	if err != nil {
		return nil, err
	}

	q.watcher = watcher

	// Caught up before the first message is read. A queuer that started
	// reading first would see every message for a session it had not yet
	// heard of, and drop it.
	if err := q.load(ctx); err != nil {
		_ = watcher.Stop()

		return nil, err
	}

	go q.follow()

	stop, err := st.ConsumeDurableFlushed(ctx, q.onDurable, q.flush, func(err error) {
		q.log.Warn("reading the durable stream", "error", err)
	})
	if err != nil {
		_ = watcher.Stop()

		return nil, fmt.Errorf("offline: %w", err)
	}

	q.stop = stop

	go q.sweep()

	q.mu.RLock()
	away := len(q.absent)
	q.mu.RUnlock()

	q.log.Info("queueing for absent sessions", "away", away)

	return q, nil
}

func newQueuer(st *store.Store, expiry time.Duration, metrics *obs.Metrics, log *slog.Logger) *Queuer {
	return &Queuer{
		store:   st,
		log:     log.With("component", "queuer"),
		metrics: metrics,
		expiry:  expiry,
		mu:      sync.RWMutex{},
		index:   newIndex(),
		absent:  make(map[string]absentSession),
		reached: time.Time{},
		watcher: nil,
		stop:    nil,
		done:    make(chan struct{}),
		once:    sync.Once{},

		inflightMu: sync.Mutex{},
		inflight:   nil,
		flushMu:    sync.Mutex{},
	}
}

// Stop ends the queuer.
func (q *Queuer) Stop() {
	q.once.Do(func() {
		close(q.done)

		if q.stop != nil {
			q.stop()
		}

		_ = q.watcher.Stop()
	})
}

// Away reports how many sessions the queuer is keeping messages for.
func (q *Queuer) Away() int {
	q.mu.RLock()
	defer q.mu.RUnlock()

	return len(q.absent)
}

// load applies every record already in the bucket. The watch marks the end
// of them with a nil entry.
func (q *Queuer) load(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, loadTimeout)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("offline: loading sessions: %w", ctx.Err())
		case entry, ok := <-q.watcher.Updates():
			if !ok {
				return errLoad
			}

			if entry == nil {
				return nil
			}

			q.apply(entry)
		}
	}
}

// follow applies every change after the initial load.
func (q *Queuer) follow() {
	for {
		select {
		case <-q.done:
			return
		case entry, ok := <-q.watcher.Updates():
			if !ok {
				return
			}

			if entry != nil {
				q.apply(entry)
			}
		}
	}
}

// apply brings the index up to date with one record.
//
// A session that comes back is not dropped from the index at once. The
// queuer reads the durable stream behind the sessions watch — by seconds in
// a burst — so when it hears a device is back, messages published while it
// was still away may be unread. Judged by the record, those were skipped:
// the device's new node had not subscribed when it handled them, and its old
// node may be the one that died. The session is kept, marked with when it
// came back, and a message is kept for it only if the stream stored it
// before then. It leaves once the queuer has read past that moment.
func (q *Queuer) apply(entry jetstream.KeyValueEntry) {
	key := entry.Key()

	q.mu.Lock()
	defer q.mu.Unlock()
	defer q.gauge()

	if entry.Operation() != jetstream.KeyValuePut {
		q.removeLocked(key)

		return
	}

	sess, err := store.DecodeSession(entry.Value())
	if err != nil {
		q.log.Warn("ignoring an unreadable session record", "key", key, "error", err)
		q.removeLocked(key)

		return
	}

	if sess.Present {
		if away, ok := q.absent[key]; ok && away.back.IsZero() {
			away.back = entry.Created()
			q.absent[key] = away
		}

		return
	}

	q.removeLocked(key)

	away := absentSession{filters: make([]string, 0, len(sess.Subscriptions)), since: sess.UpdatedAt, back: time.Time{}}
	if away.since.IsZero() {
		away.since = entry.Created()
	}

	for _, sub := range sess.Subscriptions {
		// A shared group is served by whichever member NATS picks, and a
		// member that is away is not one it can pick. Queueing for it as
		// well would put the message in front of two members of the group.
		if isShared(sub.Filter) {
			continue
		}

		mounted := sess.Tenant + "/" + sub.Filter
		q.index.subscribe(mounted, key, sub.QoS)
		away.filters = append(away.filters, mounted)
	}

	if len(away.filters) > 0 {
		q.absent[key] = away
	}
}

// gauge publishes how many sessions are away. Called with q.mu held.
func (q *Queuer) gauge() {
	if q.metrics != nil {
		q.metrics.QueuerAway.Set(float64(len(q.absent)))
	}
}

func (q *Queuer) removeLocked(key string) {
	away, ok := q.absent[key]
	if !ok {
		return
	}

	for _, filter := range away.filters {
		q.index.unsubscribe(filter, key)
	}

	delete(q.absent, key)
}

// sweep drops sessions whose records have aged out of the bucket. The
// bucket expires them silently, so without this the queuer would go on
// writing for a session nobody will ever resume, and hold it in memory for
// as long as it ran.
func (q *Queuer) sweep() {
	// No expiry, no expiring: a nil channel is never ready.
	var expired <-chan time.Time

	if q.expiry > 0 {
		expire := time.NewTicker(q.expiry / 4) //nolint:mnd // often enough that a stale entry lives at most a quarter longer
		defer expire.Stop()

		expired = expire.C
	}

	prune := time.NewTicker(pruneEvery)
	defer prune.Stop()

	for {
		select {
		case <-q.done:
			return
		case now := <-expired:
			q.mu.Lock()
			for key, away := range q.absent {
				if q.expired(away, now) {
					q.removeLocked(key)
				}
			}

			q.gauge()
			q.mu.Unlock()
		case now := <-prune.C:
			q.pruneReturned(now)
		}
	}
}

// pruneReturned lets go of the sessions that came back once the queuer has
// read past the moment they did.
func (q *Queuer) pruneReturned(now time.Time) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for key, away := range q.absent {
		if away.back.IsZero() {
			continue
		}

		if q.reached.After(away.back.Add(clockSlack)) || now.Sub(away.back) > returnedTTL {
			q.removeLocked(key)
		}
	}

	q.gauge()
}

func (q *Queuer) expired(away absentSession, now time.Time) bool {
	return q.expiry > 0 && now.Sub(away.since) > q.expiry
}

// onDurable queues one message from the durable stream for every away
// session it matches.
func (q *Queuer) onDurable(subject string, header nats.Header, data []byte) {
	if q.metrics != nil {
		q.metrics.DurableRead.WithLabelValues("queuer").Inc()
	}

	msg, ok := q.decode(subject, header, data)
	if !ok {
		return
	}

	items := q.match(msg, header.Get(headerID), storedAt(header))
	if len(items) == 0 {
		return
	}

	// Started, not waited for: the durable stream is acknowledged only
	// after flush has waited for these, so returning now loses nothing.
	writes := q.store.StartEnqueue(items)

	q.inflightMu.Lock()
	q.inflight = append(q.inflight, pendingWrites{tenant: msg.tenant, items: items, writes: writes})
	q.inflightMu.Unlock()
}

// flush waits for every queue write started so far. It runs before each
// acknowledgement of the durable stream.
//
// A write that is still failing is made again, under the same id, until it
// lands or the queuer stops. It used to be given up after thirty seconds and
// the stream acknowledged anyway, so a JetStream outage longer than that
// lost every message published to a sleeping device during it. Holding the
// acknowledgement is what makes the stream keep them; the consumer stops
// being sent more once it has twenty thousand unacknowledged, which is the
// back-pressure an outage should cause.
func (q *Queuer) flush() error {
	q.flushMu.Lock()
	defer q.flushMu.Unlock()

	q.inflightMu.Lock()
	batch := q.inflight
	q.inflight = nil
	q.inflightMu.Unlock()

	for _, p := range batch {
		stored, err := q.settle(p)

		if stored > 0 && q.metrics != nil {
			q.metrics.OfflineQueued.WithLabelValues(p.tenant).Add(float64(stored))
		}

		if err != nil {
			return err
		}
	}

	return nil
}

// errStopping reports a flush cut short because the queuer is stopping.
var errStopping = errors.New("offline: stopping with queue writes unfinished")

// settle waits for one message's writes, making them again until they land.
func (q *Queuer) settle(p pendingWrites) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), enqueueTimeout)
	stored, err := p.writes.Wait(ctx)

	cancel()

	pause := time.Second

	for err != nil {
		if !store.Transient(err) {
			q.log.Error("a message could not be queued for absent sessions and is dropped", "tenant", p.tenant, "error", err)

			return stored, nil
		}

		q.log.Warn("queueing for absent sessions, retrying", "tenant", p.tenant, "error", err)

		select {
		case <-q.done:
			return stored, errStopping
		case <-time.After(pause):
		}

		pause = min(pause+pause, enqueueTimeout)

		ctx, cancel := context.WithTimeout(context.Background(), enqueueTimeout)
		more, againErr := q.store.EnqueueMany(ctx, p.items)

		cancel()

		stored, err = stored+more, againErr
	}

	return stored, nil
}

// fabricMessage is a message from the durable stream, as the queuer needs it.
type fabricMessage struct {
	tenant string
	stored store.Message
}

// decode reads a message off the durable stream. ok is false for anything
// that is not a QoS 1 or 2 message the bridge put there.
func (q *Queuer) decode(subject string, header nats.Header, data []byte) (fabricMessage, bool) {
	qos, err := strconv.Atoi(header.Get(headerQoS))
	if err != nil || qos < 1 || qos > 2 {
		return fabricMessage{}, false //nolint:exhaustruct_v5 // nothing to return
	}

	tenantID, mqttTopic, err := topic.DecodeTopic(fabricPrefix + strings.TrimPrefix(subject, store.DurablePrefix))
	if err != nil {
		q.log.Warn("undecodable subject on the durable stream", "subject", subject, "error", err)

		return fabricMessage{}, false //nolint:exhaustruct_v5 // nothing to return
	}

	var props *store.Properties

	if raw := header.Get(headerProps); raw != "" {
		var p store.Properties
		if json.Unmarshal([]byte(raw), &p) == nil {
			props = &p
		}
	}

	return fabricMessage{
		tenant: tenantID,
		stored: store.Message{
			Topic:      mqttTopic,
			Payload:    data,
			QoS:        byte(qos),
			Retain:     false,
			Properties: props,
			StoredAt:   0, // the stream's own timestamp
		},
	}, true
}

// match lists the writes one message needs: one per away session, at the
// lower of the publish's QoS and the subscription's. stored is when the
// stream stored the message, which decides for a session that has come
// back whether it was still away at the time.
func (q *Queuer) match(msg fabricMessage, id string, stored time.Time) []store.Queued {
	now := time.Now()

	q.mu.Lock()
	defer q.mu.Unlock()

	q.reached = later(q.reached, stored)

	var items []store.Queued

	q.index.match(msg.tenant+"/"+msg.stored.Topic, func(key string, qos byte) {
		away, ok := q.absent[key]
		if !ok || q.expired(away, now) {
			return
		}

		if !away.back.IsZero() && !stored.Before(away.back.Add(clockSlack)) {
			return
		}

		// Queueing follows the subscription's QoS, not the publisher's.
		kept := msg.stored
		kept.QoS = min(kept.QoS, qos)

		if kept.QoS == 0 {
			return
		}

		items = append(items, store.Queued{Key: key, Msg: kept, ID: store.QueueID(id, key)})
	})

	return items
}

// storedAt reads the stream time the consumer put on a message. One without
// it reads as long ago, so it is kept for a session that has come back: a
// possible duplicate rather than a possible loss.
func storedAt(header nats.Header) time.Time {
	at, err := time.Parse(time.RFC3339Nano, header.Get(nats.JSTimeStamp))
	if err != nil {
		return time.Time{}
	}

	return at
}

func later(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}

	return a
}

// sharePrefix introduces a shared subscription, matched case-insensitively
// as mochi does.
const sharePrefix = "$share/"

func isShared(filter string) bool {
	return len(filter) >= len(sharePrefix) && strings.EqualFold(filter[:len(sharePrefix)], sharePrefix)
}
