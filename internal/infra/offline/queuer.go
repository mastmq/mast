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
	mqtt "github.com/mastmq/mochi/v2"
	"github.com/mastmq/mochi/v2/packets"
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
	// by session key. mochi's own index is used because it already gets
	// MQTT matching right — "#" matching its parent, "$" topics and
	// top-level wildcards — and overlapping filters of one session come
	// back merged, so a message is queued once per session.
	index  *mqtt.TopicsIndex
	absent map[string]absentSession

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
	writes *store.Pending
}

// absentSession is what the index holds for one session that is away.
type absentSession struct {
	filters []string // mounted under the tenant, as the index has them
	since   time.Time
}

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

	if expiry > 0 {
		go q.sweep()
	}

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
		index:   mqtt.NewTopicsIndex(),
		absent:  make(map[string]absentSession),
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
func (q *Queuer) apply(entry jetstream.KeyValueEntry) {
	key := entry.Key()

	q.mu.Lock()
	defer q.mu.Unlock()
	defer q.gauge()

	q.removeLocked(key)

	if entry.Operation() != jetstream.KeyValuePut {
		return
	}

	sess, err := store.DecodeSession(entry.Value())
	if err != nil {
		q.log.Warn("ignoring an unreadable session record", "key", key, "error", err)

		return
	}

	if sess.Present {
		return
	}

	away := absentSession{filters: make([]string, 0, len(sess.Subscriptions)), since: sess.UpdatedAt}
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
		q.index.Subscribe(key, packets.Subscription{Filter: mounted, Qos: sub.QoS})
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
		q.index.Unsubscribe(filter, key)
	}

	delete(q.absent, key)
}

// sweep drops sessions whose records have aged out of the bucket. The
// bucket expires them silently, so without this the queuer would go on
// writing for a session nobody will ever resume, and hold it in memory for
// as long as it ran.
func (q *Queuer) sweep() {
	ticker := time.NewTicker(q.expiry / 4) //nolint:mnd // often enough that a stale entry lives at most a quarter longer
	defer ticker.Stop()

	for {
		select {
		case <-q.done:
			return
		case now := <-ticker.C:
			q.mu.Lock()
			for key, away := range q.absent {
				if q.expired(away, now) {
					q.removeLocked(key)
				}
			}
			q.mu.Unlock()
		}
	}
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

	items := q.match(msg, header.Get(headerID))
	if len(items) == 0 {
		return
	}

	// Started, not waited for: the durable stream is acknowledged only
	// after flush has waited for these, so returning now loses nothing.
	writes := q.store.StartEnqueue(items)

	q.inflightMu.Lock()
	q.inflight = append(q.inflight, pendingWrites{tenant: msg.tenant, writes: writes})
	q.inflightMu.Unlock()
}

// flush waits for every queue write started so far. It runs before each
// acknowledgement of the durable stream.
//
// A write that still fails after enqueueTimeout of retrying is logged and
// given up: the stream has been unwritable for that long, and holding the
// acknowledgement would only have the same messages sent again into the
// same failure.
func (q *Queuer) flush() {
	q.flushMu.Lock()
	defer q.flushMu.Unlock()

	q.inflightMu.Lock()
	batch := q.inflight
	q.inflight = nil
	q.inflightMu.Unlock()

	if len(batch) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), enqueueTimeout)
	defer cancel()

	for _, p := range batch {
		stored, err := p.writes.Wait(ctx)
		if err != nil {
			q.log.Warn("queueing for absent sessions", "tenant", p.tenant, "error", err)
		}

		if stored > 0 && q.metrics != nil {
			q.metrics.OfflineQueued.WithLabelValues(p.tenant).Add(float64(stored))
		}
	}
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
// lower of the publish's QoS and the subscription's.
func (q *Queuer) match(msg fabricMessage, id string) []store.Queued {
	now := time.Now()

	q.mu.RLock()
	defer q.mu.RUnlock()

	subs := q.index.Subscribers(msg.tenant + "/" + msg.stored.Topic)

	items := make([]store.Queued, 0, len(subs.Subscriptions))

	for key, sub := range subs.Subscriptions {
		away, ok := q.absent[key]
		if !ok || q.expired(away, now) {
			continue
		}

		// Queueing follows the subscription's QoS, not the publisher's.
		stored := msg.stored
		stored.QoS = min(stored.QoS, sub.Qos)

		if stored.QoS == 0 {
			continue
		}

		items = append(items, store.Queued{Key: key, Msg: stored, ID: store.QueueID(id, key)})
	}

	return items
}

// sharePrefix introduces a shared subscription, matched case-insensitively
// as mochi does.
const sharePrefix = "$share/"

func isShared(filter string) bool {
	return len(filter) >= len(sharePrefix) && strings.EqualFold(filter[:len(sharePrefix)], sharePrefix)
}
