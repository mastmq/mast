package bridge

import (
	"context"
	"errors"
	"time"

	"github.com/mastmq/mast/internal/domain/tenant"
	"github.com/mastmq/mast/internal/infra/obs"
	"github.com/mastmq/mast/internal/infra/store"
	mqtt "github.com/mastmq/mochi/v2"
	"github.com/mastmq/mochi/v2/packets"
)

// sessionKey is where a client's durable state lives in the key-value
// buckets. The format belongs to the store, which lists and counts by it;
// see [store.SessionKey].
func sessionKey(id tenant.ID, clientID string) string {
	return store.SessionKey(string(id), clientID)
}

// persistSession records what a connected client is subscribed to, so
// another node can pick the session up, and that a connection holds it, so
// nothing queues for it.
//
// Filters are stored unmounted. The tenant is already a field, repeating it
// inside every filter would be redundant, and a human reading the bucket
// sees what the device actually asked for.
func (h *Hook) persistSession(cl *mqtt.Client, id tenant.ID, bareClientID string) {
	if h.store == nil || !outlivesConnection(cl) {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), storeTimeout)
	defer cancel()

	record := h.sessionRecord(cl, id, bareClientID, true)
	if err := h.store.PutSession(ctx, sessionKey(id, bareClientID), record); err != nil {
		h.log.Warn("persisting session", "client", cl.ID, "error", err)

		return
	}

	h.markPersisted(cl.ID, time.Now())
}

// markAway records that a lasting session's connection has gone, which is
// what tells the queuer to start keeping its messages.
//
// Nothing is written if this connection no longer holds the session: mochi
// may report the old connection's disconnect after a new one on this node
// has taken over, and the store refuses the write if a connection elsewhere
// has.
func (h *Hook) markAway(cl *mqtt.Client, id tenant.ID, bareClientID string) {
	if h.store == nil || !outlivesConnection(cl) {
		return
	}

	if current, ok := h.server.Clients.Get(cl.ID); ok && current != cl {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), storeTimeout)
	defer cancel()

	written, err := h.store.MarkAway(ctx, sessionKey(id, bareClientID), h.sessionRecord(cl, id, bareClientID, false))
	if err != nil {
		h.log.Warn("recording session as away", "client", cl.ID, "error", err)

		return
	}

	if !written {
		h.log.Debug("session already held by another connection", "client", cl.ID)
	}
}

// sessionRecord is what the sessions bucket holds for a client.
func (h *Hook) sessionRecord(cl *mqtt.Client, id tenant.ID, bareClientID string, present bool) store.Session {
	subs := cl.State.Subscriptions.GetAll()

	filters := make([]store.Subscription, 0, len(subs))
	for mounted, sub := range subs {
		filters = append(filters, store.Subscription{
			Filter: storedFilter(id, mounted),
			QoS:    sub.Qos,
		})
	}

	owner := h.ownerOf(cl)

	return store.Session{
		ClientID:      bareClientID,
		Tenant:        string(id),
		Subscriptions: filters,
		UpdatedAt:     time.Time{}, // the store stamps it
		Present:       present,
		Owner:         owner,
	}
}

// storedFilter is a mounted filter as the sessions bucket keeps it: without
// the tenant, but with any share prefix.
//
// It used to be stored through bare, which drops the share prefix along
// with the tenant, so a persistent shared subscriber restored on another
// node came back as a plain subscriber and received every message the group
// was meant to divide.
func storedFilter(id tenant.ID, mounted string) string {
	group, rest, shared := splitShare(mounted)
	if !shared {
		return unmount(id, mounted)
	}

	return sharePrefix + group + "/" + unmount(id, rest)
}

// markPersisted records when a client's session was last written.
func (h *Hook) markPersisted(mountedClientID string, at time.Time) {
	h.mu.Lock()
	h.persistedAt[mountedClientID] = at
	h.mu.Unlock()
}

// refreshSessions keeps connected sessions from ageing out of the store.
//
// The sessions bucket expires a key session.expiry after its last write,
// and a connected client with steady subscriptions writes nothing. Without
// this, a device connected for longer than the expiry lost its stored
// session while still using it, and came back from its next move between
// nodes with no subscriptions. A session is rewritten once it is half an
// expiry old, so it always has at least half an expiry left; at 300k
// devices and a 24h expiry that is about seven writes a second across the
// cluster, spread as the connect times are.
//
// Only connected clients are refreshed. A disconnected session was written
// at DISCONNECT and is meant to expire from there.
func (h *Hook) refreshSessions() {
	ticker := time.NewTicker(h.sessionExpiry / 4) //nolint:mnd // a quarter: see below
	defer ticker.Stop()

	for {
		select {
		case <-h.stop:
			return
		case now := <-ticker.C:
			h.refreshDue(now)
		}
	}
}

// refreshDue rewrites every connected persistent session last written more
// than half an expiry ago. The tick runs every quarter expiry, so a session
// is refreshed between half and three quarters of the way to expiring.
func (h *Hook) refreshDue(now time.Time) {
	due := now.Add(-h.sessionExpiry / 2) //nolint:mnd // half: see refreshSessions

	for _, cl := range h.server.Clients.GetAll() {
		if cl.Net.Inline || cl.Net.Conn == nil || cl.Closed() || !outlivesConnection(cl) {
			continue
		}

		if len(cl.State.Subscriptions.GetAll()) == 0 {
			continue // nothing stored, and nothing worth storing
		}

		identity, connected := h.identityOf(cl)
		if !connected {
			continue
		}

		h.mu.Lock()
		last, known := h.persistedAt[cl.ID]
		h.mu.Unlock()

		if known && last.After(due) {
			continue
		}

		h.persistSession(cl, identity.Tenant, unmountClient(identity.Tenant, cl.ID))
	}
}

// outlivesConnection reports whether a client's session is kept after it
// disconnects, and so whether there is any point storing it.
//
// It is mochi's own rule, inverted: a v5 session lasts while its expiry
// interval is non-zero, and a v3 session unless it is clean. A session that
// ends with its connection can never be restored anywhere, so persisting it
// was a replicated write on every SUBSCRIBE for nothing. A v5 client can
// shorten its expiry to zero on DISCONNECT; what was stored before that is
// removed by the drop OnDisconnect already makes.
func outlivesConnection(cl *mqtt.Client) bool {
	if cl.Properties.ProtocolVersion == 5 { //nolint:mnd // the MQTT protocol level, not a tunable
		return cl.Properties.Props.SessionExpiryInterval > 0
	}

	return !cl.Properties.Clean
}

// queuesForAbsence reports whether a QoS 1 or 2 message handed to cl has to
// be written to the offline queue.
//
// The absence test is the predicate mochi uses a few lines later to decide
// it cannot write to this client. Closed() alone is not it: a client that
// sent DISCONNECT with a session to keep is not marked closed, and that is
// exactly the client whose messages need keeping.
//
// The session test is what the absence test misses. A clean-session
// subscriber that disconnects while traffic is still arriving is absent for
// as long as mochi takes to remove it, and every message in that window was
// a replicated write into a queue nothing would ever drain — 38 of them in
// one QoS 2 fan-out run, from clients that were all clean.
func queuesForAbsence(cl *mqtt.Client) bool {
	return (cl.Net.Conn == nil || cl.Closed()) && outlivesConnection(cl)
}

// dropSession forgets everything durable about a client, which is what a
// clean start and an expired session both mean.
func (h *Hook) dropSession(id tenant.ID, bareClientID string) {
	if h.store == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), storeTimeout)
	defer cancel()

	if err := h.store.DeleteSession(ctx, sessionKey(id, bareClientID)); err != nil {
		h.log.Warn("dropping session", "tenant", string(id), "client", bareClientID, "error", err)
	}
}

// restoreSession puts a session back on this node.
//
// This is what makes a session portable. mochi only knows about sessions it
// has seen, so a client reconnecting to a different node arrives as a
// stranger: its subscriptions are gone and nothing routes to it. The
// subscription list is read back from the bucket and re-registered both
// with mochi, so local delivery matches, and with NATS, so messages reach
// this node at all.
//
// Whatever queued while the client was away is drained afterwards, because
// a subscription that exists but has already missed its backlog is only
// half a session.
func (h *Hook) restoreSession(cl *mqtt.Client, id tenant.ID, bareClientID string) {
	if h.store == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), storeTimeout)
	defer cancel()

	sess, err := h.store.GetSession(ctx, sessionKey(id, bareClientID))
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			h.log.Warn("reading session", "client", cl.ID, "error", err)
		}

		return
	}

	// Dated from the stored copy, not from now: a reconnect storm restores
	// every session at once, and treating each as unwritten would make the
	// refresher rewrite all of them on its next tick.
	h.markPersisted(cl.ID, sess.UpdatedAt)

	keys := make([]subKey, 0, len(sess.Subscriptions))

	for _, s := range sess.Subscriptions {
		mounted := mountFilter(id, s.Filter)
		sub := packets.Subscription{Filter: mounted, Qos: s.QoS}

		// Exactly what mochi does when it reloads subscriptions from a
		// storage hook: the topic index and the client's own list have to
		// agree, or a message matches nothing on the way out.
		if h.server.Topics.Subscribe(cl.ID, sub) {
			cl.State.Subscriptions.Add(mounted, sub)
		}

		keys = append(keys, h.keysFor(id, sub)...)
	}

	if len(keys) > 0 {
		if err := h.subs.acquire(cl.ID, keys); err != nil {
			h.log.Error("re-acquiring nats subscriptions", "client", cl.ID, "error", err)
		}
	}

	h.log.Info("session restored",
		"client", bareClientID, "tenant", string(id), "filters", len(sess.Subscriptions))

	// Recorded as held before the backlog is read, so that the queuer stops
	// keeping messages for a client that now receives them live. A message
	// it queues in the moment before it hears is delivered twice, which
	// at-least-once allows; one it skipped would be lost, which it does not.
	h.persistSession(cl, id, bareClientID)

	through, drained := h.drainOffline(cl, id, bareClientID, 0)

	go h.drainLater(cl, id, bareClientID, through, drained)

	// A device may resume a session and ask for this connection to be its
	// last: MQTT 5 with a zero expiry. It has its backlog; the record goes,
	// or the queuer would keep messages for a session that no longer exists.
	if !outlivesConnection(cl) {
		h.dropSession(id, bareClientID)
	}
}

// drainOffline delivers what arrived while the client was away.
//
// Nothing is removed until the device has acknowledged it. The queue used
// to be trimmed as soon as the backlog was written to the connection, which
// is not the device having it: a connection that dropped before its PUBACKs
// took the backlog with it, and the device reconnecting to another node
// found nothing, because mochi's inflight copy stayed on this one. Delivery
// also stops at the first message that cannot go — inflight full, the
// connection gone — so a trim never passes one that was never sent.
//
// It reads only past after, the sequence an earlier drain reached, and
// returns the sequence this one reached and whether it could read at all.
func (h *Hook) drainOffline(cl *mqtt.Client, id tenant.ID, bareClientID string, after uint64) (uint64, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), storeTimeout)
	defer cancel()

	h.log.Debug("draining offline queue", "client", cl.ID)

	key := sessionKey(id, bareClientID)

	backlog, err := h.store.ReadQueue(ctx, key, after)
	if err != nil {
		h.log.Warn("draining offline queue", "client", cl.ID, "error", err)

		return after, false
	}

	h.limitBacklog(id, &backlog)

	through, legacy := backlog.Through, backlog.Legacy
	sent := make([]sentBacklog, 0, len(backlog.Messages))

	// To this client alone. Publishing the backlog into the node instead
	// handed it to every local subscriber of the same topics, who had
	// already had it live, and re-queued it for every absent one.
	for i, msg := range backlog.Messages {
		packet, err := h.deliverTo(cl, mount(id, msg.Topic), msg, msg.QoS, false)
		if err != nil {
			h.log.Warn("delivering a queued message; the rest waits for the next look",
				"client", cl.ID, "topic", msg.Topic, "error", err)

			through, legacy = stoppedAt(backlog.Seqs[i], after, legacy)

			break
		}

		sent = append(sent, sentBacklog{seq: backlog.Seqs[i], packet: packet})
	}

	if through > after || legacy > 0 {
		go h.trimWhenAcked(cl, key, sent, after, through, legacy)
	}

	if len(sent) > 0 {
		h.count(func(m *obs.Metrics) {
			m.OfflineDelivered.WithLabelValues(m.Tenant(string(id))).Add(float64(len(sent)))
		})
		h.log.Info("offline queue delivered",
			"client", bareClientID, "tenant", string(id), "messages", len(sent))
	}

	return through, true
}

// sentBacklog is one queued message written to the client: its stream
// sequence (zero from the legacy bucket) and the packet id it went under
// (zero for one that needs no acknowledgement).
type sentBacklog struct {
	seq    uint64
	packet uint16
}

// stoppedAt is how far a drain may trim when it stopped at the message
// with stream sequence seq: up to the one before it. A message from the
// legacy bucket comes before every stream message, so stopping on one
// trims nothing at all.
func stoppedAt(seq, after, legacy uint64) (uint64, uint64) {
	if seq == 0 {
		return after, 0
	}

	return seq - 1, legacy
}

// How long a drain waits for the device to acknowledge its backlog before
// trimming only what it has, and how often it looks.
const (
	backlogAckWait = 30 * time.Second
	backlogAckPoll = 100 * time.Millisecond
)

// trimWhenAcked removes the backlog from the queue once the device has
// acknowledged it — through the last message before the first it has not,
// when the wait runs out or the connection goes. What stays is delivered
// again on a later look or the next connection: a duplicate, which
// at-least-once allows, rather than a loss.
//
// A packet id that has left the inflight store has been acknowledged. One
// still there may be a new message that reused the id, which only means
// trimming less.
func (h *Hook) trimWhenAcked(cl *mqtt.Client, key string, sent []sentBacklog, after, through, legacy uint64) {
	ticker := time.NewTicker(backlogAckPoll)
	defer ticker.Stop()

	deadline := time.Now().Add(backlogAckWait)

	for firstUnacked(cl, sent) >= 0 && !cl.Closed() && time.Now().Before(deadline) {
		<-ticker.C
	}

	if i := firstUnacked(cl, sent); i >= 0 {
		through, legacy = stoppedAt(sent[i].seq, after, legacy)
		through = max(through, after)
	}

	if through <= after {
		through = 0
	}

	if through == 0 && legacy == 0 {
		return
	}

	// A deadline of its own: in a wake storm every device's trim is a
	// replicated write at once. A trim that still fails costs storage until
	// the stream ages it out, not a repeat, since later reads start past it.
	ctx, cancel := context.WithTimeout(context.Background(), storeTimeout)
	defer cancel()

	if err := h.store.TrimQueue(ctx, key, through, legacy); err != nil {
		h.log.Debug("trimming offline queue", "client", cl.ID, "error", err)
	}
}

// firstUnacked returns the index of the first message the client has not
// acknowledged, or -1.
func firstUnacked(cl *mqtt.Client, sent []sentBacklog) int {
	for i, s := range sent {
		if s.packet == 0 {
			continue
		}

		if _, ok := cl.State.Inflight.Get(s.packet); ok {
			return i
		}
	}

	return -1
}

// drainLater reads the queue again after the first drain: until it works,
// if the first could not read it, and then twice more for stragglers.
//
// Retrying a failed read is the first half. Giving up after one failure left
// the backlog in the stream with nothing to fetch it: the session is marked
// held, so nobody asks again until the device's next reconnect, which for a
// device that stays connected may be days.
//
// The straggler checks are the second. A queuer that was still writing for
// this session — retrying through a core's leader election, say — can land
// a message after the drain read the queue. The device did not get it live,
// because it was published while the device was away, and without another
// look it waits for the next reconnect. One message in a hundred thousand
// did exactly that with a core killed under load. A check is a read any
// replica serves and almost always finds nothing.
func (h *Hook) drainLater(cl *mqtt.Client, id tenant.ID, bareClientID string, through uint64, drained bool) {
	pause := time.Second

	for range drainRetries {
		if drained {
			break
		}

		if !h.waitConnected(cl, pause) {
			return
		}

		through, drained = h.drainOffline(cl, id, bareClientID, through)
		pause *= 2
	}

	if !drained {
		h.log.Error("gave up delivering an offline backlog; it waits for the next reconnect", "client", cl.ID)

		return
	}

	for _, after := range stragglerChecks {
		if !h.waitConnected(cl, after) {
			return
		}

		through, _ = h.drainOffline(cl, id, bareClientID, through)
	}
}

// waitConnected waits d and reports whether cl is still this node's
// connection for its client id. One that has gone drains for itself next
// time.
func (h *Hook) waitConnected(cl *mqtt.Client, d time.Duration) bool {
	select {
	case <-h.stop:
		return false
	case <-time.After(d):
	}

	current, ok := h.server.Clients.Get(cl.ID)

	return ok && current == cl && !cl.Closed()
}

// drainRetries bounds the retrying of a failed drain: with the pause
// doubling from a second, about a minute.
const drainRetries = 6

// stragglerChecks are the pauses before each extra look at the queue after
// a drain. The last is past the queuer's own write deadline.
var stragglerChecks = []time.Duration{5 * time.Second, 30 * time.Second} //nolint:gochecknoglobals // a constant list

// discardOffline empties a client's queue without delivering it.
func (h *Hook) discardOffline(id tenant.ID, bareClientID string) {
	if h.store == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), storeTimeout)
	defer cancel()

	if err := h.store.DiscardQueue(ctx, sessionKey(id, bareClientID)); err != nil {
		h.log.Warn("discarding offline queue", "tenant", string(id), "client", bareClientID, "error", err)
	}
}

// OnQosPublish is called when a QoS 1 or 2 message is handed to a
// subscriber. When that subscriber is not currently connected the message
// is going into mochi's in-memory inflight and nowhere else, so it is
// written to the queue as well and replayed on reconnect — possibly by a
// different node, which is the whole point.
//
// The queuer on the core writes the same messages, and is what keeps a
// session's queue once this node is gone. This write covers the moment
// before the queuer has heard the session is away, and a broker running
// without the durable stream, where there is no queuer at all. Both name
// the message the same way, so the stream keeps one copy.
//
// QoS 0 is deliberately absent. MQTT lets a broker discard it for an absent
// client, and persisting it would turn a fire-and-forget message into
// storage that outlives the thing it described.
func (h *Hook) OnQosPublish(cl *mqtt.Client, pk packets.Packet, _ int64, _ int) {
	if !queuesForAbsence(cl) {
		return
	}

	id, ok := h.tenantOf(cl)
	if !ok {
		return
	}

	h.limitInflight(cl, id)

	if h.store == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), storeTimeout)
	defer cancel()

	// mochi blanks the topic of a packet it has an outbound alias for,
	// which it assigns even to a client that has gone away. The alias means
	// nothing to the connection the message will be replayed on, and a
	// PUBLISH with neither is a protocol error, so there is nothing sound
	// to store.
	if pk.TopicName == "" {
		h.log.Warn("not queueing a message sent by topic alias", "client", cl.ID)

		return
	}

	msg := store.Message{
		Topic:      unmount(id, pk.TopicName),
		Payload:    pk.Payload,
		QoS:        pk.FixedHeader.Qos,
		Retain:     false,
		Properties: messageProps(pk.Properties),
		StoredAt:   0, // Enqueue stamps it
	}

	key := sessionKey(id, unmountClient(id, cl.ID))

	var messageID string
	if r, ok := parseRoute(pk.Properties.ServerReference); ok {
		messageID = r.id
	}

	if _, err := h.store.Enqueue(ctx, store.Queued{Key: key, Msg: msg, ID: store.QueueID(messageID, key)}); err != nil {
		h.log.Warn("queueing for an absent client", "client", cl.ID, "error", err)

		return
	}

	h.count(func(m *obs.Metrics) { m.OfflineQueued.WithLabelValues(m.Tenant(string(id))).Inc() })
	h.log.Debug("queued for an absent client", "client", cl.ID, "topic", msg.Topic)
}
