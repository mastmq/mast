package bridge

import (
	"context"
	"errors"
	"time"

	"github.com/mastmq/mast/internal/domain/tenant"
	"github.com/mastmq/mast/internal/domain/topic"
	"github.com/mastmq/mast/internal/infra/store"
	mqtt "github.com/mastmq/mochi/v2"
	"github.com/mastmq/mochi/v2/packets"
)

// sessionKey is where a client's durable state lives in the key-value
// buckets.
//
// Both tokens are escaped for the same reason the control-plane subject
// escapes them, and because the codec's character set was chosen so that an
// encoded token is also a valid JetStream key. A client id arrives from the
// wire; unescaped it could collide with another client's key or fail to
// store at all.
func sessionKey(id tenant.ID, clientID string) string {
	return topic.EncodeToken(string(id)) + "." + topic.EncodeToken(clientID)
}

// persistSession records what a client is subscribed to, so another node
// can pick the session up.
//
// Filters are stored unmounted. The tenant is already a field, repeating it
// inside every filter would be redundant, and a human reading the bucket
// sees what the device actually asked for.
func (h *Hook) persistSession(cl *mqtt.Client, id tenant.ID, bareClientID string) {
	if h.store == nil {
		return
	}

	subs := cl.State.Subscriptions.GetAll()

	filters := make([]store.Subscription, 0, len(subs))
	for mounted, sub := range subs {
		filters = append(filters, store.Subscription{
			Filter: bare(id, mounted),
			QoS:    sub.Qos,
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), storeTimeout)
	defer cancel()

	sess := store.Session{
		ClientID:      bareClientID,
		Tenant:        string(id),
		Subscriptions: filters,
		UpdatedAt:     time.Time{}, // PutSession stamps it
	}

	if err := h.store.PutSession(ctx, sessionKey(id, bareClientID), sess); err != nil {
		h.log.Warn("persisting session", "client", cl.ID, "error", err)
	}
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

	h.drainOffline(cl, id, bareClientID)
}

// drainOffline delivers what arrived while the client was away.
func (h *Hook) drainOffline(cl *mqtt.Client, id tenant.ID, bareClientID string) {
	ctx, cancel := context.WithTimeout(context.Background(), storeTimeout)
	defer cancel()

	h.log.Debug("draining offline queue", "client", cl.ID)

	queued, err := h.store.Drain(ctx, sessionKey(id, bareClientID))
	if err != nil {
		h.log.Warn("draining offline queue", "client", cl.ID, "error", err)

		return
	}

	for _, msg := range queued {
		if err := h.server.Publish(mount(id, msg.Topic), msg.Payload, false, msg.QoS); err != nil {
			h.log.Error("delivering a queued message", "client", cl.ID, "error", err)
		}
	}

	if len(queued) > 0 {
		h.log.Info("offline queue delivered",
			"client", bareClientID, "tenant", string(id), "messages", len(queued))
	}
}

// OnQosPublish is called when a QoS 1 or 2 message is handed to a
// subscriber. When that subscriber is not currently connected the message
// is going into mochi's in-memory inflight and nowhere else, so it is
// written to the bucket as well and replayed on reconnect — possibly by a
// different node, which is the whole point.
//
// QoS 0 is deliberately absent. MQTT lets a broker discard it for an absent
// client, and persisting it would turn a fire-and-forget message into
// storage that outlives the thing it described.
func (h *Hook) OnQosPublish(cl *mqtt.Client, pk packets.Packet, _ int64, _ int) {
	// The same predicate mochi uses a few lines later to decide it cannot
	// write to this client. Closed() alone is not it: a client that sent
	// DISCONNECT with a session to keep is not marked closed, and that is
	// exactly the client whose messages need keeping.
	if h.store == nil || (cl.Net.Conn != nil && !cl.Closed()) {
		return
	}

	id, ok := h.tenantOf(cl)
	if !ok {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), storeTimeout)
	defer cancel()

	msg := store.Message{
		Topic:   unmount(id, pk.TopicName),
		Payload: pk.Payload,
		QoS:     pk.FixedHeader.Qos,
		Retain:  false,
	}

	key := sessionKey(id, unmountClient(id, cl.ID))
	if err := h.store.Enqueue(ctx, key, msg); err != nil {
		h.log.Warn("queueing for an absent client", "client", cl.ID, "error", err)

		return
	}

	h.log.Debug("queued for an absent client", "client", cl.ID, "topic", msg.Topic)
}
