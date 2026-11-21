package bridge

import (
	"cmp"
	"slices"
	"sync/atomic"

	"github.com/mastmq/mast/internal/domain/tenant"
	"github.com/mastmq/mast/internal/infra/obs"
	"github.com/mastmq/mast/internal/infra/store"
	mqtt "github.com/mastmq/mochi/v2"
	"github.com/mastmq/mochi/v2/packets"
)

// Limit names, as the limit label of mast_limit_exceeded_total carries them.
const (
	limitConnections  = "connections"
	limitPublishRate  = "publish_rate"
	limitOfflineQueue = "offline_queue"
)

// mqtt5 is the protocol level of MQTT 5, the first with reason codes on
// acknowledgements.
const mqtt5 = 5

// admit asks the limiter to count a newly authenticated connection, and
// refuses it with a CONNACK of its own when the tenant is full.
//
// mochi's OnConnectAuthenticate can only say yes or no, and no becomes "bad
// username or password", which tells a device to fix credentials that are
// fine. So the refusal is sent from here, with the right code, and the
// connection stopped before mochi writes its own: mochi's write then fails
// on the closed connection instead of reaching the client as a second
// CONNACK.
//
// MQTT 5 has Quota exceeded (0x97) for exactly this. MQTT 3.1.1 has no
// quota code; Server unavailable (0x03) is the one that tells a client to
// retry later rather than give up on its credentials.
func (h *Hook) admit(cl *mqtt.Client, identity tenant.Identity) bool {
	if h.limits == nil {
		return true
	}

	limits, err := h.limits.Admit(identity.Tenant, identity.Limits, h.holds(cl))
	if err == nil {
		return true
	}

	// Before refusing, release what mochi closed without telling us: a
	// leaked slot must not be the reason a device is turned away, and
	// waiting for the next reap would refuse it once for nothing.
	if h.reapClosed() > 0 {
		limits, err = h.limits.Admit(identity.Tenant, identity.Limits, h.holds(cl))
		if err == nil {
			return true
		}
	}

	h.log.Info("connection refused at the tenant's connection limit",
		"client", cl.ID, "tenant", string(identity.Tenant), "limit", limits.MaxConnections)
	h.count(func(m *obs.Metrics) {
		m.LimitExceeded.WithLabelValues(m.Tenant(string(identity.Tenant)), limitConnections).Inc()
	})

	code := packets.ErrQuotaExceeded
	if cl.Properties.ProtocolVersion < mqtt5 {
		// SendConnack turns this into the v3 return code 0x03.
		code = packets.ErrServerUnavailable
	}

	if err := h.server.SendConnack(cl, code, false, nil); err != nil {
		h.log.Debug("refusing a connection over its tenant's limit", "client", cl.ID, "error", err)
	}

	cl.Stop(code)

	return false
}

// holds reports whether this node already has an open connection under the
// same client id, which a new one is about to take over.
func (h *Hook) holds(cl *mqtt.Client) bool {
	existing, ok := h.server.Clients.Get(cl.ID)
	if !ok || existing == cl {
		return false
	}

	h.mu.Lock()
	_, open := h.open[existing]
	h.mu.Unlock()

	return open
}

// overRate reports whether a publish from cl takes its tenant past its
// rate. The internal listener is never limited.
func (h *Hook) overRate(cl *mqtt.Client, id tenant.ID) bool {
	return h.limits != nil && !h.isInternal(cl) && !h.limits.AllowPublish(id)
}

// rateLimited answers a publish past its tenant's rate.
//
// QoS 0 is dropped: nothing acknowledges it, and MQTT allows discarding it.
// MQTT 5 QoS 1 gets a PUBACK with Quota exceeded (0x97), so the client
// knows the message was not accepted and can slow down. It used to get
// Message rate too high (0x96), which reads better and is not allowed
// there: section 3.4.2.1 lists every PUBACK reason code and 0x96 is a
// DISCONNECT code only. paho.golang accepted it anyway, which is why the
// tests passed; mosquitto_pub dropped the connection on it as a
// protocol error, so a strict client heard a broken connection instead of
// a refusal. MQTT 5 QoS 2 is disconnected with 0x96, where it is legal,
// because mochi answers a failing reason code with a PUBACK even for QoS 2,
// which a client waiting for a PUBREC cannot read. MQTT 3.1.1 has no
// negative acknowledgement at all, so its only honest answer to a QoS 1 or
// 2 publish it will not take is closing the connection; acknowledging and
// dropping would be a lie the device cannot detect.
func (h *Hook) rateLimited(cl *mqtt.Client, pk packets.Packet, id tenant.ID) (packets.Packet, error) {
	h.count(func(m *obs.Metrics) {
		m.LimitExceeded.WithLabelValues(m.Tenant(string(id)), limitPublishRate).Inc()
	})

	switch {
	case pk.FixedHeader.Qos == 0:
		return pk, packets.ErrRejectPacket
	case cl.Properties.ProtocolVersion >= mqtt5 && pk.FixedHeader.Qos == 1:
		return pk, packets.ErrQuotaExceeded
	case cl.Properties.ProtocolVersion >= mqtt5:
		if err := h.server.DisconnectClient(cl, packets.ErrMessageRateTooHigh); err != nil {
			h.log.Debug("disconnecting a client over its tenant's rate", "client", cl.ID, "error", err)
		}

		return pk, packets.ErrRejectPacket
	default:
		cl.Stop(packets.ErrMessageRateTooHigh)

		return pk, packets.ErrRejectPacket
	}
}

// limitBacklog keeps only the newest messages of a drained queue that the
// tenant's offline limit allows, and counts the rest as discarded.
//
// They stay in the stream until the drain's trim, which removes everything
// up to the last message delivered and so takes these with it. Dropping the
// oldest is what the stream itself does at its own bound, so a tenant with a
// smaller limit sees the same policy at a smaller size.
func (h *Hook) limitBacklog(id tenant.ID, backlog *store.Backlog) {
	if h.limits == nil {
		return
	}

	keep := h.limits.OfflineQueue(id)

	drop := len(backlog.Messages) - keep
	if keep <= 0 || drop <= 0 {
		return
	}

	backlog.Messages = backlog.Messages[drop:]
	backlog.Seqs = backlog.Seqs[drop:]

	h.count(func(m *obs.Metrics) {
		m.LimitExceeded.WithLabelValues(m.Tenant(string(id)), limitOfflineQueue).Add(float64(drop))
	})
}

// limitInflight discards the oldest of what mochi holds for an absent
// client past its tenant's offline limit.
//
// A device that comes back to the node it left is resumed from mochi's own
// session, not from the stream: mochi resends its inflight messages and the
// stream's copy is discarded. Limiting only the drain would therefore limit
// a device that moved and not one that stayed, which on an all-in-one node
// is every device. mochi bounds that inflight store only by
// mqtt's MaximumInflight, thousands, so the tenant's limit is applied here,
// as each message for the absent client arrives.
//
// Trimming on every message cost a copy of the whole store per message once
// a sleeper was full, on the fan-out goroutine, for every sleeper the
// message reached. So the store is let run an eighth past the limit and
// then cut back to it in one go, which is one copy per keep/8 messages; the
// exact cut happens in OnSessionEstablish, when the device comes back,
// so what it is resent is still exactly its limit.
func (h *Hook) limitInflight(cl *mqtt.Client, id tenant.ID) {
	if h.limits == nil {
		return
	}

	keep := h.limits.OfflineQueue(id)

	// Len counts acknowledgements as well as messages, so it is only an
	// upper bound on what trimInflight counts; that is enough to skip it.
	if keep <= 0 || cl.State.Inflight.Len() <= keep+keep/offlineSlack {
		return
	}

	h.trimInflight(cl, id, keep)
}

// offlineSlack is the fraction, as its denominator, by which an absent
// client's inflight store may run past its limit before it is cut back.
const offlineSlack = 8

// OnSessionEstablish cuts the session a returning device is about to
// inherit to exactly its tenant's offline limit, which limitInflight lets
// run a little over.
//
// It runs after authentication and before mochi copies the old client's
// inflight store into the new one, so the old client is still the one in
// the client map, and only its store has to be cut. A client that is still
// connected is being taken over, not resumed: what it holds is in flight to
// a live device, not queued for an absent one, and is left alone.
func (h *Hook) OnSessionEstablish(cl *mqtt.Client, pk packets.Packet) {
	if h.limits == nil || pk.Connect.Clean {
		return
	}

	existing, ok := h.server.Clients.Get(cl.ID)
	if !ok || existing == cl || !queuesForAbsence(existing) {
		return
	}

	id, ok := h.tenantOf(cl)
	if !ok {
		return
	}

	if keep := h.limits.OfflineQueue(id); keep > 0 {
		h.trimInflight(existing, id, keep)
	}
}

// trimInflight discards the oldest PUBLISH packets in cl's inflight store
// past keep.
//
// Only PUBLISH packets are queued messages. The store also holds a PUBREL
// for a QoS 2 message the client has PUBREC'd, and a PUBREC for a QoS 2
// message the client sent and has not released. Those are protocol state,
// not messages: dropping the PUBREL leaves the client holding its packet id
// as "awaiting PUBREL", and when mochi reuses that id for a new message the
// client must take it for a duplicate and never deliver it (MQTT-4.3.3);
// dropping the PUBREC answers the client's PUBREL with "packet identifier
// not found" for a message that was published. They are also the oldest
// entries, made before the disconnect, so the first an oldest-first trim
// took. They neither count towards the limit nor are dropped, and only a
// dropped PUBLISH gives back the send quota it took.
//
// Oldest is by creation second, then packet id, which mochi allocates in
// increasing order; inside the one second in which ids wrap past 65535 the
// order is wrong and a newer message may go first. mochi's own resend
// order is no better than the second.
func (h *Hook) trimInflight(cl *mqtt.Client, id tenant.ID, keep int) {
	queued := slices.DeleteFunc(cl.State.Inflight.GetAll(false), func(pk packets.Packet) bool {
		return pk.FixedHeader.Type != packets.Publish
	})

	excess := len(queued) - keep
	if excess <= 0 {
		return
	}

	slices.SortFunc(queued, func(a, b packets.Packet) int {
		if c := cmp.Compare(a.Created, b.Created); c != 0 {
			return c
		}

		return cmp.Compare(a.PacketID, b.PacketID)
	})

	dropped := 0

	for _, pk := range queued[:excess] {
		if cl.State.Inflight.Delete(pk.PacketID) {
			atomic.AddInt64(&h.server.Info.Inflight, -1)
			cl.State.Inflight.IncreaseSendQuota()

			dropped++
		}
	}

	if dropped > 0 {
		h.count(func(m *obs.Metrics) {
			m.LimitExceeded.WithLabelValues(m.Tenant(string(id)), limitOfflineQueue).Add(float64(dropped))
		})
	}
}
