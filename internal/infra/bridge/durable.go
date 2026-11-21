package bridge

import (
	"context"
	"maps"
	"strings"

	"github.com/mastmq/mast/internal/domain/tenant"
	"github.com/mastmq/mast/internal/domain/topic"
	"github.com/mastmq/mast/internal/infra/obs"
	"github.com/mastmq/mast/internal/infra/store"
	mqtt "github.com/mastmq/mochi/v2"
	"github.com/mastmq/mochi/v2/packets"
	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// headerDurable marks a core NATS copy of a message that was also stored on
// the durable stream. A node reading the stream drops the plain copy, since
// the stream will deliver it; a node that is not reading it — an older
// version mid-rollout, or one whose consumer failed — delivers the copy as
// it always did.
const headerDurable = "Mast-Durable"

// fabricPrefix is what every subject [topic.EncodeTopic] produces begins
// with. A stored message swaps it for [store.DurablePrefix] and back.
const fabricPrefix = "t."

// startDurable begins reading the durable stream, one consumer for this
// whole node.
//
// Failing here is not fatal. The node then neither stores QoS 1 and 2
// publishes nor drops the plain copies of other nodes' ones, which is
// exactly the best-effort fabric mast had before, and says so in the log.
func (h *Hook) startDurable() {
	if h.store == nil || !h.durableQoS {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), storeTimeout)
	defer cancel()

	stop, err := h.store.ConsumeDurable(ctx, h.onDurable, func(err error) {
		h.log.Warn("reading the durable stream", "error", err)
	})
	if err != nil {
		h.log.Error("cross-node QoS 1 and 2 are best-effort on this node: the durable stream is unreachable",
			"error", err)

		return
	}

	h.stopDurable = stop
	h.durable.Store(true)

	h.log.Info("cross-node QoS 1 and 2 ride the durable stream")
}

// publishDurable stores a QoS 1 or 2 message on the durable stream and
// returns once it is replicated. Only then may the publisher be told the
// broker has it.
//
// ctx carries the trace the store is a step of; its deadline is this
// function's own.
func (h *Hook) publishDurable(ctx context.Context, msg *nats.Msg) error {
	ctx, span := h.startChild(ctx, "mast.store.durable", trace.SpanKindClient)

	stored := nats.NewMsg(store.DurablePrefix + strings.TrimPrefix(msg.Subject, fabricPrefix))
	stored.Data = msg.Data

	maps.Copy(stored.Header, msg.Header)

	ctx, cancel := context.WithTimeout(ctx, storeTimeout)
	defer cancel()

	err := h.store.PublishDurable(ctx, stored, msg.Header.Get(headerID))

	endSpan(span, err)

	if err != nil {
		h.count(func(m *obs.Metrics) { m.DurableFailures.Inc() })

		return err
	}

	return nil
}

// refuse turns down a publish the broker could not take responsibility for.
//
// An MQTT 5 client is told, with a failing reason code on its PUBACK or
// PUBREC. An MQTT 3 client has no negative acknowledgement to receive, so
// its connection is closed instead: that is what makes it reconnect and
// send the unacknowledged message again, where silently withholding the
// ack would leave it waiting for good.
func (h *Hook) refuse(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	if cl.Properties.ProtocolVersion == 5 { //nolint:mnd // the MQTT protocol level
		return pk, packets.ErrImplementationSpecificError
	}

	if err := h.server.DisconnectClient(cl, packets.ErrServerUnavailable); err != nil {
		h.log.Debug("disconnecting a client whose publish was refused", "client", cl.ID, "error", err)
	}

	return pk, packets.ErrRejectPacket
}

// onDurable delivers one message from the durable stream to this node's
// plain subscribers. Shared groups are not served from here: every node
// reads the whole stream, so a group member on each would receive it, and
// the group is served once, by the queue copy on core NATS.
func (h *Hook) onDurable(subject string, header nats.Header, data []byte) {
	if h.server == nil {
		return
	}

	h.count(func(m *obs.Metrics) { m.DurableRead.WithLabelValues("bridge").Inc() })

	fabric := fabricPrefix + strings.TrimPrefix(subject, store.DurablePrefix)

	// The stream is read whole, not by interest the way core NATS delivers,
	// so most of what a node reads here is for clients on other nodes.
	// Injecting it anyway cost a fan-out to nobody, counted it in
	// mast_messages_out_total, and with tracing on opened an mqtt.deliver
	// span on a node that delivered nothing: the soak stack showed an edge
	// with no subscriber at all reporting 1,700 messages out. Asked before
	// the dedup window, which then holds only ids this node delivers.
	if !h.subscribedHere(fabric) {
		return
	}

	// Redelivered after an acknowledgement went missing, and already handed
	// to mochi the first time.
	if !h.seen.first(header.Get(headerID)) {
		return
	}

	h.inject(fabric, header, data, "", "", viaStream)
}

// subscribedHere reports whether a message on a fabric subject matches any
// plain subscription mochi holds on this node. That includes a persistent
// session whose client is away, whose subscriptions mochi keeps indexed, so
// OnQosPublish still sees its messages and queues them. Shared groups are
// left out because the stream never serves them. A subject that does not
// decode is reported as matching, so inject logs it as before.
//
// It costs a walk of mochi's index that InjectPacket repeats for a message
// that does match; for one that does not, it replaces the whole injection.
func (h *Hook) subscribedHere(subject string) bool {
	tenantID, mqttTopic, err := topic.DecodeTopic(subject)
	if err != nil {
		return true
	}

	subs := h.server.Topics.Subscribers(mount(tenant.ID(tenantID), mqttTopic))

	return len(subs.Subscriptions) > 0 || len(subs.InlineSubscriptions) > 0
}

// storeDurable writes a QoS 1 or 2 publish to the durable stream and marks
// the fabric copy as stored, and reports false when the publish has to be
// refused because the stream could not take it. QoS 0, and a node with the
// durable fabric off, store nothing and always succeed.
func (h *Hook) storeDurable(ctx context.Context, cl *mqtt.Client, id tenant.ID, pk packets.Packet, msg *nats.Msg) bool {
	if pk.FixedHeader.Qos == 0 || !h.durable.Load() {
		return true
	}

	if err := h.publishDurable(ctx, msg); err != nil {
		trace.SpanFromContext(ctx).SetStatus(codes.Error, "refused: the durable stream could not store it")
		h.log.Warn("refusing a publish the fabric could not store",
			"client", cl.ID, "tenant", string(id), "error", err)

		return false
	}

	msg.Header.Set(headerDurable, "1")

	return true
}
