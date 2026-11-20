package bridge

import (
	"context"
	"maps"
	"slices"

	"github.com/mastmq/mast/internal/domain/tenant"
	mqtt "github.com/mastmq/mochi/v2"
	"github.com/mastmq/mochi/v2/packets"
	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	"go.opentelemetry.io/otel/trace"
)

// How a message is traced.
//
// A client's PUBLISH opens "mqtt.publish" on the ingress node, as a child of
// the trace the client named in an MQTT 5 user property "traceparent", if it
// named one. Its stores and its NATS publish are children of it, and its
// context rides the fabric in W3C headers on the NATS message — the stream
// copy included, since that copies every header. Every node that injects the
// message for its subscribers opens "mqtt.deliver" as a child of the
// publish, whichever path it arrived by. A message that carried a
// "traceparent" user property is delivered with it pointing at that
// delivery span, so a subscriber continues the same trace.
//
// The tracer is nil when tracing is off, and every path below checks that
// first. A no-op tracer would have been tidier and is not free: extracting a
// parent and wrapping it in a context allocates whether or not anything
// records it, and the QoS 0 path is judged in allocations.

// tracerName is the instrumentation scope every span here belongs to.
const tracerName = "github.com/mastmq/mast/internal/infra/bridge"

// The W3C trace context keys, which are the NATS header names and the MQTT 5
// user property keys alike. One spelling for both means a trace context set
// by a NATS-native publisher is read the same way as a bridged one.
const (
	keyTraceparent = "traceparent"
	keyTracestate  = "tracestate"
)

// Which way a message reached a node, for the delivery span.
const (
	viaCore   = "core"
	viaStream = "stream"
)

// w3c is the only propagator. Baggage is deliberately not carried: it is
// arbitrary data a client could make every node copy onto every message.
//
//nolint:gochecknoglobals // stateless, and the same value everywhere
var w3c propagation.TraceContext

// natsCarrier reads and writes trace context in NATS headers.
//
// propagation.HeaderCarrier would have done it through http.Header, which
// canonicalises "traceparent" to "Traceparent". NATS header names are case
// sensitive, so a NATS-native client reading the W3C name would miss it.
type natsCarrier nats.Header

// Get implements [propagation.TextMapCarrier].
func (c natsCarrier) Get(key string) string { return nats.Header(c).Get(key) }

// Set implements [propagation.TextMapCarrier].
func (c natsCarrier) Set(key, value string) { nats.Header(c).Set(key, value) }

// Keys implements [propagation.TextMapCarrier].
func (c natsCarrier) Keys() []string { return slices.Collect(maps.Keys(c)) }

// userCarrier reads and writes trace context in MQTT 5 user properties.
//
// Set replaces a key that is there rather than appending, because user
// property keys may repeat and a second "traceparent" would leave a
// subscriber to guess which one is current.
type userCarrier struct {
	props *[]packets.UserProperty
}

// Get implements [propagation.TextMapCarrier].
func (c userCarrier) Get(key string) string {
	for _, p := range *c.props {
		if p.Key == key {
			return p.Val
		}
	}

	return ""
}

// Set implements [propagation.TextMapCarrier].
func (c userCarrier) Set(key, value string) {
	for i := range *c.props {
		if (*c.props)[i].Key == key {
			(*c.props)[i].Val = value

			return
		}
	}

	*c.props = append(*c.props, packets.UserProperty{Key: key, Val: value})
}

// Keys implements [propagation.TextMapCarrier].
func (c userCarrier) Keys() []string {
	keys := make([]string, len(*c.props))
	for i, p := range *c.props {
		keys[i] = p.Key
	}

	return keys
}

// tracerFrom returns the bridge's tracer, or nil when tracing is off.
//
//nolint:ireturn // the tracer is an interface by design
func tracerFrom(tp trace.TracerProvider) trace.Tracer {
	if tp == nil {
		return nil
	}

	return tp.Tracer(tracerName)
}

// startPublish opens the span for a client's PUBLISH and writes its context
// onto the message bound for the fabric. It must only be called with a
// tracer.
//
//nolint:ireturn // the span is the point, and the caller ends it
func (h *Hook) startPublish(
	cl *mqtt.Client, id tenant.ID, pk packets.Packet, msg *nats.Msg,
) (context.Context, trace.Span) {
	user := pk.Properties.User
	parent := w3c.Extract(context.Background(), userCarrier{props: &user})

	ctx, span := h.tracer.Start(parent, "mqtt.publish", trace.WithSpanKind(trace.SpanKindServer))

	// Written whether or not this span is sampled: the flag in it is how
	// every other node learns the decision, and without it each would roll
	// its own and a trace would arrive in pieces.
	w3c.Inject(ctx, natsCarrier(msg.Header))

	if span.IsRecording() {
		span.SetAttributes(
			semconv.MessagingSystemKey.String("mqtt"),
			semconv.MessagingOperationTypeReceive,
			semconv.MessagingDestinationName(unmount(id, pk.TopicName)),
			semconv.MessagingMessageID(msg.Header.Get(headerID)),
			semconv.MessagingClientID(unmountClient(id, cl.ID)),
			attribute.String("mast.tenant", string(id)),
			attribute.Int("mqtt.qos", int(pk.FixedHeader.Qos)),
			attribute.Bool("mqtt.retain", pk.FixedHeader.Retain),
		)
	}

	return ctx, span
}

// startChild opens a span under ctx for one step of a publish. With tracing
// off it returns ctx and a nil span, which [endSpan] accepts.
//
//nolint:ireturn,spancheck // ended by the caller through endSpan
func (h *Hook) startChild(ctx context.Context, name string, kind trace.SpanKind) (context.Context, trace.Span) {
	if h.tracer == nil {
		return ctx, nil
	}

	return h.tracer.Start(ctx, name, trace.WithSpanKind(kind))
}

// endSpan ends span, marking it failed when err is not nil. A nil span is
// tracing off, and costs a comparison.
func endSpan(span trace.Span, err error) {
	if span == nil {
		return
	}

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	span.End()
}

// startDeliver opens the span for injecting a message from the fabric, as a
// child of the publish that sent it, and points a "traceparent" user
// property the message carries at it. It must only be called with a
// tracer.
//
// It takes the user properties and returns them rather than taking a
// pointer to the packet's properties: a pointer stored in the carrier made
// the compiler move the whole 256-byte properties struct to the heap in
// inject, on every delivery, traced or not. BenchmarkPublishQoS0 showed it
// as one allocation more with tracing off.
//
//nolint:ireturn // ended by inject, which owns the delivery
func (h *Hook) startDeliver(
	header nats.Header, user []packets.UserProperty, tenantID, mqttTopic, queue, via string,
) (trace.Span, []packets.UserProperty) {
	parent := w3c.Extract(context.Background(), natsCarrier(header))

	ctx, span := h.tracer.Start(parent, "mqtt.deliver", trace.WithSpanKind(trace.SpanKindConsumer))

	// Only a message that already carried one. Adding the property to
	// every message would change what every MQTT 5 subscriber receives, and
	// grow every delivery, for a client that never asked to be traced.
	if carrier := (userCarrier{props: &user}); carrier.Get(keyTraceparent) != "" {
		w3c.Inject(ctx, carrier)
	}

	if span.IsRecording() {
		attrs := []attribute.KeyValue{
			semconv.MessagingSystemKey.String("mqtt"),
			semconv.MessagingOperationTypeSend,
			semconv.MessagingDestinationName(mqttTopic),
			semconv.MessagingMessageID(header.Get(headerID)),
			attribute.String("mast.tenant", tenantID),
			attribute.String("mast.fabric", via),
		}

		if queue != "" {
			attrs = append(attrs, semconv.MessagingConsumerGroupName(queue))
		}

		span.SetAttributes(attrs...)
	}

	return span, user
}
