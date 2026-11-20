package bridge

import (
	"log/slog"
	"testing"

	"github.com/mastmq/mast/internal/domain/tenant"
	mqtt "github.com/mastmq/mochi/v2"
	"github.com/mastmq/mochi/v2/packets"
	"github.com/nats-io/nats.go"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// clientTrace is a W3C traceparent as an MQTT 5 client would send it,
// sampled.
const (
	clientTraceID  = "4bf92f3577b34da6a3ce929d0e0e4736"
	clientSpanID   = "00f067aa0ba902b7"
	clientTraceCtx = "00-" + clientTraceID + "-" + clientSpanID + "-01"
)

// newTracedHook is a hook with a tracer that records into memory.
func newTracedHook(t *testing.T) (*Hook, *tracetest.SpanRecorder) {
	t.Helper()

	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))

	t.Cleanup(func() { _ = tp.Shutdown(t.Context()) })

	h := New(nil, nil, nil, tenant.AllowAll{}, Options{TracerProvider: tp}, slog.New(slog.DiscardHandler))

	return h, rec
}

// TestNATSCarrierKeepsTheW3CName covers the reason natsCarrier exists:
// propagation.HeaderCarrier goes through http.Header and writes
// "Traceparent", and a NATS header is case sensitive, so a NATS-native
// subscriber looking for the W3C name would never find it.
func TestNATSCarrierKeepsTheW3CName(t *testing.T) {
	t.Parallel()

	h, _ := newTracedHook(t)

	msg := nats.NewMsg("t.acme.a")
	cl := &mqtt.Client{ID: "acme/dev"}
	pk := packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish}, TopicName: "acme/a"}

	_, span := h.startPublish(cl, "acme", pk, msg)
	span.End()

	if msg.Header.Get(keyTraceparent) == "" {
		t.Fatalf("no %q header on the fabric message; headers: %v", keyTraceparent, msg.Header)
	}
}

// TestPublishContinuesTheClientTrace is the "taken from" half of the user
// property: a PUBLISH that names a trace opens its span inside that trace,
// and the fabric message names the publish span, so whichever node
// delivers it continues the same trace.
func TestPublishContinuesTheClientTrace(t *testing.T) {
	t.Parallel()

	h, rec := newTracedHook(t)

	msg := nats.NewMsg("t.acme.a")
	cl := &mqtt.Client{ID: "acme/dev"}
	pk := packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
		TopicName:   "acme/a",
		Properties: packets.Properties{User: []packets.UserProperty{
			{Key: "other", Val: "kept"},
			{Key: keyTraceparent, Val: clientTraceCtx},
		}},
	}

	_, span := h.startPublish(cl, "acme", pk, msg)
	span.End()

	ended := rec.Ended()
	if len(ended) != 1 {
		t.Fatalf("recorded %d spans, want 1", len(ended))
	}

	got := ended[0]
	if got.SpanContext().TraceID().String() != clientTraceID {
		t.Errorf("publish span is in trace %s, want the client's %s", got.SpanContext().TraceID(), clientTraceID)
	}

	if got.Parent().SpanID().String() != clientSpanID {
		t.Errorf("publish span's parent is %s, want the client's span %s", got.Parent().SpanID(), clientSpanID)
	}

	want := "00-" + clientTraceID + "-" + got.SpanContext().SpanID().String() + "-01"
	if header := msg.Header.Get(keyTraceparent); header != want {
		t.Errorf("fabric traceparent = %q, want %q", header, want)
	}
}

// TestUserCarrierReplacesRatherThanAppends covers the "given to" half:
// user property keys may repeat, and a subscriber handed two traceparents
// could not know which one is current.
func TestUserCarrierReplacesRatherThanAppends(t *testing.T) {
	t.Parallel()

	props := []packets.UserProperty{
		{Key: "a", Val: "1"},
		{Key: keyTraceparent, Val: "old"},
	}

	c := userCarrier{props: &props}
	c.Set(keyTraceparent, "new")
	c.Set(keyTracestate, "vendor=x")

	want := []packets.UserProperty{
		{Key: "a", Val: "1"},
		{Key: keyTraceparent, Val: "new"},
		{Key: keyTracestate, Val: "vendor=x"},
	}

	if len(props) != len(want) {
		t.Fatalf("got %v, want %v", props, want)
	}

	for i := range want {
		if props[i] != want[i] {
			t.Errorf("property %d = %v, want %v", i, props[i], want[i])
		}
	}
}

// TestDeliverPointsTheUserPropertyAtItself checks that a subscriber is
// handed the delivery span, not the publisher's own, and that a message
// which never carried a traceparent is not given one.
func TestDeliverPointsTheUserPropertyAtItself(t *testing.T) {
	t.Parallel()

	h, rec := newTracedHook(t)

	header := nats.Header{}
	header.Set(keyTraceparent, clientTraceCtx)

	t.Run("carried", func(t *testing.T) {
		props := packets.Properties{User: []packets.UserProperty{{Key: keyTraceparent, Val: clientTraceCtx}}}

		var span trace.Span

		span, props.User = h.startDeliver(header, props.User, "acme", "a", "", viaCore)
		span.End()

		sc := span.SpanContext()
		want := "00-" + clientTraceID + "-" + sc.SpanID().String() + "-01"

		if got := props.User[0].Val; got != want {
			t.Errorf("subscriber's traceparent = %q, want the delivery span %q", got, want)
		}
	})

	t.Run("not carried", func(t *testing.T) {
		props := packets.Properties{User: []packets.UserProperty{{Key: "k", Val: "v"}}}

		var span trace.Span

		span, props.User = h.startDeliver(header, props.User, "acme", "a", "", viaStream)
		span.End()

		if len(props.User) != 1 {
			t.Errorf("a traceparent was added to a message that had none: %v", props.User)
		}
	})

	for _, s := range rec.Ended() {
		if s.Parent().SpanID().String() != clientSpanID {
			t.Errorf("delivery span's parent is %s, want the publish %s", s.Parent().SpanID(), clientSpanID)
		}
	}
}

// TestTracingOffBuildsNoTracer pins the off switch to the one thing the hot
// path checks.
func TestTracingOffBuildsNoTracer(t *testing.T) {
	t.Parallel()

	if h := newTestHook(t); h.tracer != nil {
		t.Fatal("a hook built without a tracer provider has a tracer")
	}
}
