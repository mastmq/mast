package broker_test

import (
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/mastmq/mast/internal/domain/admin"
	"github.com/mastmq/mast/internal/infra/config"
	natsgo "github.com/nats-io/nats.go"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

// The trace an MQTT 5 client says its publish belongs to.
const (
	clientTraceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	clientSpanID  = "00f067aa0ba902b7"
	clientTrace   = "00-" + clientTraceID + "-" + clientSpanID + "-01"
)

// exportedSpan is what the tests need of one span a node exported.
type exportedSpan struct {
	name, traceID, spanID, parentID, node string
}

// otlpSink is an OTLP/HTTP collector that keeps what it is sent. It is the
// real wire: the exporter, its protobuf encoding and the endpoint taken
// from config are all under test, not a recorder swapped in behind them.
type otlpSink struct {
	url string

	mu    sync.Mutex
	spans []exportedSpan
}

func newOTLPSink(t *testing.T) *otlpSink {
	t.Helper()

	sink := &otlpSink{url: "", mu: sync.Mutex{}, spans: nil}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)

			return
		}

		var req coltracepb.ExportTraceServiceRequest
		if err := proto.Unmarshal(body, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)

			return
		}

		sink.keep(&req)

		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	sink.url = srv.URL

	return sink
}

func (s *otlpSink) keep(req *coltracepb.ExportTraceServiceRequest) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, rs := range req.GetResourceSpans() {
		var node string

		for _, kv := range rs.GetResource().GetAttributes() {
			if kv.GetKey() == "service.instance.id" {
				node = kv.GetValue().GetStringValue()
			}
		}

		for _, ss := range rs.GetScopeSpans() {
			for _, sp := range ss.GetSpans() {
				s.spans = append(s.spans, exportedSpan{
					name:     sp.GetName(),
					traceID:  hex.EncodeToString(sp.GetTraceId()),
					spanID:   hex.EncodeToString(sp.GetSpanId()),
					parentID: hex.EncodeToString(sp.GetParentSpanId()),
					node:     node,
				})
			}
		}
	}
}

// find returns the first span that matches.
func (s *otlpSink) find(match func(exportedSpan) bool) (exportedSpan, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, sp := range s.spans {
		if match(sp) {
			return sp, true
		}
	}

	return exportedSpan{}, false
}

// userProperty returns the first value of a user property, or "".
func userProperty(p *paho.PublishProperties, key string) string {
	if p == nil {
		return ""
	}

	return p.User.Get(key)
}

// TestClusterTraceCrossesTheFabric follows one QoS 1 publish from a client
// that names its trace, through the edge it lands on, across the durable
// stream to the other edge, and out to a subscriber. Every hop has to stay
// in the client's trace, and the subscriber has to be handed the span that
// delivered to it, or tracing a message through a cluster shows a set of
// unconnected fragments — which is exactly what a trace context dropped at
// the NATS hop would produce.
func TestClusterTraceCrossesTheFabric(t *testing.T) {
	sink := newOTLPSink(t)

	c := startClusterWith(t, func(cfg *config.Config) {
		cfg.Tracing.Enabled = true
		cfg.Tracing.Endpoint = sink.url
		cfg.Tracing.Sampler = "parentbased_always_on"
	})

	subNode, pubNode := c.edges[0].NATS().ID(), c.edges[1].NATS().ID()

	sub := dialV5(t, c.addrs[0], "dashboard")
	sub.subscribe(t, "telemetry/+")

	pub := dialV5(t, c.addrs[1], "sensor")

	delivered := waitForLeaf(func() bool {
		pub.publish(t, &paho.Publish{
			Topic: "telemetry/temp", QoS: 1, Payload: []byte("21.5"),
			Properties: &paho.PublishProperties{User: paho.UserProperties{{Key: "traceparent", Value: clientTrace}}},
		})

		return len(sub.received()) > 0
	})
	if !delivered {
		t.Fatal("the publish never crossed the fabric")
	}

	handed := userProperty(sub.received()[0].Properties, "traceparent")

	parts := strings.Split(handed, "-")
	if len(parts) != 4 || parts[1] != clientTraceID || parts[2] == clientSpanID || parts[3] != "01" {
		t.Fatalf("the subscriber was handed traceparent %q; want the client's trace with the delivery's span", handed)
	}

	// Closing an edge flushes what its exporter still holds.
	c.stopEdge(1)
	c.stopEdge(0)

	deliver, ok := sink.find(func(s exportedSpan) bool { return s.spanID == parts[2] })
	if !ok {
		t.Fatalf("no exported span matches the subscriber's traceparent %q", handed)
	}

	if deliver.name != "mqtt.deliver" || deliver.traceID != clientTraceID || deliver.node != subNode {
		t.Errorf("the subscriber's span is %+v; want mqtt.deliver on the subscriber's edge %s", deliver, subNode)
	}

	publish, ok := sink.find(func(s exportedSpan) bool { return s.spanID == deliver.parentID })
	if !ok {
		t.Fatalf("the delivery's parent %s was not exported", deliver.parentID)
	}

	if publish.name != "mqtt.publish" || publish.parentID != clientSpanID || publish.node != pubNode {
		t.Errorf("the delivery's parent is %+v; want mqtt.publish under the client's span on the publisher's edge %s",
			publish, pubNode)
	}

	for _, step := range []string{"mast.store.durable", "nats.publish"} {
		if _, ok := sink.find(func(s exportedSpan) bool {
			return s.name == step && s.parentID == publish.spanID
		}); !ok {
			t.Errorf("no %s span under the publish", step)
		}
	}
}

// TestTracingOffLeavesTheMessageAlone holds the default to costing
// nothing: no trace header is added to the fabric, and a traceparent a
// client sent reaches the subscriber exactly as it was sent.
func TestTracingOffLeavesTheMessageAlone(t *testing.T) {
	node, addr := startNode(t)

	headers := make(chan natsgo.Header, 1)

	fabric, err := node.NATS().Conn().Subscribe("t.>", func(m *natsgo.Msg) {
		select {
		case headers <- m.Header:
		default:
		}
	})
	if err != nil {
		t.Fatalf("watching the fabric: %v", err)
	}

	t.Cleanup(func() { _ = fabric.Unsubscribe() })

	sub := dialV5(t, addr, "dashboard")
	sub.subscribe(t, "telemetry/+")

	pub := dialV5(t, addr, "sensor")
	pub.publish(t, &paho.Publish{
		Topic: "telemetry/temp", QoS: 1, Payload: []byte("21.5"),
		Properties: &paho.PublishProperties{User: paho.UserProperties{{Key: "traceparent", Value: clientTrace}}},
	})

	if !waitFor(func() bool { return len(sub.received()) > 0 }) {
		t.Fatal("the publish was not delivered")
	}

	if got := userProperty(sub.received()[0].Properties, "traceparent"); got != clientTrace {
		t.Errorf("with tracing off the subscriber got traceparent %q, want the client's %q unchanged", got, clientTrace)
	}

	select {
	case h := <-headers:
		if v := h.Get("traceparent"); v != "" {
			t.Errorf("with tracing off the fabric message carries traceparent %q", v)
		}
	case <-time.After(settle):
		t.Fatal("nothing crossed the fabric")
	}
}

// TestClusterAdminWithTracing runs the admin API on every node and tracing
// on the edges at once. The two arrived in separate branches that each
// adjusted the cluster's configuration their own way — one for every node,
// one for edges only — and each touched the bridge's NATS side, so this
// holds that an edge doing both still traces a message across the fabric
// and still answers and acts on an admin request for a client it holds.
func TestClusterAdminWithTracing(t *testing.T) {
	sink := newOTLPSink(t)

	c := startClusterWith(t, func(cfg *config.Config) {
		cfg.Tracing.Enabled = true
		cfg.Tracing.Endpoint = sink.url
		cfg.Tracing.Sampler = "always_on"
	}, withAdmin(t))

	subNode, pubNode := c.edges[0].NATS().ID(), c.edges[1].NATS().ID()

	sub := dialV5(t, c.addrs[0], "dashboard")
	sub.subscribe(t, "telemetry/+")

	pub := dialV5(t, c.addrs[1], "sensor")

	delivered := waitForLeaf(func() bool {
		pub.publish(t, &paho.Publish{Topic: "telemetry/temp", QoS: 1, Payload: []byte("21.5")})

		return len(sub.received()) > 0
	})
	if !delivered {
		t.Fatal("the publish never crossed the fabric")
	}

	// Asked of the core, which has tracing off and holds no client.
	var s admin.Session

	ok := waitForLeaf(func() bool {
		var status int

		status, s = getSession(t, c.core.AdminAddr(), "acme", "dashboard")

		return status == http.StatusOK && s.Connected
	})
	if !ok || s.Node != subNode {
		t.Fatalf("the core's view of the subscriber = %+v, want connected on %s", s, subNode)
	}

	// Asked of the subscriber's edge, for the publisher the other edge holds.
	status, body := call(t, http.MethodPost, c.edges[0].AdminAddr(), "/v1/tenants/acme/sessions/sensor/disconnect")
	if status != http.StatusOK {
		t.Fatalf("disconnect: %d %s", status, body)
	}

	gone := waitForLeaf(func() bool {
		status, _ := getSession(t, c.core.AdminAddr(), "acme", "sensor")

		return status == http.StatusNotFound
	})
	if !gone {
		t.Fatal("a disconnect asked of one edge did not drop the clean client on the other")
	}

	// Closing an edge flushes what its exporter still holds.
	c.stopEdge(1)
	c.stopEdge(0)

	for _, want := range []exportedSpan{
		{name: "mqtt.publish", node: pubNode, traceID: "", spanID: "", parentID: ""},
		{name: "mqtt.deliver", node: subNode, traceID: "", spanID: "", parentID: ""},
	} {
		if _, ok := sink.find(func(s exportedSpan) bool { return s.name == want.name && s.node == want.node }); !ok {
			t.Errorf("no %s span exported from %s", want.name, want.node)
		}
	}
}
