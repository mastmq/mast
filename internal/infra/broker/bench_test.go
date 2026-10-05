package broker_test

import (
	"log/slog"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/mastmq/mast/internal/domain/tenant"
	"github.com/mastmq/mast/internal/infra/broker"
	"github.com/mastmq/mast/internal/infra/config"
)

// startNode is start for benchmarks, returning the node so its NATS
// connection can be inspected.
func startNode(tb testing.TB) (*broker.Broker, string) {
	tb.Helper()

	cfg := config.Default()
	cfg.MQTT.Addr = freeAddr(tb)
	cfg.NATS.MonitorAddr = ""
	cfg.Obs.Addr = anyPort
	cfg.Core.StoreDir = tb.TempDir()

	node, err := broker.Start(tb.Context(), cfg, byUsername{}, tenant.AllowAll{}, slog.New(slog.DiscardHandler))
	if err != nil {
		tb.Fatalf("starting broker: %v", err)
	}

	tb.Cleanup(node.Close)

	return node, cfg.MQTT.Addr
}

// dialBench connects a client to the node as tenant acme.
func dialBench( //nolint:ireturn // paho's constructor returns the interface
	tb testing.TB, addr, clientID string, onMessage paho.MessageHandler,
) paho.Client {
	tb.Helper()

	opts := paho.NewClientOptions().
		AddBroker("tcp://" + addr).
		SetClientID(clientID).
		SetUsername("acme").
		SetConnectTimeout(5 * time.Second).
		SetAutoReconnect(false).
		SetOrderMatters(false)

	if onMessage != nil {
		opts.SetDefaultPublishHandler(onMessage)
	}

	c := paho.NewClient(opts)

	tok := c.Connect()
	if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		tb.Fatalf("connecting %s: %v", clientID, tok.Error())
	}

	tb.Cleanup(func() { c.Disconnect(250) })

	return c
}

// BenchmarkPublishQoS0 is the whole QoS 0 path on one node: a publisher's
// PUBLISH in, out to NATS, back over the node's subscription, through
// mochi's fan-out and onto one subscriber's connection. It is the number the
// design is judged by, so it is kept where every change runs it.
func BenchmarkPublishQoS0(b *testing.B) {
	_, addr := startNode(b)

	var received atomic.Int64

	sub := dialBench(b, addr, "sub", func(_ paho.Client, _ paho.Message) { received.Add(1) })
	if tok := sub.Subscribe("bench/#", 0, nil); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		b.Fatalf("subscribing: %v", tok.Error())
	}

	pub := dialBench(b, addr, "pub", nil)
	payload := []byte("0123456789abcdef0123456789abcdef")

	// One probe proves the subscription is live before the clock starts.
	pub.Publish("bench/probe", 0, false, payload)

	if !waitFor(func() bool { return received.Load() == 1 }) {
		b.Fatal("the subscriber never received the probe")
	}

	received.Store(0)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; b.Loop(); i++ {
		pub.Publish("bench/dev/"+strconv.Itoa(i%64), 0, false, payload)
	}

	deadline := time.Now().Add(10 * time.Second)
	for received.Load() < int64(b.N) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	b.StopTimer()

	if got := received.Load(); got < int64(b.N) {
		b.Fatalf("subscriber received %d of %d", got, b.N)
	}
}

// BenchmarkConnect measures a connection storm: what it costs the node to
// admit a client, and what the client leaves behind on the NATS connection.
func BenchmarkConnect(b *testing.B) {
	node, addr := startNode(b)

	before := node.NATS().Conn().NumSubscriptions()

	b.ReportAllocs()

	clients := make([]paho.Client, 0, b.N)

	for i := 0; b.Loop(); i++ {
		clients = append(clients, dialBench(b, addr, "storm-"+strconv.Itoa(i), nil))
	}

	b.StopTimer()
	b.ReportMetric(float64(node.NATS().Conn().NumSubscriptions()-before)/float64(b.N), "nats-subs/client")

	for _, c := range clients {
		c.Disconnect(0)
	}
}
