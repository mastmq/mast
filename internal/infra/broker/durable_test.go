package broker_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/mastmq/mast/internal/domain/tenant"
	"github.com/mastmq/mast/internal/infra/broker"
	"github.com/mastmq/mast/internal/infra/config"
	"github.com/mastmq/mast/internal/infra/natsd"
	"github.com/nats-io/nats.go/jetstream"
)

// startWithoutDurableStream starts a node and then deletes the durable
// stream from under it, so every QoS 1 or 2 publish fails to be stored.
func startWithoutDurableStream(t *testing.T) string {
	t.Helper()

	cfg := config.Default()
	cfg.MQTT.Addr = freeAddr(t)
	cfg.NATS.MonitorAddr = ""
	cfg.Obs.Addr = anyPort
	cfg.Core.StoreDir = t.TempDir()

	node, err := broker.Start(t.Context(), cfg, tenant.Static{Tenant: "acme"}, tenant.AllowAll{},
		slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("starting broker: %v", err)
	}

	t.Cleanup(node.Close)

	js, err := jetstream.NewWithDomain(node.NATS().Conn(), natsd.JetStreamDomain)
	if err != nil {
		t.Fatal(err)
	}

	if err := js.DeleteStream(t.Context(), "mast_qos"); err != nil {
		t.Fatalf("deleting the durable stream: %v", err)
	}

	return cfg.MQTT.Addr
}

// TestUnstorableQoS1IsRefusedV5 is the other half of #11: a QoS 1 publish
// the broker could not store must not be acknowledged as if it had been.
// An MQTT 5 client is told, with a failing reason code.
func TestUnstorableQoS1IsRefusedV5(t *testing.T) {
	t.Parallel()

	addr := startWithoutDurableStream(t)
	pub := dialV5(t, addr, "unlucky")

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	resp, err := pub.client.Publish(ctx, &paho.Publish{Topic: "orders/1", QoS: 1, Payload: []byte("go")})

	// paho reports a failing PUBACK as an error carrying the response.
	if err == nil && (resp == nil || resp.ReasonCode < 0x80) {
		t.Fatalf("an unstored QoS 1 publish was acknowledged as a success: %+v", resp)
	}

	if resp != nil && resp.ReasonCode < 0x80 {
		t.Errorf("reason code %#x, want a failure (>= 0x80)", resp.ReasonCode)
	}
}

// TestUnstorableQoS1IsRefusedV3 covers an MQTT 3 client, which has no
// failing acknowledgement to receive. Its connection is closed instead, so
// it reconnects and sends the unacknowledged message again.
func TestUnstorableQoS1IsRefusedV3(t *testing.T) {
	t.Parallel()

	addr := startWithoutDurableStream(t)
	pub := connect(t, addr, "unlucky-v3", "acme")

	tok := pub.client.Publish("orders/1", 1, false, "go")
	acked := tok.WaitTimeout(3*time.Second) && tok.Error() == nil

	if acked && pub.client.IsConnectionOpen() {
		t.Fatal("an unstored QoS 1 publish was acknowledged and the connection kept")
	}

	if !waitFor(func() bool { return !pub.client.IsConnectionOpen() }) {
		t.Error("the connection of a client whose publish was refused was kept open")
	}
}
