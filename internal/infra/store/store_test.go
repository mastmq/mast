package store_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mastmq/mast/internal/infra/store"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// open starts an embedded JetStream server and returns a store over it.
func open(t *testing.T) *store.Store {
	t.Helper()

	opts := &natsserver.Options{
		ServerName: "store-test",
		DontListen: true,
		JetStream:  true,
		StoreDir:   t.TempDir(),
		NoSigs:     true,
		NoLog:      true,
	}

	ns, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("starting nats: %v", err)
	}

	go ns.Start()

	if !ns.ReadyForConnections(15 * time.Second) {
		t.Fatal("nats did not become ready")
	}

	t.Cleanup(ns.Shutdown)

	nc, err := nats.Connect("", nats.InProcessServer(ns))
	if err != nil {
		t.Fatalf("connecting: %v", err)
	}

	t.Cleanup(nc.Close)

	s, err := store.Open(context.Background(), nc, 1, time.Hour)
	if err != nil {
		t.Fatalf("opening store: %v", err)
	}

	return s
}

func TestRetained(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := open(t)

	put := func(key, topic, payload string) {
		t.Helper()

		if err := s.PutRetained(ctx, key, store.Message{
			Topic: topic, Payload: []byte(payload), QoS: 1, Retain: true,
		}); err != nil {
			t.Fatalf("PutRetained(%s): %v", key, err)
		}
	}

	put("t.acme.sensors.a.temp", "sensors/a/temp", "21")
	put("t.acme.sensors.b.temp", "sensors/b/temp", "22")
	put("t.other.sensors.a.temp", "sensors/a/temp", "99")

	// A subscription filter is also a KV watch pattern, which is the whole
	// point of the codec's key-safe escaping.
	got, err := s.MatchRetained(ctx, []string{"t.acme.sensors.>"})
	if err != nil {
		t.Fatalf("MatchRetained: %v", err)
	}

	if len(got) != 2 {
		t.Errorf("wildcard match returned %d messages, want 2 (other tenant must not appear)", len(got))
	}

	single, err := s.MatchRetained(ctx, []string{"t.acme.sensors.*.temp"})
	if err != nil {
		t.Fatalf("MatchRetained single-level: %v", err)
	}

	if len(single) != 2 {
		t.Errorf("single-level wildcard returned %d, want 2", len(single))
	}

	// An empty payload clears the value, as MQTT requires.
	if err := s.PutRetained(ctx, "t.acme.sensors.a.temp", store.Message{Topic: "sensors/a/temp"}); err != nil {
		t.Fatalf("clearing retained: %v", err)
	}

	after, err := s.MatchRetained(ctx, []string{"t.acme.sensors.>"})
	if err != nil {
		t.Fatalf("MatchRetained after clear: %v", err)
	}

	if len(after) != 1 {
		t.Errorf("after clearing one value, %d remain, want 1", len(after))
	}
}

func TestSessions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := open(t)

	if _, err := s.GetSession(ctx, "t.acme.absent"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("GetSession on a missing key = %v, want ErrNotFound", err)
	}

	want := store.Session{
		ClientID: "dev-1",
		Tenant:   "acme",
		Subscriptions: []store.Subscription{
			{Filter: "a/b", QoS: 1},
			{Filter: "c/#", QoS: 2},
		},
	}

	if err := s.PutSession(ctx, "t.acme.dev-1", want); err != nil {
		t.Fatalf("PutSession: %v", err)
	}

	got, err := s.GetSession(ctx, "t.acme.dev-1")
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}

	if len(got.Subscriptions) != 2 || got.Subscriptions[1].Filter != "c/#" {
		t.Errorf("subscriptions round-tripped as %v", got.Subscriptions)
	}

	if got.UpdatedAt.IsZero() {
		t.Error("UpdatedAt was not stamped")
	}

	if err := s.DeleteSession(ctx, "t.acme.dev-1"); err != nil {
		t.Fatalf("DeleteSession: %v", err)
	}

	if _, err := s.GetSession(ctx, "t.acme.dev-1"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("session survived deletion: %v", err)
	}
}

func TestOfflineQueue(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := open(t)

	for i := range 5 {
		if err := s.Enqueue(ctx, "t.acme.dev-1", store.Message{
			Topic: "a/b", Payload: []byte{byte(i)}, QoS: 1,
		}); err != nil {
			t.Fatalf("Enqueue %d: %v", i, err)
		}
	}

	drained, err := s.Drain(ctx, "t.acme.dev-1")
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}

	if len(drained) != 5 {
		t.Fatalf("drained %d messages, want 5", len(drained))
	}

	if drained[0].Payload[0] != 0 || drained[4].Payload[0] != 4 {
		t.Error("queue did not preserve publish order")
	}

	// Draining is destructive; a second drain finds nothing.
	again, err := s.Drain(ctx, "t.acme.dev-1")
	if err != nil {
		t.Fatalf("second Drain: %v", err)
	}

	if len(again) != 0 {
		t.Errorf("second drain returned %d messages, want 0", len(again))
	}
}

// TestOfflineQueueIsBounded pins the property that stops one absent device
// subscribed to a chatty topic from growing until the bucket does.
func TestOfflineQueueIsBounded(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := open(t)

	const sent = 250
	for i := range sent {
		if err := s.Enqueue(ctx, "t.acme.chatty", store.Message{
			Topic: "a/b", Payload: []byte{byte(i)}, QoS: 0,
		}); err != nil {
			t.Fatalf("Enqueue %d: %v", i, err)
		}
	}

	drained, err := s.Drain(ctx, "t.acme.chatty")
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}

	if len(drained) != 100 {
		t.Errorf("queue held %d messages, want it capped at 100", len(drained))
	}

	// The oldest are the ones dropped, so the newest must have survived.
	if last := drained[len(drained)-1].Payload[0]; last != byte(sent-1) {
		t.Errorf("newest message is %d, want %d — the wrong end was dropped", last, byte(sent-1))
	}
}
