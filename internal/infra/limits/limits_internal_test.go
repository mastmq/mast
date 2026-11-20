package limits

import (
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/mastmq/mast/internal/domain/tenant"
	"github.com/mastmq/mast/internal/infra/config"
	"github.com/mastmq/mast/internal/infra/natsd"
	"github.com/nats-io/nats.go"
)

// local is a limiter that shares nothing, on a clock the test moves.
func local(opts Options) (*Limiter, *time.Time) {
	clock := time.Unix(1_700_000_000, 0)
	l := New(nil, opts, slog.New(slog.DiscardHandler))
	l.now = func() time.Time { return clock }

	return l, &clock
}

func TestAdmitRefusesPastTheLimit(t *testing.T) {
	t.Parallel()

	l, _ := local(Options{Defaults: tenant.Limits{MaxConnections: 2, OfflineQueue: 100}, Interval: time.Second})

	for i := range 2 {
		if _, err := l.Admit("acme", tenant.Overrides{}, false); err != nil {
			t.Fatalf("connection %d of 2 refused: %v", i+1, err)
		}
	}

	if _, err := l.Admit("acme", tenant.Overrides{}, false); !errors.Is(err, ErrTooManyConnections) {
		t.Fatalf("a third connection past a limit of 2: err = %v", err)
	}

	// Another tenant is not affected by acme's count.
	if _, err := l.Admit("globex", tenant.Overrides{}, false); err != nil {
		t.Errorf("globex refused because acme is full: %v", err)
	}

	// A takeover replaces a connection rather than adding one.
	if _, err := l.Admit("acme", tenant.Overrides{}, true); err != nil {
		t.Errorf("a takeover at the limit was refused: %v", err)
	}

	l.Release("acme")
	l.Release("acme")

	if _, err := l.Admit("acme", tenant.Overrides{}, false); err != nil {
		t.Errorf("a connection after two were released was refused: %v", err)
	}
}

func TestOverridesLayer(t *testing.T) {
	t.Parallel()

	l, _ := local(Options{
		Defaults: tenant.Limits{MaxConnections: 1, OfflineQueue: 100},
		Tenants:  map[tenant.ID]tenant.Overrides{"acme": {MaxConnections: new(3), OfflineQueue: new(10)}},
		Ceiling:  100,
		Interval: time.Second,
	})

	got, err := l.Admit("acme", tenant.Overrides{}, false)
	if err != nil {
		t.Fatalf("Admit: %v", err)
	}

	if got.MaxConnections != 3 || got.OfflineQueue != 10 {
		t.Errorf("configured override = %+v, want 3 connections and a queue of 10", got)
	}

	// The authentication backend has the last word, field by field.
	got, err = l.Admit("acme", tenant.Overrides{OfflineQueue: new(5)}, false)
	if err != nil {
		t.Fatalf("Admit: %v", err)
	}

	if got.MaxConnections != 3 || got.OfflineQueue != 5 {
		t.Errorf("with an auth override = %+v, want 3 connections and a queue of 5", got)
	}

	if q := l.OfflineQueue("acme"); q != 5 {
		t.Errorf("OfflineQueue = %d, want 5", q)
	}

	// An override cannot raise a queue past what the stream keeps.
	if _, err := l.Admit("acme", tenant.Overrides{OfflineQueue: new(1000)}, false); err != nil {
		t.Fatalf("Admit: %v", err)
	}

	if q := l.OfflineQueue("acme"); q != 100 {
		t.Errorf("OfflineQueue past the ceiling = %d, want the ceiling of 100", q)
	}
}

// TestAuthOverridesAreNotUndone covers a tenant whose devices hold tokens
// from different days. One that says nothing about limits, or that was
// issued before the limits in force, used to reset the tenant on the node
// it connected to, so the limit there depended on which device came last.
func TestAuthOverridesAreNotUndone(t *testing.T) {
	t.Parallel()

	l, clock := local(Options{Defaults: tenant.Limits{MaxConnections: 5000, OfflineQueue: 100}, Interval: time.Second})

	admit := func(o tenant.Overrides) int {
		t.Helper()

		got, err := l.Admit("acme", o, false)
		if err != nil {
			t.Fatalf("Admit: %v", err)
		}

		return got.MaxConnections
	}

	planChanged := clock.Add(-time.Hour)
	before := planChanged.Add(-24 * time.Hour)

	if got := admit(tenant.Overrides{MaxConnections: new(500), IssuedAt: planChanged}); got != 500 {
		t.Fatalf("the new plan's token: limit %d, want 500", got)
	}

	if got := admit(tenant.Overrides{MaxConnections: new(5000), IssuedAt: before}); got != 500 {
		t.Errorf("a token from the old plan put the limit back to %d", got)
	}

	if got := admit(tenant.Overrides{}); got != 500 {
		t.Errorf("a token with no limits claim reset the limit to %d", got)
	}

	if got := admit(tenant.Overrides{MaxConnections: new(800), IssuedAt: *clock}); got != 800 {
		t.Errorf("a newer token: limit %d, want 800", got)
	}
}

func TestPublishRateIsPerTenant(t *testing.T) {
	t.Parallel()

	l, clock := local(Options{
		Defaults: tenant.Limits{PublishRate: 2, PublishBurst: 2, OfflineQueue: 100},
		Interval: time.Second,
	})

	for _, id := range []tenant.ID{"acme", "globex"} {
		if _, err := l.Admit(id, tenant.Overrides{}, false); err != nil {
			t.Fatalf("Admit %s: %v", id, err)
		}
	}

	for i := range 2 {
		if !l.AllowPublish("acme") {
			t.Fatalf("publish %d of acme's burst of 2 was refused", i+1)
		}
	}

	if l.AllowPublish("acme") {
		t.Error("acme's third publish in the same instant was admitted")
	}

	if !l.AllowPublish("globex") {
		t.Error("globex was refused for acme's rate")
	}

	*clock = clock.Add(500 * time.Millisecond)

	if !l.AllowPublish("acme") {
		t.Error("the publish acme earned in half a second at 2/s was refused")
	}

	// A tenant with no connection here has nothing to limit.
	if !l.AllowPublish("initech") {
		t.Error("a tenant unknown to the node was refused")
	}
}

// TestConnectionLimitIsClusterWide runs two limiters as two nodes on one
// NATS server and requires each to count the other's connections.
func TestConnectionLimitIsClusterWide(t *testing.T) {
	nc := connect(t)

	opts := Options{
		Defaults: tenant.Limits{MaxConnections: 3, OfflineQueue: 100},
		Interval: 50 * time.Millisecond,
	}

	opts.NodeID = "edge-a"
	a := New(nc, opts, slog.New(slog.DiscardHandler))
	opts.NodeID = "edge-b"
	b := New(nc, opts, slog.New(slog.DiscardHandler))

	for _, l := range []*Limiter{a, b} {
		if err := l.Start(); err != nil {
			t.Fatalf("Start: %v", err)
		}

		t.Cleanup(l.Stop)
	}

	for range 2 {
		if _, err := a.Admit("acme", tenant.Overrides{}, false); err != nil {
			t.Fatalf("Admit on a: %v", err)
		}
	}

	if _, err := b.Admit("acme", tenant.Overrides{}, false); err != nil {
		t.Fatalf("Admit on b: %v", err)
	}

	// Settled by what b reports, not by a sleep.
	waitFor(t, func() bool { return a.Connections("acme") == 3 && b.Connections("acme") == 3 })

	if _, err := a.Admit("acme", tenant.Overrides{}, false); !errors.Is(err, ErrTooManyConnections) {
		t.Fatalf("a fourth connection across two nodes past a limit of 3: err = %v", err)
	}

	// b's connection goes; a hears the zero and admits one in its place.
	b.Release("acme")

	waitFor(t, func() bool { return a.Connections("acme") == 2 })

	if _, err := a.Admit("acme", tenant.Overrides{}, false); err != nil {
		t.Errorf("a connection after b released its own was refused: %v", err)
	}
}

func connect(t *testing.T) *nats.Conn {
	t.Helper()

	cfg := config.Default()
	cfg.NATS.MonitorAddr = ""
	cfg.Core.StoreDir = t.TempDir()

	s, err := natsd.Start(cfg, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("starting nats: %v", err)
	}

	t.Cleanup(s.Shutdown)

	return s.Conn()
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met within 5s")
		}

		time.Sleep(10 * time.Millisecond)
	}
}
