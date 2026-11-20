package natsd_test

import (
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/mastmq/mast/internal/infra/config"
	"github.com/mastmq/mast/internal/infra/natsd"
)

// TestEdgeWithoutItsCoreIsNotReady covers an edge whose core cannot be
// reached at all. The embedded server is up and accepts its in-process
// connection, which is everything /healthz asks, and still the node can
// carry nothing across the cluster.
func TestEdgeWithoutItsCoreIsNotReady(t *testing.T) {
	cfg := config.Default()
	cfg.Role = config.RoleEdge
	cfg.NATS.Name = "edge-no-core"
	cfg.NATS.MonitorAddr = ""
	// A port that was free a moment ago and that nothing listens on: the
	// leaf dial is refused and retried for as long as the test runs.
	cfg.Edge.CoreURLs = []string{"nats-leaf://" + reservePort(t)}

	s, err := natsd.Start(cfg, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("starting the edge: %v", err)
	}

	t.Cleanup(s.Shutdown)

	err = s.Ready()
	if !errors.Is(err, natsd.ErrNotServing) {
		t.Fatalf("an edge with no core reports ready: %v", err)
	}
}

// TestStandaloneIsReadyOnceJetStreamIs covers the all-in-one node, which is
// its own core: ready means its JetStream is up.
func TestStandaloneIsReadyOnceJetStreamIs(t *testing.T) {
	cfg := config.Default()
	cfg.Role = config.RoleAllInOne
	cfg.NATS.Name = "standalone-ready"
	cfg.NATS.ClientAddr = ""
	cfg.NATS.MonitorAddr = ""
	cfg.Core.StoreDir = t.TempDir()

	s, err := natsd.Start(cfg, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("starting: %v", err)
	}

	t.Cleanup(s.Shutdown)

	deadline := time.Now().Add(10 * time.Second)
	for err = s.Ready(); err != nil && time.Now().Before(deadline); err = s.Ready() {
		time.Sleep(20 * time.Millisecond)
	}

	if err != nil {
		t.Fatalf("a standalone node never became ready: %v", err)
	}
}
