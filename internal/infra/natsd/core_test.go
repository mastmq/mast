package natsd_test

import (
	"log/slog"
	"net"
	"testing"

	"github.com/mastmq/mast/internal/infra/config"
	"github.com/mastmq/mast/internal/infra/natsd"
)

// TestCoreListensForRoutesWithoutAClientAddress covers a core started from
// the example configuration, where nats.client_addr is empty.
//
// That used to mean no client listener at all, and nats-server opens its
// route listener only once the client accept loop runs, so the core never
// listened for routes and gave up starting after fifteen seconds. The chart
// and the Ansible role both set an address, which is how it went unseen.
func TestCoreListensForRoutesWithoutAClientAddress(t *testing.T) {
	cfg := config.Default()
	cfg.Role = config.RoleCore
	cfg.NATS.Name = "core-no-client-addr"
	cfg.NATS.ClientAddr = ""
	cfg.NATS.MonitorAddr = ""
	cfg.Core.StoreDir = t.TempDir()
	// A concrete port: nats-server reads cluster port 0 as "no cluster",
	// which would pass this test without testing anything. The window
	// between releasing it and the core taking it only risks a bind
	// failure, which fails loudly rather than passing.
	cfg.Core.ListenAddr = reservePort(t)
	// Itself, as the chart lists every core: JetStream in a cluster will
	// not start with no routes at all, and a route to itself is dropped.
	cfg.Core.Routes = []string{"nats-route://" + cfg.Core.ListenAddr}

	s, err := natsd.Start(cfg, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("a core with no client address did not start: %v", err)
	}

	t.Cleanup(s.Shutdown)

	if s.ClusterAddr() == "" {
		t.Error("the core is not listening for routes")
	}
}

func reservePort(t *testing.T) string {
	t.Helper()

	l, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserving a port: %v", err)
	}

	addr := l.Addr().String()
	_ = l.Close()

	return addr
}
