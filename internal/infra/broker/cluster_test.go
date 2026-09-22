package broker_test

import (
	"log/slog"
	"testing"
	"time"

	"github.com/mastmq/mast/internal/domain/tenant"
	"github.com/mastmq/mast/internal/infra/broker"
	"github.com/mastmq/mast/internal/infra/config"
)

// leafSettle bounds how long an edge is given to establish its leaf
// connection and for interest to propagate to the core. Interest
// propagation is what cross-node delivery waits on, and it is not
// instantaneous: subscribing on one edge and publishing on another
// immediately afterwards can race the interest update.
const leafSettle = 5 * time.Second

// cluster is a core node and the edges attached to it.
type cluster struct {
	core  *broker.Broker
	edges []*broker.Broker
	// addrs are the MQTT listeners, one per edge, in the same order.
	addrs []string
}

// startCluster brings up one core and n edge nodes in this process.
//
// This is the topology mast is actually deployed in and the one the
// single-node tests cannot reach: the core carries JetStream and accepts
// leaf connections, each edge terminates MQTT and joins the core as a leaf
// node, and the two tiers talk over a real socket rather than net.Pipe.
//
// Everything that is a cross-node property — delivery between edges, tenant
// isolation holding across the fabric, session ownership — is invisible to a
// single node, so before this existed those were verified by hand against
// three processes and then not verified again.
func startCluster(t *testing.T, resolver tenant.Resolver, n int) *cluster {
	t.Helper()

	log := slog.New(slog.DiscardHandler)

	leafAddr := freeAddr(t)

	coreCfg := config.Default()
	coreCfg.Role = config.RoleCore
	coreCfg.NATS.Name = "core"
	coreCfg.NATS.MonitorAddr = ""
	coreCfg.Obs.Addr = ""
	coreCfg.Core.StoreDir = t.TempDir()
	coreCfg.Core.LeafAddr = leafAddr
	coreCfg.MQTT.Addr = "" // a core terminates no MQTT

	core, err := broker.Start(t.Context(), coreCfg, resolver, tenant.AllowAll{}, log)
	if err != nil {
		t.Fatalf("starting core: %v", err)
	}

	c := &cluster{core: core, edges: nil, addrs: nil}

	// The edges come down before the core does: an edge whose core has
	// already gone is an edge logging a lost leaf connection through a test
	// logger that may itself be finished with.
	t.Cleanup(func() {
		for _, e := range c.edges {
			e.Close()
		}

		core.Close()
	})

	for i := range n {
		cfg := config.Default()
		cfg.Role = config.RoleEdge
		cfg.NATS.Name = "edge-" + string(rune('a'+i))
		cfg.NATS.MonitorAddr = ""
		// Every edge would otherwise bind the same default metrics port.
		cfg.Obs.Addr = ""
		cfg.MQTT.Addr = freeAddr(t)
		cfg.Edge.CoreURLs = []string{"nats-leaf://" + leafAddr}

		edge, err := broker.Start(t.Context(), cfg, resolver, tenant.AllowAll{}, log)
		if err != nil {
			t.Fatalf("starting edge %d: %v", i, err)
		}

		c.edges = append(c.edges, edge)
		c.addrs = append(c.addrs, cfg.MQTT.Addr)
	}

	return c
}

// waitForLeaf blocks until cond holds, using the longer leaf deadline.
func waitForLeaf(cond func() bool) bool {
	deadline := time.Now().Add(leafSettle)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}

		time.Sleep(20 * time.Millisecond)
	}

	return cond()
}

// TestClusterCrossNodeDelivery is the property the README claimed and no
// test held: a message published on one edge reaches a subscriber on
// another.
func TestClusterCrossNodeDelivery(t *testing.T) {
	c := startCluster(t, byUsername{}, 2)

	sub := connect(t, c.addrs[0], "sub", "acme")
	sub.subscribe(t, "sensors/+/temp")

	pub := connect(t, c.addrs[1], "pub", "acme")

	// Interest has to reach the core before a publish on the other edge can
	// match it, so retry rather than publishing once into a race.
	got := waitForLeaf(func() bool {
		pub.publish(t, "sensors/a/temp", "21")

		return len(sub.messages()) > 0
	})
	if !got {
		t.Fatalf("nothing crossed the fabric; got %v", sub.messages())
	}
}

// TestClusterTenantIsolation checks that the boundary holds across the
// fabric and not merely inside one process, which is the case that would
// actually matter in a breach.
func TestClusterTenantIsolation(t *testing.T) {
	c := startCluster(t, byUsername{}, 2)

	acme := connect(t, c.addrs[0], "acme-sub", "acme")
	acme.subscribe(t, "secrets/#")

	other := connect(t, c.addrs[0], "other-sub", "other")
	other.subscribe(t, "secrets/#")

	pub := connect(t, c.addrs[1], "acme-pub", "acme")

	if !waitForLeaf(func() bool {
		pub.publish(t, "secrets/one", "acme-only")

		return len(acme.messages()) > 0
	}) {
		t.Fatal("the tenant's own message never arrived, so the isolation check proves nothing")
	}

	if got := other.messages(); len(got) != 0 {
		t.Errorf("tenant boundary leaked across the fabric: other received %v", got)
	}
}
