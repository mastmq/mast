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

// startCluster brings up one core and two edge nodes in this process.
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
func startCluster(t *testing.T) *cluster {
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

	core, err := broker.Start(t.Context(), coreCfg, byUsername{}, tenant.AllowAll{}, log)
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

	// Two edges, because every cross-node property needs exactly two: one
	// to act on and one to observe from. A third proves nothing further
	// and costs a JetStream server per test.
	const edges = 2

	for i := range edges {
		cfg := config.Default()
		cfg.Role = config.RoleEdge
		cfg.NATS.Name = "edge-" + string(rune('a'+i))
		cfg.NATS.MonitorAddr = ""
		// Every edge would otherwise bind the same default metrics port.
		cfg.Obs.Addr = ""
		cfg.MQTT.Addr = freeAddr(t)
		cfg.Edge.CoreURLs = []string{"nats-leaf://" + leafAddr}

		edge, err := broker.Start(t.Context(), cfg, byUsername{}, tenant.AllowAll{}, log)
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
	c := startCluster(t)

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
	c := startCluster(t)

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

// TestClusterTakeoverAcrossNodes is the regression test for #13.
//
// MQTT requires a second connection with an existing client id to displace
// the first. mochi enforces that inside a node; across the fabric nothing
// did, so a device that reconnected to a different edge — which is every
// rolling deploy and every load-balanced reconnect — left two live sessions
// receiving the same traffic.
func TestClusterTakeoverAcrossNodes(t *testing.T) {
	c := startCluster(t)

	first := connect(t, c.addrs[0], "rover", "acme")
	first.subscribe(t, "fleet/rover")

	// Give the claim time to reach the core before the second edge makes
	// its own, or the notice races the subscription that must hear it.
	if !waitForLeaf(func() bool { return true }) {
		t.Fatal("unreachable")
	}

	second := connect(t, c.addrs[1], "rover", "acme")

	if !waitForLeaf(func() bool { return !first.client.IsConnected() }) {
		t.Fatal("the same client id connected on another node did not displace the first")
	}

	if !second.client.IsConnected() {
		t.Fatal("the displacing client is not connected")
	}
}

// TestClusterTakeoverIsScopedToTenant is the other half, across the fabric:
// the same client id in a different tenant is a different client and must
// be left alone. Getting this wrong turns takeover into a cross-tenant
// denial of service that works from any node.
func TestClusterTakeoverIsScopedToTenant(t *testing.T) {
	c := startCluster(t)

	acme := connect(t, c.addrs[0], "rover", "acme")
	acme.subscribe(t, "fleet/rover")

	other := connect(t, c.addrs[1], "rover", "other")
	other.subscribe(t, "fleet/rover")

	// Long enough that a takeover notice would have arrived and acted.
	if waitForLeaf(func() bool { return !acme.client.IsConnected() }) {
		t.Fatal("a different tenant reusing the client id disconnected acme across the fabric")
	}

	if !other.client.IsConnected() {
		t.Fatal("the other tenant's client is not connected")
	}
}
