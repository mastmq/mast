package broker_test

import (
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"
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
	// obs are the metrics endpoints, in the same order. Tests read them
	// rather than sleeping: the broker already publishes exactly the
	// counters that say whether the thing under test has happened yet.
	obs []string
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
	coreCfg.Obs.Addr = anyPort
	coreCfg.Core.StoreDir = t.TempDir()
	coreCfg.Core.LeafAddr = leafAddr
	coreCfg.MQTT.Addr = "" // a core terminates no MQTT

	core, err := broker.Start(t.Context(), coreCfg, byUsername{}, tenant.AllowAll{}, log)
	if err != nil {
		t.Fatalf("starting core: %v", err)
	}

	c := &cluster{core: core, edges: nil, addrs: nil, obs: nil}

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
		// The kernel picks; the bound address is read back below.
		cfg.Obs.Addr = anyPort
		cfg.MQTT.Addr = freeAddr(t)
		cfg.Edge.CoreURLs = []string{"nats-leaf://" + leafAddr}

		edge, err := broker.Start(t.Context(), cfg, byUsername{}, tenant.AllowAll{}, log)
		if err != nil {
			t.Fatalf("starting edge %d: %v", i, err)
		}

		c.edges = append(c.edges, edge)
		c.addrs = append(c.addrs, cfg.MQTT.Addr)
		c.obs = append(c.obs, edge.ObsAddr())
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

// awaitInterest blocks until a message published on the far edge actually
// reaches sub.
//
// This is the only reliable proof that interest has propagated across the
// leaf connection. The obvious alternative — waitForLeaf on a constant —
// returns on its first check and proves nothing, which is what these tests
// did until one of them failed under the load of a full -race run while
// passing five times in a row on its own.
func (c *cluster) awaitInterest(t *testing.T, sub *collector, tenantName string) {
	t.Helper()

	const probe = "mast-test/interest"

	sub.subscribeQoS(t, probe, 0)

	pub := connect(t, c.addrs[1], "interest-probe-"+tenantName, tenantName)

	reached := waitForLeaf(func() bool {
		pub.publish(t, probe, "ping")

		for _, m := range sub.messages() {
			if strings.HasPrefix(m, probe+"=") {
				return true
			}
		}

		return false
	})
	if !reached {
		t.Fatal("interest never propagated between the edges")
	}

	pub.client.Disconnect(100)
}

// awaitMetric blocks until a counter or gauge on one edge reaches want.
//
// This replaces waiting on wall-clock time for something that happens on
// another node. A publisher's PUBACK comes from its own ingress node and
// says nothing about whether the node that owns the absent session has
// finished queueing, so a test that reconnects straight after publishing
// is racing a write it cannot see. The broker already counts it.
func awaitMetric(t *testing.T, obsAddr, sample string, want float64) bool {
	t.Helper()

	deadline := time.Now().Add(leafSettle)
	for time.Now().Before(deadline) {
		if readMetric(t, obsAddr, sample) >= want {
			return true
		}

		time.Sleep(20 * time.Millisecond)
	}

	return readMetric(t, obsAddr, sample) >= want
}

// readMetric returns one sample's value, or zero when it is absent — a
// counter that has never been incremented is not exported at all.
func readMetric(t *testing.T, obsAddr, sample string) float64 {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+obsAddr+"/metrics", nil)
	if err != nil {
		return 0
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0
	}

	for line := range strings.SplitSeq(string(body), "\n") {
		rest, found := strings.CutPrefix(line, sample)
		if !found {
			continue
		}

		value, err := strconv.ParseFloat(strings.TrimSpace(rest), 64)
		if err != nil {
			continue
		}

		return value
	}

	return 0
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

	// The claim has to reach the core before the second edge publishes its
	// own, or the notice arrives before the subscription that must hear it.
	c.awaitInterest(t, first, "acme")

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

// TestClusterSessionMovesBetweenNodes is the regression test for #8.
//
// A persistent session used to live only in the memory of the node that
// accepted it. A device reconnecting to a different edge arrived as a
// stranger: its subscriptions were gone, and so was anything published
// while it was away.
func TestClusterSessionMovesBetweenNodes(t *testing.T) {
	c := startCluster(t)

	// clean_session=false, so the session is meant to outlive the
	// connection.
	first := persistent(t, c.addrs[0], "wanderer", "acme")
	first.subscribeQoS(t, "fleet/wanderer/cmd", 1)

	// The subscription has to have reached the core before the device goes
	// away, or the publish below finds no interest and nothing is queued.
	c.awaitInterest(t, first, "acme")

	first.client.Disconnect(100)

	// Somebody publishes while it is away. QoS 1, because that is what
	// MQTT promises to keep for an absent session.
	pub := connect(t, c.addrs[1], "dispatcher", "acme")

	tok := pub.client.Publish("fleet/wanderer/cmd", 1, false, "go-north")
	if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("publishing while the session was away: %v", tok.Error())
	}

	// Wait for the node that owns the session to say it has stored the
	// message. Reconnecting before that races a write on another node.
	if !awaitMetric(t, c.obs[0], `mast_offline_queued_total{tenant="acme"} `, 1) {
		t.Fatal("nothing was queued for the absent session")
	}

	// It comes back on the other edge.
	second := persistent(t, c.addrs[1], "wanderer", "acme")

	// By content, not by count: a probe message from awaitInterest would
	// satisfy a length check without proving anything about the backlog.
	delivered := waitForLeaf(func() bool {
		return slices.Contains(second.messages(), "fleet/wanderer/cmd=go-north")
	})
	if !delivered {
		t.Errorf("the session did not follow the device to the other node; got %v", second.messages())
	}
}

// TestClusterSessionReturnsToItsFirstNode covers a device that goes to
// another node and comes back, which a load balancer does routinely.
//
// Taking a session over released this node's NATS subscriptions but left
// mochi's copy of the session in place. The device coming back inherited it:
// subscriptions with no NATS interest behind them, so it received nothing,
// and it looked like a local session, so nothing restored it from the
// bucket either.
func TestClusterSessionReturnsToItsFirstNode(t *testing.T) {
	c := startCluster(t)

	first := persistent(t, c.addrs[0], "boomerang", "acme")
	first.subscribeQoS(t, "fleet/boomerang/cmd", 1)
	c.awaitInterest(t, first, "acme")
	first.client.Disconnect(100)

	away := persistent(t, c.addrs[1], "boomerang", "acme")

	// The claim has to reach the first node before the device goes back,
	// or it is the return that the first node sees taken over.
	if !awaitMetric(t, c.obs[1], `mast_connections_open{tenant="acme"} `, 1) {
		t.Fatal("the device never connected to the second node")
	}

	time.Sleep(time.Second)
	away.client.Disconnect(100)

	back := persistent(t, c.addrs[0], "boomerang", "acme")
	pub := connect(t, c.addrs[1], "dispatcher", "acme")

	delivered := waitForLeaf(func() bool {
		pub.publish(t, "fleet/boomerang/cmd", "home")

		return slices.Contains(back.messages(), "fleet/boomerang/cmd=home")
	})
	if !delivered {
		t.Errorf("the device back on its first node receives nothing; got %v", back.messages())
	}
}

// TestClusterLocalResumeClearsTheQueue covers a message that was queued for
// an absent device and then delivered by mochi on the same node.
//
// OnQosPublish writes the message to the bucket at the moment mochi puts it
// in the session's inflight. A device resuming on the same node gets it from
// the inflight, and the bucket copy used to stay behind, so the next time
// the device came up on another node it received the message again.
func TestClusterLocalResumeClearsTheQueue(t *testing.T) {
	c := startCluster(t)

	// Its own tenant, so the counters read below are this test's alone.
	const tenantName = "globex"

	first := persistent(t, c.addrs[0], "homebody", tenantName)
	first.subscribeQoS(t, "fleet/homebody/cmd", 1)
	c.awaitInterest(t, first, tenantName)
	first.client.Disconnect(100)

	pub := connect(t, c.addrs[1], "dispatcher", tenantName)

	sent := []string{"one", "two"}
	for _, payload := range sent {
		tok := pub.client.Publish("fleet/homebody/cmd", 1, false, payload)
		if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
			t.Fatalf("publishing while the session was away: %v", tok.Error())
		}
	}

	if !awaitMetric(t, c.obs[0], `mast_offline_queued_total{tenant="`+tenantName+`"} `, float64(len(sent))) {
		t.Fatal("the messages were not queued for the absent session")
	}

	again := persistent(t, c.addrs[0], "homebody", tenantName)

	received := waitForLeaf(func() bool {
		got := again.messages()

		return slices.Contains(got, "fleet/homebody/cmd=one") && slices.Contains(got, "fleet/homebody/cmd=two")
	})
	if !received {
		t.Fatalf("resuming on the same node did not deliver the backlog; got %v", again.messages())
	}

	again.client.Disconnect(100)

	elsewhere := persistent(t, c.addrs[1], "homebody", tenantName)

	// Long enough for a drain to have happened, if there were anything left
	// to drain.
	time.Sleep(settle)

	if got := elsewhere.messages(); len(got) != 0 {
		t.Errorf("messages already delivered were replayed on the other node: %v", got)
	}
}

// TestClusterDrainReachesOnlyTheReturningClient covers the offline backlog
// being replayed to everybody on the node.
//
// The drain published each queued message into the node, which handed it
// to every local subscriber of the topic — clients that had already had it
// live — and re-queued it for every absent one.
func TestClusterDrainReachesOnlyTheReturningClient(t *testing.T) {
	c := startCluster(t)

	device := persistent(t, c.addrs[0], "traveller", "acme")
	device.subscribeQoS(t, "fleet/traveller/cmd", 1)
	c.awaitInterest(t, device, "acme")

	// On the node the device will come back to.
	monitor := connect(t, c.addrs[1], "monitor", "acme")
	monitor.subscribe(t, "fleet/#")

	device.client.Disconnect(100)

	pub := connect(t, c.addrs[1], "dispatcher", "acme")

	tok := pub.client.Publish("fleet/traveller/cmd", 1, false, "go")
	if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("publishing while the session was away: %v", tok.Error())
	}

	if !awaitMetric(t, c.obs[0], `mast_offline_queued_total{tenant="acme"} `, 1) {
		t.Fatal("nothing was queued for the absent session")
	}

	back := persistent(t, c.addrs[1], "traveller", "acme")
	if !waitForLeaf(func() bool { return slices.Contains(back.messages(), "fleet/traveller/cmd=go") }) {
		t.Fatalf("the backlog did not follow the device; got %v", back.messages())
	}

	time.Sleep(settle)

	seen := 0

	for _, m := range monitor.messages() {
		if m == "fleet/traveller/cmd=go" {
			seen++
		}
	}

	if seen != 1 {
		t.Errorf("the monitor received the message %d times, want 1 (live only): %v", seen, monitor.messages())
	}
}

// TestClusterTakeoverKeepsTheGaugeHonest checks the open-connections gauge on
// the node that loses a live client to another.
//
// mochi reports that disconnect from the client's goroutine after the
// takeover has already forgotten the client's tenant, so the gauge on the
// losing node used to stay one too high for every takeover.
func TestClusterTakeoverKeepsTheGaugeHonest(t *testing.T) {
	c := startCluster(t)

	const sample = `mast_connections_open{tenant="acme"} `

	first := connect(t, c.addrs[0], "rover", "acme")
	first.subscribe(t, "fleet/rover")
	c.awaitInterest(t, first, "acme")

	// awaitInterest's own probe publisher lives on the other node and has
	// gone by now; only the rover is left here.
	if !awaitMetric(t, c.obs[0], sample, 1) {
		t.Fatalf("the gauge never counted the first connection")
	}

	connect(t, c.addrs[1], "rover", "acme")

	if !waitForLeaf(func() bool { return !first.client.IsConnected() }) {
		t.Fatal("the second connection did not displace the first")
	}

	settled := waitForLeaf(func() bool { return readMetric(t, c.obs[0], sample) == 0 })
	if !settled {
		t.Errorf("the losing node still counts %v open connections, want 0", readMetric(t, c.obs[0], sample))
	}
}
