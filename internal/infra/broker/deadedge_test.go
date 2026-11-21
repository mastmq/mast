package broker_test

import (
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mastmq/mast/internal/domain/liveness"
	"github.com/mastmq/mast/internal/infra/config"
)

// deathSettle bounds how long a test waits for the cores to notice a node
// that stopped beating: the threshold, a check, and the replay after it.
const deathSettle = liveness.DeadAfter + 15*time.Second

// waitUntil polls cond until it holds or d passes.
func waitUntil(d time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}

		time.Sleep(50 * time.Millisecond)
	}

	return cond()
}

// count returns how many of msgs are on mqttTopic.
func count(msgs []string, mqttTopic string) int {
	n := 0

	for _, m := range msgs {
		if strings.HasPrefix(m, mqttTopic+"=") {
			n++
		}
	}

	return n
}

// sleepOnEdgeZero connects a persistent QoS 1 subscriber to the first edge
// and waits until its record says that edge holds it — the state a device
// is in when its edge is killed under it.
func sleepOnEdgeZero(t *testing.T, c *cluster, clientID, tenantName, mqttTopic string) {
	t.Helper()

	dev := persistent(t, c.addrs[0], clientID, tenantName)
	dev.subscribeQoS(t, mqttTopic, 1)
	c.awaitInterest(t, dev, tenantName)

	key := tenantName + "." + clientID

	if !waitForLeaf(func() bool {
		sess, err := c.edges[1].Store().GetSession(t.Context(), key)

		return err == nil && sess.Present && sess.Node != "" && len(sess.Subscriptions) > 0
	}) {
		t.Fatal("the session was never recorded as held by its edge")
	}
}

func publishQoS1(t *testing.T, pub *collector, mqttTopic, prefix string, n int) {
	t.Helper()

	for i := range n {
		tok := pub.client.Publish(mqttTopic, 1, false, prefix+strconv.Itoa(i))
		if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
			t.Fatalf("publish %s%d: %v", prefix, i, tok.Error())
		}
	}
}

// TestClusterKilledEdgeSessionsAreQueuedFor covers a device connected to an
// edge that is killed outright.
//
// A clean shutdown records every session as away and the cores queue for
// them; a killed edge records nothing. Its sessions stayed on record as
// held, nothing kept their messages, and everything published to those
// devices until they reconnected was dropped — the one delivery gap the
// guide had to admit for a cluster that is otherwise healthy. The cores now
// declare an edge dead when its heartbeats stop, take over its sessions,
// and replay what was published since it was last heard from.
//
// The messages published straight after the kill are the ones that matter:
// they fall between the death and its detection, when the queuer read them
// with the session still marked held.
func TestClusterKilledEdgeSessionsAreQueuedFor(t *testing.T) {
	c := startCluster(t)

	const (
		tenantName = "hooli"
		cmd        = "fleet/crashed/cmd"
	)

	sleepOnEdgeZero(t, c, "crashed", tenantName, cmd)

	c.edges[0].Kill()
	c.edges[0] = nil

	pub := connect(t, c.addrs[1], "dispatcher", tenantName)

	const gap = 5

	publishQoS1(t, pub, cmd, "gap-", gap)

	if !waitUntil(deathSettle, func() bool {
		return readMetric(t, c.core.ObsAddr(), "mast_queuer_reaped_sessions_total ") >= 1
	}) {
		t.Fatal("the core never took over the killed edge's session")
	}

	if !waitUntil(deathSettle, func() bool {
		return readMetric(t, c.core.ObsAddr(), "mast_queuer_replayed_total ") >= gap
	}) {
		t.Errorf("the replay queued %v of the %d messages published before the edge was declared dead",
			readMetric(t, c.core.ObsAddr(), "mast_queuer_replayed_total "), gap)
	}

	// And after: the session is away in the queuer's index now, so the live
	// path keeps these.
	const after = 3

	publishQoS1(t, pub, cmd, "after-", after)
	awaitMetric(t, c.core.ObsAddr(), `mast_offline_queued_total{tenant="`+tenantName+`"} `, after)

	back := persistent(t, c.addrs[1], "crashed", tenantName)

	waitForLeaf(func() bool { return count(back.messages(), cmd) >= gap+after })

	got := back.messages()
	for _, want := range []string{"gap-0", "gap-4", "after-0", "after-2"} {
		if !slices.Contains(got, cmd+"="+want) {
			t.Errorf("%s never reached the device after its edge was killed: %v", want, got)
		}
	}

	if n := count(got, cmd); n != gap+after {
		t.Errorf("the device received %d messages, want %d: %v", n, gap+after, got)
	}
}

// TestClusterDeviceLeavingAKilledEdgeGetsTheGap covers the usual order of
// events, which the test above does not: a device on a killed edge sees its
// connection reset and reconnects to another edge within a second or two,
// long before the cores have declared the edge dead.
//
// It is restored there with nothing queued, and whatever was published
// while it was between the two edges was queued by nobody — the session
// was marked held by the dead edge until the new one wrote it held again.
// The cores remember which node a session moved off, replay the gap for it
// once that node is declared dead, and the new edge's straggler check
// delivers it.
func TestClusterDeviceLeavingAKilledEdgeGetsTheGap(t *testing.T) {
	c := startCluster(t)

	const (
		tenantName = "piedpiper"
		cmd        = "fleet/mover/cmd"
	)

	sleepOnEdgeZero(t, c, "mover", tenantName, cmd)

	c.edges[0].Kill()
	c.edges[0] = nil

	pub := connect(t, c.addrs[1], "dispatcher", tenantName)

	const gap = 5

	publishQoS1(t, pub, cmd, "gap-", gap)

	back := persistent(t, c.addrs[1], "mover", tenantName)

	// The straggler check thirty seconds after the connect is what delivers
	// the replay; the cores need the threshold and a check to get there.
	if !waitUntil(45*time.Second, func() bool { return count(back.messages(), cmd) >= gap }) {
		t.Fatalf("%d of %d messages published while the device moved off a killed edge reached it: %v",
			count(back.messages(), cmd), gap, back.messages())
	}
}

// TestClusterKilledEdgeQueueIsBounded holds the replay to the tenant's
// offline queue limit. The two arrived in separate changes and meet only
// here: the replay writes straight to the queue stream, past the live
// path, so the bound has to come from the drain like any other queue's. A
// replay that bypassed it would hand a device every message of the gap
// however little its tenant may keep.
func TestClusterKilledEdgeQueueIsBounded(t *testing.T) {
	const (
		tenantName = "acme"
		cmd        = "fleet/bounded/cmd"
		keep, gap  = 3, 10
	)

	c := startCluster(t, func(cfg *config.Config) {
		cfg.Limits.Tenants = map[string]config.TenantLimits{tenantName: {OfflineQueue: new(keep)}}
	})

	sleepOnEdgeZero(t, c, "bounded", tenantName, cmd)

	c.edges[0].Kill()
	c.edges[0] = nil

	pub := connect(t, c.addrs[1], "dispatcher", tenantName)
	publishQoS1(t, pub, cmd, "gap-", gap)

	if !waitUntil(deathSettle, func() bool {
		return readMetric(t, c.core.ObsAddr(), "mast_queuer_replayed_total ") >= gap
	}) {
		t.Fatalf("the replay queued %v of the %d messages published before the edge was declared dead",
			readMetric(t, c.core.ObsAddr(), "mast_queuer_replayed_total "), gap)
	}

	back := persistent(t, c.addrs[1], "bounded", tenantName)

	limit := `mast_limit_exceeded_total{limit="offline_queue",tenant="` + tenantName + `"} `
	if !awaitMetric(t, c.obs[1], limit, gap-keep) {
		t.Errorf("the edge discarded %v replayed messages, want %d", readMetric(t, c.obs[1], limit), gap-keep)
	}

	waitForLeaf(func() bool { return count(back.messages(), cmd) >= keep })

	want := make([]string, 0, keep)
	for i := gap - keep; i < gap; i++ {
		want = append(want, cmd+"=gap-"+strconv.Itoa(i))
	}

	got := slices.DeleteFunc(back.messages(), func(m string) bool { return !strings.HasPrefix(m, cmd+"=") })
	slices.Sort(got)

	if !slices.Equal(got, want) {
		t.Errorf("a device whose edge was killed was sent %v, want the newest %d: %v", got, keep, want)
	}
}

// TestClusterLiveEdgeIsNotDeclaredDead guards the other side of the
// threshold: an edge that is merely idle keeps beating, and a device held
// there is never queued for.
func TestClusterLiveEdgeIsNotDeclaredDead(t *testing.T) {
	c := startCluster(t)

	sleepOnEdgeZero(t, c, "idle", "globex", "fleet/idle/cmd")

	time.Sleep(liveness.DeadAfter + 3*liveness.Interval)

	if n := readMetric(t, c.core.ObsAddr(), "mast_queuer_nodes_dead_total "); n != 0 {
		t.Fatalf("the core declared %v live nodes dead", n)
	}

	if n := readMetric(t, c.core.ObsAddr(), "mast_queuer_away_sessions "); n != 0 {
		t.Errorf("the core keeps messages for %v sessions, all of them connected", n)
	}
}
