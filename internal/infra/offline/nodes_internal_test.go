package offline

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/mastmq/mast/internal/domain/liveness"
	"github.com/mastmq/mast/internal/infra/obs"
	"github.com/mastmq/mast/internal/infra/store"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"
)

// heldOn is a record of the session held by node.
func heldOn(t *testing.T, node string, at time.Time) entry {
	t.Helper()

	value, err := json.Marshal(sessionOn(node, at))
	if err != nil {
		t.Fatal(err)
	}

	return entry{key: "acme.dev-1", op: jetstream.KeyValuePut, created: at, value: value}
}

func sessionOn(node string, at time.Time) store.Session {
	return store.Session{
		ClientID:      "dev-1",
		Tenant:        "acme",
		Subscriptions: []store.Subscription{{Filter: "dev/1/cmd", QoS: 1}},
		UpdatedAt:     at,
		Present:       true,
		Owner:         "o",
		Node:          node,
	}
}

var errNoLeader = errors.New("no leader")

var command = fabricMessage{ //nolint:gochecknoglobals // a fixture
	tenant: "acme",
	stored: store.Message{Topic: "dev/1/cmd", Payload: []byte("x"), QoS: 1},
}

func quietQueuer() *Queuer {
	return newQueuer(nil, time.Hour, nil, slog.New(slog.DiscardHandler))
}

// pastThreshold is a check time by which a node last heard from at start
// is dead.
func pastThreshold(start time.Time) time.Time {
	return start.Add(liveness.DeadAfter + 2*liveness.Interval)
}

// A node that stops beating has its held sessions taken over: from then on
// the live path keeps their messages.
func TestDeadNodeSessionsAreQueuedFor(t *testing.T) {
	t.Parallel()

	q := quietQueuer()
	start := time.Now()

	q.apply(heldOn(t, "edge-a", start))

	if items := q.match(command, "before", start); len(items) != 0 {
		t.Fatalf("a held session was queued for: %v", items)
	}

	work := q.checkNodes(t.Context(), pastThreshold(start))
	if len(work) != 1 || work[0].node != "edge-a" || len(work[0].keys) != 1 {
		t.Fatalf("work = %+v, want edge-a's one session", work)
	}

	if !work[0].since.Before(start) {
		t.Errorf("the replay starts at %v, after the node was last heard from at %v", work[0].since, start)
	}

	q.mu.Lock()
	targets, adopted := q.adoptLocked(work[0], map[string]store.Session{"acme.dev-1": sessionOn("edge-a", start)})
	q.mu.Unlock()

	if adopted != 1 || len(targets) != 1 {
		t.Fatalf("adopted %d, replaying %d; want 1 and 1", adopted, len(targets))
	}

	if items := q.match(command, "after", time.Now()); len(items) != 1 {
		t.Errorf("after its node died, a message was kept for %d sessions, want 1", len(items))
	}

	// The device comes back on another node.
	back := time.Now()
	q.apply(heldOn(t, "edge-b", back))

	if items := q.match(command, "later", back.Add(time.Second)); len(items) != 0 {
		t.Errorf("a device back on another node was still queued for: %v", items)
	}
}

// A node that beats again was never dead: its sessions are handed back.
func TestRevivedNodeGetsItsSessionsBack(t *testing.T) {
	t.Parallel()

	q := quietQueuer()
	start := time.Now()

	q.apply(heldOn(t, "edge-a", start))

	work := q.checkNodes(t.Context(), pastThreshold(start))

	q.mu.Lock()
	q.adoptLocked(work[0], map[string]store.Session{"acme.dev-1": sessionOn("edge-a", start)})
	q.mu.Unlock()

	q.onBeat(&nats.Msg{Subject: liveness.Subject("edge-a")})

	if items := q.match(command, "after", time.Now().Add(time.Second)); len(items) != 0 {
		t.Errorf("a session of a revived node was still queued for: %v", items)
	}

	q.mu.Lock()
	node := q.held["acme.dev-1"]
	q.mu.Unlock()

	if node != "edge-a" {
		t.Errorf("after the revival the session is held by %q, want edge-a, so a real death later is caught", node)
	}
}

// A node that beats while its records are being read keeps them.
func TestNodeRevivedDuringTheReadKeepsItsSessions(t *testing.T) {
	t.Parallel()

	q := quietQueuer()
	start := time.Now()

	q.apply(heldOn(t, "edge-a", start))

	work := q.checkNodes(t.Context(), pastThreshold(start))

	q.onBeat(&nats.Msg{Subject: liveness.Subject("edge-a")})

	q.mu.Lock()
	targets, adopted := q.adoptLocked(work[0], map[string]store.Session{"acme.dev-1": sessionOn("edge-a", start)})
	q.mu.Unlock()

	if adopted != 0 || len(targets) != 0 {
		t.Errorf("a node that beat again lost %d sessions and has %d replayed", adopted, len(targets))
	}
}

// A device that moved off the node before it was declared dead is replayed
// for up to the moment it moved, and not taken over: its new node has it.
func TestSessionThatMovedOffADeadNodeIsReplayedUntilItMoved(t *testing.T) {
	t.Parallel()

	q := quietQueuer()
	start := time.Now()
	moved := start.Add(time.Second)

	q.apply(heldOn(t, "edge-a", start))
	q.apply(heldOn(t, "edge-b", moved))

	// edge-b keeps beating; edge-a does not.
	now := pastThreshold(start)
	q.live.Beat("edge-b", now)

	work := q.checkNodes(t.Context(), now)
	if len(work) != 1 || work[0].node != "edge-a" {
		t.Fatalf("work = %+v, want edge-a only", work)
	}

	if len(work[0].keys) != 1 || !work[0].keys[0].until.Equal(moved) {
		t.Fatalf("targets = %+v, want the session up to %v", work[0].keys, moved)
	}

	q.mu.Lock()
	targets, adopted := q.adoptLocked(work[0], map[string]store.Session{"acme.dev-1": sessionOn("edge-b", moved)})
	q.mu.Unlock()

	if adopted != 0 || len(targets) != 1 {
		t.Errorf("adopted %d, replaying %d; want 0 and 1", adopted, len(targets))
	}

	if items := q.match(command, "live", time.Now()); len(items) != 0 {
		t.Errorf("a session held by a live node was queued for: %v", items)
	}
}

// A held record that arrives naming a node already dead is that node's
// last write, late: not a device coming back.
func TestLateRecordFromADeadNodeIsTakenOver(t *testing.T) {
	t.Parallel()

	q := quietQueuer()
	start := time.Now()

	q.live.Heard("edge-a", start)
	q.checkNodes(t.Context(), pastThreshold(start))

	q.apply(heldOn(t, "edge-a", start))

	work := q.checkNodes(t.Context(), pastThreshold(start).Add(liveness.Interval))
	if len(work) != 1 || len(work[0].keys) != 1 || work[0].keys[0].key != "acme.dev-1" {
		t.Fatalf("work = %+v, want the late record taken over", work)
	}
}

// A core that cannot reach the cluster judges nobody: to it every node is
// silent.
func TestUnreachableCoreDeclaresNobodyDead(t *testing.T) {
	t.Parallel()

	q := quietQueuer()
	q.reachable = func(context.Context) error { return errNoLeader }

	start := time.Now()
	q.apply(heldOn(t, "edge-a", start))

	if work := q.checkNodes(t.Context(), pastThreshold(start)); len(work) != 0 {
		t.Fatalf("an isolated core took over %+v", work)
	}

	if _, dead := q.live.Dead("edge-a"); dead {
		t.Error("an isolated core declared a node dead")
	}
}

// A record from an older node names none, and is never judged.
func TestRecordWithoutANodeIsNeverTakenOver(t *testing.T) {
	t.Parallel()

	q := quietQueuer()
	start := time.Now()

	q.apply(heldOn(t, "", start))

	if work := q.checkNodes(t.Context(), pastThreshold(start)); len(work) != 0 {
		t.Fatalf("a record naming no node was taken over: %+v", work)
	}
}

// A node that shut down cleanly delivered until it closed: the sessions
// that moved off it are not replayed, whichever arrives first, the goodbye
// or the record of the move. What it still holds on record is taken over.
func TestGoodbyeSparesTheReplayOfSessionsThatMoved(t *testing.T) {
	t.Parallel()

	bye := &nats.Msg{Subject: liveness.Subject("edge-a"), Data: []byte(liveness.Goodbye)}

	for _, goodbyeFirst := range []bool{false, true} {
		q := quietQueuer()
		start := time.Now()

		q.apply(heldOn(t, "edge-a", start))

		if goodbyeFirst {
			q.onBeat(bye)
		}

		q.apply(heldOn(t, "edge-b", start.Add(time.Second)))

		if !goodbyeFirst {
			q.onBeat(bye)
		}

		now := pastThreshold(start)
		q.live.Beat("edge-b", now)

		for _, r := range q.checkNodes(t.Context(), now) {
			if len(r.keys) != 0 {
				t.Errorf("goodbye first: %v; a session that moved off a node that said goodbye is replayed: %+v",
					goodbyeFirst, r)
			}
		}
	}
}

// A verdict postponed because the core could not reach the cluster — the
// sessions leader moving after a core kill, at the moment an edge is
// killed — still replays from the dead node's last beat, and still covers
// the session that moved off it, however long the postponement. Both were
// lost once: the excuse moved the last beat forward, and the move was
// forgotten after a fixed twenty-five seconds.
func TestPostponedVerdictStillReplaysFromTheLastBeat(t *testing.T) {
	t.Parallel()

	q := quietQueuer()
	start := time.Now()
	moved := start.Add(time.Second)

	q.apply(heldOn(t, "edge-a", start))
	q.apply(heldOn(t, "edge-b", moved))

	unreachable := true
	q.reachable = func(context.Context) error {
		if unreachable {
			return errNoLeader
		}

		return nil
	}

	var work []reaping

	// Checked every interval, as watchNodes does; the cluster is out of
	// reach from just before the threshold until well past the old memory.
	for at := liveness.Interval; at <= 45*time.Second; at += liveness.Interval {
		now := start.Add(at)
		unreachable = at >= liveness.DeadAfter && at < 30*time.Second

		q.live.Beat("edge-b", now)

		work = append(work, q.checkNodes(t.Context(), now)...)
	}

	if len(work) != 1 || work[0].node != "edge-a" {
		t.Fatalf("work = %+v, want edge-a once", work)
	}

	// The record was heard a moment after start, on the queuer's own clock.
	if got := work[0].since.Sub(start); got < -replayLead || got > -replayLead+time.Second {
		t.Errorf("the replay starts at %v from the last beat, want about %v: before the beat, not the excuse",
			got, -replayLead)
	}

	if len(work[0].keys) != 1 || !work[0].keys[0].until.Equal(moved) {
		t.Errorf("targets = %+v, want the session that moved, up to %v", work[0].keys, moved)
	}
}

// A move off a node that keeps beating is forgotten once no death of the
// node could replay back to it.
func TestDepartureFromALiveNodeIsForgotten(t *testing.T) {
	t.Parallel()

	q := quietQueuer()
	start := time.Now()

	q.apply(heldOn(t, "edge-a", start))
	q.apply(heldOn(t, "edge-b", start.Add(time.Second)))

	for at := liveness.Interval; at <= 2*replayLead; at += liveness.Interval {
		now := start.Add(at)
		q.live.Beat("edge-a", now)
		q.live.Beat("edge-b", now)
		q.checkNodes(t.Context(), now)
	}

	q.mu.Lock()
	left := len(q.left)
	q.mu.Unlock()

	if left != 0 {
		t.Errorf("%d departures remembered from a node still beating well past the move", left)
	}
}

// A node that said goodbye is taken over like a dead one, but not counted
// as a death: a rolling deploy is not a string of kills.
func TestGoodbyeIsNotCountedAsADeath(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	q := newQueuer(nil, time.Hour, obs.NewMetrics(reg, obs.Sources{}, 0), slog.New(slog.DiscardHandler))
	start := time.Now()

	q.apply(heldOn(t, "edge-a", start))
	q.onBeat(&nats.Msg{Subject: liveness.Subject("edge-a"), Data: []byte(liveness.Goodbye)})

	work := q.checkNodes(t.Context(), pastThreshold(start))
	if len(work) != 1 || len(work[0].keys) != 1 {
		t.Fatalf("work = %+v, want the session it still held taken over", work)
	}

	if n := counterValue(t, reg, "mast_queuer_nodes_dead_total"); n != 0 {
		t.Errorf("a node that said goodbye was counted dead %v times", n)
	}

	q.mu.Lock()
	remembered := len(q.farewell)
	q.mu.Unlock()

	if remembered != 0 {
		t.Errorf("the goodbye is still remembered after its node was judged")
	}

	// A node killed outright is counted.
	q.apply(heldOn(t, "edge-c", start))
	q.checkNodes(t.Context(), pastThreshold(start).Add(liveness.Interval))
	q.checkNodes(t.Context(), pastThreshold(pastThreshold(start)))

	if n := counterValue(t, reg, "mast_queuer_nodes_dead_total"); n != 1 {
		t.Errorf("a killed node was counted %v times, want once", n)
	}
}

func counterValue(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()

	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}

	for _, f := range families {
		if f.GetName() == name {
			return f.GetMetric()[0].GetCounter().GetValue()
		}
	}

	t.Fatalf("no metric %s", name)

	return 0
}
