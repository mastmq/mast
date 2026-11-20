package liveness_test

import (
	"testing"
	"time"

	"github.com/mastmq/mast/internal/domain/liveness"
)

var epoch = time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC) //nolint:gochecknoglobals // a fixed clock for the tests

// tick checks every interval from start through end, as the queuer does,
// and returns every death declared on the way.
func tick(tr *liveness.Tracker, from, to time.Duration, beat func(at time.Time)) []liveness.Death {
	var deaths []liveness.Death

	for at := from; at <= to; at += liveness.Interval {
		now := epoch.Add(at)
		if beat != nil {
			beat(now)
		}

		deaths = append(deaths, tr.Check(now)...)
	}

	return deaths
}

func TestASilentNodeIsDeclaredDeadOnce(t *testing.T) {
	t.Parallel()

	tr := liveness.NewTracker(liveness.DeadAfter)
	tr.Beat("edge-a", epoch)

	if deaths := tick(tr, 0, liveness.DeadAfter, nil); len(deaths) != 0 {
		t.Fatalf("declared dead within the threshold: %v", deaths)
	}

	deaths := tick(tr, liveness.DeadAfter+liveness.Interval, 3*liveness.DeadAfter, nil)
	if len(deaths) != 1 || deaths[0].Node != "edge-a" || !deaths[0].LastSeen.Equal(epoch) {
		t.Fatalf("deaths = %v, want edge-a once, last seen at the epoch", deaths)
	}

	if last, dead := tr.Dead("edge-a"); !dead || !last.Equal(epoch) {
		t.Errorf("Dead(edge-a) = %v, %v", last, dead)
	}
}

func TestABeatingNodeStaysAlive(t *testing.T) {
	t.Parallel()

	tr := liveness.NewTracker(liveness.DeadAfter)

	deaths := tick(tr, 0, 5*liveness.DeadAfter, func(now time.Time) { tr.Beat("edge-a", now) })
	if len(deaths) != 0 {
		t.Fatalf("a node beating every interval was declared dead: %v", deaths)
	}
}

// A node named by a session record but never heard from — it died before
// this core started — is given the threshold from the moment it is named,
// and then declared dead.
func TestANodeOnlyNamedByARecordGetsTheThreshold(t *testing.T) {
	t.Parallel()

	tr := liveness.NewTracker(liveness.DeadAfter)
	tr.Heard("edge-gone", epoch)

	if deaths := tick(tr, 0, liveness.DeadAfter, nil); len(deaths) != 0 {
		t.Fatalf("declared dead before the threshold: %v", deaths)
	}

	if deaths := tick(tr, liveness.DeadAfter+liveness.Interval, 2*liveness.DeadAfter, nil); len(deaths) != 1 {
		t.Fatalf("deaths = %v, want one", deaths)
	}
}

// A heartbeat from a node declared dead is a false positive, and the caller
// is told so it can undo what it did.
func TestABeatAfterDeathRevives(t *testing.T) {
	t.Parallel()

	tr := liveness.NewTracker(liveness.DeadAfter)
	tr.Beat("edge-a", epoch)
	tick(tr, 0, 2*liveness.DeadAfter, nil)

	if !tr.Beat("edge-a", epoch.Add(2*liveness.DeadAfter)) {
		t.Fatal("a beat from a dead node did not report a revival")
	}

	if _, dead := tr.Dead("edge-a"); dead {
		t.Error("the node is still dead after beating")
	}

	if tr.Beat("edge-a", epoch.Add(2*liveness.DeadAfter+liveness.Interval)) {
		t.Error("an ordinary beat reported a revival")
	}
}

// The checker's own stall must not read as the whole cluster dying.
func TestALateCheckDeclaresNobody(t *testing.T) {
	t.Parallel()

	tr := liveness.NewTracker(liveness.DeadAfter)
	tr.Beat("edge-a", epoch)
	tr.Beat("edge-b", epoch)
	tr.Check(epoch)

	stalled := epoch.Add(3 * liveness.DeadAfter)

	if tr.Overdue(stalled) {
		t.Error("a late check reported an overdue node")
	}

	if deaths := tr.Check(stalled); len(deaths) != 0 {
		t.Fatalf("a check after a stall declared %v dead", deaths)
	}

	// From the stall on, the clock runs as usual: still silent a threshold
	// later is dead.
	deaths := tick(tr, 3*liveness.DeadAfter+liveness.Interval, 4*liveness.DeadAfter+liveness.Interval, nil)
	if len(deaths) != 2 {
		t.Fatalf("deaths after the stall = %v, want both nodes", deaths)
	}
}

func TestExcuseRestartsTheClock(t *testing.T) {
	t.Parallel()

	tr := liveness.NewTracker(liveness.DeadAfter)
	tr.Beat("edge-a", epoch)
	tick(tr, 0, liveness.DeadAfter, nil)

	now := epoch.Add(liveness.DeadAfter + liveness.Interval)
	if !tr.Overdue(now) {
		t.Fatal("a node silent past the threshold is not overdue")
	}

	tr.Excuse(now)

	if deaths := tr.Check(now); len(deaths) != 0 {
		t.Errorf("an excused node was declared dead: %v", deaths)
	}
}

func TestSubjectRoundTrips(t *testing.T) {
	t.Parallel()

	node, ok := liveness.NodeOf(liveness.Subject("NABCDEF"))
	if !ok || node != "NABCDEF" {
		t.Errorf("NodeOf(Subject) = %q, %v", node, ok)
	}

	for _, subject := range []string{"mast.node.", "mast.session.a.b", "mast.node.a.b", "t.acme.x"} {
		if _, ok := liveness.NodeOf(subject); ok {
			t.Errorf("NodeOf(%q) accepted it", subject)
		}
	}
}
