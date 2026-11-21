package quota_test

import (
	"testing"
	"time"

	"github.com/mastmq/mast/internal/domain/quota"
)

// epoch is any fixed instant; the tests move time by hand from it.
var epoch = time.Unix(1_700_000_000, 0) //nolint:gochecknoglobals // a constant time.Time cannot be declared const

func TestBucketAdmitsTheBurstThenTheRate(t *testing.T) {
	t.Parallel()

	b := quota.NewBucket(10, 5, epoch)

	for i := range 5 {
		if !b.Allow(epoch) {
			t.Fatalf("publish %d of a burst of 5 refused", i+1)
		}
	}

	if b.Allow(epoch) {
		t.Fatal("a sixth publish in the same instant was admitted past a burst of 5")
	}

	// Ten a second is one every 100ms.
	if !b.Allow(epoch.Add(100 * time.Millisecond)) {
		t.Error("the token earned in 100ms at 10/s was not there")
	}

	if b.Allow(epoch.Add(150 * time.Millisecond)) {
		t.Error("half a token was spent as a whole one")
	}
}

func TestBucketNeverHoldsMoreThanItsBurst(t *testing.T) {
	t.Parallel()

	b := quota.NewBucket(100, 3, epoch)
	later := epoch.Add(time.Hour)

	admitted := 0

	for b.Allow(later) {
		admitted++
	}

	if admitted != 3 {
		t.Errorf("an hour idle admitted %d at once, want the burst of 3", admitted)
	}
}

func TestBucketIgnoresAClockGoingBack(t *testing.T) {
	t.Parallel()

	b := quota.NewBucket(1, 1, epoch)
	b.Allow(epoch)

	if b.Allow(epoch.Add(-time.Hour)) {
		t.Error("a clock going backwards refilled the bucket")
	}

	if !b.Allow(epoch.Add(time.Second)) {
		t.Error("the bucket stopped refilling after the clock went back")
	}
}

func TestBucketResetKeepsWhatWasSpent(t *testing.T) {
	t.Parallel()

	b := quota.NewBucket(1, 10, epoch)
	for range 10 {
		b.Allow(epoch)
	}

	b.Reset(1000, 20, epoch)

	if b.Allow(epoch) {
		t.Error("raising the limit refilled a bucket the tenant had already emptied")
	}

	if !b.Allow(epoch.Add(time.Millisecond)) {
		t.Error("the raised rate did not apply")
	}
}

func TestCensusSumsFreshReports(t *testing.T) {
	t.Parallel()

	c := quota.NewCensus(5 * time.Second)
	c.Observe("edge-a", map[string]int{"acme": 3, "globex": 1}, epoch)
	c.Observe("edge-b", map[string]int{"acme": 4}, epoch.Add(time.Second))

	if got := c.Remote("acme", epoch.Add(2*time.Second)); got != 7 {
		t.Errorf("acme across two nodes = %d, want 7", got)
	}

	if got := c.Remote("initech", epoch); got != 0 {
		t.Errorf("a tenant nobody reported = %d, want 0", got)
	}
}

func TestCensusForgetsADeadNode(t *testing.T) {
	t.Parallel()

	c := quota.NewCensus(5 * time.Second)
	c.Observe("edge-a", map[string]int{"acme": 3}, epoch)
	c.Observe("edge-b", map[string]int{"acme": 4}, epoch.Add(4*time.Second))

	// edge-a stopped reporting: its devices are reconnecting elsewhere and
	// will be counted there, so its last report must not count them again.
	if got := c.Remote("acme", epoch.Add(6*time.Second)); got != 4 {
		t.Errorf("acme after edge-a went quiet = %d, want edge-b's 4", got)
	}

	c.Prune(epoch.Add(6 * time.Second))
	c.Prune(epoch.Add(20 * time.Second))

	if got := c.Remote("acme", epoch.Add(4*time.Second)); got != 0 {
		t.Errorf("acme after pruning everything = %d, want 0", got)
	}
}

func TestCensusZeroClearsATenant(t *testing.T) {
	t.Parallel()

	c := quota.NewCensus(time.Minute)
	c.Observe("edge-a", map[string]int{"acme": 3}, epoch)
	c.Observe("edge-a", map[string]int{"acme": 0}, epoch.Add(time.Second))

	if got := c.Remote("acme", epoch.Add(time.Second)); got != 0 {
		t.Errorf("acme after edge-a reported zero = %d, want 0", got)
	}
}

func TestCensusMergesPartialReports(t *testing.T) {
	t.Parallel()

	// A node with many tenants reports them in several messages; the second
	// must not erase what the first said.
	c := quota.NewCensus(time.Minute)
	c.Observe("edge-a", map[string]int{"acme": 3}, epoch)
	c.Observe("edge-a", map[string]int{"globex": 2}, epoch)

	if got := c.Remote("acme", epoch); got != 3 {
		t.Errorf("acme after a report about globex = %d, want 3", got)
	}
}
