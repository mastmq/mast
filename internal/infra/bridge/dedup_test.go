package bridge

import (
	"testing"
	"time"

	"github.com/nats-io/nuid"
)

func TestSeenForgetsAfterTwoWindows(t *testing.T) {
	t.Parallel()

	now := time.Unix(0, 0)
	s := newSeenAt(func() time.Time { return now })

	if !s.first("m1") {
		t.Fatal("a new id was reported as seen")
	}

	if s.first("m1") {
		t.Fatal("a repeated id was reported as new")
	}

	// One rotation moves it to the previous generation, where it must still
	// count: a copy arriving just after a rotation is still a copy.
	now = now.Add(dedupWindow)

	if s.first("m1") {
		t.Fatal("an id was forgotten after a single window")
	}

	now = now.Add(2 * dedupWindow)

	if !s.first("m1") {
		t.Fatal("an id was remembered past two windows, so memory is not bounded by time")
	}
}

func TestSeenNeverDropsAnUnnamedMessage(t *testing.T) {
	t.Parallel()

	s := newSeen()

	// A message published straight to NATS carries no id. Treating the
	// empty string as an id would deliver the first and drop every other.
	for range 3 {
		if !s.first("") {
			t.Fatal("a message without an id was dropped as a duplicate")
		}
	}
}

func TestSeenSpreadsConsecutiveIDsAcrossShards(t *testing.T) {
	t.Parallel()

	// One publisher's ids differ only in their counter, which is what a
	// burst from one device looks like. All of them on one shard would make
	// the sharding decorative.
	ids := nuid.New()
	hit := make(map[int]int)

	for range 1000 {
		hit[shardOf(ids.Next())]++
	}

	if len(hit) < seenShards/2 {
		t.Fatalf("1000 consecutive ids landed on only %d of %d shards", len(hit), seenShards)
	}
}
