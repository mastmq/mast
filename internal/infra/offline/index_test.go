package offline

import (
	"fmt"
	"maps"
	"math/rand/v2"
	"runtime"
	"strings"
	"testing"

	mqtt "github.com/mastmq/mochi/v2"
	"github.com/mastmq/mochi/v2/packets"
)

// TestIndexMatchesLikeMochi holds the queuer's index to mochi's answers on
// random filters and topics. mochi's index on the edge decides what a
// connected device is sent, so the queuer has to keep exactly that for one
// that is away: no more, which would deliver what the device never
// subscribed to, and no less, which would lose it.
func TestIndexMatchesLikeMochi(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2)) //nolint:gosec // reproducible test input, not security

	// A small alphabet so filters and topics actually collide, with the
	// edge cases mochi has rules for: "$" topics, empty levels, and "#"
	// and "+" at every position.
	words := []string{"a", "b", "$s", "", "c"}

	level := func(wild bool) string {
		if wild {
			switch rng.IntN(4) {
			case 0:
				return "+"
			case 1:
				return "#"
			}
		}

		return words[rng.IntN(len(words))]
	}

	path := func(wild bool) string {
		n := 1 + rng.IntN(4)
		parts := make([]string, 0, n)

		for i := range n {
			p := level(wild)
			parts = append(parts, p)

			if p == "#" || i == n-1 {
				break
			}
		}

		return strings.Join(parts, "/")
	}

	for round := range 200 {
		ours, theirs := newIndex(), mqtt.NewTopicsIndex()
		subscribed := map[string][]string{}

		for s := range 30 {
			key := fmt.Sprintf("s%d", s)

			for range 1 + rng.IntN(3) {
				filter := path(true)
				qos := []byte{0, 1, 2}[rng.IntN(3)]

				ours.subscribe(filter, key, qos)
				theirs.Subscribe(key, packets.Subscription{Filter: filter, Qos: qos})
				subscribed[key] = append(subscribed[key], filter)
			}
		}

		// Remove some, so pruning is held to the same answers.
		for s := range 10 {
			key := fmt.Sprintf("s%d", s)
			for _, filter := range subscribed[key] {
				ours.unsubscribe(filter, key)
				theirs.Unsubscribe(filter, key)
			}
		}

		for range 50 {
			topic := path(false)

			want := map[string]byte{}
			for key, sub := range theirs.Subscribers(topic).Subscriptions {
				want[key] = sub.Qos
			}

			got := map[string]byte{}

			ours.match(topic, func(key string, qos byte) { got[key] = qos })

			if !maps.Equal(got, want) {
				t.Fatalf("round %d, topic %q: got %v, mochi %v", round, topic, got, want)
			}
		}
	}
}

func TestIndexPrunesWhatItEmpties(t *testing.T) {
	x := newIndex()

	for i := range 100 {
		x.subscribe(fmt.Sprintf("t/devices/%d/cmd", i), fmt.Sprintf("k%d", i), 1)
		x.subscribe(fmt.Sprintf("t/devices/%d/cfg/#", i), fmt.Sprintf("k%d", i), 1)
	}

	for i := range 100 {
		x.unsubscribe(fmt.Sprintf("t/devices/%d/cmd", i), fmt.Sprintf("k%d", i))
		x.unsubscribe(fmt.Sprintf("t/devices/%d/cfg/#", i), fmt.Sprintf("k%d", i))
	}

	if !x.root.empty() {
		t.Fatalf("index still holds nodes after every subscription went: %+v", x.root)
	}
}

// TestIndexIsSmallerThanMochis measures what one away session costs in
// each index, shaped like a fleet: a topic of its own per device, one of
// them a wildcard. mochi's cost is why this index exists; the bound keeps
// it from creeping back.
func TestIndexIsSmallerThanMochis(t *testing.T) {
	const sessions = 20000

	filters := make([][2]string, sessions)
	keys := make([]string, sessions)

	for i := range sessions {
		keys[i] = fmt.Sprintf("default.dev-%06d", i)
		filters[i] = [2]string{
			fmt.Sprintf("default/devices/%06d/cmd", i),
			fmt.Sprintf("default/devices/%06d/cfg/#", i),
		}
	}

	ours := heapGrowth(func() any {
		x := newIndex()
		for i := range sessions {
			x.subscribe(filters[i][0], keys[i], 1)
			x.subscribe(filters[i][1], keys[i], 1)
		}

		return x
	}) / sessions

	theirs := heapGrowth(func() any {
		x := mqtt.NewTopicsIndex()
		for i := range sessions {
			x.Subscribe(keys[i], packets.Subscription{Filter: filters[i][0], Qos: 1})
			x.Subscribe(keys[i], packets.Subscription{Filter: filters[i][1], Qos: 1})
		}

		return x
	}) / sessions

	t.Logf("bytes per away session: this index %d, mochi's %d", ours, theirs)

	if ours > 600 {
		t.Errorf("this index costs %d bytes per session; it was built to stay under 600", ours)
	}
}

func heapGrowth(build func() any) uint64 {
	var before, after runtime.MemStats

	runtime.GC()
	runtime.ReadMemStats(&before)

	kept := build()

	runtime.GC()
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(kept)

	return after.HeapAlloc - before.HeapAlloc
}
