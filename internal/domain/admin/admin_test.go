package admin_test

import (
	"slices"
	"strconv"
	"testing"

	"github.com/mastmq/mast/internal/domain/admin"
)

func ids(sessions []admin.Session) []string {
	out := make([]string, 0, len(sessions))
	for _, s := range sessions {
		out = append(out, s.ClientID)
	}

	return out
}

func named(names ...string) []admin.Session {
	out := make([]admin.Session, 0, len(names))
	for _, n := range names {
		out = append(out, admin.Session{ClientID: n})
	}

	return out
}

func TestWindowIsSortedStrictlyAfterTheCursorAndOneOver(t *testing.T) {
	t.Parallel()

	got := admin.Window(named("d", "a", "c", "b", "e"), "a", 2)

	if want := []string{"b", "c", "d"}; !slices.Equal(ids(got), want) {
		t.Fatalf("window = %v, want %v", ids(got), want)
	}
}

// TestMergeAcrossNodesPagesWithoutLoss walks every page of clients spread
// over three nodes and the store, the way the API does, and requires each
// client exactly once. A merge that trusted one node's short list, or cut a
// page before seeing every source's window, loses clients at page edges.
func TestMergeAcrossNodesPagesWithoutLoss(t *testing.T) {
	t.Parallel()

	var nodes [3][]admin.Session

	var stored []admin.Session

	want := make([]string, 0, 50)

	for i := range 50 {
		id := "c" + strconv.Itoa(100+i)
		want = append(want, id)

		if i%5 == 0 {
			stored = append(stored, admin.Session{ClientID: id, Persistent: true})
		} else {
			nodes[i%3] = append(nodes[i%3], admin.Session{ClientID: id, Connected: true})
		}

		// Some connected clients are also stored.
		if i%7 == 1 {
			stored = append(stored, admin.Session{ClientID: id, Persistent: true})
		}
	}

	const limit = 7

	var (
		got   []string
		after string
		pages int
	)

	for {
		pages++
		if pages > 50 {
			t.Fatal("paging did not terminate")
		}

		live := make([]admin.Session, 0, len(nodes)*(limit+1))
		for _, n := range nodes {
			live = append(live, admin.Window(n, after, limit)...)
		}

		page, more := admin.Merge(live, admin.Window(stored, after, limit), limit)
		if len(page) > limit {
			t.Fatalf("page of %d, limit %d", len(page), limit)
		}

		got = append(got, ids(page)...)

		if !more {
			break
		}

		after = page[len(page)-1].ClientID
	}

	if !slices.Equal(got, want) {
		t.Fatalf("paged = %v\nwant    %v", got, want)
	}
}

func TestMergePrefersTheConnectionAndKeepsTheRecord(t *testing.T) {
	t.Parallel()

	live := []admin.Session{{ClientID: "rover", Connected: true, Node: "edge-a"}}
	stored := []admin.Session{{ClientID: "rover", Persistent: true}, {ClientID: "sleeper", Persistent: true}}

	page, more := admin.Merge(live, stored, 10)
	if more {
		t.Fatal("two sessions reported as more than a page of ten")
	}

	if len(page) != 2 {
		t.Fatalf("page = %+v, want rover once and sleeper", page)
	}

	rover := page[0]
	if !rover.Connected || rover.Node != "edge-a" || !rover.Persistent {
		t.Fatalf("rover = %+v, want connected on edge-a and persistent", rover)
	}

	if page[1].Connected {
		t.Fatalf("sleeper = %+v, want not connected", page[1])
	}
}

func TestMergeReportsAFullPage(t *testing.T) {
	t.Parallel()

	if _, more := admin.Merge(named("a", "b"), nil, 2); more {
		t.Fatal("exactly a page reported as more")
	}

	if _, more := admin.Merge(named("a", "b", "c"), nil, 2); !more {
		t.Fatal("a page and one over reported as the last page")
	}
}

func TestClampLimit(t *testing.T) {
	t.Parallel()

	for in, want := range map[int]int{0: admin.DefaultLimit, -3: admin.DefaultLimit, 5: 5, 1 << 20: admin.MaxLimit} {
		if got := admin.ClampLimit(in); got != want {
			t.Errorf("ClampLimit(%d) = %d, want %d", in, got, want)
		}
	}
}

func TestSum(t *testing.T) {
	t.Parallel()

	s := admin.Sum("acme", []admin.NodeStats{
		{Node: "a", Connections: 2, Subscriptions: 3},
		{Node: "b", Connections: 1, Subscriptions: 4},
	})

	if s.Connections != 3 || s.Subscriptions != 7 || s.Nodes != 2 || s.Tenant != "acme" {
		t.Fatalf("sum = %+v", s)
	}
}
