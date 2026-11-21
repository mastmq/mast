package admin_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"testing"

	"github.com/mastmq/mast/internal/domain/admin"
)

// heavy is a connected client holding n filters, the shape that made one
// node's reply outgrow max_payload.
func heavy(id string, n int) admin.Session {
	subs := make([]admin.Subscription, 0, n)
	for i := range n {
		subs = append(subs, admin.Subscription{Filter: fmt.Sprintf("devices/%s/cmd/%02d/#", id, i), QoS: 1})
	}

	return admin.Session{ClientID: id, Connected: true, Node: "edge-a", Subscriptions: subs}
}

func decodeList(t *testing.T, data []byte) admin.ListReply {
	t.Helper()

	var reply admin.ListReply
	if err := json.Unmarshal(data, &reply); err != nil {
		t.Fatalf("decoding %s: %v", data, err)
	}

	return reply
}

// TestPagingSurvivesANodeWhoseWindowOutgrowsAMessage walks every page of a
// tenant whose clients on one node are too heavy for that node's whole
// window to fit one message, the way the API does, and requires each client
// exactly once.
//
// Before the byte budget, that node's reply was refused by NATS and the
// node counted as absent; the page came from the other node alone, and its
// cursor skipped every client the heavy node held up to it.
func TestPagingSurvivesANodeWhoseWindowOutgrowsAMessage(t *testing.T) {
	t.Parallel()

	const (
		limit = 10
		// About four heavy clients to a message.
		budget = 4 * 1100
	)

	var edgeA, edgeB []admin.Session

	want := make([]string, 0, 40)

	for i := range 40 {
		id := "c" + strconv.Itoa(100+i)
		want = append(want, id)

		if i%2 == 0 {
			edgeA = append(edgeA, heavy(id, 24))
		} else {
			//nolint:exhaustruct // a light client needs no more
			edgeB = append(edgeB, admin.Session{ClientID: id, Connected: true, Node: "edge-b"})
		}
	}

	var (
		got       []string
		after     string
		pages     int
		truncated bool
	)

	for {
		pages++
		if pages > 100 {
			t.Fatal("paging did not terminate")
		}

		var (
			live  []admin.Session
			until string
		)

		for name, node := range map[string][]admin.Session{"edge-a": edgeA, "edge-b": edgeB} {
			data, err := admin.EncodeList(name, admin.Window(node, after, limit), budget)
			if err != nil {
				t.Fatalf("encoding %s: %v", name, err)
			}

			if len(data) > budget {
				t.Fatalf("%s replied with %d bytes, budget %d", name, len(data), budget)
			}

			reply := decodeList(t, data)
			live = append(live, reply.Sessions...)

			if reply.Truncated {
				truncated = true

				if last := reply.Sessions[len(reply.Sessions)-1].ClientID; until == "" || last < until {
					until = last
				}
			}
		}

		page, more := admin.Merge(live, nil, limit, until)
		got = append(got, ids(page)...)

		if !more {
			break
		}

		after = page[len(page)-1].ClientID
	}

	if !truncated {
		t.Fatal("no reply was cut, so this test proves nothing; lower the budget")
	}

	if !slices.Equal(got, want) {
		t.Fatalf("paged = %v\nwant    %v", got, want)
	}
}

func TestEncodeListMarksWhereItStopped(t *testing.T) {
	t.Parallel()

	window := []admin.Session{heavy("a", 20), heavy("b", 20), heavy("c", 20)}

	one, err := json.Marshal(window[0])
	if err != nil {
		t.Fatal(err)
	}

	// Room for the first two and the envelope, not the third.
	reply := decodeList(t, mustEncodeList(t, window, 2*len(one)+200))
	if !reply.Truncated || !slices.Equal(ids(reply.Sessions), []string{"a", "b"}) {
		t.Fatalf("reply = truncated %v, %v; want truncated after a and b", reply.Truncated, ids(reply.Sessions))
	}

	if reply.Node != "edge-a" {
		t.Fatalf("node = %q", reply.Node)
	}

	reply = decodeList(t, mustEncodeList(t, window, 1<<20))
	if reply.Truncated || len(reply.Sessions) != 3 {
		t.Fatalf("a window that fits came back truncated %v with %d sessions", reply.Truncated, len(reply.Sessions))
	}

	reply = decodeList(t, mustEncodeList(t, nil, 1<<20))
	if reply.Truncated || reply.Sessions == nil {
		t.Fatalf("an empty window = %+v, want an empty list, not truncated", reply)
	}
}

// TestEncodeListAlwaysSendsTheFirstClient guards progress: a reply cut
// before its first client would bound the page before the cursor, and the
// same request would be cut the same way forever.
func TestEncodeListAlwaysSendsTheFirstClient(t *testing.T) {
	t.Parallel()

	const budget = 600

	data := mustEncodeList(t, []admin.Session{heavy("a", 200), heavy("b", 1)}, budget)
	if len(data) > budget {
		t.Fatalf("reply of %d bytes, budget %d", len(data), budget)
	}

	reply := decodeList(t, data)
	if !reply.Truncated || len(reply.Sessions) != 1 {
		t.Fatalf("reply = %+v, want only a, truncated", reply)
	}

	a := reply.Sessions[0]
	if !a.SubscriptionsTruncated || len(a.Subscriptions) == 0 || len(a.Subscriptions) >= 200 {
		t.Fatalf("a came back with %d of 200 filters, truncated %v; want some, marked",
			len(a.Subscriptions), a.SubscriptionsTruncated)
	}
}

func TestEncodeSessionTrimsFiltersToFit(t *testing.T) {
	t.Parallel()

	s := heavy("rover", 100)

	data, err := admin.EncodeSession(s, 1<<20)
	if err != nil {
		t.Fatal(err)
	}

	var whole admin.Session
	if err := json.Unmarshal(data, &whole); err != nil {
		t.Fatal(err)
	}

	if whole.SubscriptionsTruncated || len(whole.Subscriptions) != 100 {
		t.Fatalf("a session that fits lost filters: %d, truncated %v", len(whole.Subscriptions), whole.SubscriptionsTruncated)
	}

	for _, budget := range []int{len(data) - 1, len(data) / 2, 300} {
		data, err := admin.EncodeSession(s, budget)
		if err != nil {
			t.Fatalf("budget %d: %v", budget, err)
		}

		if len(data) > budget {
			t.Fatalf("budget %d: encoded %d bytes", budget, len(data))
		}

		var cut admin.Session
		if err := json.Unmarshal(data, &cut); err != nil {
			t.Fatal(err)
		}

		if !cut.SubscriptionsTruncated || !slices.Equal(cut.Subscriptions, s.Subscriptions[:len(cut.Subscriptions)]) {
			t.Fatalf("budget %d: want the first filters, marked truncated; got %d, %v",
				budget, len(cut.Subscriptions), cut.SubscriptionsTruncated)
		}
	}

	if _, err := admin.EncodeSession(s, 10); !errors.Is(err, admin.ErrTooSmall) {
		t.Fatalf("a budget smaller than the bare session: err = %v, want ErrTooSmall", err)
	}
}

func TestMergeStopsWhereANodeStoppedShort(t *testing.T) {
	t.Parallel()

	page, more := admin.Merge(named("a", "b", "c", "d"), nil, 10, "b")
	if !more || !slices.Equal(ids(page), []string{"a", "b"}) {
		t.Fatalf("page = %v, more %v; want a and b, and more", ids(page), more)
	}
}

func TestFailureOf(t *testing.T) {
	t.Parallel()

	data, err := json.Marshal(admin.Failure{Node: "edge-a", Error: "nats: maximum payload exceeded"})
	if err != nil {
		t.Fatal(err)
	}

	if f, ok := admin.FailureOf(data); !ok || f.Node != "edge-a" {
		t.Fatalf("a failure was not recognised: %+v %v", f, ok)
	}

	for _, answer := range []any{admin.ListReply{Node: "edge-a"}, admin.NodeStats{Node: "edge-a"}, heavy("a", 2)} {
		data, err := json.Marshal(answer)
		if err != nil {
			t.Fatal(err)
		}

		if _, ok := admin.FailureOf(data); ok {
			t.Fatalf("%s taken for a failure", data)
		}
	}
}

func mustEncodeList(t *testing.T, window []admin.Session, budget int) []byte {
	t.Helper()

	data, err := admin.EncodeList("edge-a", window, budget)
	if err != nil {
		t.Fatalf("encoding: %v", err)
	}

	return data
}
