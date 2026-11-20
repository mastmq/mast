package bridge

import (
	"log/slog"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/mastmq/mast/internal/domain/tenant"
	"github.com/mastmq/mast/internal/infra/limits"
	mqtt "github.com/mastmq/mochi/v2"
	"github.com/mastmq/mochi/v2/packets"
)

// limitedHook is a hook over a bare mochi server whose tenants may keep that
// many messages for an absent session.
func limitedHook(t *testing.T, keep int) *Hook {
	t.Helper()

	h := newTestHook(t)
	h.server = mqtt.New(nil)
	h.limits = limits.New(nil, limits.Options{
		Defaults: tenant.Limits{OfflineQueue: keep},
		Interval: time.Second,
	}, slog.New(slog.DiscardHandler))

	return h
}

// held lists what cl's inflight store holds, as type and packet id.
func held(cl *mqtt.Client) []string {
	all := cl.State.Inflight.GetAll(false)
	out := make([]string, 0, len(all))

	for _, pk := range all {
		out = append(out, packets.PacketNames[pk.FixedHeader.Type]+":"+strconv.Itoa(int(pk.PacketID)))
	}

	slices.Sort(out)

	return out
}

// TestTrimKeepsQoS2State is the QoS 2 loss a review found: the oldest
// entries in an absent client's inflight store were dropped whatever they
// were, and the oldest are usually a PUBREL for a message the client had
// PUBREC'd and a PUBREC for one it had sent, both from before it went.
// Dropping the PUBREL leaves the client holding the id as awaiting release,
// and the next new message under that id is taken for a duplicate.
func TestTrimKeepsQoS2State(t *testing.T) {
	h := limitedHook(t, 3)
	cl := h.server.NewClient(nil, "tcp", "acme.sleeper", false)

	// A PUBREL for a message the client PUBREC'd, and a PUBREC for one it
	// sent, both from before it went.
	cl.State.Inflight.Set(packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Pubrel, Qos: 1}, PacketID: 1, Created: 100,
	})
	cl.State.Inflight.Set(packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Pubrec}, PacketID: 2, Created: 100,
	})

	for id := uint16(3); id <= 8; id++ {
		cl.State.Inflight.Set(packets.Packet{
			FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
			PacketID:    id,
			Created:     200 + int64(id),
		})
	}

	h.limitInflight(cl, "acme")

	want := []string{"Publish:6", "Publish:7", "Publish:8", "Pubrec:2", "Pubrel:1"}
	if got := held(cl); !slices.Equal(got, want) {
		t.Errorf("after the trim the store holds %v, want %v", got, want)
	}
}

// TestTrimLetsTheStoreRunOver holds the trim to its amortised schedule: an
// eighth over the limit is left alone, one more is cut back to the limit.
func TestTrimLetsTheStoreRunOver(t *testing.T) {
	const keep = 16

	h := limitedHook(t, keep)
	cl := h.server.NewClient(nil, "tcp", "acme.sleeper", false)

	add := func(id uint16) {
		cl.State.Inflight.Set(packets.Packet{
			FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
			PacketID:    id,
			Created:     int64(id),
		})
		h.limitInflight(cl, "acme")
	}

	for id := uint16(1); id <= keep+keep/offlineSlack; id++ {
		add(id)
	}

	if n := cl.State.Inflight.Len(); n != keep+keep/offlineSlack {
		t.Fatalf("an eighth over the limit was cut to %d", n)
	}

	add(keep + keep/offlineSlack + 1)

	if n := cl.State.Inflight.Len(); n != keep {
		t.Fatalf("past an eighth over, the store holds %d, want %d", n, keep)
	}

	if _, ok := cl.State.Inflight.Get(keep + keep/offlineSlack + 1); !ok {
		t.Error("the newest message was cut")
	}
}
