package bridge

import (
	"strconv"
	"testing"

	"github.com/mastmq/mast/internal/domain/tenant"
	"github.com/mastmq/mochi/v2/packets"
	"github.com/nats-io/nuid"
)

// seen.first runs once per copy of every message a node receives, from the
// handler goroutine of whichever NATS subscription delivered it, so it is
// exercised in parallel here as it is in production. Each goroutine has its
// own id generator: the package-level nuid.Next holds a global lock, and
// this measures ours, not theirs.
func BenchmarkSeenFirst(b *testing.B) {
	s := newSeen()

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		ids := nuid.New()
		for pb.Next() {
			s.first(ids.Next())
		}
	})
}

// fabricMessage is the publish path's allocation budget: everything a QoS 0
// publish costs beyond encoding the topic and handing it to NATS.
func BenchmarkFabricMessage(b *testing.B) {
	h := newTestHook(b)
	pk := packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish},
		Payload:     []byte("42.1"),
	}

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			h.fabricMessage("t.acme.devices.rover.temp", pk)
		}
	})
}

// unmount runs several times per publish and once per delivery.
func BenchmarkUnmount(b *testing.B) {
	id := tenant.ID("acme")
	mounted := mount(id, "devices/rover-7/telemetry/gps")

	b.ReportAllocs()

	for b.Loop() {
		if unmount(id, mounted) == "" {
			b.Fatal("empty")
		}
	}
}

// identityOf is consulted on every inbound packet, every ACL check and
// every outbound PUBLISH, from every connection's goroutine at once.
func BenchmarkIdentityOf(b *testing.B) {
	h := newTestHook(b)

	const clients = 1000

	cls := make([]string, clients)
	for i := range cls {
		cls[i] = "acme/dev-" + strconv.Itoa(i)
		h.tenants.Store(cls[i], tenant.Identity{Tenant: "acme", Superuser: false})
	}

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if _, ok := h.identityOf(fakeClient(cls[i%clients])); !ok {
				b.Fatal("missing")
			}

			i++
		}
	})
}
