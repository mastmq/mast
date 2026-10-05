package bridge

import (
	"sync"

	"github.com/nats-io/nuid"
)

// newIDs returns the pool a hook draws message ids from.
//
// nuid.Next, the package-level generator, serialises every caller on one
// mutex, and every publish on a node goes through it. Under eight
// concurrent publishers an id cost 125ns there against 11ns from a
// generator of one's own, so each goroutine borrows one from a pool
// instead. Uniqueness holds across generators because each is seeded with
// its own 12-byte random prefix, which it draws again whenever its counter
// wraps: two generators would have to share a prefix for two ids to
// collide, and that is a 2^-71 event per pair.
func newIDs() sync.Pool {
	return sync.Pool{New: func() any { return nuid.New() }}
}

// newMessageID returns a fresh Mast-Id.
func (h *Hook) newMessageID() string {
	gen, ok := h.ids.Get().(*nuid.NUID)
	if !ok {
		return nuid.Next() // the pool only ever holds generators; this is belt and braces
	}

	id := gen.Next()
	h.ids.Put(gen)

	return id
}
