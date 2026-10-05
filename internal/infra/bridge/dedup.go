package bridge

import (
	"sync"
	"time"
)

// dedupWindow is how long a message id is remembered.
//
// Every copy of one message that a node receives comes off the same NATS
// connection, back to back, so the copies normally arrive microseconds apart.
// The window only has to outlast the case where one subscription's handler is
// behind another's, and it is two generations long, so an id is remembered
// for between one and two windows.
const dedupWindow = 5 * time.Second

// seenShards is how many independent windows the ids are spread over.
//
// Every NATS subscription on the node delivers from its own goroutine, and
// each delivery takes this lock once. One lock made a node's receive path
// serial at exactly the point it fans out: with eight handlers running, an
// insert cost four times what the map work did. Sixteen shards leave the
// handlers to collide about as often as they would on the map anyway.
const seenShards = 16

// seen remembers which messages this node has already handed to mochi.
//
// It exists because a node holds one NATS subscription per distinct filter,
// not one per message: a client on "a/#" and another on "a/b/c" put two
// subscriptions on the node, a publish to "a/b/c" matches both, and NATS
// delivers it twice. Each copy used to go through mochi's whole local fan-out,
// so both clients received the message twice — once per overlapping filter
// anywhere on the node, which is unbounded.
//
// Two maps swapped on a timer rather than an LRU, because the hot path should
// cost a map lookup and not a list splice, and because what bounds memory
// here is time, not count.
type seen struct {
	shards [seenShards]seenShard
	now    func() time.Time
}

// seenShard is one slice of the id space with its own lock and generations.
type seenShard struct {
	mu      sync.Mutex
	current map[string]struct{}
	prev    map[string]struct{}
	rotated time.Time
}

func newSeen() *seen {
	return newSeenAt(time.Now)
}

// newSeenAt builds a window that reads the clock through now, so a test can
// move time instead of waiting for it.
func newSeenAt(now func() time.Time) *seen {
	s := &seen{shards: [seenShards]seenShard{}, now: now}

	start := now()
	for i := range s.shards {
		s.shards[i] = seenShard{
			mu:      sync.Mutex{},
			current: make(map[string]struct{}),
			prev:    make(map[string]struct{}),
			rotated: start,
		}
	}

	return s
}

// first records id and reports whether this is the first time it was seen.
// An empty id is always first: a message that reached the subject without
// going through a bridge carries none, and there is nothing to compare it to.
func (s *seen) first(id string) bool {
	if id == "" {
		return true
	}

	sh := &s.shards[shardOf(id)]

	sh.mu.Lock()
	defer sh.mu.Unlock()

	if now := s.now(); now.Sub(sh.rotated) >= dedupWindow {
		// The generation being dropped is cleared and reused rather than
		// replaced, so a rotation costs no allocation and the map keeps
		// the size the traffic has taught it.
		sh.prev, sh.current = sh.current, sh.prev
		clear(sh.current)
		sh.rotated = now
	}

	if _, ok := sh.current[id]; ok {
		return false
	}

	if _, ok := sh.prev[id]; ok {
		return false
	}

	sh.current[id] = struct{}{}

	return true
}

// shardOf picks a shard from the tail of an id.
//
// A NUID is a random prefix followed by a counter, so consecutive ids from
// one publisher differ only in their last bytes; hashing just those spreads a
// burst from one client across the shards rather than onto one of them.
func shardOf(id string) int {
	const (
		fnvOffset = 2166136261
		fnvPrime  = 16777619
		tail      = 4
	)

	h := uint32(fnvOffset)
	for i := max(0, len(id)-tail); i < len(id); i++ {
		h = (h ^ uint32(id[i])) * fnvPrime
	}

	return int(h % seenShards)
}
