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
	mu      sync.Mutex
	current map[string]struct{}
	prev    map[string]struct{}
	rotated time.Time
	now     func() time.Time
}

func newSeen() *seen {
	return &seen{
		mu:      sync.Mutex{},
		current: make(map[string]struct{}),
		prev:    make(map[string]struct{}),
		rotated: time.Now(),
		now:     time.Now,
	}
}

// first records id and reports whether this is the first time it was seen.
// An empty id is always first: a message that reached the subject without
// going through a bridge carries none, and there is nothing to compare it to.
func (s *seen) first(id string) bool {
	if id == "" {
		return true
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if now := s.now(); now.Sub(s.rotated) >= dedupWindow {
		s.prev, s.current = s.current, make(map[string]struct{}, len(s.current))
		s.rotated = now
	}

	if _, ok := s.current[id]; ok {
		return false
	}

	if _, ok := s.prev[id]; ok {
		return false
	}

	s.current[id] = struct{}{}

	return true
}
