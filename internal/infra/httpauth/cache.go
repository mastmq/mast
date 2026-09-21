package httpauth

import (
	"sync"
	"time"
)

// cache is a bounded, TTL-expiring set of decisions.
//
// It exists because authorization is asked on every publish. Without a cache
// the policy server becomes the throughput ceiling of the broker and every
// message pays a network round trip; with one, a steady fleet asks about each
// distinct (client, topic, action) once per TTL.
//
// Eviction is deliberately crude. When the cache is full it drops expired
// entries, and if that is not enough it drops an arbitrary handful — Go's map
// iteration order makes that effectively random. Random eviction from a large
// cache costs a few extra round trips; an LRU's bookkeeping costs a lock on
// every read, on the hottest path in the process.
type cache struct {
	ttl time.Duration
	max int

	mu      sync.Mutex
	entries map[string]cacheEntry
}

type cacheEntry struct {
	allow   bool
	expires time.Time
}

// evictBatch is how many arbitrary entries to drop when a full cache has
// nothing expired to reclaim.
const evictBatch = 64

func newCache(ttl time.Duration, capacity int) *cache {
	return &cache{
		ttl:     ttl,
		max:     capacity,
		mu:      sync.Mutex{},
		entries: make(map[string]cacheEntry),
	}
}

// enabled reports whether caching is on. A zero TTL or size disables it,
// which is the right setting for a policy server whose answers must take
// effect immediately.
func (c *cache) enabled() bool { return c != nil && c.ttl > 0 && c.max > 0 }

// get returns a cached decision that has not expired.
func (c *cache) get(key string) (bool, bool) {
	if !c.enabled() {
		return false, false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	e, ok := c.entries[key]
	if !ok || time.Now().After(e.expires) {
		return false, false
	}

	return e.allow, true
}

// put records a decision.
func (c *cache) put(key string, allow bool) {
	if !c.enabled() {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.entries) >= c.max {
		c.evictLocked()
	}

	c.entries[key] = cacheEntry{allow: allow, expires: time.Now().Add(c.ttl)}
}

func (c *cache) evictLocked() {
	now := time.Now()

	for k, e := range c.entries {
		if now.After(e.expires) {
			delete(c.entries, k)
		}
	}

	if len(c.entries) < c.max {
		return
	}

	dropped := 0

	for k := range c.entries {
		delete(c.entries, k)

		dropped++
		if dropped >= evictBatch {
			return
		}
	}
}

// size reports the number of entries held, for metrics and tests.
func (c *cache) size() int {
	if !c.enabled() {
		return 0
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.entries)
}
