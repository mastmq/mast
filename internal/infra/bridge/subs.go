package bridge

import (
	"fmt"
	"sync"

	"github.com/nats-io/nats.go"
)

// subKey identifies one NATS subscription. The queue is part of the identity
// because a plain subscription and a queue subscription to the same subject
// are different things: the first delivers to everyone, the second to one
// member of the group.
type subKey struct {
	subject string
	queue   string
}

// entry is a live NATS subscription and the number of MQTT clients that need
// it.
type entry struct {
	sub *nats.Subscription
	n   int
}

// registry keeps exactly one NATS subscription per distinct subject, however
// many local MQTT clients want it.
//
// This is the difference between O(distinct topics) and O(devices)
// subscriptions on a node. With 25k devices per process the second number is
// the one that hurts, and most fleets have far fewer distinct filter shapes
// than devices.
type registry struct {
	nc      *nats.Conn
	handler nats.MsgHandler

	mu       sync.Mutex
	entries  map[subKey]*entry
	byClient map[string]map[subKey]struct{}
}

func newRegistry(nc *nats.Conn, handler nats.MsgHandler) *registry {
	return &registry{
		mu:       sync.Mutex{},
		nc:       nc,
		handler:  handler,
		entries:  make(map[subKey]*entry),
		byClient: make(map[string]map[subKey]struct{}),
	}
}

// acquire ensures a NATS subscription exists for every key and records that
// clientID depends on it. Acquiring a key a client already holds is a no-op,
// which is what MQTT resubscription semantics require.
func (r *registry) acquire(clientID string, keys []subKey) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	held, ok := r.byClient[clientID]
	if !ok {
		held = make(map[subKey]struct{})
		r.byClient[clientID] = held
	}

	for _, key := range keys {
		if _, dup := held[key]; dup {
			continue
		}

		if err := r.acquireLocked(key); err != nil {
			return err
		}

		held[key] = struct{}{}
	}

	return nil
}

func (r *registry) acquireLocked(key subKey) error {
	if e, ok := r.entries[key]; ok {
		e.n++

		return nil
	}

	var (
		sub *nats.Subscription
		err error
	)

	if key.queue == "" {
		sub, err = r.nc.Subscribe(key.subject, r.handler)
	} else {
		sub, err = r.nc.QueueSubscribe(key.subject, key.queue, r.handler)
	}

	if err != nil {
		return fmt.Errorf("bridge: subscribing to %q: %w", key.subject, err)
	}

	r.entries[key] = &entry{sub: sub, n: 1}

	return nil
}

// release drops one client's claim on the given keys, unsubscribing from NATS
// when the last claim goes.
func (r *registry) release(clientID string, keys []subKey) {
	r.mu.Lock()
	defer r.mu.Unlock()

	held, ok := r.byClient[clientID]
	if !ok {
		return
	}

	for _, key := range keys {
		if _, hasKey := held[key]; !hasKey {
			continue
		}

		delete(held, key)
		r.releaseLocked(key)
	}

	if len(held) == 0 {
		delete(r.byClient, clientID)
	}
}

// releaseAll drops every claim a client holds, which is what a disconnect
// means for a clean session.
func (r *registry) releaseAll(clientID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for key := range r.byClient[clientID] {
		r.releaseLocked(key)
	}

	delete(r.byClient, clientID)
}

func (r *registry) releaseLocked(key subKey) {
	e, ok := r.entries[key]
	if !ok {
		return
	}

	e.n--
	if e.n > 0 {
		return
	}

	// The subscription is going away regardless of what unsubscribing reports.
	_ = e.sub.Unsubscribe()

	delete(r.entries, key)
}

// count reports the number of live NATS subscriptions, for metrics and tests.
func (r *registry) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.entries)
}
