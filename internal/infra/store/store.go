// Package store holds the durable state a mast cluster shares: retained
// messages, session state, and the queues that wait for offline clients.
//
// Everything here lives in JetStream key-value buckets, and that is the
// single most important design decision in mast. The obvious alternative —
// a JetStream consumer per subscription, which is what nats-server's own
// MQTT listener does — does not survive contact with a real fleet: consumers
// are Raft state machines, a server holds on the order of 2k of them, and
// 50k devices with two subscriptions each would want 100k. Keys cost
// essentially nothing by comparison, so every durable thing mast keeps is
// shaped as a key rather than as a consensus group.
//
// Keys are the encoded subject from the topic codec, which is why that codec
// escapes down to a character set a KV key accepts: a subscription filter is
// then also a KV watch pattern, and retained lookup is a wildcard scan rather
// than a second index.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Bucket names. They are shared by every node in a cluster.
const (
	bucketRetained = "mast_retained"
	bucketSessions = "mast_sessions"
	bucketOffline  = "mast_offline"
)

// maxOfflineMessages bounds how much is kept for one absent client.
//
// The bound is the point: without it a single disconnected device subscribed
// to a chatty topic would grow until the bucket did. MQTT lets a broker
// discard, and discarding the oldest is the least surprising choice.
const maxOfflineMessages = 100

// casRetries bounds optimistic-concurrency retries when two nodes append to
// the same client's queue at once.
const casRetries = 5

// ErrNotFound is returned when a key does not exist.
var ErrNotFound = errors.New("store: not found")

// Message is a retained or queued MQTT message.
type Message struct {
	Topic   string `json:"topic"`
	Payload []byte `json:"payload"`
	QoS     byte   `json:"qos"`
	Retain  bool   `json:"retain,omitempty"`

	// Properties are the MQTT 5 properties the publisher set, nil for a
	// message that had none — which is every MQTT 3 message, so the entries
	// written before this field existed read back unchanged.
	Properties *Properties `json:"props,omitempty"`

	// StoredAt is when the message was written, in Unix seconds. A message
	// expiry interval counts from publication, so without it a retained or
	// queued message could never be known to have expired.
	StoredAt int64 `json:"at,omitempty"`
}

// Properties are the MQTT 5 PUBLISH properties that belong to the message
// rather than to one hop of it.
//
// Topic alias and subscription identifier are left out on purpose: both are
// negotiated per connection, and carrying one to a different client would
// be wrong rather than merely redundant. The same shape travels in the
// Mast-Props header on the fabric, so a message on the wire and a message at
// rest are described by one type.
type Properties struct {
	// PayloadFormat is a pointer because 0 ("unspecified bytes") is a value
	// a publisher can send, distinct from not sending it.
	PayloadFormat   *byte          `json:"pf,omitempty"`
	MessageExpiry   uint32         `json:"exp,omitempty"`
	ContentType     string         `json:"ct,omitempty"`
	ResponseTopic   string         `json:"rt,omitempty"`
	CorrelationData []byte         `json:"cd,omitempty"`
	User            []UserProperty `json:"up,omitempty"`
}

// UserProperty is one MQTT 5 user property. They are a list, not a map:
// a key may repeat and the order is part of the message.
type UserProperty struct {
	Key   string `json:"k"`
	Value string `json:"v"`
}

// Expired reports whether a stored message's expiry interval has passed.
// A message with no interval, or stored before StoredAt existed, never
// expires.
func (m Message) Expired(now time.Time) bool {
	if m.Properties == nil || m.Properties.MessageExpiry == 0 || m.StoredAt == 0 {
		return false
	}

	return now.Unix() >= m.StoredAt+int64(m.Properties.MessageExpiry)
}

// Subscription is one filter a session holds.
type Subscription struct {
	Filter string `json:"filter"`
	QoS    byte   `json:"qos"`
}

// Session is what a client with clean_start=false expects to find waiting.
type Session struct {
	ClientID      string         `json:"client_id"`
	Tenant        string         `json:"tenant"`
	Subscriptions []Subscription `json:"subscriptions"`
	UpdatedAt     time.Time      `json:"updated_at"`
}

// Store is the durable state of a mast cluster.
type Store struct {
	retained jetstream.KeyValue
	sessions jetstream.KeyValue
	offline  jetstream.KeyValue
}

// Open creates the buckets if they are absent and returns a handle.
//
// replicas is the replication factor for each bucket; it must not exceed the
// number of JetStream peers or bucket creation fails.
func Open(ctx context.Context, nc *nats.Conn, domain string, replicas int, ttl time.Duration) (*Store, error) {
	// Addressed by domain rather than through the default $JS.API prefix.
	// An edge node runs its own JetStream-less server, which answers that
	// prefix itself with "jetstream not enabled" instead of forwarding it,
	// so an edge could never open a bucket at all. $JS.<domain>.API is an
	// ordinary subject and crosses the leaf connection to the core.
	js, err := jetstream.NewWithDomain(nc, domain)
	if err != nil {
		return nil, fmt.Errorf("store: jetstream: %w", err)
	}

	s := new(Store)

	for _, spec := range []struct {
		name string
		ttl  time.Duration
		into *jetstream.KeyValue
	}{
		// Retained messages have no expiry: that is what retention means.
		{bucketRetained, 0, &s.retained},
		// Sessions and queues expire, so an abandoned device does not hold
		// storage forever.
		{bucketSessions, ttl, &s.sessions},
		{bucketOffline, ttl, &s.offline},
	} {
		kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
			Bucket:   spec.name,
			TTL:      spec.ttl,
			Replicas: replicas,
			History:  1,
		})
		if err != nil {
			return nil, fmt.Errorf("store: bucket %s: %w", spec.name, err)
		}

		*spec.into = kv
	}

	return s, nil
}

// PutRetained stores the last known value for a topic. An empty payload
// clears it, which is what MQTT says a retained publish with no payload
// means.
func (s *Store) PutRetained(ctx context.Context, key string, msg Message) error {
	if len(msg.Payload) == 0 {
		return s.DeleteRetained(ctx, key)
	}

	msg.StoredAt = time.Now().Unix()

	encoded, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("store: encoding retained message: %w", err)
	}

	if _, err := s.retained.Put(ctx, key, encoded); err != nil {
		return fmt.Errorf("store: putting retained %s: %w", key, err)
	}

	return nil
}

// DeleteRetained clears the last known value for a topic.
func (s *Store) DeleteRetained(ctx context.Context, key string) error {
	if err := s.retained.Delete(ctx, key); err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return fmt.Errorf("store: deleting retained %s: %w", key, err)
	}

	return nil
}

// MatchRetained returns every retained message whose key matches one of the
// given subject filters.
//
// The filters are subject patterns from the topic codec, so this is a KV
// wildcard scan rather than a lookup against a second index — the reason the
// codec produces KV-safe keys in the first place.
func (s *Store) MatchRetained(ctx context.Context, filters []string) ([]Message, error) {
	if len(filters) == 0 {
		return nil, nil
	}

	keys, err := s.retained.ListKeysFiltered(ctx, filters...)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return nil, nil
		}

		return nil, fmt.Errorf("store: listing retained: %w", err)
	}

	var out []Message

	now := time.Now()

	for key := range keys.Keys() {
		entry, err := s.retained.Get(ctx, key)
		if err != nil {
			if errors.Is(err, jetstream.ErrKeyNotFound) {
				continue // deleted between listing and reading
			}

			return nil, fmt.Errorf("store: reading retained %s: %w", key, err)
		}

		var msg Message
		if err := json.Unmarshal(entry.Value(), &msg); err != nil {
			continue // a value we cannot read is not worth failing a subscribe over
		}

		// The bucket has no per-key TTL, so an expired message is found
		// only when something asks for it. Deleting it here is what stops
		// it being found again; a failed delete costs nothing but a retry
		// on the next subscribe.
		if msg.Expired(now) {
			_ = s.retained.Delete(ctx, key)

			continue
		}

		out = append(out, msg)
	}

	return out, nil
}

// PutSession records a session's state.
func (s *Store) PutSession(ctx context.Context, key string, sess Session) error {
	sess.UpdatedAt = time.Now().UTC()

	encoded, err := json.Marshal(sess)
	if err != nil {
		return fmt.Errorf("store: encoding session: %w", err)
	}

	if _, err := s.sessions.Put(ctx, key, encoded); err != nil {
		return fmt.Errorf("store: putting session %s: %w", key, err)
	}

	return nil
}

// GetSession returns a stored session, or [ErrNotFound].
func (s *Store) GetSession(ctx context.Context, key string) (Session, error) {
	entry, err := s.sessions.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return Session{}, ErrNotFound
		}

		return Session{}, fmt.Errorf("store: reading session %s: %w", key, err)
	}

	var sess Session
	if err := json.Unmarshal(entry.Value(), &sess); err != nil {
		return Session{}, fmt.Errorf("store: decoding session %s: %w", key, err)
	}

	return sess, nil
}

// DeleteSession forgets a session and its queue, which is what
// clean_start=true means.
//
// It reads before it deletes. A KV delete is not free when there is nothing
// to delete: it writes a tombstone, and every write is a replicated one on
// the core. Almost every client that asks for this never had a session, so
// the read is what normally happens — and a read is served by any replica
// without consensus. Deleting blind cost a clean client two replicated
// writes per CONNECT and two more per DISCONNECT, and a storm of 20k clean
// connections queued them past their deadline while each CONNECT waited.
func (s *Store) DeleteSession(ctx context.Context, key string) error {
	for _, target := range []struct {
		kv   jetstream.KeyValue
		what string
	}{
		{s.sessions, "session"},
		{s.offline, "queue"},
	} {
		if _, err := target.kv.Get(ctx, key); err != nil {
			if errors.Is(err, jetstream.ErrKeyNotFound) {
				continue
			}

			return fmt.Errorf("store: reading %s %s: %w", target.what, key, err)
		}

		if err := target.kv.Delete(ctx, key); err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
			return fmt.Errorf("store: deleting %s %s: %w", target.what, key, err)
		}
	}

	return nil
}

// Enqueue appends a message to an absent client's queue, dropping the oldest
// once the queue is full.
//
// The whole queue is one KV entry updated under its revision, so two nodes
// appending at once cannot lose a message; one of them retries. That trades
// contention for atomicity, which is the right way round while a client is
// offline and its traffic is by definition not hot.
func (s *Store) Enqueue(ctx context.Context, key string, msg Message) error {
	msg.StoredAt = time.Now().Unix()

	for attempt := range casRetries {
		queue, revision, err := s.readQueue(ctx, key)
		if err != nil {
			return err
		}

		queue = append(queue, msg)
		if len(queue) > maxOfflineMessages {
			queue = queue[len(queue)-maxOfflineMessages:]
		}

		encoded, err := json.Marshal(queue)
		if err != nil {
			return fmt.Errorf("store: encoding queue: %w", err)
		}

		if revision == 0 {
			_, err = s.offline.Create(ctx, key, encoded)
		} else {
			_, err = s.offline.Update(ctx, key, encoded, revision)
		}

		if err == nil {
			return nil
		}

		if attempt == casRetries-1 {
			return fmt.Errorf("store: appending to queue %s: %w", key, err)
		}
	}

	return nil
}

// Drain returns and clears everything waiting for a client.
func (s *Store) Drain(ctx context.Context, key string) ([]Message, error) {
	queue, revision, err := s.readQueue(ctx, key)
	if err != nil {
		return nil, err
	}

	if revision == 0 || len(queue) == 0 {
		return nil, nil
	}

	if err := s.offline.Delete(ctx, key); err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, fmt.Errorf("store: clearing queue %s: %w", key, err)
	}

	now := time.Now()

	return slices.DeleteFunc(queue, func(m Message) bool { return m.Expired(now) }), nil
}

// readQueue returns a client's queue and the revision it was read at. A
// revision of zero means the key does not exist yet.
func (s *Store) readQueue(ctx context.Context, key string) ([]Message, uint64, error) {
	entry, err := s.offline.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, 0, nil
		}

		return nil, 0, fmt.Errorf("store: reading queue %s: %w", key, err)
	}

	var queue []Message
	if err := json.Unmarshal(entry.Value(), &queue); err != nil {
		// A corrupt queue is not worth refusing a connection over. Return the
		// revision so the next write overwrites it, and start the client with
		// an empty queue rather than locking it out of the broker.
		//nolint:nilerr // deliberate: recover by discarding, not by failing
		return nil, entry.Revision(), nil
	}

	return queue, entry.Revision(), nil
}
