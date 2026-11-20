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
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Bucket names. They are shared by every node in a cluster.
const (
	bucketRetained = "mast_retained"
	bucketSessions = "mast_sessions"
)

// casRetries bounds optimistic-concurrency retries when two nodes write the
// same session record at once.
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

	// Present is whether a connection holds the session right now. The
	// queuer reads it to know whom to keep messages for, which is why it is
	// written on every reconnect and not only when subscriptions change:
	// otherwise a device that came back would go on being queued for,
	// and receive everything twice on its next return. A record written
	// before the field existed reads as absent, which is the safe side.
	Present bool `json:"present,omitempty"`
	// Owner is the connection that last wrote the record, as the owner
	// token of its session claim. A node noticing a disconnect late must not
	// mark away a session another node's connection already holds.
	Owner string `json:"owner,omitempty"`
	// Node is the node whose connection holds the session, by its embedded
	// server's id, written with Present. The cores watch every node's
	// heartbeat, and a node that stops beating without having recorded its
	// sessions as away — killed outright — has its sessions queued for as
	// if it had. A record without it, from a node older than the field, is
	// never judged that way.
	Node string `json:"node,omitempty"`
}

// Store is the durable state of a mast cluster.
type Store struct {
	retained jetstream.KeyValue
	sessions jetstream.KeyValue

	// offline is the stream absent clients' queues live on, one subject per
	// session. legacyOffline is the bucket they lived in before, read and
	// emptied on the way out so an upgrade does not drop what was waiting;
	// nil once no such bucket exists.
	offline       jetstream.Stream
	legacyOffline jetstream.KeyValue

	// js is kept for the durable fabric stream, which is a stream rather
	// than a bucket and is opened separately by [Store.OpenDurable].
	js jetstream.JetStream

	// direct reads many messages with one request, the only kind of bulk
	// read that creates no consumer. apiPrefix is where it sends them.
	direct    *directReader
	apiPrefix string
}

// ErrNotProvisioned is returned by [Bind] while the streams it binds to do
// not exist yet: the core tier has not created them.
var ErrNotProvisioned = errors.New("store: not provisioned by the core yet")

// newStore is a handle with nothing opened yet.
func newStore(nc *nats.Conn, domain string) (*Store, error) {
	// Addressed by domain rather than through the default $JS.API prefix.
	// An edge node runs its own JetStream-less server, which answers that
	// prefix itself with "jetstream not enabled" instead of forwarding it,
	// so an edge could never open a bucket at all. $JS.<domain>.API is an
	// ordinary subject and crosses the leaf connection to the core.
	js, err := jetstream.NewWithDomain(nc, domain,
		jetstream.WithPublishAsyncTimeout(publishAckWait),
		jetstream.WithPublishAsyncMaxPending(publishAsyncMaxPending))
	if err != nil {
		return nil, fmt.Errorf("store: jetstream: %w", err)
	}

	s := new(Store)
	s.js = js
	s.apiPrefix = "$JS." + domain + ".API."
	s.direct = newDirectReader(nc)

	return s, nil
}

// Bind opens the buckets and streams the core tier created, and changes
// nothing about them. It is what an edge does.
//
// An edge used to create them, and so to configure them — with its own
// core.replicas, which nothing sets on an edge and which defaults to 1. A
// cluster whose cores asked for three replicas therefore ran every bucket
// with one, and any edge starting up put them back to one if anything had
// changed that. Only a node that carries JetStream knows how many peers
// there are; an edge waits for one to have done it.
func Bind(ctx context.Context, nc *nats.Conn, domain string) (*Store, error) {
	s, err := newStore(nc, domain)
	if err != nil {
		return nil, err
	}

	for name, into := range map[string]*jetstream.KeyValue{
		bucketRetained: &s.retained,
		bucketSessions: &s.sessions,
	} {
		kv, err := s.js.KeyValue(ctx, name)
		if errors.Is(err, jetstream.ErrBucketNotFound) {
			return nil, fmt.Errorf("%w: bucket %s", ErrNotProvisioned, name)
		}

		if err != nil {
			return nil, fmt.Errorf("store: bucket %s: %w", name, err)
		}

		*into = kv
	}

	stream, err := s.js.Stream(ctx, streamOffline)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		return nil, fmt.Errorf("%w: stream %s", ErrNotProvisioned, streamOffline)
	}

	if err != nil {
		return nil, fmt.Errorf("store: stream %s: %w", streamOffline, err)
	}

	s.offline = stream

	if err := s.bindLegacy(ctx); err != nil {
		return nil, err
	}

	return s, nil
}

// BindDurable checks that the durable stream exists, without changing it.
func (s *Store) BindDurable(ctx context.Context) error {
	if _, err := s.js.Stream(ctx, streamDurable); err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return fmt.Errorf("%w: stream %s", ErrNotProvisioned, streamDurable)
		}

		return fmt.Errorf("store: stream %s: %w", streamDurable, err)
	}

	return nil
}

// Reconcile puts back the replica count of anything another node changed.
// It returns the names it corrected.
//
// Edges no longer configure streams, but an edge of an older version still
// does when it starts, and a rolling upgrade has some of those. A core that
// checks now and then is what makes the cluster end up where it was asked
// to be rather than where the last old edge left it.
func (s *Store) Reconcile(ctx context.Context, replicas int) ([]string, error) {
	var fixed []string

	for _, name := range []string{
		kvStreamPrefix + bucketRetained, kvStreamPrefix + bucketSessions, streamOffline, streamDurable,
	} {
		stream, err := s.js.Stream(ctx, name)
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			continue // the durable stream, when durable QoS is off
		}

		if err != nil {
			return fixed, fmt.Errorf("store: stream %s: %w", name, err)
		}

		cfg := stream.CachedInfo().Config
		if cfg.Replicas == replicas {
			continue
		}

		cfg.Replicas = replicas
		if _, err := s.js.UpdateStream(ctx, cfg); err != nil {
			return fixed, fmt.Errorf("store: restoring %d replicas on %s: %w", replicas, name, err)
		}

		fixed = append(fixed, name)
	}

	return fixed, nil
}

// Open creates the buckets and streams if they are absent, sets their
// configuration if they are not, and returns a handle. Only a node carrying
// JetStream calls it; an edge calls [Bind].
//
// replicas is the replication factor for each bucket; it must not exceed the
// number of JetStream peers or bucket creation fails.
func Open(ctx context.Context, nc *nats.Conn, domain string, replicas int, ttl time.Duration) (*Store, error) {
	s, err := newStore(nc, domain)
	if err != nil {
		return nil, err
	}

	for _, spec := range []struct {
		name string
		ttl  time.Duration
		into *jetstream.KeyValue
	}{
		// Retained messages have no expiry: that is what retention means.
		{bucketRetained, 0, &s.retained},
		// Sessions expire, so an abandoned device does not hold storage
		// forever.
		{bucketSessions, ttl, &s.sessions},
	} {
		kv, err := s.js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
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

	if err := s.openOffline(ctx, replicas, ttl); err != nil {
		return nil, err
	}

	return s, nil
}

// Close releases what the store holds on the connection.
func (s *Store) Close() { s.direct.close() }

// Durable fabric names. The stream sits outside the "t." namespace for the
// same reason the session control plane does: every filter a client can
// subscribe to encodes to "t.<tenant>...", so no client can read it or
// write to it.
const (
	streamDurable = "mast_qos"

	// DurablePrefix replaces the leading "t." of a fabric subject on the
	// stream, so a stored message still names its tenant and topic.
	DurablePrefix = "q."
)

// durableDedupWindow is how long the stream remembers a message id. It only
// has to cover a publisher's retry of the same store call.
const durableDedupWindow = 2 * time.Minute

// OpenDurable creates the stream QoS 1 and 2 messages cross the fabric on,
// if it is absent.
//
// This is the answer to cross-node QoS being at-most-once: the ingress node
// acknowledges a QoS 1 or 2 publish only once it is stored here, and every
// node reads the stream through one consumer of its own. It is one stream
// and one consumer per node — never one per subscription or session, which
// is the design mast exists to avoid.
//
// maxAge bounds how long a node may be cut off and still catch up. Nothing
// acknowledges messages on this stream, so age is what removes them.
func (s *Store) OpenDurable(ctx context.Context, replicas int, maxAge time.Duration) error {
	_, err := s.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:       streamDurable,
		Subjects:   []string{DurablePrefix + ">"},
		Retention:  jetstream.LimitsPolicy,
		Discard:    jetstream.DiscardOld,
		Storage:    jetstream.FileStorage,
		Replicas:   replicas,
		MaxAge:     maxAge,
		Duplicates: min(durableDedupWindow, maxAge),
		// Read back by time when a node dies without saying so: the queuer
		// replays what was published since the node was last heard from,
		// with batched direct gets any replica answers, rather than a
		// consumer.
		AllowDirect: true,
	})
	if err != nil {
		return fmt.Errorf("store: stream %s: %w", streamDurable, err)
	}

	return nil
}

// ReplayDurable calls fn with every message the durable stream stored at or
// after since, oldest first, in the same form [DurableHandler] receives a
// message from the consumer: the subject as stored, the headers with
// [nats.JSTimeStamp] set to when it was stored, and the payload.
//
// It reads with batched direct gets, not a consumer. It runs when a node
// is declared dead, to queue what was published between its last
// heartbeat and the moment the cores noticed, which no live reader can go
// back for.
func (s *Store) ReplayDurable(ctx context.Context, since time.Time, fn DurableHandler) (int, error) {
	read := 0

	err := s.direct.eachSince(ctx, s.apiPrefix, streamDurable, DurablePrefix+">", since,
		func(m *nats.Msg, _ uint64) bool {
			read++

			fn(m.Header.Get(nats.JSSubject), m.Header, m.Data)

			return true
		})
	if err != nil {
		return read, fmt.Errorf("store: replaying %s: %w", streamDurable, err)
	}

	return read, nil
}

// Reachable reports whether the sessions bucket's stream has a leader this
// node can reach, and whether that leader is in touch with a majority of
// its replicas. A node cut off from the rest of the cluster fails one or
// the other, and it is the one node whose opinion of who else is alive
// must not be acted on: every other node would look dead to it.
//
// Reaching the leader alone was the first check, and it is not enough. A
// core cut off together with the edge it serves may itself lead the
// stream, and it answers its own request: Raft keeps a leader without
// quorum for up to twenty seconds, past the ten in which an edge is
// declared dead. Its view of its replicas tells the truth sooner. Each one
// answers the leader's heartbeat every second, and one that has not been
// heard from for [replicaSilence] is on the far side of a split.
func (s *Store) Reachable(ctx context.Context) error {
	stream, err := s.js.Stream(ctx, kvStreamPrefix+bucketSessions)
	if err != nil {
		return fmt.Errorf("store: reaching the sessions stream: %w", err)
	}

	return quorate(stream.CachedInfo().Cluster)
}

// replicaSilence is how long a stream leader may go without hearing from a
// replica before the replica is counted as out of reach. Raft heartbeats
// every second, so three is two missed beats of slack: the same allowance
// nats-server gives a follower judging its own leader.
const replicaSilence = 3 * time.Second

// errNoQuorum is a stream leader that is not in touch with a majority of
// its replicas.
var errNoQuorum = errors.New("store: the sessions stream's leader cannot hear a majority of its replicas")

// quorate reports whether a stream's leader, as its own info describes it,
// hears a majority of the stream's replicas, itself included. A stream that
// is not replicated has no one to be cut off from.
func quorate(cluster *jetstream.ClusterInfo) error {
	if cluster == nil {
		return nil
	}

	if cluster.Leader == "" {
		return fmt.Errorf("%w: no leader", errNoQuorum)
	}

	heard := 1 // the leader, which answered

	for _, r := range cluster.Replicas {
		// Active is zero for a replica never heard from, as well as for
		// one heard from this instant; only the second counts.
		if r != nil && !r.Offline && r.Active > 0 && r.Active < replicaSilence {
			heard++
		}
	}

	if size := len(cluster.Replicas) + 1; 2*heard <= size {
		return fmt.Errorf("%w: %d of %d", errNoQuorum, heard, size)
	}

	return nil
}

// PublishDurable stores one message on the durable stream and returns once
// it is replicated. id makes a retried store call idempotent.
func (s *Store) PublishDurable(ctx context.Context, msg *nats.Msg, id string) error {
	if _, err := s.js.PublishMsg(ctx, msg, jetstream.WithMsgID(id)); err != nil {
		return fmt.Errorf("store: durable publish to %s: %w", msg.Subject, err)
	}

	return nil
}

// publishAsyncMaxPending bounds queue writes in flight from one node. A
// pipelined queuer fanning one message out to thousands of away sessions
// has that many in flight at once, past nats.go's default of 4000.
const publishAsyncMaxPending = 1 << 15

// DurableHandler receives one message from the durable stream. A message
// can arrive more than once, so a handler must tolerate that; the bridge
// tells repeats apart by the Mast-Id header the publisher set.
type DurableHandler func(subject string, header nats.Header, data []byte)

// ConsumeDurable delivers every message stored on the durable stream from
// now on, and returns a function that stops it.
//
// What carries a message across a node being cut off is noticing that a
// delivery went missing. While a leaf connection is down, or before the
// core has noticed it is, what the core sends the node is lost in transit,
// and the node's own NATS connection never drops. JetStream numbers every
// delivery to a consumer, so a delivery that arrives out of turn proves the
// ones before it were lost, and the node replaces the consumer with one
// that starts after the last message it did handle.
//
// Two designs failed on the way. An ordered consumer does the same gap
// check but acknowledges nothing, so a delivery lost at the tail of a burst
// is never shown up by a later one; here JetStream sends it again after
// durableAckWait, and that redelivery is itself out of turn. And relying on
// redelivery alone does not work under acknowledge-all: acknowledging the
// first message to arrive after a loss acknowledges the lost ones with it,
// so under steady traffic nothing was ever sent again. Nothing past a gap
// is handled or acknowledged for that reason.
//
// A handler may still see a message twice, after an acknowledgement goes
// missing, which is why messages carry an id.
//
// The consumer is ephemeral, so a node that goes away without stopping does
// not leave one behind for longer than durableInactive.
func (s *Store) ConsumeDurable(
	ctx context.Context,
	handle DurableHandler,
	onErr func(error),
) (func(), error) {
	return s.ConsumeDurableFlushed(ctx, handle, nil, onErr)
}

// ConsumeDurableFlushed is [Store.ConsumeDurable] for a handler that
// finishes its work after it returns. flush is called before every
// acknowledgement and must not return nil until everything handled so far
// is done, because an acknowledgement tells the stream never to send those
// messages to this node again. On an error nothing is acknowledged.
func (s *Store) ConsumeDurableFlushed(
	ctx context.Context,
	handle DurableHandler,
	flush func() error,
	onErr func(error),
) (func(), error) {
	// Everything already on the stream counts as handled: a node reads what
	// is published from now on, as the plain fabric always did. Starting at
	// an explicit sequence rather than "new" gives the very first gap a
	// floor to restart from.
	stream, err := s.js.Stream(ctx, streamDurable)
	if err != nil {
		return nil, fmt.Errorf("store: stream %s: %w", streamDurable, err)
	}

	c := &durableConsumer{
		store:      s,
		handle:     handle,
		flush:      flush,
		onErr:      onErr,
		mu:         sync.Mutex{},
		floor:      stream.CachedInfo().State.LastSeq,
		name:       "",
		delivered:  0,
		restarting: false,
		stopped:    false,
		current:    nil,
		unacked:    nil,
		pending:    0,
		done:       make(chan struct{}),
	}

	if err := c.start(ctx); err != nil {
		return nil, err
	}

	go c.flushAcks()

	return c.stop, nil
}

// errDeliveryLost reports a gap in the deliveries to this node's consumer.
var errDeliveryLost = errors.New("store: durable deliveries lost in transit, replaying")

// Durable consumer tuning. The ack wait is how long a delivery lost at the
// tail of a burst waits to be sent again, so it is short; the handler only
// hands a message to mochi and never blocks. The inactive threshold is how
// long a node can be unreachable before its consumer is removed, and
// matches how long the stream keeps a message anyway.
const (
	durableAckWait       = 3 * time.Second
	durableMaxAckPending = 20000
	durableMaxDeliver    = 20
	durableInactive      = 5 * time.Minute
	durableHeartbeat     = time.Second
	durablePullExpiry    = 10 * time.Second
	durablePullBatch     = 2000

	// Acknowledgements are cumulative and batched: acknowledging message N
	// acknowledges everything before it, so one ack every durableAckEvery
	// messages, or every durableAckFlush when traffic stops, does the work
	// of one per message. Per-message acks roughly doubled the core's
	// JetStream work and let a 10k msg/s QoS 1 load build a backlog.
	durableAckEvery = 64
	durableAckFlush = 100 * time.Millisecond
)

// durableConsumer is one node's reading of the durable stream.
type durableConsumer struct {
	store  *Store
	handle DurableHandler
	flush  func() error
	onErr  func(error)

	mu sync.Mutex

	// floor is the stream sequence through which every message has been
	// handled, and where a replacement consumer starts. It only advances on
	// a delivery that arrived in turn, which is what makes that true.
	floor uint64

	// name is the consumer being read and delivered the number of the last
	// delivery from it that was handled. A message from any other consumer
	// is a leftover of one that was replaced, and is ignored.
	name      string
	delivered uint64

	// restarting is set while a consumer is being replaced, so a second
	// gap or error does not start a second replacement.
	restarting bool
	stopped    bool
	current    jetstream.ConsumeContext

	// unacked is the latest handled message not yet acknowledged, and
	// pending how many were handled since the last acknowledgement.
	unacked jetstream.Msg
	pending int
	done    chan struct{}
}

// start creates a consumer that begins just after floor and reads it.
func (c *durableConsumer) start(ctx context.Context) error {
	c.mu.Lock()
	from := c.floor + 1
	c.mu.Unlock()

	consumer, err := c.store.js.CreateConsumer(ctx, streamDurable, jetstream.ConsumerConfig{
		FilterSubject:     DurablePrefix + ">",
		DeliverPolicy:     jetstream.DeliverByStartSequencePolicy,
		OptStartSeq:       from,
		AckPolicy:         jetstream.AckAllPolicy,
		AckWait:           durableAckWait,
		MaxAckPending:     durableMaxAckPending,
		MaxDeliver:        durableMaxDeliver,
		InactiveThreshold: durableInactive,
	})
	if err != nil {
		return fmt.Errorf("store: consumer on %s: %w", streamDurable, err)
	}

	c.mu.Lock()
	c.name, c.delivered = consumer.CachedInfo().Name, 0
	c.unacked, c.pending = nil, 0
	c.mu.Unlock()

	consuming, err := consumer.Consume(c.receive,
		jetstream.ConsumeErrHandler(c.failed),
		jetstream.PullMaxMessages(durablePullBatch),
		jetstream.PullHeartbeat(durableHeartbeat),
		jetstream.PullExpiry(durablePullExpiry),
	)
	if err != nil {
		return fmt.Errorf("store: consuming %s: %w", streamDurable, err)
	}

	c.mu.Lock()
	c.current = consuming
	stopped := c.stopped
	c.mu.Unlock()

	// Stopped while this was being created: stop has already run, and
	// would not have seen it.
	if stopped {
		consuming.Stop()
	}

	return nil
}

func (c *durableConsumer) receive(m jetstream.Msg) {
	meta, err := m.Metadata()
	if err != nil {
		c.onErr(fmt.Errorf("store: durable message without metadata: %w", err))

		return
	}

	c.mu.Lock()
	if c.restarting || meta.Consumer != c.name {
		c.mu.Unlock()

		return
	}

	if meta.Sequence.Consumer != c.delivered+1 {
		lost := fmt.Errorf("%w: delivery %d after %d", errDeliveryLost, meta.Sequence.Consumer, c.delivered)
		c.mu.Unlock()
		c.replace(lost)

		return
	}
	c.mu.Unlock()

	// When the stream stored it, in the header a direct get uses for the
	// same thing. The queuer decides by it whether a device was away when a
	// message was published, rather than when the queuer got round to it.
	header := m.Headers()
	if header == nil {
		header = nats.Header{}
	}

	header.Set(nats.JSTimeStamp, meta.Timestamp.UTC().Format(time.RFC3339Nano))

	c.handle(m.Subject(), header, m.Data())

	c.mu.Lock()
	// A redelivery after a late acknowledgement is in turn but behind the
	// floor, which must not move back.
	c.floor = max(c.floor, meta.Sequence.Stream)

	if meta.Consumer != c.name {
		c.mu.Unlock()

		return
	}

	c.delivered = meta.Sequence.Consumer
	c.unacked = m
	c.pending++
	due := c.pending >= durableAckEvery
	c.mu.Unlock()

	if due {
		c.ack()
	}
}

// ack acknowledges everything handled so far.
func (c *durableConsumer) ack() {
	c.mu.Lock()
	m := c.unacked
	c.unacked, c.pending = nil, 0
	c.mu.Unlock()

	if m == nil {
		return
	}

	// After taking m, so the flush covers it: m was handled before it was
	// recorded as the latest.
	if c.flush != nil {
		if err := c.flush(); err != nil {
			c.onErr(fmt.Errorf("store: not acknowledging durable messages: %w", err))

			return
		}
	}

	if err := m.Ack(); err != nil {
		c.onErr(fmt.Errorf("store: acknowledging durable messages: %w", err))
	}
}

// flushAcks acknowledges the tail of a burst, which would otherwise wait
// for durableAckEvery more messages and be sent again in the meantime.
func (c *durableConsumer) flushAcks() {
	ticker := time.NewTicker(durableAckFlush)
	defer ticker.Stop()

	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
			c.ack()
		}
	}
}

// failed reports a consume error and, when the consumer itself is gone,
// replaces it.
//
// A missed heartbeat and a pull nobody answers are replaced too, and that is
// the part learned by failing. The consumer is ephemeral and lives on one
// core; when that core dies, the client is told only that heartbeats
// stopped, and it goes on pulling from a consumer that is not there. Logged
// and left, a core killed under load stopped every reader whose consumer it
// held — the queuers on the other cores included — and they read nothing
// more for as long as the node ran, while the bridge went on dropping the
// plain copies those readers were meant to replace. A replacement starts
// from the floor, so whatever the dead consumer had not handed over is
// delivered again, and readers already tolerate repeats.
func (c *durableConsumer) failed(_ jetstream.ConsumeContext, err error) {
	if errors.Is(err, jetstream.ErrConsumerDeleted) || errors.Is(err, jetstream.ErrConsumerNotFound) ||
		errors.Is(err, jetstream.ErrNoHeartbeat) || errors.Is(err, nats.ErrNoResponders) {
		c.replace(err)

		return
	}

	c.onErr(err)
}

// replace stops reading the current consumer and starts another at the
// floor, unless that is already under way.
func (c *durableConsumer) replace(reason error) {
	c.mu.Lock()
	if c.stopped || c.restarting {
		c.mu.Unlock()

		return
	}

	c.restarting = true
	old, oldName := c.current, c.name
	c.current, c.unacked, c.pending = nil, nil, 0
	c.mu.Unlock()

	c.onErr(reason)

	go c.restart(old, oldName)
}

// restart replaces a consumer, and keeps trying for as long as the node
// runs. Giving up is not an option that fails safe: the bridge goes on
// dropping the plain copies of stored messages, so a node reading nothing
// would deliver no QoS 1 or 2 from other nodes at all.
func (c *durableConsumer) restart(old jetstream.ConsumeContext, oldName string) {
	if old != nil {
		old.Stop()
	}

	// Removed now rather than left to durableInactive, so that the core
	// does not hold two consumers for this node. If the core is out of
	// reach this fails, and the inactive threshold is the fallback.
	ctx, cancel := context.WithTimeout(context.Background(), durableHeartbeat)
	_ = c.store.js.DeleteConsumer(ctx, streamDurable, oldName)

	cancel()

	for {
		select {
		case <-c.done:
			return
		default:
		}

		ctx, cancel := context.WithTimeout(context.Background(), durablePullExpiry)
		err := c.start(ctx)

		cancel()

		if err == nil {
			c.mu.Lock()
			c.restarting = false
			c.mu.Unlock()

			return
		}

		c.onErr(err)

		select {
		case <-c.done:
			return
		case <-time.After(durableHeartbeat):
		}
	}
}

func (c *durableConsumer) stop() {
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()

		return
	}

	c.stopped = true
	current := c.current
	c.mu.Unlock()

	close(c.done)
	c.ack()

	if current != nil {
		current.Stop()
	}
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
// The filters are subject patterns from the topic codec, so this is a scan
// of the bucket's own subjects rather than a lookup against a second index —
// the reason the codec produces KV-safe keys in the first place.
//
// It reads with batched direct gets. The obvious call, ListKeysFiltered,
// is a watch, and a watch is an ordered consumer: every SUBSCRIBE asking
// for retained messages created one on the core and then fetched each key
// with a round trip of its own. A reconnect storm of 25k subscribers was
// 25k consumer creations, which is the cost this whole design exists to
// avoid. A direct get is served by any replica and creates nothing. The
// bucket keeps one revision per key, so walking its subjects in order
// yields exactly the current value of each, and a delete marker in place of
// any that were cleared.
func (s *Store) MatchRetained(ctx context.Context, filters []string) ([]Message, error) {
	var out []Message

	now := time.Now()
	prefix := kvSubjectPrefix + bucketRetained + "."

	for _, filter := range filters {
		err := s.direct.each(ctx, s.apiPrefix, kvStreamPrefix+bucketRetained, prefix+filter, 0,
			func(m *nats.Msg, _ uint64) bool {
				if op := m.Header.Get(kvOperationHeader); op == kvOpDelete || op == kvOpPurge {
					return true
				}

				var msg Message
				if err := json.Unmarshal(m.Data, &msg); err != nil {
					return true // a value we cannot read is not worth failing a subscribe over
				}

				// The bucket has no per-key TTL, so an expired message is
				// found only when something asks for it. Deleting it here is
				// what stops it being found again; a failed delete costs
				// nothing but a retry on the next subscribe.
				if msg.Expired(now) {
					_ = s.retained.Delete(ctx, strings.TrimPrefix(m.Header.Get(nats.JSSubject), prefix))

					return true
				}

				out = append(out, msg)

				return true
			})
		if err != nil {
			return nil, fmt.Errorf("store: reading retained: %w", err)
		}
	}

	return out, nil
}

// The bucket's stream is an implementation detail of JetStream KV, but a
// stable one: every client library names it this way.
const (
	kvStreamPrefix    = "KV_"
	kvSubjectPrefix   = "$KV."
	kvOperationHeader = "KV-Operation"
	kvOpDelete        = "DEL"
	kvOpPurge         = "PURGE"
)

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

// MarkAway records that a session's connection has gone, unless another
// connection has written it since.
//
// A node can notice a disconnect late — a dead TCP connection is found by
// keepalive — by which time the device may be on another node. Written
// blind, its record of the session as away would stand over the new
// owner's, and the queuer would keep messages for a client that is
// receiving them live. It is a read and a revision-checked write, and
// written reports whether it happened.
func (s *Store) MarkAway(ctx context.Context, key string, sess Session) (bool, error) {
	sess.Present = false
	sess.UpdatedAt = time.Now().UTC()

	encoded, err := json.Marshal(sess)
	if err != nil {
		return false, fmt.Errorf("store: encoding session: %w", err)
	}

	for range casRetries {
		current, revision, err := s.leaderSession(ctx, key)

		switch {
		case errors.Is(err, errNoRecord):
			// Nothing to mark: a session with no record has nothing to
			// queue for, and one whose record was deleted — by a clean
			// start on another node, say — must stay deleted. Creating it
			// here brought back subscriptions that clean start had dropped.
			return false, nil
		case err != nil:
			return false, err
		case current.Owner != "" && current.Owner != sess.Owner:
			return false, nil
		}

		if _, err = s.sessions.Update(ctx, key, encoded, revision); err == nil {
			return true, nil
		}
	}

	return false, fmt.Errorf("store: marking session %s away: %w", key, errContended)
}

// errNoRecord reports a session the stream leader holds no live record for.
var errNoRecord = errors.New("store: no session record")

// errLeaderRead reports a stream message get the leader refused.
var errLeaderRead = errors.New("store: reading from the stream leader")

// leaderReply is a stream message get's answer.
type leaderReply struct {
	Error *struct {
		Code    int    `json:"code"`
		ErrCode int    `json:"err_code"`
		Desc    string `json:"description"`
	} `json:"error"`
	Message *struct {
		Seq    uint64 `json:"seq"`
		Header []byte `json:"hdrs"`
		Data   []byte `json:"data"`
	} `json:"message"`
}

func decodeLeaderSession(key string, data []byte) (Session, uint64, error) {
	var none Session

	var reply leaderReply
	if err := json.Unmarshal(data, &reply); err != nil {
		return none, 0, fmt.Errorf("store: reading session %s: %w", key, err)
	}

	switch {
	case reply.Error != nil && reply.Error.Code == http.StatusNotFound, reply.Message == nil:
		return none, 0, errNoRecord
	case reply.Error != nil:
		return none, 0, fmt.Errorf("%w: session %s: %d %s", errLeaderRead, key, reply.Error.ErrCode, reply.Error.Desc)
	}

	// A delete or purge is a record too, marked in its header.
	op := string(reply.Message.Header)
	if strings.Contains(op, "KV-Operation: DEL") || strings.Contains(op, "KV-Operation: PURGE") {
		return none, 0, errNoRecord
	}

	var current Session
	if err := json.Unmarshal(reply.Message.Data, &current); err != nil {
		// An unreadable record is overwritten rather than kept.
		return none, reply.Message.Seq, nil //nolint:nilerr // see above
	}

	return current, reply.Message.Seq, nil
}

// errContended reports a revision-checked write that kept losing.
var errContended = errors.New("store: write kept losing to concurrent writers")

// DecodeSession reads one record from the sessions bucket.
func DecodeSession(value []byte) (Session, error) {
	var sess Session
	if err := json.Unmarshal(value, &sess); err != nil {
		return Session{}, fmt.Errorf("store: decoding session: %w", err)
	}

	return sess, nil
}

// WatchSessions follows every change to the sessions bucket. It is one
// ordered consumer for the whole node, which is how the queuer learns who
// is away without asking per message.
//
//nolint:ireturn // nats.go offers the watcher only as an interface
func (s *Store) WatchSessions(ctx context.Context) (jetstream.KeyWatcher, error) {
	w, err := s.sessions.WatchAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("store: watching sessions: %w", err)
	}

	return w, nil
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
	if _, err := s.sessions.Get(ctx, key); err == nil {
		if err := s.sessions.Delete(ctx, key); err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
			return fmt.Errorf("store: deleting session %s: %w", key, err)
		}
	} else if !errors.Is(err, jetstream.ErrKeyNotFound) {
		return fmt.Errorf("store: reading session %s: %w", key, err)
	}

	return s.DiscardQueue(ctx, key)
}

// leaderSession reads a session record from the sessions stream's leader.
//
// Not the bucket's Get, which is a direct get any replica may answer, and a
// replica can be a moment behind. A device that subscribed and disconnected
// at once had its record written and then looked up on a replica that had
// not yet seen it; MarkAway took that for a deleted session and wrote
// nothing, the record stayed as the subscribe left it — held — and no queuer
// kept that sleeper's messages. Twenty of five thousand in one drill. A
// stream message get is answered by the leader only, which has every write
// it acknowledged. It costs a request to the leader, once per disconnect of
// a lasting session, beside the replicated write that follows anyway.
func (s *Store) leaderSession(ctx context.Context, key string) (Session, uint64, error) {
	req, err := json.Marshal(map[string]string{"last_by_subj": "$KV." + bucketSessions + "." + key})
	if err != nil {
		return Session{}, 0, fmt.Errorf("store: encoding session read: %w", err)
	}

	subject := s.apiPrefix + "STREAM.MSG.GET." + kvStreamPrefix + bucketSessions

	msg, err := s.js.Conn().RequestWithContext(ctx, subject, req)
	if err != nil {
		return Session{}, 0, fmt.Errorf("store: reading session %s: %w", key, err)
	}

	return decodeLeaderSession(key, msg.Data)
}
