package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/mastmq/mast/internal/domain/topic"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Offline queue names. Like the durable stream, the subjects sit outside
// the "t." namespace, so no client filter can reach them.
const (
	streamOffline = "mast_offline"
	offlinePrefix = "o."

	// legacyBucketOffline is where queues lived when each was one KV entry.
	legacyBucketOffline = "mast_offline"
)

// maxOfflineMessages bounds how much is kept for one absent client.
//
// The bound is the point: without it a single disconnected device subscribed
// to a chatty topic would grow until the stream did. MQTT lets a broker
// discard, and discarding the oldest is the least surprising choice — which
// the stream now does itself, per subject.
const maxOfflineMessages = 100

// offlineDedupWindow is how long the stream remembers a queued message's id.
// The edge that held the session and the queuer on every core write the
// same message for the same session, within moments of each other, and the
// id is what makes that one message rather than several.
const offlineDedupWindow = 2 * time.Minute

// enqueueChunk bounds how many asynchronous publishes are outstanding at
// once, inside nats.go's own limit on pending acknowledgements.
const enqueueChunk = 1024

// Headers on a queued message. The payload is the message body as it was
// published, so a queue holds no base64 and no JSON around it.
const (
	headerTopic = "Mast-Topic"
	headerQoS   = "Mast-Qos"
	headerProps = "Mast-Props"
)

// openOffline creates the stream absent clients' queues live on.
//
// A queue used to be one KV entry holding the whole list as JSON, rewritten
// under its revision for every message. That made each append cost the size
// of the queue, turned every payload into base64, and stopped accepting
// anything at all once a hundred ordinary messages added up to more than a
// NATS message may hold. One subject per session, with the stream keeping
// the newest hundred, makes an append one small write and leaves the
// discarding to the server.
func (s *Store) openOffline(ctx context.Context, replicas int, ttl time.Duration) error {
	dedup := offlineDedupWindow
	if ttl > 0 {
		dedup = min(dedup, ttl)
	}

	stream, err := s.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:              streamOffline,
		Subjects:          []string{offlinePrefix + ">"},
		Retention:         jetstream.LimitsPolicy,
		Discard:           jetstream.DiscardOld,
		MaxMsgsPerSubject: maxOfflineMessages,
		MaxAge:            ttl,
		Storage:           jetstream.FileStorage,
		Replicas:          replicas,
		Duplicates:        dedup,
		// Reads are served by any replica, which is what makes a drain on
		// reconnect cheap on the core during a storm.
		AllowDirect: true,
	})
	if err != nil {
		return fmt.Errorf("store: stream %s: %w", streamOffline, err)
	}

	s.offline = stream

	return s.bindLegacy(ctx)
}

// bindLegacy opens the bucket queues lived in before, if a cluster still
// has one.
func (s *Store) bindLegacy(ctx context.Context) error {
	legacy, err := s.js.KeyValue(ctx, legacyBucketOffline)

	switch {
	case err == nil:
		s.legacyOffline = legacy
	case !errors.Is(err, jetstream.ErrBucketNotFound):
		return fmt.Errorf("store: bucket %s: %w", legacyBucketOffline, err)
	}

	return nil
}

// Queued is one message bound for one absent session.
type Queued struct {
	// Key is the session's key, as for the sessions bucket.
	Key string
	Msg Message
	// ID names this message for this session, so that two nodes queueing it
	// store it once. Empty means no deduplication.
	ID string
}

// QueueID names one message for one session. Every node that queues it
// derives the same name from the publish's own id, which is what the
// stream deduplicates on.
func QueueID(messageID, key string) string {
	if messageID == "" {
		return ""
	}

	return messageID + ":" + key
}

// Enqueue appends one message to an absent client's queue. The oldest is
// dropped once the queue holds [maxOfflineMessages].
func (s *Store) Enqueue(ctx context.Context, q Queued) (bool, error) {
	stored, err := s.EnqueueMany(ctx, []Queued{q})

	return stored == 1, err
}

// EnqueueMany appends to many queues at once and returns once every write is
// acknowledged, reporting how many were new rather than repeats of a write
// another node already made.
func (s *Store) EnqueueMany(ctx context.Context, items []Queued) (int, error) {
	stored := 0

	for len(items) > 0 {
		chunk := items[:min(len(items), enqueueChunk)]
		items = items[len(chunk):]

		n, err := s.enqueueChunk(ctx, chunk)
		stored += n

		if err != nil {
			return stored, err
		}
	}

	return stored, nil
}

// enqueueChunk publishes a bounded run of writes and waits for all of them,
// sending again any the stream did not settle, until ctx runs out.
//
// Sending again is safe because every write carries its id, and it is what
// keeps one lost acknowledgement from stalling everything behind it. With
// nothing but ctx to bound the wait, a write in flight to a stream leader
// that died never heard back, and the queuer — which handles messages in
// order — sat out the whole thirty seconds on it while every message behind
// it waited: a core killed under load stopped all queueing on the others
// for just long enough that the devices it was for had come back and found
// nothing.
//
// "Duplicate message id is in process" is retried too. It means another
// node's write of the same message is still being replicated — the normal
// case here, since the edge and every core write each message — and that
// write can still fail. Asked again once it has settled, the stream says
// either that it has the message or that it does not.
func (s *Store) enqueueChunk(ctx context.Context, chunk []Queued) (int, error) {
	stored := 0

	for attempt := 0; len(chunk) > 0; attempt++ {
		n, again, err := s.enqueueOnce(ctx, chunk)
		stored += n

		if err != nil {
			return stored, err
		}

		chunk = again

		if len(chunk) > 0 {
			select {
			case <-ctx.Done():
				return stored, fmt.Errorf("store: queueing on %s: %w", chunk[0].Key, ctx.Err())
			case <-time.After(min(retryPause<<attempt, maxRetryPause)):
			}
		}
	}

	return stored, nil
}

// How long a queued write waits for its acknowledgement before it is sent
// again, and how long between attempts. A stream leader election takes a
// few seconds; the pause doubles up to the cap so a long one is not hammered.
const (
	publishAckWait = 2 * time.Second
	retryPause     = 20 * time.Millisecond
	maxRetryPause  = time.Second
)

// jsErrDuplicateInProcess is JetStream's answer to a message id another
// publish of the same id is still being stored under, and
// jsErrClusterNotAvailable its answer while a stream has no leader.
const (
	jsErrDuplicateInProcess  jetstream.ErrorCode = 10158
	jsErrClusterNotAvailable jetstream.ErrorCode = 10008
)

// retryable reports a write the stream did not settle either way, which is
// safe to send again under the same id.
func retryable(err error) bool {
	if apiErr, ok := errors.AsType[*jetstream.APIError](err); ok {
		return apiErr.ErrorCode == jsErrDuplicateInProcess || apiErr.ErrorCode == jsErrClusterNotAvailable
	}

	return errors.Is(err, jetstream.ErrAsyncPublishTimeout) ||
		errors.Is(err, jetstream.ErrNoStreamResponse) ||
		errors.Is(err, nats.ErrNoResponders) ||
		errors.Is(err, nats.ErrTimeout)
}

// enqueueOnce publishes each write once, and returns the ones the stream
// did not settle.
func (s *Store) enqueueOnce(ctx context.Context, chunk []Queued) (int, []Queued, error) {
	sent, again, err := s.sendQueued(chunk)
	if err != nil {
		return 0, nil, err
	}

	stored, unsettled, err := waitSent(ctx, sent)

	return stored, append(again, unsettled...), err
}

// waitSent waits for writes in flight, returning how many were new and
// which the stream did not settle.
func waitSent(ctx context.Context, sent []sentQueued) (int, []Queued, error) {
	stored := 0

	var (
		again    []Queued
		firstErr error
	)

	for _, p := range sent {
		select {
		case ack := <-p.f.Ok():
			if !ack.Duplicate {
				stored++
			}
		case err := <-p.f.Err():
			switch {
			case retryable(err):
				again = append(again, p.q)
			case firstErr == nil:
				firstErr = fmt.Errorf("store: queueing on %s: %w", p.f.Msg().Subject, err)
			}
		case <-ctx.Done():
			return stored, nil, fmt.Errorf("store: queueing: %w", ctx.Err())
		}
	}

	return stored, again, firstErr
}

// Pending is a set of queue writes started and not yet waited for.
type Pending struct {
	store *Store
	sent  []sentQueued
	again []Queued
	err   error
}

// StartEnqueue begins writing to many queues and returns without waiting.
// [Pending.Wait] finishes the job.
//
// This is what lets the queuer read the next message while the writes for
// the last one are still being replicated. Waiting on each message's writes
// before reading the next made the queuer as fast as one replicated round
// trip per message, and a burst of a hundred thousand fell twenty seconds
// behind — far enough that devices woke before their messages were queued.
func (s *Store) StartEnqueue(items []Queued) *Pending {
	sent, again, err := s.sendQueued(items)

	return &Pending{store: s, sent: sent, again: again, err: err}
}

// Wait returns once every write is stored, sending again what the stream
// did not settle, and reports how many were new.
func (p *Pending) Wait(ctx context.Context) (int, error) {
	if p.err != nil {
		return 0, p.err
	}

	stored, unsettled, err := waitSent(ctx, p.sent)
	if err != nil {
		return stored, err
	}

	more, err := p.store.EnqueueMany(ctx, append(p.again, unsettled...))

	return stored + more, err
}

// sendQueued starts every write, setting aside the ones nats.go would not
// take yet.
func (s *Store) sendQueued(chunk []Queued) ([]sentQueued, []Queued, error) {
	var again []Queued

	sent := make([]sentQueued, 0, len(chunk))

	for _, q := range chunk {
		f, err := s.publishQueued(q)

		switch {
		case err == nil:
			sent = append(sent, sentQueued{q: q, f: f})
		case retryable(err) || errors.Is(err, jetstream.ErrTooManyStalledMsgs):
			again = append(again, q)
		default:
			return nil, nil, err
		}
	}

	return sent, again, nil
}

// sentQueued is one write in flight.
type sentQueued struct {
	q Queued
	f jetstream.PubAckFuture
}

//nolint:ireturn // nats.go offers the future only as an interface
func (s *Store) publishQueued(q Queued) (jetstream.PubAckFuture, error) {
	msg, err := queuedMsg(q)
	if err != nil {
		return nil, err
	}

	var opts []jetstream.PublishOpt
	if q.ID != "" {
		opts = append(opts, jetstream.WithMsgID(q.ID))
	}

	f, err := s.js.PublishMsgAsync(msg, opts...)
	if err != nil {
		return nil, fmt.Errorf("store: queueing for %s: %w", q.Key, err)
	}

	return f, nil
}

func queuedMsg(q Queued) (*nats.Msg, error) {
	msg := nats.NewMsg(offlinePrefix + q.Key)
	msg.Data = q.Msg.Payload
	// Escaped: a topic is arbitrary UTF-8 and may hold a line break, which
	// in a header would end it and start another.
	msg.Header.Set(headerTopic, topic.EncodeToken(q.Msg.Topic))
	msg.Header.Set(headerQoS, strconv.Itoa(int(q.Msg.QoS)))

	if q.Msg.Properties != nil {
		props, err := json.Marshal(q.Msg.Properties)
		if err != nil {
			return nil, fmt.Errorf("store: encoding properties: %w", err)
		}

		msg.Header.Set(headerProps, string(props))
	}

	return msg, nil
}

// ReadQueue returns what is waiting for a client, oldest first, without
// removing it, and the stream sequence the read reached. Hand that sequence
// to [Store.TrimQueue] once the messages are delivered.
//
// Reading and removing are two steps on purpose. The queue used to be read
// and deleted in one call before anything was delivered, so a node dying in
// between lost the backlog, and a message queued between the read and the
// delete was deleted unread. A trim removes only what was read.
//
// after skips what an earlier read already returned: a later look at the
// same queue must not deliver again what a trim that failed left behind.
// Zero reads everything.
func (s *Store) ReadQueue(ctx context.Context, key string, after uint64) ([]Message, uint64, error) {
	out, err := s.drainLegacy(ctx, key)
	if err != nil {
		return nil, 0, err
	}

	through := after

	now := time.Now()

	err = s.direct.each(ctx, s.apiPrefix, streamOffline, offlinePrefix+key, after+1, func(m *nats.Msg, seq uint64) bool {
		through = seq

		if msg, ok := decodeQueued(m); ok && !msg.Expired(now) {
			out = append(out, msg)
		}

		return true
	})
	if err != nil {
		return nil, 0, fmt.Errorf("store: reading queue %s: %w", key, err)
	}

	return out, through, nil
}

// TrimQueue removes a client's queued messages up to and including the
// stream sequence through. Anything queued after the read that returned it
// stays.
func (s *Store) TrimQueue(ctx context.Context, key string, through uint64) error {
	if through == 0 {
		return nil
	}

	// A purge's sequence is the first message it keeps.
	err := s.offline.Purge(ctx, jetstream.WithPurgeSubject(offlinePrefix+key), jetstream.WithPurgeSequence(through+1))
	if err != nil {
		return fmt.Errorf("store: trimming queue %s: %w", key, err)
	}

	return nil
}

// DiscardQueue empties a client's queue.
//
// It reads before it purges, for the same reason [Store.DeleteSession] does:
// nearly every caller has nothing queued, a read is served by any replica,
// and a purge is a replicated write.
func (s *Store) DiscardQueue(ctx context.Context, key string) error {
	if _, err := s.drainLegacy(ctx, key); err != nil {
		return err
	}

	subject := offlinePrefix + key

	if _, err := s.offline.GetLastMsgForSubject(ctx, subject); err != nil {
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			return nil
		}

		return fmt.Errorf("store: reading queue %s: %w", key, err)
	}

	if err := s.offline.Purge(ctx, jetstream.WithPurgeSubject(subject)); err != nil {
		return fmt.Errorf("store: discarding queue %s: %w", key, err)
	}

	return nil
}

// drainLegacy returns and removes what an older version left in the queue
// bucket for this client.
func (s *Store) drainLegacy(ctx context.Context, key string) ([]Message, error) {
	if s.legacyOffline == nil {
		return nil, nil
	}

	entry, err := s.legacyOffline.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, nil
		}

		return nil, fmt.Errorf("store: reading legacy queue %s: %w", key, err)
	}

	if err := s.legacyOffline.Delete(ctx, key); err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, fmt.Errorf("store: clearing legacy queue %s: %w", key, err)
	}

	var queue []Message
	if err := json.Unmarshal(entry.Value(), &queue); err != nil {
		return nil, nil //nolint:nilerr // a corrupt legacy queue is discarded, not fatal
	}

	return queue, nil
}

// decodeQueued turns a message read off the queue stream back into a
// [Message]. The time it was stored is the stream's own timestamp.
func decodeQueued(m *nats.Msg) (Message, bool) {
	name, err := topic.DecodeToken(m.Header.Get(headerTopic))
	if err != nil {
		return Message{}, false //nolint:exhaustruct_v5 // nothing to return
	}

	qos, err := strconv.Atoi(m.Header.Get(headerQoS))
	if err != nil || qos < 1 || qos > 2 {
		return Message{}, false //nolint:exhaustruct_v5 // nothing to return
	}

	msg := Message{
		Topic:      name,
		Payload:    m.Data,
		QoS:        byte(qos),
		Retain:     false,
		Properties: nil,
		StoredAt:   0,
	}

	if raw := m.Header.Get(headerProps); raw != "" {
		var props Properties
		if json.Unmarshal([]byte(raw), &props) == nil {
			msg.Properties = &props
		}
	}

	if at, err := time.Parse(time.RFC3339Nano, m.Header.Get(nats.JSTimeStamp)); err == nil {
		msg.StoredAt = at.Unix()
	}

	return msg, true
}
