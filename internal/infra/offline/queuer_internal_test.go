package offline

import (
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/mastmq/mast/internal/infra/store"
	"github.com/nats-io/nats.go/jetstream"
)

// TestReturnedSessionKeepsWhatWasPublishedWhileAway covers a queuer behind
// the durable stream when a device comes back. It used to judge by the
// record — present, so nothing kept — and a message published while the
// device was away, which its new node had not subscribed in time to send
// it, was skipped. It now judges by when the stream stored the message.
func TestReturnedSessionKeepsWhatWasPublishedWhileAway(t *testing.T) {
	t.Parallel()

	q := newQueuer(nil, time.Hour, nil, slog.New(slog.DiscardHandler))

	away := time.Now().Add(-time.Minute)
	back := time.Now()

	q.apply(record(t, "acme.dev-1", false, away))
	q.apply(record(t, "acme.dev-1", true, back))

	msg := fabricMessage{tenant: "acme", stored: store.Message{Topic: "dev/1/cmd", Payload: []byte("x"), QoS: 1}}

	if items := q.match(msg, "id-before", back.Add(-2*time.Second)); len(items) != 1 {
		t.Errorf("a message stored while the device was away was kept for %d sessions, want 1", len(items))
	}

	if items := q.match(msg, "id-after", back.Add(2*time.Second)); len(items) != 0 {
		t.Errorf("a message stored after the device came back was kept for %d sessions, want 0", len(items))
	}

	// Having read past the moment it came back, the queuer lets it go.
	q.pruneReturned(time.Now())

	if n := q.Away(); n != 0 {
		t.Errorf("after the queuer caught up, %d sessions are still held", n)
	}
}

func TestDeletedSessionLeavesAtOnce(t *testing.T) {
	t.Parallel()

	q := newQueuer(nil, time.Hour, nil, slog.New(slog.DiscardHandler))

	q.apply(record(t, "acme.dev-1", false, time.Now()))
	q.apply(entry{key: "acme.dev-1", op: jetstream.KeyValueDelete, created: time.Now(), value: nil})

	if n := q.Away(); n != 0 {
		t.Errorf("a deleted session is still held: %d", n)
	}
}

func record(t *testing.T, key string, present bool, at time.Time) entry {
	t.Helper()

	value, err := json.Marshal(store.Session{
		ClientID:      "dev-1",
		Tenant:        "acme",
		Subscriptions: []store.Subscription{{Filter: "dev/1/cmd", QoS: 1}},
		UpdatedAt:     at,
		Present:       present,
		Owner:         "o",
	})
	if err != nil {
		t.Fatal(err)
	}

	return entry{key: key, op: jetstream.KeyValuePut, created: at, value: value}
}

// entry is a sessions bucket record as the watch delivers it.
type entry struct {
	key     string
	op      jetstream.KeyValueOp
	created time.Time
	value   []byte
}

func (e entry) Bucket() string                  { return "mast_sessions" }
func (e entry) Key() string                     { return e.key }
func (e entry) Value() []byte                   { return e.value }
func (e entry) Revision() uint64                { return 1 }
func (e entry) Created() time.Time              { return e.created }
func (e entry) Delta() uint64                   { return 0 }
func (e entry) Operation() jetstream.KeyValueOp { return e.op }
