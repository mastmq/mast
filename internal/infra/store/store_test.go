package store_test

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/mastmq/mast/internal/infra/natsd"
	"github.com/mastmq/mast/internal/infra/store"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// open starts an embedded JetStream server and returns a store over it.
func open(t *testing.T) *store.Store {
	t.Helper()

	s, _ := openWithConn(t)

	return s
}

// openWithConn also returns the connection, for a test that has to look at
// JetStream behind the store's back.
func openWithConn(t *testing.T) (*store.Store, *nats.Conn) {
	t.Helper()

	opts := &natsserver.Options{
		ServerName: "store-test",
		DontListen: true,
		JetStream:  true,
		// Matches what natsd configures, so the test addresses the domain
		// the same way production does.
		JetStreamDomain: natsd.JetStreamDomain,
		StoreDir:        t.TempDir(),
		NoSigs:          true,
		NoLog:           true,
	}

	ns, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("starting nats: %v", err)
	}

	go ns.Start()

	if !ns.ReadyForConnections(15 * time.Second) {
		t.Fatal("nats did not become ready")
	}

	t.Cleanup(ns.Shutdown)

	nc, err := nats.Connect("", nats.InProcessServer(ns))
	if err != nil {
		t.Fatalf("connecting: %v", err)
	}

	t.Cleanup(nc.Close)

	s, err := store.Open(context.Background(), nc, natsd.JetStreamDomain, 1, time.Hour)
	if err != nil {
		t.Fatalf("opening store: %v", err)
	}

	t.Cleanup(s.Close)

	return s, nc
}

// bare starts JetStream with nothing provisioned and returns a connection.
func bare(t *testing.T) *nats.Conn {
	t.Helper()

	opts := &natsserver.Options{
		ServerName: "store-bare", DontListen: true, JetStream: true,
		JetStreamDomain: natsd.JetStreamDomain, StoreDir: t.TempDir(), NoSigs: true, NoLog: true,
	}

	ns, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("starting nats: %v", err)
	}

	go ns.Start()

	if !ns.ReadyForConnections(15 * time.Second) {
		t.Fatal("nats did not become ready")
	}

	t.Cleanup(ns.Shutdown)

	nc, err := nats.Connect("", nats.InProcessServer(ns))
	if err != nil {
		t.Fatalf("connecting: %v", err)
	}

	t.Cleanup(nc.Close)

	return nc
}

func TestRetained(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := open(t)

	put := func(key, topic, payload string) {
		t.Helper()

		if err := s.PutRetained(ctx, key, store.Message{
			Topic: topic, Payload: []byte(payload), QoS: 1, Retain: true,
		}); err != nil {
			t.Fatalf("PutRetained(%s): %v", key, err)
		}
	}

	put("t.acme.sensors.a.temp", "sensors/a/temp", "21")
	put("t.acme.sensors.b.temp", "sensors/b/temp", "22")
	put("t.other.sensors.a.temp", "sensors/a/temp", "99")

	// A subscription filter is also a KV watch pattern, which is the whole
	// point of the codec's key-safe escaping.
	got, err := s.MatchRetained(ctx, []string{"t.acme.sensors.>"})
	if err != nil {
		t.Fatalf("MatchRetained: %v", err)
	}

	if len(got) != 2 {
		t.Errorf("wildcard match returned %d messages, want 2 (other tenant must not appear)", len(got))
	}

	single, err := s.MatchRetained(ctx, []string{"t.acme.sensors.*.temp"})
	if err != nil {
		t.Fatalf("MatchRetained single-level: %v", err)
	}

	if len(single) != 2 {
		t.Errorf("single-level wildcard returned %d, want 2", len(single))
	}

	// An empty payload clears the value, as MQTT requires.
	if err := s.PutRetained(ctx, "t.acme.sensors.a.temp", store.Message{Topic: "sensors/a/temp"}); err != nil {
		t.Fatalf("clearing retained: %v", err)
	}

	after, err := s.MatchRetained(ctx, []string{"t.acme.sensors.>"})
	if err != nil {
		t.Fatalf("MatchRetained after clear: %v", err)
	}

	if len(after) != 1 {
		t.Errorf("after clearing one value, %d remain, want 1", len(after))
	}
}

func TestSessions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := open(t)

	if _, err := s.GetSession(ctx, "t.acme.absent"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("GetSession on a missing key = %v, want ErrNotFound", err)
	}

	want := store.Session{
		ClientID: "dev-1",
		Tenant:   "acme",
		Subscriptions: []store.Subscription{
			{Filter: "a/b", QoS: 1},
			{Filter: "c/#", QoS: 2},
		},
	}

	if err := s.PutSession(ctx, "t.acme.dev-1", want); err != nil {
		t.Fatalf("PutSession: %v", err)
	}

	got, err := s.GetSession(ctx, "t.acme.dev-1")
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}

	if len(got.Subscriptions) != 2 || got.Subscriptions[1].Filter != "c/#" {
		t.Errorf("subscriptions round-tripped as %v", got.Subscriptions)
	}

	if got.UpdatedAt.IsZero() {
		t.Error("UpdatedAt was not stamped")
	}

	if err := s.DeleteSession(ctx, "t.acme.dev-1"); err != nil {
		t.Fatalf("DeleteSession: %v", err)
	}

	if _, err := s.GetSession(ctx, "t.acme.dev-1"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("session survived deletion: %v", err)
	}
}

// drain reads a whole queue and trims what it read, the way the bridge does
// once the messages are delivered.
func drain(t *testing.T, s *store.Store, key string) []store.Message {
	t.Helper()

	got, through, err := s.ReadQueue(context.Background(), key, 0)
	if err != nil {
		t.Fatalf("ReadQueue(%s): %v", key, err)
	}

	if err := s.TrimQueue(context.Background(), key, through); err != nil {
		t.Fatalf("TrimQueue(%s): %v", key, err)
	}

	return got
}

func enqueue(t *testing.T, s *store.Store, key, id string, msg store.Message) bool {
	t.Helper()

	stored, err := s.Enqueue(context.Background(), store.Queued{Key: key, Msg: msg, ID: id})
	if err != nil {
		t.Fatalf("Enqueue(%s): %v", key, err)
	}

	return stored
}

func TestOfflineQueue(t *testing.T) {
	t.Parallel()

	s := open(t)

	for i := range 5 {
		enqueue(t, s, "acme.dev-1", "", store.Message{Topic: "a/b", Payload: []byte{byte(i)}, QoS: 1})
	}

	drained := drain(t, s, "acme.dev-1")
	if len(drained) != 5 {
		t.Fatalf("drained %d messages, want 5", len(drained))
	}

	if drained[0].Payload[0] != 0 || drained[4].Payload[0] != 4 {
		t.Error("queue did not preserve publish order")
	}

	if drained[0].StoredAt == 0 {
		t.Error("a queued message does not know when it was stored, so it can never expire")
	}

	if again := drain(t, s, "acme.dev-1"); len(again) != 0 {
		t.Errorf("second drain returned %d messages, want 0", len(again))
	}
}

// TestOfflineQueueTrimKeepsWhatItDidNotRead is the race the old drain had:
// it read the queue and then deleted the key, so anything another node
// appended in between was deleted without ever being read.
func TestOfflineQueueTrimKeepsWhatItDidNotRead(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := open(t)

	enqueue(t, s, "acme.dev-1", "", store.Message{Topic: "a/b", Payload: []byte("first"), QoS: 1})

	read, through, err := s.ReadQueue(ctx, "acme.dev-1", 0)
	if err != nil || len(read) != 1 {
		t.Fatalf("ReadQueue = %d messages, %v", len(read), err)
	}

	// Arrives between the read and the trim.
	enqueue(t, s, "acme.dev-1", "", store.Message{Topic: "a/b", Payload: []byte("late"), QoS: 1})

	if err := s.TrimQueue(ctx, "acme.dev-1", through); err != nil {
		t.Fatalf("TrimQueue: %v", err)
	}

	left := drain(t, s, "acme.dev-1")
	if len(left) != 1 || string(left[0].Payload) != "late" {
		t.Errorf("after trimming what was read, the queue holds %v, want only the late message", left)
	}
}

// TestReadQueueAfterSkipsWhatWasRead covers a trim that failed: the next
// look at the queue must not hand the device again what it already has.
func TestReadQueueAfterSkipsWhatWasRead(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := open(t)

	enqueue(t, s, "acme.dev-1", "", store.Message{Topic: "a/b", Payload: []byte("delivered"), QoS: 1})

	_, through, err := s.ReadQueue(ctx, "acme.dev-1", 0)
	if err != nil {
		t.Fatalf("ReadQueue: %v", err)
	}

	// No trim: as if it had timed out.
	enqueue(t, s, "acme.dev-1", "", store.Message{Topic: "a/b", Payload: []byte("straggler"), QoS: 1})

	got, _, err := s.ReadQueue(ctx, "acme.dev-1", through)
	if err != nil {
		t.Fatalf("ReadQueue after: %v", err)
	}

	if len(got) != 1 || string(got[0].Payload) != "straggler" {
		t.Errorf("a second look returned %v, want only the straggler", got)
	}
}

// TestOfflineQueueDeduplicates is what lets the edge and every core queue
// the same message for the same session without the device receiving it
// once per writer.
func TestOfflineQueueDeduplicates(t *testing.T) {
	t.Parallel()

	s := open(t)
	msg := store.Message{Topic: "a/b", Payload: []byte("once"), QoS: 1}

	if !enqueue(t, s, "acme.dev-1", "m1:acme.dev-1", msg) {
		t.Fatal("the first write was reported as a duplicate")
	}

	if enqueue(t, s, "acme.dev-1", "m1:acme.dev-1", msg) {
		t.Error("the second write of the same id was stored again")
	}

	if got := drain(t, s, "acme.dev-1"); len(got) != 1 {
		t.Errorf("queue holds %d copies, want 1", len(got))
	}
}

// TestOfflineQueueHoldsLargeMessages covers the queue being one KV entry:
// a hundred messages of a few kilobytes, base64 inside JSON, outgrew what a
// NATS message may hold, and from then on every append for that device
// failed.
func TestOfflineQueueHoldsLargeMessages(t *testing.T) {
	t.Parallel()

	s := open(t)
	payload := make([]byte, 64<<10)

	for i := range 150 {
		payload[0] = byte(i)
		enqueue(t, s, "acme.camera", "", store.Message{Topic: "img", Payload: payload, QoS: 1})
	}

	got := drain(t, s, "acme.camera")
	if len(got) != 100 {
		t.Fatalf("queue held %d large messages, want 100", len(got))
	}

	if got[99].Payload[0] != 149 || len(got[99].Payload) != len(payload) {
		t.Error("the newest large message did not survive intact")
	}
}

// TestOfflineQueueKeepsTheTopicExact covers a topic that cannot sit in a
// header as it is: MQTT allows a line break in one.
func TestOfflineQueueKeepsTheTopicExact(t *testing.T) {
	t.Parallel()

	s := open(t)

	for _, name := range []string{"a/b", "/lead", "dots.and spaces", "line\r\nbreak", "ünï/çødé", "a//b/"} {
		enqueue(t, s, "acme.exact", "", store.Message{
			Topic: name, Payload: []byte("x"), QoS: 2,
			Properties: &store.Properties{ContentType: "text/plain", User: []store.UserProperty{{Key: "k", Value: "v"}}},
		})

		got := drain(t, s, "acme.exact")
		if len(got) != 1 || got[0].Topic != name || got[0].QoS != 2 {
			t.Fatalf("queued %q, read back %+v", name, got)
		}

		if got[0].Properties == nil || got[0].Properties.ContentType != "text/plain" || len(got[0].Properties.User) != 1 {
			t.Errorf("properties did not survive the queue: %+v", got[0].Properties)
		}
	}
}

// TestOfflineQueueIsBounded pins the property that stops one absent device
// subscribed to a chatty topic from growing until the stream does.
func TestOfflineQueueIsBounded(t *testing.T) {
	t.Parallel()

	s := open(t)

	const sent = 250
	for i := range sent {
		enqueue(t, s, "acme.chatty", "", store.Message{Topic: "a/b", Payload: []byte{byte(i)}, QoS: 1})
	}

	drained := drain(t, s, "acme.chatty")
	if len(drained) != 100 {
		t.Errorf("queue held %d messages, want it capped at 100", len(drained))
	}

	// The oldest are the ones dropped, so the newest must have survived.
	if last := drained[len(drained)-1].Payload[0]; last != byte(sent-1) {
		t.Errorf("newest message is %d, want %d — the wrong end was dropped", last, byte(sent-1))
	}
}

// TestDeleteSessionDiscardsTheQueue is what clean_start=true means.
func TestDeleteSessionDiscardsTheQueue(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := open(t)

	enqueue(t, s, "acme.fresh", "", store.Message{Topic: "a/b", Payload: []byte("stale"), QoS: 1})

	if err := s.DeleteSession(ctx, "acme.fresh"); err != nil {
		t.Fatalf("DeleteSession: %v", err)
	}

	if got := drain(t, s, "acme.fresh"); len(got) != 0 {
		t.Errorf("a clean start kept %d queued messages", len(got))
	}

	// And a session that never had anything is no error.
	if err := s.DeleteSession(ctx, "acme.never"); err != nil {
		t.Errorf("DeleteSession on nothing: %v", err)
	}
}

// TestMarkAwayRespectsANewerOwner is a node noticing a disconnect late,
// after the device has already come back on another node.
func TestMarkAwayRespectsANewerOwner(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := open(t)

	held := store.Session{ClientID: "mover", Tenant: "acme", Present: true, Owner: "new"}
	if err := s.PutSession(ctx, "acme.mover", held); err != nil {
		t.Fatalf("PutSession: %v", err)
	}

	written, err := s.MarkAway(ctx, "acme.mover", store.Session{ClientID: "mover", Tenant: "acme", Owner: "old"})
	if err != nil {
		t.Fatalf("MarkAway: %v", err)
	}

	got, _ := s.GetSession(ctx, "acme.mover")
	if written || !got.Present || got.Owner != "new" {
		t.Errorf("a stale disconnect overwrote the new owner: written=%v, record=%+v", written, got)
	}

	written, err = s.MarkAway(ctx, "acme.mover", store.Session{ClientID: "mover", Tenant: "acme", Owner: "new"})
	if err != nil || !written {
		t.Fatalf("the owner could not mark its own session away: %v", err)
	}

	if got, _ := s.GetSession(ctx, "acme.mover"); got.Present {
		t.Error("the session is still present after its owner disconnected")
	}
}

// TestRetainedPagesPastOneBatch covers a wildcard subscription over more
// retained topics than one direct get returns.
func TestRetainedPagesPastOneBatch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := open(t)

	const topics = 700

	for i := range topics {
		key := "t.acme.fleet." + strconv.Itoa(i)

		msg := store.Message{Topic: "fleet/" + strconv.Itoa(i), Payload: []byte("v"), QoS: 1, Retain: true}
		if err := s.PutRetained(ctx, key, msg); err != nil {
			t.Fatalf("PutRetained: %v", err)
		}
	}

	// Overwrite and clear a few, so the scan meets superseded and deleted
	// entries as well as live ones.
	for i := range 10 {
		key := "t.acme.fleet." + strconv.Itoa(i)

		msg := store.Message{Topic: "fleet/" + strconv.Itoa(i), Payload: nil, QoS: 1, Retain: true}
		if err := s.PutRetained(ctx, key, msg); err != nil {
			t.Fatalf("clearing: %v", err)
		}
	}

	got, err := s.MatchRetained(ctx, []string{"t.acme.fleet.>"})
	if err != nil {
		t.Fatalf("MatchRetained: %v", err)
	}

	if len(got) != topics-10 {
		t.Errorf("matched %d retained messages, want %d", len(got), topics-10)
	}
}

func TestMessageExpiry(t *testing.T) {
	t.Parallel()

	stored := time.Unix(1_000, 0)
	msg := store.Message{
		Topic: "flash/sale", Payload: []byte("now"), QoS: 1, Retain: true,
		Properties: &store.Properties{MessageExpiry: 10},
		StoredAt:   stored.Unix(),
	}

	if msg.Expired(stored.Add(9 * time.Second)) {
		t.Error("expired a second before its interval was up")
	}

	if !msg.Expired(stored.Add(10 * time.Second)) {
		t.Error("still live once its interval was up")
	}

	// An entry written before StoredAt existed has no clock to count from.
	// Guessing would either expire everything at once or nothing ever; not
	// expiring is the one that loses no data.
	legacy := msg
	legacy.StoredAt = 0

	if legacy.Expired(stored.Add(time.Hour)) {
		t.Error("a message with no stored time was expired")
	}
}

// TestRetainedReadsOldEntries guards the upgrade: buckets already hold
// entries written before properties and a stored time existed, and those
// must still be served rather than skipped or failed.
func TestRetainedReadsOldEntries(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := open(t)

	if err := s.PutRetained(ctx, "t.acme.old", store.Message{
		Topic: "old", Payload: []byte("v1"), QoS: 1, Retain: true, Properties: nil, StoredAt: 0,
	}); err != nil {
		t.Fatalf("PutRetained: %v", err)
	}

	got, err := s.MatchRetained(ctx, []string{"t.acme.old"})
	if err != nil {
		t.Fatalf("MatchRetained: %v", err)
	}

	if len(got) != 1 || got[0].Properties != nil || got[0].StoredAt == 0 {
		t.Errorf("got %+v, want one message with no properties and a stored time", got)
	}
}

// TestExpiredRetainedIsNotReturned checks that an expired retained message
// stays gone across repeated reads. The first read also deletes it, but the
// store's API cannot tell a deleted key from a skipped one, so that part is
// not asserted here.
func TestExpiredRetainedIsNotReturned(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := open(t)

	if err := s.PutRetained(ctx, "t.acme.flash", store.Message{
		Topic: "flash", Payload: []byte("now"), QoS: 1, Retain: true,
		Properties: &store.Properties{MessageExpiry: 1},
		StoredAt:   0,
	}); err != nil {
		t.Fatalf("PutRetained: %v", err)
	}

	time.Sleep(2100 * time.Millisecond)

	for range 2 {
		got, err := s.MatchRetained(ctx, []string{"t.acme.flash"})
		if err != nil {
			t.Fatalf("MatchRetained: %v", err)
		}

		if len(got) != 0 {
			t.Fatalf("an expired retained message was returned: %+v", got)
		}
	}
}

func TestDurableStreamDeliversWhatIsStored(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := open(t)

	if err := s.OpenDurable(ctx, 1, time.Minute); err != nil {
		t.Fatalf("OpenDurable: %v", err)
	}

	got := make(chan string, 4)

	stop, err := s.ConsumeDurable(ctx, func(subject string, _ nats.Header, data []byte) {
		got <- subject + "=" + string(data)
	}, func(err error) { t.Logf("consume: %v", err) })
	if err != nil {
		t.Fatalf("ConsumeDurable: %v", err)
	}

	t.Cleanup(stop)

	msg := nats.NewMsg(store.DurablePrefix + "acme.orders")
	msg.Data = []byte("go")

	if err := s.PublishDurable(ctx, msg, "id-1"); err != nil {
		t.Fatalf("PublishDurable: %v", err)
	}

	// The same id again is the publisher retrying, and must not deliver twice.
	if err := s.PublishDurable(ctx, msg, "id-1"); err != nil {
		t.Fatalf("PublishDurable retry: %v", err)
	}

	select {
	case m := <-got:
		if m != store.DurablePrefix+"acme.orders=go" {
			t.Errorf("delivered %q", m)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a stored message was never delivered")
	}

	select {
	case m := <-got:
		t.Errorf("a retried publish was delivered twice: %q", m)
	case <-time.After(500 * time.Millisecond):
	}
}

// TestRetainedReplayCreatesNoConsumer pins the reason MatchRetained reads
// with direct gets: listing keys is a watch, and a watch is a consumer on
// the core per SUBSCRIBE.
func TestRetainedReplayCreatesNoConsumer(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, nc := openWithConn(t)

	msg := store.Message{Topic: "a", Payload: []byte("v"), QoS: 1, Retain: true}
	if err := s.PutRetained(ctx, "t.acme.a", msg); err != nil {
		t.Fatalf("PutRetained: %v", err)
	}

	js, err := jetstream.NewWithDomain(nc, natsd.JetStreamDomain)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}

	// Counted through the JetStream API tally rather than by looking for
	// consumers: a watcher's consumer is gone again moments after the call,
	// so a test that looked would pass against the code it guards against.
	// A direct get is not an API call; creating a consumer is.
	calls := func() uint64 {
		info, err := js.AccountInfo(ctx)
		if err != nil {
			t.Fatalf("account info: %v", err)
		}

		return info.API.Total
	}

	before := calls()

	for range 20 {
		if _, err := s.MatchRetained(ctx, []string{"t.acme.>"}); err != nil {
			t.Fatalf("MatchRetained: %v", err)
		}
	}

	// The second AccountInfo is itself one call.
	if made := calls() - before - 1; made != 0 {
		t.Errorf("20 retained replays made %d JetStream API calls, want 0", made)
	}
}

// TestBindWaitsForTheCore is the edge's side of who configures storage: it
// creates nothing, and says so until a core has.
func TestBindWaitsForTheCore(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	nc := bare(t)

	if _, err := store.Bind(ctx, nc, natsd.JetStreamDomain); !errors.Is(err, store.ErrNotProvisioned) {
		t.Fatalf("Bind before the core provisioned = %v, want ErrNotProvisioned", err)
	}

	js, err := jetstream.NewWithDomain(nc, natsd.JetStreamDomain)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}

	if names := js.StreamNames(ctx); names.Err() == nil {
		for name := range names.Name() {
			t.Errorf("Bind created stream %s", name)
		}
	}

	core, err := store.Open(ctx, nc, natsd.JetStreamDomain, 1, time.Hour)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	t.Cleanup(core.Close)

	if err := core.OpenDurable(ctx, 1, time.Minute); err != nil {
		t.Fatalf("OpenDurable: %v", err)
	}

	edge, err := store.Bind(ctx, nc, natsd.JetStreamDomain)
	if err != nil {
		t.Fatalf("Bind after the core provisioned: %v", err)
	}

	t.Cleanup(edge.Close)

	if err := edge.BindDurable(ctx); err != nil {
		t.Errorf("BindDurable: %v", err)
	}

	// And the edge's handle works against what the core made.
	queued := store.Queued{Key: "acme.d", Msg: store.Message{Topic: "a", Payload: []byte("x"), QoS: 1}}
	if _, err := edge.Enqueue(ctx, queued); err != nil {
		t.Errorf("Enqueue through a bound store: %v", err)
	}

	if got := drain(t, core, "acme.d"); len(got) != 1 {
		t.Errorf("the core read %d messages the edge queued, want 1", len(got))
	}
}

// TestReconcileLeavesAMatchingStoreAlone: a store already configured as
// asked costs a few reads a minute and no writes.
func TestReconcileLeavesAMatchingStoreAlone(t *testing.T) {
	t.Parallel()

	s := open(t)

	fixed, err := s.Reconcile(context.Background(), 1)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if len(fixed) != 0 {
		t.Errorf("Reconcile changed %v on a store that matched", fixed)
	}
}
