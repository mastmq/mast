package store

import (
	"context"
	"sync"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// lostInTransit stands in for a delivery the consumer did receive: only
// what receive reads off a message is implemented.
type lostInTransit struct {
	jetstream.Msg

	subject  string
	consumer string
	dseq     uint64
	sseq     uint64
}

func (m lostInTransit) Subject() string      { return m.subject }
func (m lostInTransit) Headers() nats.Header { return nats.Header{} }
func (m lostInTransit) Data() []byte         { return nil }
func (m lostInTransit) Ack() error           { return nil }

func (m lostInTransit) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{
		Sequence: jetstream.SequencePair{Consumer: m.dseq, Stream: m.sseq},
		Consumer: m.consumer,
	}, nil
}

// TestDurableGapIsReplayed is the loss that acknowledge-all hid. A node
// that receives delivery 3 without delivery 2 must not handle or
// acknowledge it: acknowledging 3 acknowledges 2, which JetStream then
// never sends again. It has to replace its consumer from the last message
// it did handle, so the lost one arrives after all.
func TestDurableGapIsReplayed(t *testing.T) {
	ns, err := natsserver.NewServer(&natsserver.Options{
		DontListen:      true,
		JetStream:       true,
		JetStreamDomain: "test",
		StoreDir:        t.TempDir(),
		NoSigs:          true,
		NoLog:           true,
	})
	if err != nil {
		t.Fatal(err)
	}

	go ns.Start()

	if !ns.ReadyForConnections(15 * time.Second) {
		t.Fatal("nats did not become ready")
	}

	t.Cleanup(ns.Shutdown)

	nc, err := nats.Connect("", nats.InProcessServer(ns))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(nc.Close)

	ctx := context.Background()

	s, err := Open(ctx, nc, "test", 1, time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	if err := s.OpenDurable(ctx, 1, time.Minute); err != nil {
		t.Fatal(err)
	}

	for _, subject := range []string{"q.1", "q.2", "q.3"} {
		if err := s.PublishDurable(ctx, nats.NewMsg(subject), subject); err != nil {
			t.Fatal(err)
		}
	}

	var (
		mu      sync.Mutex
		handled []string
	)

	c := &durableConsumer{
		store: s,
		handle: func(subject string, _ nats.Header, _ []byte) {
			mu.Lock()

			handled = append(handled, subject)
			mu.Unlock()
		},
		onErr: func(err error) { t.Log(err) },
		name:  "before",
		done:  make(chan struct{}),
	}
	t.Cleanup(c.stop)

	c.receive(lostInTransit{subject: "q.1", consumer: "before", dseq: 1, sseq: 1})
	c.receive(lostInTransit{subject: "q.3", consumer: "before", dseq: 3, sseq: 3})

	deadline := time.Now().Add(10 * time.Second)

	for time.Now().Before(deadline) {
		mu.Lock()

		got := append([]string(nil), handled...)
		mu.Unlock()

		if len(got) >= 3 {
			if got[0] != "q.1" || got[1] != "q.2" || got[2] != "q.3" {
				t.Fatalf("handled %v, want [q.1 q.2 q.3]", got)
			}

			return
		}

		time.Sleep(20 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()

	t.Fatalf("handled %v: the delivery lost before q.3 was never replayed", handled)
}
