package natsd_test

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mastmq/mast/internal/infra/config"
	"github.com/mastmq/mast/internal/infra/natsd"
	"github.com/nats-io/nats.go"
)

// settleTimeout bounds how long a test waits for an asynchronous callback.
// The NATS client dispatches these on its own goroutine, so there is nothing
// to synchronise on but the effect.
const settleTimeout = 5 * time.Second

// recorder captures log records, because the counter and the log line are
// two different promises: an operator reads the line, an alert reads the
// counter, and this issue was that neither existed.
type recorder struct {
	mu      sync.Mutex
	records []slog.Record
}

func (r *recorder) Enabled(context.Context, slog.Level) bool { return true }

func (r *recorder) Handle(_ context.Context, rec slog.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.records = append(r.records, rec.Clone())

	return nil
}

func (r *recorder) WithAttrs([]slog.Attr) slog.Handler { return r }

func (r *recorder) WithGroup(string) slog.Handler { return r }

// count reports how many records at level contain substr in their message.
func (r *recorder) count(level slog.Level, substr string) int {
	r.mu.Lock()
	defer r.mu.Unlock()

	n := 0

	for _, rec := range r.records {
		if rec.Level == level && strings.Contains(rec.Message, substr) {
			n++
		}
	}

	return n
}

// start brings up an all-in-one node logging into rec.
func start(t *testing.T, rec *recorder) *natsd.Server {
	t.Helper()

	cfg := config.Default()
	cfg.NATS.Name = "natsd-test"
	cfg.Core.StoreDir = t.TempDir()
	// Binding the monitoring port would make two of these collide.
	cfg.NATS.MonitorAddr = ""

	s, err := natsd.Start(cfg, slog.New(rec))
	if err != nil {
		t.Fatalf("starting natsd: %v", err)
	}

	return s
}

// waitFor polls until done reports true or the timeout expires.
func waitFor(t *testing.T, what string, done func() bool) {
	t.Helper()

	deadline := time.Now().Add(settleTimeout)
	for time.Now().Before(deadline) {
		if done() {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for %s", what)
}

// TestSlowConsumerIsReported is the regression test for the silent drop.
//
// A blocked subscription handler with a one-message pending limit is exactly
// the shape of the production failure: the publisher succeeds, the messages
// are discarded inside the client, and before this handler existed nothing
// anywhere recorded it.
func TestSlowConsumerIsReported(t *testing.T) {
	rec := &recorder{mu: sync.Mutex{}, records: nil}
	s := start(t, rec)

	t.Cleanup(s.Shutdown)

	nc := s.Conn()

	// The handler blocks until the test releases it, so everything behind
	// the first message piles up against the limit set below.
	release := make(chan struct{})

	sub, err := nc.Subscribe("t.acme.hot", func(*nats.Msg) { <-release })
	if err != nil {
		t.Fatalf("subscribing: %v", err)
	}

	defer close(release)

	if err := sub.SetPendingLimits(1, 1024); err != nil {
		t.Fatalf("setting pending limits: %v", err)
	}

	for range 200 {
		if err := nc.Publish("t.acme.hot", []byte("x")); err != nil {
			t.Fatalf("publishing: %v", err)
		}
	}

	if err := nc.Flush(); err != nil {
		t.Fatalf("flushing: %v", err)
	}

	waitFor(t, "the slow consumer to be counted", func() bool {
		return s.Counts().SlowConsumers > 0
	})

	counts := s.Counts()
	if counts.AsyncErrors < counts.SlowConsumers {
		t.Errorf("async errors %d is below slow consumers %d; every slow consumer is also an async error",
			counts.AsyncErrors, counts.SlowConsumers)
	}

	if n := rec.count(slog.LevelError, "slow consumer"); n == 0 {
		t.Error("messages were dropped and nothing was logged, which is the whole bug")
	}
}

// TestCleanShutdownIsQuiet guards the closing flag. Without it every ordinary
// stop reports losing the fabric, and an error that fires on every deploy is
// an error nobody reads.
func TestCleanShutdownIsQuiet(t *testing.T) {
	rec := &recorder{mu: sync.Mutex{}, records: nil}
	s := start(t, rec)

	s.Shutdown()

	if n := rec.count(slog.LevelError, "nats connection"); n != 0 {
		t.Errorf("clean shutdown logged %d connection errors; it should log none", n)
	}
}
