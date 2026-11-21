// Package limits enforces per-tenant limits on one node, and shares the
// node's connection counts with the rest of the cluster so that a
// connection limit means the tenant's connections everywhere.
//
// # Scope of each limit
//
// A connection limit is cluster-wide. Every node publishes how many
// connections it holds for each tenant on one core NATS subject every
// interval, and holds one subscription that hears everyone else's. That is
// one subscription and one message per node per interval, whatever the
// number of devices — nothing in JetStream, so no consumer, no stream and
// no replicated write on a CONNECT. The price is that the count is as old as
// the last report: connections accepted on several nodes within one
// interval are not seen by each other, and the tenant can overshoot by that
// many. A strict count would need a replicated increment on every CONNECT,
// which is exactly what a reconnect storm of 25k devices cannot afford.
//
// A publish rate is per node. A token bucket per tenant per node costs a
// lock and no network; a cluster-wide one needs either a shared counter on
// the hot path or an apportioning of the rate between nodes that guesses
// where the tenant's publishers are. Both were worse than a limit whose
// scope is stated.
//
// An offline queue limit is per session, applied when the queue is
// drained: a reconnecting device is sent only the newest messages its
// tenant may keep. The stream itself keeps the largest limit configured,
// because JetStream bounds messages per subject for a whole stream, not per
// subject prefix.
package limits

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mastmq/mast/internal/domain/quota"
	"github.com/mastmq/mast/internal/domain/tenant"
	"github.com/nats-io/nats.go"
)

// Subject carries every node's connection counts. Like the session control
// plane it sits outside the "t." namespace, so no client filter can reach it.
const Subject = "mast.limits.connections"

// reportChunk bounds how many tenants one report names, so a node holding
// connections for a great many tenants stays well inside a NATS message.
const reportChunk = 2000

// freshReports is how many intervals a report counts for. One would let a
// single late message drop a node's connections from everyone's count.
const freshReports = 3

// ErrTooManyConnections is returned when admitting a connection would take
// its tenant past its connection limit.
var ErrTooManyConnections = errors.New("limits: the tenant has reached its connection limit")

// Options configures a [Limiter].
type Options struct {
	// Defaults apply to every tenant.
	Defaults tenant.Limits
	// Tenants overrides the defaults for named tenants.
	Tenants map[tenant.ID]tenant.Overrides
	// Ceiling is the most messages the offline stream keeps per session, which
	// no tenant's queue can exceed. Zero means no ceiling.
	Ceiling int
	// Interval is how often this node reports its connection counts.
	Interval time.Duration
	// NodeID tells this node's reports from everyone else's. It must be unique
	// per process: the embedded server's id is.
	NodeID string
}

// Limiter holds every tenant's limits and usage on this node.
type Limiter struct {
	nc   *nats.Conn
	opts Options
	log  *slog.Logger
	now  func() time.Time

	// tenants is a *usage per tenant. Read on every publish from every
	// connection's goroutine and written once per tenant, which is the shape
	// sync.Map serves without a lock.
	tenants sync.Map

	mu     sync.Mutex
	census *quota.Census

	sub      *nats.Subscription
	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once
	// started is whether the report loop runs, and so whether Stop has
	// anything to wait for.
	started atomic.Bool
}

// usage is one tenant's limits and what it is using of them on this node.
type usage struct {
	// limited is whether the tenant has a publish rate at all, read without
	// the lock so an unlimited tenant's publish costs one atomic load.
	limited atomic.Bool

	mu     sync.Mutex
	limits tenant.Limits
	// auth is the newest overrides authentication stated for the tenant on
	// this node, which stay until newer ones replace them.
	auth     tenant.Overrides
	conns    int
	reported int
	bucket   quota.Bucket
}

// report is one node's connection counts, or a part of them.
type report struct {
	Node   string         `json:"node"`
	Counts map[string]int `json:"counts"`
}

// New builds a limiter. It shares nothing with the cluster until
// [Limiter.Start]; a nil connection keeps it local, which is what a test of
// the arithmetic wants.
func New(nc *nats.Conn, opts Options, log *slog.Logger) *Limiter {
	return &Limiter{
		nc:       nc,
		opts:     opts,
		log:      log.With("component", "limits"),
		now:      time.Now,
		tenants:  sync.Map{},
		mu:       sync.Mutex{},
		census:   quota.NewCensus(freshReports * opts.Interval),
		sub:      nil,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
		stopOnce: sync.Once{},
		started:  atomic.Bool{},
	}
}

// Start subscribes to the other nodes' reports and begins sending this
// node's own.
func (l *Limiter) Start() error {
	if l.nc == nil {
		return nil
	}

	sub, err := l.nc.Subscribe(Subject, l.onReport)
	if err != nil {
		return fmt.Errorf("limits: subscribing to %s: %w", Subject, err)
	}

	l.sub = sub

	l.started.Store(true)

	go l.reportLoop()

	return nil
}

// Stop ends the reports. It is safe to call more than once.
func (l *Limiter) Stop() {
	l.stopOnce.Do(func() {
		close(l.stop)

		if l.sub != nil {
			// The connection is closing with the node regardless.
			_ = l.sub.Unsubscribe()
		}

		if l.started.Load() {
			<-l.done
		}
	})
}

// Admit counts a new connection for id, unless that would take the tenant
// past its connection limit. auth is what the connection's authentication
// said about the tenant's limits. It becomes the tenant's limits on this
// node when it supersedes what is in force (see [tenant.Overrides.Supersedes]):
// the newest word from the service that owns tenants is the best one there
// is, and one that says nothing, or was issued before it, changes nothing.
//
// replacing is true when the connection takes over a client id this node
// already holds. Such a connection is always admitted: the old one is about
// to be released, and refusing would leave a device whose connection
// silently died unable to come back while the tenant sat at its limit.
func (l *Limiter) Admit(id tenant.ID, auth tenant.Overrides, replacing bool) (tenant.Limits, error) {
	now := l.now()

	l.mu.Lock()
	remote := l.census.Remote(string(id), now)
	l.mu.Unlock()

	u := l.usage(id, now)

	u.mu.Lock()
	defer u.mu.Unlock()

	if auth.Supersedes(u.auth) {
		u.auth = auth
	}

	u.apply(l.configured(id).With(u.auth), now)

	if u.limits.MaxConnections > 0 && !replacing && u.conns+remote >= u.limits.MaxConnections {
		return u.limits, fmt.Errorf("%w: %d of %d", ErrTooManyConnections, u.conns+remote, u.limits.MaxConnections)
	}

	u.conns++

	return u.limits, nil
}

// Release uncounts a connection [Limiter.Admit] counted.
func (l *Limiter) Release(id tenant.ID) {
	v, ok := l.tenants.Load(id)
	if !ok {
		return
	}

	u, _ := v.(*usage)

	u.mu.Lock()
	u.conns = max(0, u.conns-1)
	u.mu.Unlock()
}

// AllowPublish reports whether id may publish once more now, and takes the
// publish out of its rate if so.
func (l *Limiter) AllowPublish(id tenant.ID) bool {
	v, ok := l.tenants.Load(id)
	if !ok {
		return true // no connection was admitted for it here
	}

	u, _ := v.(*usage)
	if !u.limited.Load() {
		return true
	}

	u.mu.Lock()
	defer u.mu.Unlock()

	return u.bucket.Allow(l.now())
}

// OfflineQueue is how many queued messages one of id's sessions may be sent
// on reconnecting: the tenant's limit, never more than the stream keeps.
func (l *Limiter) OfflineQueue(id tenant.ID) int {
	queue := l.configured(id).OfflineQueue

	if v, ok := l.tenants.Load(id); ok {
		u, _ := v.(*usage)

		u.mu.Lock()
		queue = u.limits.OfflineQueue
		u.mu.Unlock()
	}

	if l.opts.Ceiling > 0 && (queue <= 0 || queue > l.opts.Ceiling) {
		return l.opts.Ceiling
	}

	return queue
}

// Connections is how many connections id holds across the cluster, as far
// as this node knows.
func (l *Limiter) Connections(id tenant.ID) int {
	now := l.now()

	l.mu.Lock()
	total := l.census.Remote(string(id), now)
	l.mu.Unlock()

	if v, ok := l.tenants.Load(id); ok {
		u, _ := v.(*usage)

		u.mu.Lock()
		total += u.conns
		u.mu.Unlock()
	}

	return total
}

// configured is a tenant's limits before authentication says anything.
func (l *Limiter) configured(id tenant.ID) tenant.Limits {
	return l.opts.Defaults.With(l.opts.Tenants[id])
}

// usage returns id's entry, creating it with the configured limits.
func (l *Limiter) usage(id tenant.ID, now time.Time) *usage {
	if v, ok := l.tenants.Load(id); ok {
		u, _ := v.(*usage)

		return u
	}

	limits := l.configured(id)
	fresh := &usage{
		limited:  atomic.Bool{},
		mu:       sync.Mutex{},
		limits:   limits,
		auth:     tenant.Overrides{}, //nolint:exhaustruct_v5 // none stated yet
		conns:    0,
		reported: 0,
		bucket:   quota.NewBucket(limits.PublishRate, limits.Burst(), now),
	}
	fresh.limited.Store(limits.PublishRate > 0)

	v, _ := l.tenants.LoadOrStore(id, fresh)
	u, _ := v.(*usage)

	return u
}

// apply makes limits the tenant's, keeping what it has spent of its rate.
// The caller holds u.mu.
func (u *usage) apply(limits tenant.Limits, now time.Time) {
	if limits == u.limits {
		return
	}

	u.limits = limits
	u.bucket.Reset(limits.PublishRate, limits.Burst(), now)
	u.limited.Store(limits.PublishRate > 0)
}

// reportLoop sends this node's counts every interval and forgets reports
// nobody renewed.
func (l *Limiter) reportLoop() {
	defer close(l.done)

	ticker := time.NewTicker(l.opts.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-l.stop:
			return
		case <-ticker.C:
			l.publishCounts()

			l.mu.Lock()
			l.census.Prune(l.now())
			l.mu.Unlock()
		}
	}
}

// publishCounts reports every tenant this node holds connections for, and
// once more as zero each tenant it has just stopped holding any for, so the
// other nodes stop counting them now rather than when the report ages out.
func (l *Limiter) publishCounts() {
	counts := make(map[string]int)

	l.tenants.Range(func(k, v any) bool {
		id, _ := k.(tenant.ID)
		u, _ := v.(*usage)

		u.mu.Lock()
		if u.conns > 0 || u.reported > 0 {
			counts[string(id)] = u.conns
		}

		u.reported = u.conns
		u.mu.Unlock()

		return true
	})

	for len(counts) > 0 {
		chunk := make(map[string]int, min(len(counts), reportChunk))

		for id, n := range counts {
			if len(chunk) == reportChunk {
				break
			}

			chunk[id] = n
			delete(counts, id)
		}

		l.send(chunk)
	}
}

// send publishes one report.
func (l *Limiter) send(counts map[string]int) {
	data, err := json.Marshal(report{Node: l.opts.NodeID, Counts: counts})
	if err != nil {
		l.log.Error("encoding a connection report", "error", err)

		return
	}

	if err := l.nc.Publish(Subject, data); err != nil {
		l.log.Warn("publishing a connection report", "error", err)
	}
}

// onReport records another node's counts.
func (l *Limiter) onReport(msg *nats.Msg) {
	var r report
	if err := json.Unmarshal(msg.Data, &r); err != nil {
		l.log.Warn("ignoring an unreadable connection report", "error", err)

		return
	}

	// This node's own count is exact and current; its report is neither.
	if r.Node == "" || r.Node == l.opts.NodeID {
		return
	}

	l.mu.Lock()
	l.census.Observe(r.Node, r.Counts, l.now())
	l.mu.Unlock()
}
