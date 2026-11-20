// Package broker assembles a running mast node from its parts: the embedded
// NATS server, the MQTT to NATS bridge, and the MQTT listeners.
//
// It exists so that the command and the tests build a node the same way. A
// test that assembles the pieces by hand is a test of something other than
// what ships.
package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mastmq/mast/internal/domain/tenant"
	"github.com/mastmq/mast/internal/infra/admin"
	"github.com/mastmq/mast/internal/infra/bridge"
	"github.com/mastmq/mast/internal/infra/config"
	"github.com/mastmq/mast/internal/infra/mqttd"
	"github.com/mastmq/mast/internal/infra/natsd"
	"github.com/mastmq/mast/internal/infra/obs"
	"github.com/mastmq/mast/internal/infra/offline"
	"github.com/mastmq/mast/internal/infra/store"
	mqtt "github.com/mastmq/mochi/v2"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// storeOpenTimeout bounds bucket creation at startup.
const storeOpenTimeout = 30 * time.Second

// Broker is a running mast node.
type Broker struct {
	nats   *natsd.Server
	hook   *bridge.Hook
	server *mqtt.Server
	store  *store.Store
	obs    *obs.Server
	admin  *admin.Server
	log    *slog.Logger

	// serving is set once the node does everything its role does: on an
	// edge or an all-in-one node, once MQTT is being served over an open
	// store. Readiness is false before it, whatever the fabric says.
	serving atomic.Bool
	// closeOnce makes Close safe to call twice, which a test that stops a
	// node early and then runs its cleanup does.
	closeOnce sync.Once

	// life ends the background work a node owns past Start: the queuer, and
	// on a core the loop that keeps trying to start it.
	life     context.Context //nolint:containedctx // the node's own lifetime, cancelled by Close
	shutdown context.CancelFunc
	wg       sync.WaitGroup

	queueMu sync.Mutex
	queuer  *offline.Queuer
	// queueStore is the store a core opens for its queuer. A core has no
	// bridge, so it is not b.store, which tests read from edges.
	queueStore *store.Store
}

// ObsAddr reports where metrics, health and pprof are actually served,
// which is the only way to learn it when the configured address asked for
// port zero.
func (b *Broker) ObsAddr() string { return b.obs.Addr() }

// AdminAddr reports where the admin API is served, empty when it is off.
func (b *Broker) AdminAddr() string { return b.admin.Addr() }

// Store exposes the durable state, for tests and for administrative tools.
func (b *Broker) Store() *store.Store { return b.store }

// NATS exposes the embedded server, for monitoring and for tests.
func (b *Broker) NATS() *natsd.Server { return b.nats }

// Subscriptions reports how many NATS subscriptions this node holds. It is
// the number to watch: it should track distinct topic filters, not devices.
func (b *Broker) Subscriptions() int {
	if b.hook == nil {
		return 0
	}

	return b.hook.Subscriptions()
}

// Start brings up a node for the configured role.
//
// The resolver and policy are injected rather than constructed here because
// they are the seam a deployment replaces: mast does not own tenant lifecycle.
func Start(
	ctx context.Context,
	cfg config.Config,
	resolver tenant.Resolver,
	policy tenant.Policy,
	log *slog.Logger,
) (*Broker, error) {
	nats, err := natsd.Start(cfg, log)
	if err != nil {
		return nil, err
	}

	life, shutdown := context.WithCancel(context.WithoutCancel(ctx))

	b := &Broker{
		nats: nats, hook: nil, server: nil, store: nil, obs: nil, admin: nil, log: log,
		serving: atomic.Bool{}, closeOnce: sync.Once{},
		life: life, shutdown: shutdown, wg: sync.WaitGroup{},
		queueMu: sync.Mutex{}, queuer: nil, queueStore: nil,
	}

	// The subscription gauge reads through the hook, so the registry is
	// built before it and handed the accessor.
	registry := newRegistry()
	metrics := obs.NewMetrics(registry, sources(b, nats))

	// Before the role check, because every role needs it. It used to come
	// after, so a core — the node holding Raft and every bucket — served no
	// metrics, no health and no pprof at all.
	b.obs, err = obs.Serve(ctx, cfg.Obs.Addr, registry, b.Ready, log)
	if err != nil {
		b.abort()

		return nil, err
	}

	// A core node carries storage and consensus and terminates no MQTT. It
	// does queue for absent sessions, in the background: a core cannot
	// open a stream until enough of its peers are up to elect a leader, and
	// a core that waited for that inside Start would never finish starting
	// the peers it is waiting for.
	if cfg.Role == config.RoleCore {
		b.wg.Go(func() { b.runCore(b.life, cfg, metrics) })
	} else if err := b.startMQTTSide(ctx, cfg, resolver, policy, metrics); err != nil {
		b.abort()

		return nil, err
	}

	// Last, so that on an edge the store it reads is already open and set:
	// the handlers read b.store from their own goroutines, and starting
	// them before it was written would be a race.
	b.admin, err = admin.Serve(ctx, cfg.Admin, nats.Conn(), b.sharedStore, log)
	if err != nil {
		b.Close()

		return nil, err
	}

	b.serving.Store(true)

	return b, nil
}

// errStarting is what readiness reports while Start has not finished.
var errStarting = errors.New("broker: still starting")

// Ready reports whether this node can serve, which is what /readyz answers.
//
// The fabric is asked every time rather than remembered, because readiness
// is lost as well as gained: an edge whose leaf connection drops can take
// no message across the cluster, and a load balancer should stop sending it
// devices until the link is back. A core is ready when nats-server says its
// JetStream is, which in a cluster already includes a meta leader and
// current streams; mast adds nothing of its own on top.
func (b *Broker) Ready() error {
	if !b.serving.Load() {
		return errStarting
	}

	if err := b.nats.Ready(); err != nil {
		return fmt.Errorf("broker: %w", err)
	}

	return nil
}

// openStore opens the shared buckets, waiting first for the link that makes
// them reachable.
//
// An edge carries no JetStream and reaches the core's over its leaf
// connection, which is established asynchronously after the server comes
// up. A bucket opened before then is answered by this node's own
// JetStream-less server rather than by the core, so the wait is not a
// courtesy: without it an edge that starts before the core dies outright.
func openStore(ctx context.Context, cfg config.Config, nats *natsd.Server) (*store.Store, error) {
	if cfg.Role == config.RoleEdge {
		if err := nats.WaitForLeaf(ctx); err != nil {
			return nil, err
		}
	}

	// A leaf connection counts as up before the core's JetStream API is
	// reachable through it — the interest that routes $JS.<domain>.API to
	// the core arrives after the connection does — and a core may still be
	// bringing JetStream up. Either way the answer is "jetstream not
	// enabled" or no responder at all, and both mean "not yet", not "never".
	// Refusing to start on them made an edge fail under a loaded CI runner
	// and would crash-loop one through every core restart. A core that
	// really has no JetStream still fails, once ctx runs out.
	const retry = 250 * time.Millisecond

	for {
		// Each attempt gets its own deadline. Until a core tier has elected
		// a meta leader nothing answers a JetStream request at all, so an
		// attempt does not fail, it waits — and one that waited for the
		// whole of ctx turned a ten-second election into a thirty-second
		// start, every time.
		attempt, cancel := context.WithTimeout(ctx, openAttempt)
		st, err := open(attempt, cfg, nats)

		cancel()

		unanswered := errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil
		if err == nil || (!notReadyYet(err) && !unanswered) {
			return st, err
		}

		select {
		case <-ctx.Done():
			return nil, err
		case <-time.After(retry):
		}
	}
}

// open provisions the store on a node that carries JetStream, and binds to
// what the core provisioned on one that does not.
func open(ctx context.Context, cfg config.Config, nats *natsd.Server) (*store.Store, error) {
	if cfg.Role == config.RoleEdge {
		st, err := store.Bind(ctx, nats.Conn(), natsd.JetStreamDomain)
		if err == nil && cfg.Fabric.DurableQoS {
			err = st.BindDurable(ctx)
		}

		return st, err
	}

	st, err := store.Open(ctx, nats.Conn(), natsd.JetStreamDomain, cfg.Core.Replicas, cfg.Session.Expiry)
	if err == nil && cfg.Fabric.DurableQoS {
		err = st.OpenDurable(ctx, cfg.Core.Replicas, cfg.Fabric.DurableMaxAge)
	}

	return st, err
}

// jsStreamCreateFailed is JetStream's general "stream creation failed"
// code, which it also returns when two requests create the same stream at
// once: the loser sees the winner's half-written metadata as "no such file
// or directory". Two edges joining a fresh core together did exactly that,
// and one died on its first bucket.
const jsStreamCreateFailed jetstream.ErrorCode = 10069

// notReadyYet reports whether a store error means JetStream is not
// reachable yet, or is still busy creating what was asked for, rather than
// that something is wrong with the request.
//
// A creation failure is retried on the same footing as "not enabled": the
// next attempt finds the stream the other node finished creating, and a
// stream that genuinely cannot be created keeps failing until ctx runs out,
// which is how the other two cases already end.
func notReadyYet(err error) bool {
	var apiErr *jetstream.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorCode == jsStreamCreateFailed {
		return true
	}

	return errors.Is(err, store.ErrNotProvisioned) ||
		errors.Is(err, jetstream.ErrJetStreamNotEnabled) ||
		errors.Is(err, jetstream.ErrJetStreamNotEnabledForAccount) ||
		errors.Is(err, natsgo.ErrNoResponders)
}

// newRegistry is a Prometheus registry with the runtime collectors that
// every mast process should publish whatever else it is doing.
func newRegistry() *prometheus.Registry {
	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.NewGoCollector())
	//nolint:exhaustruct_v5 // the collector's defaults are what we want
	registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	return registry
}

// sources wires the live values the metrics read at scrape time.
//
// natsd keeps the asynchronous tallies and obs publishes them; neither
// imports the other, so the conversion happens here, where the node is
// assembled and both are already in scope.
func sources(b *Broker, nats *natsd.Server) obs.Sources {
	return obs.Sources{
		Subscriptions: func() float64 { return float64(b.Subscriptions()) },
		NATS: func() obs.NATSCounts {
			c := nats.Counts()

			return obs.NATSCounts{
				AsyncErrors:   c.AsyncErrors,
				SlowConsumers: c.SlowConsumers,
				Disconnects:   c.Disconnects,
				Reconnects:    c.Reconnects,
			}
		},
	}
}

// QueuerReady reports whether this node is queueing for absent sessions.
func (b *Broker) QueuerReady() bool {
	b.queueMu.Lock()
	defer b.queueMu.Unlock()

	return b.queuer != nil
}

// Close stops the node, MQTT first so that no new message enters the bridge
// after the fabric beneath it has gone.
func (b *Broker) Close() {
	b.closeOnce.Do(b.close)
}

func (b *Broker) close() {
	b.admin.Close()
	b.obs.Close()

	if b.server != nil {
		if err := b.server.Close(); err != nil {
			b.log.Warn("closing mqtt server", "error", err)
		}
	}

	b.shutdown()
	b.wg.Wait()
	b.stopQueuer()

	if b.store != nil {
		b.store.Close()
	}

	b.queueMu.Lock()
	if b.queueStore != nil {
		b.queueStore.Close()
	}
	b.queueMu.Unlock()

	b.nats.Shutdown()
}

// sharedStore is the store this node reads the cluster's durable state
// through: its own on a node that serves MQTT, the queuer's on a core once
// it has provisioned, nil until then.
func (b *Broker) sharedStore() *store.Store {
	if b.store != nil {
		return b.store
	}

	b.queueMu.Lock()
	defer b.queueMu.Unlock()

	return b.queueStore
}

// openAttempt bounds one attempt to open the store, inside the overall
// storeOpenTimeout.
const openAttempt = 5 * time.Second

// coreRetry is how long a core waits between attempts to provision.
const coreRetry = time.Second

// runCore is a core's own work past serving NATS: it provisions the store,
// queues for absent sessions, and keeps the store configured as asked.
//
// It runs in the background and retries for as long as the core runs. Until
// it has succeeded the edges wait, since they no longer create anything
// themselves, and an away session is queued for only by its last edge.
func (b *Broker) runCore(life context.Context, cfg config.Config, metrics *obs.Metrics) {
	st := b.provision(life, cfg, metrics)
	if st == nil {
		return
	}

	ticker := time.NewTicker(reconcileEvery)
	defer ticker.Stop()

	for {
		select {
		case <-life.Done():
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(life, storeOpenTimeout)
			fixed, err := st.Reconcile(ctx, cfg.Core.Replicas)

			cancel()

			if err != nil {
				b.log.Warn("checking the store's replicas", "error", err)
			}

			if len(fixed) > 0 {
				b.log.Warn("restored the configured replicas on streams another node had changed",
					"streams", fixed, "replicas", cfg.Core.Replicas)
			}
		}
	}
}

// reconcileEvery is how often a core checks the store's replica counts.
const reconcileEvery = time.Minute

// provision opens the store and starts the queuer, retrying until both
// work or the core stops. It returns nil only when the core is stopping.
func (b *Broker) provision(life context.Context, cfg config.Config, metrics *obs.Metrics) *store.Store {
	for {
		openCtx, cancel := context.WithTimeout(life, storeOpenTimeout)
		st, err := openStore(openCtx, cfg, b.nats)

		cancel()

		if err == nil && cfg.Fabric.DurableQoS {
			if err = b.startQueuer(st, cfg, metrics); err != nil {
				st.Close()
			}
		}

		if err == nil {
			b.queueMu.Lock()
			b.queueStore = st
			b.queueMu.Unlock()

			return st
		}

		b.log.Warn("the core has not provisioned the store yet; edges wait for it", "error", err)

		select {
		case <-life.Done():
			return nil
		case <-time.After(coreRetry):
		}
	}
}

// startQueuer begins queueing for absent sessions from st.
func (b *Broker) startQueuer(st *store.Store, cfg config.Config, metrics *obs.Metrics) error {
	q, err := offline.Start(b.life, st, cfg.Session.Expiry, metrics, b.log)
	if err != nil {
		return fmt.Errorf("broker: starting the queuer: %w", err)
	}

	b.queueMu.Lock()
	b.queuer = q
	b.queueMu.Unlock()

	return nil
}

func (b *Broker) stopQueuer() {
	b.queueMu.Lock()
	q := b.queuer
	b.queuer = nil
	b.queueMu.Unlock()

	if q != nil {
		q.Stop()
	}
}

// startMQTTSide opens the store and serves MQTT over it: everything a node
// that is not a core does.
func (b *Broker) startMQTTSide(
	ctx context.Context,
	cfg config.Config,
	resolver tenant.Resolver,
	policy tenant.Policy,
	metrics *obs.Metrics,
) error {
	// Opening the store is fatal rather than degraded. A broker that
	// silently drops retained messages and offline queues looks like it is
	// working, which is worse than refusing to start.
	openCtx, cancel := context.WithTimeout(ctx, storeOpenTimeout)
	defer cancel()

	st, err := openStore(openCtx, cfg, b.nats)
	if err != nil {
		return err
	}

	b.store = st

	// An all-in-one node is its own core, and so its own queuer.
	if cfg.Role == config.RoleAllInOne && cfg.Fabric.DurableQoS {
		if err := b.startQueuer(b.store, cfg, metrics); err != nil {
			return err
		}
	}

	return b.serveMQTT(cfg, resolver, policy, metrics)
}

// abort undoes a Start that failed part of the way through.
func (b *Broker) abort() {
	b.stopQueuer()
	b.shutdown()

	if b.store != nil {
		b.store.Close()
	}

	b.nats.Shutdown()
}

// serveMQTT builds the bridge and the MQTT listeners over an open store, and
// starts serving.
func (b *Broker) serveMQTT(
	cfg config.Config,
	resolver tenant.Resolver,
	policy tenant.Policy,
	metrics *obs.Metrics,
) error {
	b.hook = bridge.New(b.nats.Conn(), b.store, resolver, policy, bridge.Options{
		Metrics:          metrics,
		InternalListener: mqttd.InternalListenerID,
		InternalTenant:   tenant.ID(cfg.Tenant.Default),
		// The embedded server's id rather than the configured name: the
		// name defaults to the role, so every edge pod would answer to
		// "mast-edge" and a log line naming one would name them all.
		NodeID:        b.nats.ID(),
		SessionExpiry: cfg.Session.Expiry,
		DurableQoS:    cfg.Fabric.DurableQoS,
	}, b.log)

	server, err := mqttd.New(cfg, b.hook, b.log)
	if err != nil {
		return err
	}

	b.server = server

	// Serve starts the listener goroutines and returns; it does not block.
	if err := b.server.Serve(); err != nil {
		return fmt.Errorf("broker: serving mqtt: %w", err)
	}

	return nil
}
