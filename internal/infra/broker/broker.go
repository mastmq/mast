// Package broker assembles a running mast node from its parts: the embedded
// NATS server, the MQTT to NATS bridge, and the MQTT listeners.
//
// It exists so that the command and the tests build a node the same way. A
// test that assembles the pieces by hand is a test of something other than
// what ships.
package broker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/mastmq/mast/internal/domain/tenant"
	"github.com/mastmq/mast/internal/infra/bridge"
	"github.com/mastmq/mast/internal/infra/config"
	"github.com/mastmq/mast/internal/infra/mqttd"
	"github.com/mastmq/mast/internal/infra/natsd"
	"github.com/mastmq/mast/internal/infra/obs"
	"github.com/mastmq/mast/internal/infra/store"
	mqtt "github.com/mastmq/mochi/v2"
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
	log    *slog.Logger
}

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

	b := &Broker{nats: nats, hook: nil, server: nil, store: nil, obs: nil, log: log}

	// A core node carries storage and consensus and terminates no MQTT.
	if cfg.Role == config.RoleCore {
		return b, nil
	}

	// Opening the store is fatal rather than degraded. A broker that
	// silently drops retained messages and offline queues looks like it is
	// working, which is worse than refusing to start.
	openCtx, cancel := context.WithTimeout(ctx, storeOpenTimeout)
	defer cancel()

	b.store, err = store.Open(openCtx, nats.Conn(), cfg.Core.Replicas, cfg.Session.Expiry)
	if err != nil {
		nats.Shutdown()

		return nil, err
	}

	// The subscription gauge reads through the hook, so the registry is
	// built before it and handed the accessor.
	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.NewGoCollector())
	//nolint:exhaustruct_v5 // the collector's defaults are what we want
	registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	metrics := obs.NewMetrics(registry, sources(b, nats))

	b.hook = bridge.New(nats.Conn(), b.store, resolver, policy, bridge.Options{
		Metrics:          metrics,
		InternalListener: mqttd.InternalListenerID,
		InternalTenant:   tenant.ID(cfg.Tenant.Default),
	}, log)

	b.obs = obs.Serve(cfg.Obs.Addr, registry, log)

	b.server, err = mqttd.New(cfg, b.hook, log)
	if err != nil {
		nats.Shutdown()

		return nil, err
	}

	// Serve starts the listener goroutines and returns; it does not block.
	if err := b.server.Serve(); err != nil {
		nats.Shutdown()

		return nil, fmt.Errorf("broker: serving mqtt: %w", err)
	}

	return b, nil
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

// Close stops the node, MQTT first so that no new message enters the bridge
// after the fabric beneath it has gone.
func (b *Broker) Close() {
	b.obs.Close()

	if b.server != nil {
		if err := b.server.Close(); err != nil {
			b.log.Warn("closing mqtt server", "error", err)
		}
	}

	b.nats.Shutdown()
}
