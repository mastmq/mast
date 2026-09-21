// Package broker assembles a running mast node from its parts: the embedded
// NATS server, the MQTT to NATS bridge, and the MQTT listeners.
//
// It exists so that the command and the tests build a node the same way. A
// test that assembles the pieces by hand is a test of something other than
// what ships.
package broker

import (
	"fmt"
	"log/slog"

	"github.com/mastmq/mast/internal/domain/tenant"
	"github.com/mastmq/mast/internal/infra/bridge"
	"github.com/mastmq/mast/internal/infra/config"
	"github.com/mastmq/mast/internal/infra/mqttd"
	"github.com/mastmq/mast/internal/infra/natsd"
	mqtt "github.com/mochi-mqtt/server/v2"
)

// Broker is a running mast node.
type Broker struct {
	nats   *natsd.Server
	hook   *bridge.Hook
	server *mqtt.Server
	log    *slog.Logger
}

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
	cfg config.Config,
	resolver tenant.Resolver,
	policy tenant.Policy,
	log *slog.Logger,
) (*Broker, error) {
	nats, err := natsd.Start(cfg, log)
	if err != nil {
		return nil, err
	}

	b := &Broker{nats: nats, hook: nil, server: nil, log: log}

	// A core node carries storage and consensus and terminates no MQTT.
	if cfg.Role == config.RoleCore {
		return b, nil
	}

	b.hook = bridge.New(nats.Conn(), resolver, policy, log)

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

// Close stops the node, MQTT first so that no new message enters the bridge
// after the fabric beneath it has gone.
func (b *Broker) Close() {
	if b.server != nil {
		if err := b.server.Close(); err != nil {
			b.log.Warn("closing mqtt server", "error", err)
		}
	}

	b.nats.Shutdown()
}
