// Package mqttd builds the mochi-mqtt server that terminates device
// connections.
package mqttd

import (
	"crypto/tls"
	"fmt"
	"log/slog"
	"math"

	"github.com/mastmq/mast/internal/infra/bridge"
	"github.com/mastmq/mast/internal/infra/config"
	mqtt "github.com/mastmq/mochi/v2"
	"github.com/mastmq/mochi/v2/listeners"
)

// InternalListenerID names the unauthenticated listener. The bridge matches
// on it to decide whether a connection has to prove anything.
const InternalListenerID = "internal"

// New builds an MQTT server with the bridge attached and the configured
// listeners registered. The server is not yet serving when it returns.
func New(cfg config.Config, hook *bridge.Hook, log *slog.Logger) (*mqtt.Server, error) {
	opts := &mqtt.Options{
		// The inline client is how the bridge injects messages that arrived
		// from NATS, so it is not optional here.
		InlineClient: true,
		Logger:       log.With("component", "mqtt"),
	}

	server := mqtt.New(opts)
	server.Options.Capabilities.MaximumClientWritesPending = clampWrites(cfg.MQTT.MaxWritesPending)

	hook.Attach(server)

	if err := server.AddHook(hook, nil); err != nil {
		return nil, fmt.Errorf("mqttd: adding bridge hook: %w", err)
	}

	tlsConfig, err := tlsConfig(cfg)
	if err != nil {
		return nil, err
	}

	if cfg.MQTT.Addr != "" {
		l := listeners.NewTCP(listeners.Config{
			Type:      listeners.TypeTCP,
			ID:        "tcp",
			Address:   cfg.MQTT.Addr,
			TLSConfig: tlsConfig,
		})
		if err := server.AddListener(l); err != nil {
			return nil, fmt.Errorf("mqttd: adding tcp listener on %s: %w", cfg.MQTT.Addr, err)
		}
	}

	// The internal listener deliberately carries no TLS: it is for in-cluster
	// traffic that the network already protects, and giving it a certificate
	// would imply a trust boundary it does not have.
	if cfg.MQTT.InternalAddr != "" {
		l := listeners.NewTCP(listeners.Config{
			Type:      listeners.TypeTCP,
			ID:        InternalListenerID,
			Address:   cfg.MQTT.InternalAddr,
			TLSConfig: nil,
		})
		if err := server.AddListener(l); err != nil {
			return nil, fmt.Errorf("mqttd: adding internal listener on %s: %w", cfg.MQTT.InternalAddr, err)
		}
	}

	if cfg.MQTT.WSAddr != "" {
		l := listeners.NewWebsocket(listeners.Config{
			Type:      listeners.TypeWS,
			ID:        "ws",
			Address:   cfg.MQTT.WSAddr,
			TLSConfig: tlsConfig,
		})
		if err := server.AddListener(l); err != nil {
			return nil, fmt.Errorf("mqttd: adding websocket listener on %s: %w", cfg.MQTT.WSAddr, err)
		}
	}

	return server, nil
}

// clampWrites narrows the configured queue bound to the int32 mochi stores it
// in. A value this large is a misconfiguration either way, but silently
// wrapping it negative would turn that into a hang.
func clampWrites(n int) int32 {
	if n < 0 {
		return 0
	}

	if n > math.MaxInt32 {
		return math.MaxInt32
	}

	return int32(n)
}

// tlsConfig loads the listener certificate, returning nil when none is
// configured. Both halves must be present or neither.
func tlsConfig(cfg config.Config) (*tls.Config, error) {
	if cfg.MQTT.TLSCert == "" && cfg.MQTT.TLSKey == "" {
		return nil, nil //nolint:nilnil // no certificate configured is not an error
	}

	if cfg.MQTT.TLSCert == "" || cfg.MQTT.TLSKey == "" {
		return nil, ErrIncompleteTLS
	}

	cert, err := tls.LoadX509KeyPair(cfg.MQTT.TLSCert, cfg.MQTT.TLSKey)
	if err != nil {
		return nil, fmt.Errorf("mqttd: loading tls keypair: %w", err)
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}, nil
}
