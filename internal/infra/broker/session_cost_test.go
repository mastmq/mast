package broker_test

import (
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/mastmq/mast/internal/domain/tenant"
	"github.com/mastmq/mast/internal/infra/broker"
	"github.com/mastmq/mast/internal/infra/config"
	"github.com/mastmq/mast/internal/infra/store"
)

// TestOnlyLastingSessionsAreStored covers what a session costs the store.
//
// Every write to a bucket is a replicated write on the core, and a clean
// session used to cost several: its subscriptions were persisted on every
// SUBSCRIBE although nothing could ever restore them, and it was deleted on
// CONNECT and again on DISCONNECT whether or not anything was there. A bench
// run of 20k clean clients queued those writes past their five-second
// deadline, and since CONNECT waited for them, one connection in five timed
// out.
func TestOnlyLastingSessionsAreStored(t *testing.T) {
	t.Parallel()

	cfg := config.Default()
	cfg.MQTT.Addr = freeAddr(t)
	cfg.NATS.MonitorAddr = ""
	cfg.Obs.Addr = anyPort
	cfg.Core.StoreDir = t.TempDir()

	node, err := broker.Start(t.Context(), cfg, tenant.Static{Tenant: "acme"}, tenant.AllowAll{},
		slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("starting broker: %v", err)
	}

	t.Cleanup(node.Close)

	clean := connect(t, cfg.MQTT.Addr, "clean-dev", "acme")
	clean.subscribeQoS(t, "fleet/clean-dev", 1)

	lasting := persistent(t, cfg.MQTT.Addr, "lasting-dev", "acme")
	lasting.subscribeQoS(t, "fleet/lasting-dev", 1)

	if _, err := node.Store().GetSession(t.Context(), "acme.lasting-dev"); err != nil {
		t.Errorf("a persistent session was not stored: %v", err)
	}

	if _, err := node.Store().GetSession(t.Context(), "acme.clean-dev"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("a clean session was stored, which nothing can ever restore: err=%v", err)
	}
}

// TestLiveSessionOutlastsTheBucketTTL covers a session that stays connected
// longer than session.expiry without changing its subscriptions.
//
// The bucket's TTL counts from the last write, and a session was written
// only on SUBSCRIBE and UNSUBSCRIBE. So a device that held one connection
// for more than a day lost its stored session while still using it, and the
// next time it landed on another node — which a dropped mobile connection
// does routinely — it came back with no subscriptions. The documented rule
// is that expiry counts from disconnect, and this holds the broker to it.
func TestLiveSessionOutlastsTheBucketTTL(t *testing.T) {
	t.Parallel()

	const expiry = 2 * time.Second

	cfg := config.Default()
	cfg.MQTT.Addr = freeAddr(t)
	cfg.NATS.MonitorAddr = ""
	cfg.Obs.Addr = anyPort
	cfg.Core.StoreDir = t.TempDir()
	cfg.Session.Expiry = expiry

	node, err := broker.Start(t.Context(), cfg, tenant.Static{Tenant: "acme"}, tenant.AllowAll{},
		slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("starting broker: %v", err)
	}

	t.Cleanup(node.Close)

	stored := func(key string) bool {
		_, err := node.Store().GetSession(t.Context(), key)

		return err == nil
	}

	longLived := persistent(t, cfg.MQTT.Addr, "long-lived", "acme")
	longLived.subscribeQoS(t, "fleet/long-lived", 1)

	// Twice the expiry, connected the whole time and changing nothing.
	time.Sleep(2 * expiry)

	if !stored("acme.long-lived") {
		t.Error("a connected session expired from the store while it was in use")
	}

	leaver := persistent(t, cfg.MQTT.Addr, "leaver", "acme")
	leaver.subscribeQoS(t, "fleet/leaver", 1)

	// Most of an expiry connected, then away for less than one: expired if
	// the clock started at SUBSCRIBE, alive if it started at DISCONNECT.
	time.Sleep(expiry * 3 / 4)
	leaver.client.Disconnect(100)
	time.Sleep(expiry / 2)

	if !stored("acme.leaver") {
		t.Error("a session expired before session.expiry had passed since its disconnect")
	}
}
