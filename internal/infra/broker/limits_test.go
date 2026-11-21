package broker_test

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	paho5 "github.com/eclipse/paho.golang/paho"
	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/mastmq/mast/internal/domain/tenant"
	"github.com/mastmq/mast/internal/infra/broker"
	"github.com/mastmq/mast/internal/infra/config"
)

// MQTT 5 reason codes the limits answer with. Rate too high is legal only
// in a DISCONNECT, so a refused QoS 1 publish is acknowledged with Quota
// exceeded.
const (
	reasonRateTooHigh   = 0x96
	reasonQuotaExceeded = 0x97
)

// startLimited brings up an all-in-one node with limits set by mutate, and
// returns its MQTT and metrics addresses.
func startLimited(t *testing.T, mutate func(*config.Limits)) (string, string) {
	t.Helper()

	cfg := config.Default()
	cfg.MQTT.Addr = freeAddr(t)
	cfg.NATS.MonitorAddr = ""
	cfg.Obs.Addr = anyPort
	cfg.Core.StoreDir = t.TempDir()
	mutate(&cfg.Limits)

	node, err := broker.Start(t.Context(), cfg, byUsername{}, tenant.AllowAll{}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("starting broker: %v", err)
	}

	t.Cleanup(node.Close)

	return cfg.MQTT.Addr, node.ObsAddr()
}

// connectV5 sends an MQTT 5 CONNECT for tenant acme and returns the CONNACK
// whatever it says, where dialV5 fails the test on anything but success.
func connectV5(t *testing.T, addr, clientID string) *paho5.Connack {
	t.Helper()

	var d net.Dialer

	conn, err := d.DialContext(t.Context(), "tcp", addr)
	if err != nil {
		t.Fatalf("dialling %s: %v", addr, err)
	}

	client := paho5.NewClient(paho5.ClientConfig{Conn: conn})

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	ack, err := client.Connect(ctx, &paho5.Connect{
		ClientID: clientID, Username: "acme", UsernameFlag: true, CleanStart: true, KeepAlive: 30,
	})
	if ack == nil {
		t.Fatalf("connecting %s: no CONNACK: %v", clientID, err)
	}

	if err == nil {
		t.Cleanup(func() { _ = client.Disconnect(&paho5.Disconnect{ReasonCode: 0}) })
	}

	return ack
}

// tryV3 connects an MQTT 3.1.1 client and returns the error the broker
// answered with, if any.
//
//nolint:ireturn // paho's constructor returns the interface
func tryV3(t *testing.T, addr, clientID, tenantName string) (paho.Client, error) {
	t.Helper()

	client := paho.NewClient(paho.NewClientOptions().
		AddBroker("tcp://" + addr).
		SetClientID(clientID).
		SetUsername(tenantName).
		SetConnectTimeout(5 * time.Second).
		SetProtocolVersion(4).
		SetConnectRetry(false).
		SetAutoReconnect(false))

	tok := client.Connect()
	if !tok.WaitTimeout(5 * time.Second) {
		t.Fatalf("connecting %s timed out", clientID)
	}

	if tok.Error() == nil {
		t.Cleanup(func() { client.Disconnect(100) })
	}

	return client, tok.Error() //nolint:wrapcheck // the tests match paho's sentinel errors
}

// TestConnectionLimitRefusesWithQuotaExceeded holds the over-limit CONNACK
// to what each protocol version can say: 0x97 for MQTT 5, and 0x03 for
// 3.1.1, which has no quota code and where "server unavailable" tells a
// device to come back later rather than to doubt its credentials.
func TestConnectionLimitRefusesWithQuotaExceeded(t *testing.T) {
	addr, obsAddr := startLimited(t, func(l *config.Limits) { l.MaxConnections = 2 })

	connect(t, addr, "first", "acme")
	connect(t, addr, "second", "acme")

	if ack := connectV5(t, addr, "third-v5"); ack.ReasonCode != reasonQuotaExceeded {
		t.Errorf("a v5 connection past the limit got CONNACK %#x, want %#x", ack.ReasonCode, reasonQuotaExceeded)
	}

	if _, err := tryV3(t, addr, "third-v3", "acme"); !errors.Is(err, packets.ErrorRefusedServerUnavailable) {
		t.Errorf("a v3 connection past the limit: %v, want server unavailable", err)
	}

	// The limit is the tenant's, not the node's.
	if _, err := tryV3(t, addr, "neighbour", "globex"); err != nil {
		t.Errorf("another tenant was refused for acme's limit: %v", err)
	}

	// A device whose connection is still open here, coming back under the
	// same id, takes it over rather than being counted twice; refusing it
	// would lock out a device whose old connection silently died.
	if _, err := tryV3(t, addr, "first", "acme"); err != nil {
		t.Errorf("a takeover at the limit was refused: %v", err)
	}

	if got := readMetric(t, obsAddr, `mast_limit_exceeded_total{limit="connections",tenant="acme"} `); got != 2 {
		t.Errorf("mast_limit_exceeded_total for connections = %v, want 2", got)
	}
}

// TestConnectionLimitFreesOnDisconnect is the half that leaks if a release
// is ever missed: the limit would fill and never empty.
func TestConnectionLimitFreesOnDisconnect(t *testing.T) {
	addr, _ := startLimited(t, func(l *config.Limits) { l.MaxConnections = 1 })

	first := connect(t, addr, "first", "acme")
	first.client.Disconnect(100)

	if !waitFor(func() bool {
		client, err := tryV3(t, addr, "second", "acme")
		if err == nil {
			client.Disconnect(100)
		}

		return err == nil
	}) {
		t.Fatal("the connection limit was not freed when the only connection went")
	}
}

// TestPublishRateV5 covers a tenant override from configuration and the
// MQTT 5 answer to a QoS 1 publish past the rate: a PUBACK with Quota
// exceeded, so the publisher knows the message was not taken. It once
// expected Message rate too high (0x96), which MQTT 5 allows only in a
// DISCONNECT; paho.golang read it without complaint, and the soak stack's
// mosquitto_pub dropped the connection on it as a protocol error.
func TestPublishRateV5(t *testing.T) {
	addr, obsAddr := startLimited(t, func(l *config.Limits) {
		l.Tenants = map[string]config.TenantLimits{
			"acme": {PublishRate: new(0.01), PublishBurst: new(2)},
		}
	})

	pub := dialV5(t, addr, "chatty")

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	codes := make([]byte, 0, 3)

	for i := range 3 {
		resp, _ := pub.client.Publish(ctx, &paho5.Publish{Topic: "m/" + strconv.Itoa(i), QoS: 1, Payload: []byte("x")})
		if resp == nil {
			t.Fatalf("publish %d got no PUBACK", i)
		}

		codes = append(codes, resp.ReasonCode)
	}

	if !slices.Equal(codes[:2], []byte{0, 0}) || codes[2] != reasonQuotaExceeded {
		t.Errorf("PUBACK codes for a burst of 2 then one more = %#x, want [0 0 %#x]", codes, reasonQuotaExceeded)
	}

	if got := readMetric(t, obsAddr, `mast_limit_exceeded_total{limit="publish_rate",tenant="acme"} `); got != 1 {
		t.Errorf("mast_limit_exceeded_total for publish_rate = %v, want 1", got)
	}

	// The default is unlimited, and acme's override is acme's alone.
	other := connect(t, addr, "other", "globex")
	for i := range 5 {
		tok := other.client.Publish("m", 1, false, strconv.Itoa(i))
		if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
			t.Fatalf("globex publish %d was refused: %v", i, tok.Error())
		}
	}
}

// TestPublishRateV3Disconnects covers MQTT 3.1.1, whose PUBACK cannot
// refuse anything. Acknowledging and dropping would be a loss the device
// never learns of, so the connection is closed and the message resent on
// the next one.
func TestPublishRateV3Disconnects(t *testing.T) {
	addr, _ := startLimited(t, func(l *config.Limits) {
		l.PublishRate = 0.01
		l.PublishBurst = 1
	})

	pub := connect(t, addr, "chatty-v3", "acme")

	tok := pub.client.Publish("m", 1, false, "first")
	if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("the first publish, inside the burst, was refused: %v", tok.Error())
	}

	pub.client.Publish("m", 1, false, "second")

	if !waitFor(func() bool { return !pub.client.IsConnectionOpen() }) {
		t.Error("a v3 client publishing QoS 1 past its rate kept its connection")
	}
}

// TestOfflineQueueLimit sends a returning device only the newest messages
// its tenant may keep. On one node that is mochi's own session being held
// to the limit; TestClusterOfflineQueueLimit covers the stream's queue.
//
// Sixteen is the case where the store runs an eighth over the limit while
// the device is away, and is cut to exactly the limit only as it returns.
func TestOfflineQueueLimit(t *testing.T) {
	for _, tc := range []struct{ keep, sent int }{{3, 10}, {16, 20}} {
		t.Run(strconv.Itoa(tc.keep), func(t *testing.T) {
			offlineQueueLimit(t, tc.keep, tc.sent)
		})
	}
}

func offlineQueueLimit(t *testing.T, keep, sent int) {
	t.Helper()

	addr, obsAddr := startLimited(t, func(l *config.Limits) {
		l.Tenants = map[string]config.TenantLimits{"acme": {OfflineQueue: new(keep)}}
	})

	sleeper := persistent(t, addr, "sleeper", "acme")
	sleeper.subscribeQoS(t, "fleet/sleeper/cmd", 1)
	sleeper.client.Disconnect(100)

	pub := connect(t, addr, "dispatcher", "acme")
	for i := range sent {
		tok := pub.client.Publish("fleet/sleeper/cmd", 1, false, strconv.Itoa(i))
		if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
			t.Fatalf("publish %d: %v", i, tok.Error())
		}
	}

	if !awaitMetric(t, obsAddr, `mast_offline_queued_total{tenant="acme"} `, float64(sent)) {
		t.Fatalf("only %v of %d messages were queued",
			readMetric(t, obsAddr, `mast_offline_queued_total{tenant="acme"} `), sent)
	}

	back := persistent(t, addr, "sleeper", "acme")

	want := make([]string, 0, keep)
	for i := sent - keep; i < sent; i++ {
		want = append(want, "fleet/sleeper/cmd="+strconv.Itoa(i))
	}

	slices.Sort(want)

	waitFor(func() bool { return len(back.messages()) >= keep })

	// Anything else that arrives would be a message past the limit; give it
	// the chance to before judging.
	time.Sleep(200 * time.Millisecond)

	// Sorted: a device resuming on the node it left is resent mochi's
	// inflight store, which mochi orders only to the second. Which messages
	// arrive is what the limit decides; their order is not.
	got := slices.DeleteFunc(back.messages(), func(m string) bool { return !strings.HasPrefix(m, "fleet/") })
	slices.Sort(got)

	if !slices.Equal(got, want) {
		t.Errorf("a returning device was sent %v, want the newest %d: %v", got, keep, want)
	}

	discarded := readMetric(t, obsAddr, `mast_limit_exceeded_total{limit="offline_queue",tenant="acme"} `)
	if discarded != float64(sent-keep) {
		t.Errorf("mast_limit_exceeded_total for offline_queue = %v, want %d", discarded, sent-keep)
	}
}

// TestClusterConnectionLimit is the reason the count is shared: a tenant
// with one connection on each of two edges is at a limit of two everywhere,
// though neither edge holds two itself.
func TestClusterConnectionLimit(t *testing.T) {
	c := startCluster(t, func(cfg *config.Config) {
		cfg.Limits.MaxConnections = 2
		cfg.Limits.SyncInterval = 100 * time.Millisecond
	})

	connect(t, c.addrs[0], "on-a", "acme")
	connect(t, c.addrs[1], "on-b", "acme")

	// Until edge a hears edge b's report it admits a second connection of
	// its own, which it then releases; the limit is reached once it has.
	refused := waitForLeaf(func() bool {
		client, err := tryV3(t, c.addrs[0], "third", "acme")
		if err == nil {
			client.Disconnect(100)

			return false
		}

		return errors.Is(err, packets.ErrorRefusedServerUnavailable)
	})
	if !refused {
		t.Fatal("edge a never counted the connection acme holds on edge b")
	}
}

// TestClusterOfflineQueueLimit covers a device that comes back on another
// edge, which is sent its queue from the stream. The stream keeps the
// default hundred; the tenant may keep three, and the drain sends three.
func TestClusterOfflineQueueLimit(t *testing.T) {
	const keep, sent = 3, 10

	c := startCluster(t, func(cfg *config.Config) {
		cfg.Limits.Tenants = map[string]config.TenantLimits{"acme": {OfflineQueue: new(keep)}}
	})

	sleeper := persistent(t, c.addrs[0], "sleeper", "acme")
	sleeper.subscribeQoS(t, "fleet/sleeper/cmd", 1)
	c.awaitInterest(t, sleeper, "acme")
	sleeper.client.Disconnect(100)

	if !waitForLeaf(func() bool {
		sess, err := c.edges[1].Store().GetSession(t.Context(), "acme.sleeper")

		return err == nil && !sess.Present
	}) {
		t.Fatal("the session was never recorded as away")
	}

	pub := connect(t, c.addrs[1], "dispatcher", "acme")
	for i := range sent {
		tok := pub.client.Publish("fleet/sleeper/cmd", 1, false, strconv.Itoa(i))
		if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
			t.Fatalf("publish %d: %v", i, tok.Error())
		}
	}

	awaitMetric(t, c.core.ObsAddr(), `mast_offline_queued_total{tenant="acme"} `, sent)

	back := persistent(t, c.addrs[1], "sleeper", "acme")

	fleet := func() []string {
		return slices.DeleteFunc(back.messages(), func(m string) bool { return !strings.HasPrefix(m, "fleet/") })
	}

	waitForLeaf(func() bool { return len(fleet()) >= keep })

	if !awaitMetric(t, c.obs[1], `mast_limit_exceeded_total{limit="offline_queue",tenant="acme"} `, sent-keep) {
		t.Errorf("edge b discarded %v queued messages, want %d",
			readMetric(t, c.obs[1], `mast_limit_exceeded_total{limit="offline_queue",tenant="acme"} `), sent-keep)
	}

	want := []string{"fleet/sleeper/cmd=7", "fleet/sleeper/cmd=8", "fleet/sleeper/cmd=9"}
	if got := fleet(); !slices.Equal(got, want) {
		t.Errorf("a device returning on another edge was sent %v, want the newest %d: %v", got, keep, want)
	}
}
