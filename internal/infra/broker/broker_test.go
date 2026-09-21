package broker_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/mastmq/mast/internal/domain/tenant"
	"github.com/mastmq/mast/internal/infra/auth"
	"github.com/mastmq/mast/internal/infra/broker"
	"github.com/mastmq/mast/internal/infra/config"
)

const settle = 2 * time.Second

// byUsername resolves each connection to the tenant named in its username,
// which is enough to exercise isolation without an identity system.
type byUsername struct{}

func (byUsername) Resolve(_ context.Context, creds tenant.Credentials) (tenant.Identity, error) {
	if creds.Username == "" {
		return tenant.Identity{}, tenant.ErrUnauthenticated
	}

	return tenant.Identity{Tenant: tenant.ID(creds.Username), Superuser: false}, nil
}

// start brings up an all-in-one node on free ports and returns its MQTT
// address.
func start(t *testing.T, resolver tenant.Resolver) string {
	t.Helper()

	cfg := config.Default()
	cfg.MQTT.Addr = freeAddr(t)
	cfg.NATS.MonitorAddr = ""
	cfg.Core.StoreDir = t.TempDir()

	log := slog.New(slog.DiscardHandler)

	node, err := broker.Start(t.Context(), cfg, resolver, tenant.AllowAll{}, log)
	if err != nil {
		t.Fatalf("starting broker: %v", err)
	}

	t.Cleanup(node.Close)

	return cfg.MQTT.Addr
}

func freeAddr(t *testing.T) string {
	t.Helper()

	var lc net.ListenConfig

	l, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserving port: %v", err)
	}

	addr := l.Addr().String()

	if err := l.Close(); err != nil {
		t.Fatalf("releasing port: %v", err)
	}

	return addr
}

// collector is a connected client that records what it receives.
type collector struct {
	client paho.Client

	mu  sync.Mutex
	got []string
}

func (c *collector) messages() []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]string(nil), c.got...)
}

func connect(t *testing.T, addr, clientID, username string) *collector {
	t.Helper()

	return dial(t, addr, clientID, username, "")
}

// connectWithPassword is for the HTTP backend, whose test service reads the
// tenant out of the password.
func connectWithPassword(t *testing.T, addr, clientID, password string) *collector {
	t.Helper()

	return dial(t, addr, clientID, "user", password)
}

func dial(t *testing.T, addr, clientID, username, password string) *collector {
	t.Helper()

	c := &collector{client: nil, mu: sync.Mutex{}, got: nil}

	opts := paho.NewClientOptions().
		AddBroker("tcp://" + addr).
		SetClientID(clientID).
		SetUsername(username).
		SetPassword(password).
		SetConnectTimeout(5 * time.Second).
		SetAutoReconnect(false)

	c.client = paho.NewClient(opts)

	tok := c.client.Connect()
	if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("connecting %s: %v", clientID, tok.Error())
	}

	t.Cleanup(func() { c.client.Disconnect(250) })

	return c
}

func (c *collector) subscribe(t *testing.T, filter string) {
	t.Helper()

	tok := c.client.Subscribe(filter, 0, func(_ paho.Client, m paho.Message) {
		c.mu.Lock()
		defer c.mu.Unlock()

		c.got = append(c.got, m.Topic()+"="+string(m.Payload()))
	})
	if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribing to %s: %v", filter, tok.Error())
	}
}

func (c *collector) publish(t *testing.T, mqttTopic, payload string) {
	t.Helper()

	tok := c.client.Publish(mqttTopic, 0, false, payload)
	if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("publishing to %s: %v", mqttTopic, tok.Error())
	}
}

// waitFor polls until cond holds or the deadline passes, so a passing test
// does not pay a fixed sleep.
func waitFor(cond func() bool) bool {
	deadline := time.Now().Add(settle)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}

		time.Sleep(10 * time.Millisecond)
	}

	return cond()
}

func TestEndToEnd(t *testing.T) {
	t.Parallel()

	addr := start(t, tenant.Static{Tenant: "acme"})

	cases := []struct {
		name    string
		filter  string
		publish string
		want    string
	}{
		{"exact", "a/b", "a/b", "a/b=hello"},
		{"single wildcard", "a/+/c", "a/x/c", "a/x/c=hello"},
		{"multi wildcard", "a/#", "a/b/c/d", "a/b/c/d=hello"},

		// MQTT's "#" matches the parent level itself. NATS's ">" does not, so
		// this only works because FilterSubjects opens a second subscription.
		{"hash matches parent", "p/#", "p", "p=hello"},

		// A '.' in a topic level is where nats-server's own mapping loses
		// information. The codec must hand back the exact topic.
		{"dot in level", "sensor/#", "sensor/temp.1", "sensor/temp.1=hello"},

		{"leading slash", "/lead/#", "/lead/x", "/lead/x=hello"},
		{"empty middle level", "e/#", "e//f", "e//f=hello"},
		{"non ascii", "fa/#", "fa/دما", "fa/دما=hello"},
	}

	// Deliberately sequential: the cases share one broker and their filters
	// overlap, so "a/#" would otherwise catch the "a/+/c" case's publish.
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sub := connect(t, addr, fmt.Sprintf("sub-%d", i), "acme")
			sub.subscribe(t, tc.filter)

			pub := connect(t, addr, fmt.Sprintf("pub-%d", i), "acme")
			pub.publish(t, tc.publish, "hello")

			if !waitFor(func() bool { return len(sub.messages()) > 0 }) {
				t.Fatalf("filter %q never received a publish to %q", tc.filter, tc.publish)
			}

			if got := sub.messages()[0]; got != tc.want {
				t.Errorf("received %q, want %q", got, tc.want)
			}
		})
	}
}

// TestTenantIsolation is the test that matters. mochi's topic tree has no
// tenant concept, so without the mount every tenant subscribing to "a/b"
// would share one node in it and read each other's traffic.
func TestTenantIsolation(t *testing.T) {
	t.Parallel()

	addr := start(t, byUsername{})

	alice := connect(t, addr, "alice", "tenant-a")
	alice.subscribe(t, "shared/topic")

	bob := connect(t, addr, "bob", "tenant-b")
	bob.subscribe(t, "shared/topic")

	carol := connect(t, addr, "carol", "tenant-a")
	carol.publish(t, "shared/topic", "for-tenant-a-only")

	if !waitFor(func() bool { return len(alice.messages()) > 0 }) {
		t.Fatal("same-tenant subscriber never received the message")
	}

	if got := alice.messages()[0]; got != "shared/topic=for-tenant-a-only" {
		t.Errorf("alice received %q", got)
	}

	// Give a leak every chance to show up before declaring isolation.
	time.Sleep(settle)

	if got := bob.messages(); len(got) != 0 {
		t.Errorf("tenant isolation breached: other tenant received %v", got)
	}
}

// TestSharedSubscription checks the feature NATS's own MQTT listener and
// RabbitMQ both lack: $share maps onto a NATS queue group, so exactly one
// member of the group gets each message.
func TestSharedSubscription(t *testing.T) {
	t.Parallel()

	addr := start(t, tenant.Static{Tenant: "acme"})

	var received atomic.Int64

	for i := range 2 {
		c := connect(t, addr, fmt.Sprintf("worker-%d", i), "acme")

		tok := c.client.Subscribe("$share/group1/work/items", 0, func(_ paho.Client, _ paho.Message) {
			received.Add(1)
		})
		if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
			t.Fatalf("subscribing worker %d: %v", i, tok.Error())
		}
	}

	pub := connect(t, addr, "producer", "acme")

	const sent = 6
	for i := range sent {
		pub.publish(t, "work/items", fmt.Sprintf("job-%d", i))
	}

	if !waitFor(func() bool { return received.Load() >= sent }) {
		t.Fatalf("group received %d of %d messages", received.Load(), sent)
	}

	// Each message must be delivered once across the group, not once per
	// member. Wait past the arrival of the last one to catch duplicates.
	time.Sleep(settle)

	if got := received.Load(); got != sent {
		t.Errorf("group received %d messages, want exactly %d (duplicated delivery)", got, sent)
	}
}

// TestSubscriptionsAreDeduplicated guards the property that makes the node
// scale: NATS subscriptions track distinct filters, not devices.
func TestSubscriptionsAreDeduplicated(t *testing.T) {
	t.Parallel()

	cfg := config.Default()
	cfg.MQTT.Addr = freeAddr(t)
	cfg.NATS.MonitorAddr = ""
	cfg.Core.StoreDir = t.TempDir()

	log := slog.New(slog.DiscardHandler)

	node, err := broker.Start(t.Context(), cfg, tenant.Static{Tenant: "acme"}, tenant.AllowAll{}, log)
	if err != nil {
		t.Fatalf("starting broker: %v", err)
	}

	t.Cleanup(node.Close)

	const devices = 20
	for i := range devices {
		c := connect(t, cfg.MQTT.Addr, fmt.Sprintf("device-%d", i), "acme")
		c.subscribe(t, "fleet/telemetry")
	}

	if !waitFor(func() bool { return node.Subscriptions() == 1 }) {
		t.Errorf("%d devices on one filter produced %d NATS subscriptions, want 1",
			devices, node.Subscriptions())
	}
}

// TestHTTPAuthEndToEnd drives a real MQTT client against a node whose
// authentication and authorization come from an HTTP service, which is how
// most deployments will run it.
// policyServer stands in for a deployment's own auth service. It reads the
// tenant out of the password and refuses the "forbidden/" subtree.
func policyServer(t *testing.T) *httptest.Server {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decoding request: %v", err)
		}

		w.Header().Set("Content-Type", "application/json")

		var reply map[string]any

		if _, isAuthz := req["action"]; isAuthz {
			// Nobody may touch the forbidden subtree.
			topic, _ := req["topic"].(string)
			reply = map[string]any{"allow": !strings.HasPrefix(topic, "forbidden/")}
		} else {
			// The password is the tenant; anything else is refused.
			password, _ := req["password"].(string)
			reply = map[string]any{"allow": password != "", "tenant": password}
		}

		if err := json.NewEncoder(w).Encode(reply); err != nil {
			t.Errorf("encoding reply: %v", err)
		}
	}))

	t.Cleanup(srv.Close)

	return srv
}

func TestHTTPAuthEndToEnd(t *testing.T) {
	t.Parallel()

	policy := policyServer(t)

	cfg := config.Default()
	cfg.MQTT.Addr = freeAddr(t)
	cfg.NATS.MonitorAddr = ""
	cfg.Core.StoreDir = t.TempDir()
	cfg.Auth.Mode = config.AuthHTTP
	cfg.Auth.HTTP.AuthnURL = policy.URL
	cfg.Auth.HTTP.AuthzURL = policy.URL

	log := slog.New(slog.DiscardHandler)

	resolver, pol, err := auth.Build(cfg, log)
	if err != nil {
		t.Fatalf("building auth: %v", err)
	}

	node, err := broker.Start(t.Context(), cfg, resolver, pol, log)
	if err != nil {
		t.Fatalf("starting broker: %v", err)
	}

	t.Cleanup(node.Close)

	// Deliberately sequential: the subtests share one broker and one auth
	// service, and the isolation case depends on what ran before it.
	t.Run("rejects bad credentials", func(t *testing.T) {
		opts := paho.NewClientOptions().
			AddBroker("tcp://" + cfg.MQTT.Addr).
			SetClientID("nobody").
			SetPassword("").
			SetConnectTimeout(5 * time.Second).
			SetAutoReconnect(false)

		tok := paho.NewClient(opts).Connect()
		tok.WaitTimeout(5 * time.Second)

		if tok.Error() == nil {
			t.Error("a connection the auth service refused was accepted")
		}
	})

	t.Run("delivers within the tenant the service named", func(t *testing.T) {
		sub := connectWithPassword(t, cfg.MQTT.Addr, "http-sub", "tenant-x")
		sub.subscribe(t, "data/#")

		pub := connectWithPassword(t, cfg.MQTT.Addr, "http-pub", "tenant-x")
		pub.publish(t, "data/reading", "42")

		if !waitFor(func() bool { return len(sub.messages()) > 0 }) {
			t.Fatal("message was not delivered within the tenant")
		}
	})

	t.Run("isolates tenants the service named differently", func(t *testing.T) {
		other := connectWithPassword(t, cfg.MQTT.Addr, "http-other", "tenant-y")
		other.subscribe(t, "data/#")

		pub := connectWithPassword(t, cfg.MQTT.Addr, "http-pub2", "tenant-x")
		pub.publish(t, "data/reading", "42")

		time.Sleep(settle)

		if got := other.messages(); len(got) != 0 {
			t.Errorf("a differently-tenanted client received %v", got)
		}
	})

	t.Run("denies a topic the service refuses", func(t *testing.T) {
		sub := connectWithPassword(t, cfg.MQTT.Addr, "http-sub3", "tenant-z")
		sub.subscribe(t, "forbidden/#")

		pub := connectWithPassword(t, cfg.MQTT.Addr, "http-pub3", "tenant-z")
		pub.publish(t, "forbidden/secret", "nope")

		time.Sleep(settle)

		if got := sub.messages(); len(got) != 0 {
			t.Errorf("a topic the policy server denied was delivered: %v", got)
		}
	})
}
