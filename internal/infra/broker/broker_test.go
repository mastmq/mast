package broker_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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
	"github.com/golang-jwt/jwt/v5"
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

// TestMetricsEndpoint covers a setting that used to be a lie: obs.addr was
// configurable, the Helm chart wired a port and a ServiceMonitor to it, and
// nothing served anything there.
func TestMetricsEndpoint(t *testing.T) {
	t.Parallel()

	cfg := config.Default()
	cfg.MQTT.Addr = freeAddr(t)
	cfg.NATS.MonitorAddr = ""
	cfg.Core.StoreDir = t.TempDir()
	cfg.Obs.Addr = freeAddr(t)

	log := slog.New(slog.DiscardHandler)

	node, err := broker.Start(t.Context(), cfg, tenant.Static{Tenant: "acme"}, tenant.AllowAll{}, log)
	if err != nil {
		t.Fatalf("starting broker: %v", err)
	}

	t.Cleanup(node.Close)

	// Generate something worth counting.
	sub := connect(t, cfg.MQTT.Addr, "metrics-sub", "acme")
	sub.subscribe(t, "metrics/#")
	connect(t, cfg.MQTT.Addr, "metrics-pub", "acme").publish(t, "metrics/x", "1")

	if !waitFor(func() bool { return len(sub.messages()) > 0 }) {
		t.Fatal("message not delivered, so there is nothing to count")
	}

	body := scrape(t, "http://"+cfg.Obs.Addr+"/metrics")

	for _, want := range []string{
		`mast_connections_total{tenant="acme"}`,
		`mast_connections_open{tenant="acme"}`,
		`mast_messages_in_total{tenant="acme"}`,
		`mast_messages_out_total{tenant="acme"}`,
		"mast_nats_subscriptions",
		"go_goroutines",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics does not expose %s", want)
		}
	}

	if health := scrape(t, "http://"+cfg.Obs.Addr+"/healthz"); !strings.Contains(health, "ok") {
		t.Errorf("/healthz returned %q", health)
	}

	// pprof earns its place the first time a broker leaks goroutines under
	// load and cannot be redeployed with a debug build.
	if idx := scrape(t, "http://"+cfg.Obs.Addr+"/debug/pprof/"); !strings.Contains(idx, "goroutine") {
		t.Error("/debug/pprof is not served")
	}
}

func scrape(t *testing.T, url string) string {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("building request: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}

	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading %s: %v", url, err)
	}

	return string(body)
}

// TestJWTAuthEndToEnd covers the arrangement most deployments want once they
// have both backends: authentication verified locally from a token, so a
// reconnect storm touches no service, while topic decisions still go to a
// policy server over HTTP.
func TestJWTAuthEndToEnd(t *testing.T) {
	t.Parallel()

	const secret = "shared-signing-secret"

	var aclCalls atomic.Int64

	policy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		aclCalls.Add(1)

		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decoding: %v", err)
		}

		topic, _ := req["topic"].(string)

		w.Header().Set("Content-Type", "application/json")

		if err := json.NewEncoder(w).Encode(map[string]any{
			"result": map[bool]string{true: "allow", false: "deny"}[!strings.HasPrefix(topic, "denied/")],
		}); err != nil {
			t.Errorf("encoding: %v", err)
		}
	}))
	t.Cleanup(policy.Close)

	cfg := config.Default()
	cfg.MQTT.Addr = freeAddr(t)
	cfg.NATS.MonitorAddr = ""
	cfg.Core.StoreDir = t.TempDir()
	cfg.Tenant.Default = "fallback"
	cfg.Auth.Mode = config.AuthJWT
	cfg.Auth.JWT.Algorithms = []string{"HS256"}
	cfg.Auth.JWT.HMACSecret = secret
	cfg.Auth.JWT.TenantClaim = "tenant"
	cfg.Auth.JWT.SuperuserClaim = "admin"
	// Authorization still goes over HTTP: the two backends compose.
	cfg.Auth.HTTP.Wire = "emqx"
	cfg.Auth.HTTP.AuthzURL = policy.URL
	cfg.Auth.HTTP.Timeout = 2 * time.Second

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

	token := func(claims jwt.MapClaims) string {
		t.Helper()

		claims["exp"] = time.Now().Add(time.Hour).Unix()

		signed, signErr := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(secret))
		if signErr != nil {
			t.Fatalf("signing: %v", signErr)
		}

		return signed
	}

	t.Run("an unsigned client is refused", func(t *testing.T) {
		opts := paho.NewClientOptions().AddBroker("tcp://" + cfg.MQTT.Addr).
			SetClientID("no-token").SetUsername("nonsense").
			SetConnectTimeout(5 * time.Second).SetAutoReconnect(false)

		tok := paho.NewClient(opts).Connect()
		tok.WaitTimeout(5 * time.Second)

		if tok.Error() == nil {
			t.Error("a client with no valid token connected")
		}
	})

	t.Run("the tenant comes from the token", func(t *testing.T) {
		sub := connect(t, cfg.MQTT.Addr, "jwt-sub", token(jwt.MapClaims{"tenant": "tenant-a"}))
		sub.subscribe(t, "data/#")

		pub := connect(t, cfg.MQTT.Addr, "jwt-pub", token(jwt.MapClaims{"tenant": "tenant-a"}))
		pub.publish(t, "data/x", "hello")

		if !waitFor(func() bool { return len(sub.messages()) > 0 }) {
			t.Fatal("same-tenant delivery failed")
		}
	})

	t.Run("a different tenant claim is isolated", func(t *testing.T) {
		other := connect(t, cfg.MQTT.Addr, "jwt-other", token(jwt.MapClaims{"tenant": "tenant-b"}))
		other.subscribe(t, "data/#")

		pub := connect(t, cfg.MQTT.Addr, "jwt-pub2", token(jwt.MapClaims{"tenant": "tenant-a"}))
		pub.publish(t, "data/x", "hello")

		time.Sleep(settle)

		if got := other.messages(); len(got) != 0 {
			t.Errorf("a token naming another tenant received %v", got)
		}
	})

	t.Run("the HTTP policy still governs topics", func(t *testing.T) {
		sub := connect(t, cfg.MQTT.Addr, "jwt-denied", token(jwt.MapClaims{"tenant": "tenant-c"}))
		sub.subscribe(t, "denied/#")

		pub := connect(t, cfg.MQTT.Addr, "jwt-pub3", token(jwt.MapClaims{"tenant": "tenant-c"}))
		pub.publish(t, "denied/secret", "nope")

		time.Sleep(settle)

		if got := sub.messages(); len(got) != 0 {
			t.Errorf("a topic the policy denied was delivered: %v", got)
		}

		if aclCalls.Load() == 0 {
			t.Error("the policy server was never consulted")
		}
	})

	t.Run("a superuser token skips the policy", func(t *testing.T) {
		before := aclCalls.Load()

		sub := connect(t, cfg.MQTT.Addr, "jwt-admin",
			token(jwt.MapClaims{"tenant": "tenant-d", "admin": true}))
		sub.subscribe(t, "denied/#")

		pub := connect(t, cfg.MQTT.Addr, "jwt-admin-pub",
			token(jwt.MapClaims{"tenant": "tenant-d", "admin": true}))
		pub.publish(t, "denied/secret", "allowed-for-admin")

		if !waitFor(func() bool { return len(sub.messages()) > 0 }) {
			t.Error("a superuser was denied a topic the policy refuses for others")
		}

		if aclCalls.Load() != before {
			t.Errorf("the policy was consulted %d times for a superuser", aclCalls.Load()-before)
		}
	})
}

// TestConnectionMetrics pins two gauge bugs found while load testing.
//
// Connections on the internal listener were not counted at all, because
// authentication returned early on that path — so a node holding thousands
// of them reported none, and the gauge was blind to the one listener a load
// test is most likely to use.
//
// And a persistent session inflated the count: its disconnect deliberately
// does not forget the session, and the decrement had been attached to that
// same branch, so each reconnect added one and nothing ever subtracted.
func TestConnectionMetrics(t *testing.T) {
	t.Parallel()

	cfg := config.Default()
	cfg.MQTT.Addr = freeAddr(t)
	cfg.MQTT.InternalAddr = freeAddr(t)
	cfg.NATS.MonitorAddr = ""
	cfg.Core.StoreDir = t.TempDir()
	cfg.Obs.Addr = freeAddr(t)
	// The internal listener bypasses the resolver and places its clients in
	// tenant.default, so the two have to agree for both listeners to land in
	// one tenant. A deployment where they disagree has two tenants without
	// meaning to.
	cfg.Tenant.Default = "acme"

	log := slog.New(slog.DiscardHandler)

	node, err := broker.Start(t.Context(), cfg, tenant.Static{Tenant: "acme"}, tenant.AllowAll{}, log)
	if err != nil {
		t.Fatalf("starting broker: %v", err)
	}

	t.Cleanup(node.Close)

	open := func() int {
		t.Helper()

		for line := range strings.SplitSeq(scrape(t, "http://"+cfg.Obs.Addr+"/metrics"), "\n") {
			if after, ok := strings.CutPrefix(line, `mast_connections_open{tenant="acme"} `); ok {
				var n int
				if _, err := fmt.Sscanf(after, "%d", &n); err == nil {
					return n
				}
			}
		}

		return 0
	}

	t.Run("internal listener connections are counted", func(t *testing.T) {
		before := open()

		c := connect(t, cfg.MQTT.InternalAddr, "internal-counted", "")
		if !waitFor(func() bool { return open() == before+1 }) {
			t.Errorf("an internal-listener connection did not move the gauge: %d -> %d", before, open())
		}

		c.client.Disconnect(200)

		if !waitFor(func() bool { return open() == before }) {
			t.Errorf("the gauge did not drop on disconnect: %d, want %d", open(), before)
		}
	})

	t.Run("a persistent session does not inflate the count", func(t *testing.T) {
		before := open()

		for range 3 {
			c := connectAs(t, cfg.MQTT.Addr, "persistent-counted", false)

			if !waitFor(func() bool { return open() == before+1 }) {
				t.Fatalf("connect did not move the gauge: %d, want %d", open(), before+1)
			}

			c.Disconnect(200)

			if !waitFor(func() bool { return open() == before }) {
				t.Fatalf("reconnecting a persistent session left the gauge at %d, want %d", open(), before)
			}
		}
	})
}
