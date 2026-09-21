package httpauth_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mastmq/mast/internal/domain/tenant"
	"github.com/mastmq/mast/internal/infra/httpauth"
)

func discard() *slog.Logger { return slog.New(slog.DiscardHandler) }

// server is a stand-in policy service that records what it was asked.
type server struct {
	*httptest.Server

	calls   atomic.Int64
	lastReq map[string]any
}

func newServer(t *testing.T, handle func(req map[string]any) (int, any)) *server {
	t.Helper()

	s := &server{Server: nil, calls: atomic.Int64{}, lastReq: nil}

	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.calls.Add(1)

		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decoding request: %v", err)
		}

		s.lastReq = req

		status, body := handle(req)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)

		if err := json.NewEncoder(w).Encode(body); err != nil {
			t.Errorf("encoding reply: %v", err)
		}
	}))

	t.Cleanup(s.Close)

	return s
}

func allowTenant(id string) func(map[string]any) (int, any) {
	return func(map[string]any) (int, any) {
		return http.StatusOK, map[string]any{"allow": true, "tenant": id}
	}
}

func TestResolveAllows(t *testing.T) {
	t.Parallel()

	srv := newServer(t, allowTenant("acme"))

	c, err := httpauth.New(httpauth.Options{AuthnURL: srv.URL, Timeout: time.Second}, discard())
	if err != nil {
		t.Fatalf("building client: %v", err)
	}

	id, err := c.Resolve(context.Background(), tenant.Credentials{
		ClientID:        "dev-1",
		Username:        "u",
		Password:        []byte("p"),
		RemoteAddr:      "10.0.0.1:52000",
		ProtocolVersion: 5,
		CleanStart:      true,
	})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	if id != "acme" {
		t.Errorf("tenant = %q, want acme", id)
	}

	// Every CONNECT field the service might key on must actually arrive.
	for field, want := range map[string]any{
		"client_id":        "dev-1",
		"username":         "u",
		"password":         "p",
		"remote_addr":      "10.0.0.1:52000",
		"protocol_version": float64(5),
		"clean_start":      true,
	} {
		if got := srv.lastReq[field]; got != want {
			t.Errorf("request field %q = %v, want %v", field, got, want)
		}
	}
}

func TestResolveDenies(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		handle func(map[string]any) (int, any)
		want   error
	}{
		{
			"explicit deny",
			func(map[string]any) (int, any) { return http.StatusOK, map[string]any{"allow": false} },
			httpauth.ErrDenied,
		},
		{
			// Allowing without naming a tenant is unusable: the tenant is the
			// isolation boundary, so mast cannot invent one.
			"allow without tenant",
			func(map[string]any) (int, any) { return http.StatusOK, map[string]any{"allow": true} },
			httpauth.ErrNoTenant,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newServer(t, tc.handle)

			c, err := httpauth.New(httpauth.Options{AuthnURL: srv.URL, Timeout: time.Second}, discard())
			if err != nil {
				t.Fatalf("building client: %v", err)
			}

			if _, err := c.Resolve(context.Background(), tenant.Credentials{}); !errors.Is(err, tc.want) {
				t.Errorf("Resolve error = %v, want %v", err, tc.want)
			}
		})
	}
}

// TestResolveFailsClosed pins the rule that there is no fail-open for
// authentication, whatever OnError says: admitting a connection whose tenant
// is unknown would mean inventing an isolation boundary.
func TestResolveFailsClosed(t *testing.T) {
	t.Parallel()

	srv := newServer(t, func(map[string]any) (int, any) {
		return http.StatusInternalServerError, map[string]any{}
	})

	c, err := httpauth.New(httpauth.Options{
		AuthnURL: srv.URL,
		Timeout:  time.Second,
		OnError:  httpauth.FailAllow,
	}, discard())
	if err != nil {
		t.Fatalf("building client: %v", err)
	}

	if _, err := c.Resolve(context.Background(), tenant.Credentials{}); err == nil {
		t.Error("Resolve succeeded against a failing service even though authn must fail closed")
	}
}

func TestAllows(t *testing.T) {
	t.Parallel()

	srv := newServer(t, func(req map[string]any) (int, any) {
		return http.StatusOK, map[string]any{"allow": req["topic"] == "allowed/topic"}
	})

	c, err := httpauth.New(httpauth.Options{
		AuthnURL: srv.URL,
		AuthzURL: srv.URL,
		Timeout:  time.Second,
	}, discard())
	if err != nil {
		t.Fatalf("building client: %v", err)
	}

	access := tenant.Access{
		Tenant: "acme", ClientID: "dev-1", Username: "u",
		RemoteAddr: "10.0.0.1:1", Topic: "allowed/topic", Write: true,
	}

	if !c.Allows(context.Background(), access) {
		t.Error("allowed topic was denied")
	}

	if srv.lastReq["action"] != "publish" {
		t.Errorf("action = %v, want publish", srv.lastReq["action"])
	}

	access.Topic = "other/topic"
	if c.Allows(context.Background(), access) {
		t.Error("denied topic was allowed")
	}

	access.Write = false
	_ = c.Allows(context.Background(), access)

	if srv.lastReq["action"] != "subscribe" {
		t.Errorf("action = %v, want subscribe", srv.lastReq["action"])
	}
}

func TestAllowsFailMode(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		mode httpauth.FailMode
		want bool
	}{
		{httpauth.FailDeny, false},
		{httpauth.FailAllow, true},
	} {
		t.Run(string(tc.mode), func(t *testing.T) {
			t.Parallel()

			srv := newServer(t, func(map[string]any) (int, any) {
				return http.StatusBadGateway, map[string]any{}
			})

			c, err := httpauth.New(httpauth.Options{
				AuthnURL: srv.URL, AuthzURL: srv.URL,
				Timeout: time.Second, OnError: tc.mode,
				CacheTTL: time.Minute, CacheSize: 100,
			}, discard())
			if err != nil {
				t.Fatalf("building client: %v", err)
			}

			if got := c.Allows(context.Background(), tenant.Access{Topic: "a/b"}); got != tc.want {
				t.Errorf("Allows with %s = %v, want %v", tc.mode, got, tc.want)
			}

			// An unreachable service is not a decision. Caching it would turn
			// a blip into an outage lasting a whole TTL.
			if n := c.CacheSize(); n != 0 {
				t.Errorf("failure was cached: %d entries", n)
			}
		})
	}
}

// TestAllowsCaches guards the property that keeps the policy server off the
// hot path: authorization is asked on every publish.
func TestAllowsCaches(t *testing.T) {
	t.Parallel()

	srv := newServer(t, func(map[string]any) (int, any) {
		return http.StatusOK, map[string]any{"allow": true}
	})

	c, err := httpauth.New(httpauth.Options{
		AuthnURL: srv.URL, AuthzURL: srv.URL, Timeout: time.Second,
		CacheTTL: time.Minute, CacheSize: 100,
	}, discard())
	if err != nil {
		t.Fatalf("building client: %v", err)
	}

	access := tenant.Access{Tenant: "acme", ClientID: "dev-1", Topic: "a/b", Write: true}
	for range 50 {
		if !c.Allows(context.Background(), access) {
			t.Fatal("cached decision flipped to deny")
		}
	}

	if n := srv.calls.Load(); n != 1 {
		t.Errorf("policy server called %d times for 50 publishes, want 1", n)
	}

	// A different topic is a different question.
	access.Topic = "c/d"
	_ = c.Allows(context.Background(), access)

	if n := srv.calls.Load(); n != 2 {
		t.Errorf("a distinct topic did not produce a new call: %d calls", n)
	}
}

func TestCacheExpires(t *testing.T) {
	t.Parallel()

	srv := newServer(t, func(map[string]any) (int, any) {
		return http.StatusOK, map[string]any{"allow": true}
	})

	c, err := httpauth.New(httpauth.Options{
		AuthnURL: srv.URL, AuthzURL: srv.URL, Timeout: time.Second,
		CacheTTL: 50 * time.Millisecond, CacheSize: 100,
	}, discard())
	if err != nil {
		t.Fatalf("building client: %v", err)
	}

	access := tenant.Access{Tenant: "acme", ClientID: "dev-1", Topic: "a/b", Write: true}
	_ = c.Allows(context.Background(), access)

	time.Sleep(120 * time.Millisecond)

	_ = c.Allows(context.Background(), access)

	if n := srv.calls.Load(); n != 2 {
		t.Errorf("policy server called %d times across a TTL boundary, want 2", n)
	}
}

func TestNoAuthzURLAllowsWithinTenant(t *testing.T) {
	t.Parallel()

	srv := newServer(t, allowTenant("acme"))

	c, err := httpauth.New(httpauth.Options{AuthnURL: srv.URL, Timeout: time.Second}, discard())
	if err != nil {
		t.Fatalf("building client: %v", err)
	}

	if !c.Allows(context.Background(), tenant.Access{Tenant: "acme", Topic: "anything/#"}) {
		t.Error("with no authz endpoint, a topic inside the tenant should be allowed")
	}

	if n := srv.calls.Load(); n != 0 {
		t.Errorf("authz was called %d times with no authz_url configured", n)
	}
}

func TestHeadersAreSent(t *testing.T) {
	t.Parallel()

	var got atomic.Value

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.Store(r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/json")

		if err := json.NewEncoder(w).Encode(map[string]any{"allow": true, "tenant": "acme"}); err != nil {
			t.Errorf("encoding reply: %v", err)
		}
	}))
	t.Cleanup(srv.Close)

	c, err := httpauth.New(httpauth.Options{
		AuthnURL: srv.URL, Timeout: time.Second,
		Headers: map[string]string{"Authorization": "Bearer secret"},
	}, discard())
	if err != nil {
		t.Fatalf("building client: %v", err)
	}

	if _, err := c.Resolve(context.Background(), tenant.Credentials{}); err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	if got.Load() != "Bearer secret" {
		t.Errorf("Authorization header = %v, want %q", got.Load(), "Bearer secret")
	}
}

func TestNewValidates(t *testing.T) {
	t.Parallel()

	if _, err := httpauth.New(httpauth.Options{}, discard()); !errors.Is(err, httpauth.ErrNoAuthnURL) {
		t.Errorf("missing authn_url error = %v, want %v", err, httpauth.ErrNoAuthnURL)
	}

	_, err := httpauth.New(httpauth.Options{AuthnURL: "http://x", OnError: "maybe"}, discard())
	if !errors.Is(err, httpauth.ErrBadFailMode) {
		t.Errorf("bad on_error error = %v, want %v", err, httpauth.ErrBadFailMode)
	}
}

// TestEMQXWire pins compatibility with an auth service written for EMQX v5.
//
// The shapes here are taken from snapp-incubator/soteria, which is what the
// cluster mast is replacing actually talks to: requests carry token,
// username, password and client_id or topic and action, and the reply is
// {"result":"allow"|"deny"} with HTTP 200 either way.
func TestEMQXWire(t *testing.T) {
	t.Parallel()

	var authnBody, authzBody atomic.Value

	srv := newServer(t, func(req map[string]any) (int, any) {
		if _, isAuthz := req["action"]; isAuthz {
			authzBody.Store(req)

			topic, _ := req["topic"].(string)

			return http.StatusOK, map[string]any{"result": allowDeny(topic == "allowed")}
		}

		authnBody.Store(req)

		username, _ := req["username"].(string)

		return http.StatusOK, map[string]any{
			"result":       allowDeny(username == "internal:tok"),
			"is_superuser": false,
		}
	})

	c, err := httpauth.New(httpauth.Options{
		Wire:     httpauth.WireEMQX,
		Tenant:   "ignite",
		AuthnURL: srv.URL,
		AuthzURL: srv.URL,
		Timeout:  time.Second,
	}, discard())
	if err != nil {
		t.Fatalf("building client: %v", err)
	}

	id, err := c.Resolve(context.Background(), tenant.Credentials{
		ClientID: "dev-1",
		Username: "internal:tok",
		Password: []byte(""),
	})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	// EMQX has no tenant field, so the configured one applies.
	if id != "ignite" {
		t.Errorf("tenant = %q, want the configured ignite", id)
	}

	got, _ := authnBody.Load().(map[string]any)
	for field, want := range map[string]any{
		"client_id": "dev-1",
		"username":  "internal:tok",
		// The token repeats the username: that is how these deployments
		// carry a bearer credential, and the service looks in either place.
		"token": "internal:tok",
	} {
		if got[field] != want {
			t.Errorf("authn body %q = %v, want %v", field, got[field], want)
		}
	}

	_, err = c.Resolve(context.Background(), tenant.Credentials{Username: "bogus"})
	if !errors.Is(err, httpauth.ErrDenied) {
		t.Errorf("a denied token was accepted: %v", err)
	}

	checkEMQXAuthz(t, c, &authzBody)
}

// checkEMQXAuthz covers the authorization half of the EMQX wire.
func checkEMQXAuthz(t *testing.T, c *httpauth.Client, authzBody *atomic.Value) {
	t.Helper()

	if !c.Allows(context.Background(), tenant.Access{
		Tenant: "ignite", Username: "internal:tok", Topic: "allowed", Write: true,
	}) {
		t.Error("an allowed topic was refused")
	}

	acl, _ := authzBody.Load().(map[string]any)
	if acl["action"] != "publish" || acl["topic"] != "allowed" || acl["token"] != "internal:tok" {
		t.Errorf("authz body = %v", acl)
	}

	if c.Allows(context.Background(), tenant.Access{
		Tenant: "ignite", Username: "internal:tok", Topic: "denied", Write: false,
	}) {
		t.Error("a denied topic was permitted")
	}
}

func allowDeny(ok bool) string {
	if ok {
		return "allow"
	}

	return "deny"
}

func TestEMQXWireNeedsTenant(t *testing.T) {
	t.Parallel()

	_, err := httpauth.New(httpauth.Options{
		Wire: httpauth.WireEMQX, AuthnURL: "http://x",
	}, discard())
	if !errors.Is(err, httpauth.ErrNoTenantConfigured) {
		t.Errorf("error = %v, want ErrNoTenantConfigured", err)
	}
}
