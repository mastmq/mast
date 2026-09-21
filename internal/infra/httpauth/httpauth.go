// Package httpauth authenticates and authorizes MQTT connections by asking an
// HTTP service.
//
// This is the seam most deployments will use: mast does not own tenant
// lifecycle, so the service that already knows which device belongs to which
// customer gets to answer both questions. The wire format is plain JSON over
// POST, so the service can be anything.
//
// # Authentication
//
// On CONNECT, mast posts the connect fields to AuthnURL:
//
//	{"client_id":"dev-1","username":"u","password":"p",
//	 "remote_addr":"10.0.0.1:52000","protocol_version":5,"clean_start":true}
//
// and expects
//
//	{"allow":true,"tenant":"acme"}
//
// The tenant in that reply is authoritative for the whole connection. It is
// what every topic is mounted under and what every later authorization
// question carries, and the client can never influence it again.
//
// # Authorization
//
// On each publish and subscribe, mast posts to AuthzURL:
//
//	{"tenant":"acme","client_id":"dev-1","username":"u",
//	 "remote_addr":"10.0.0.1:52000","topic":"a/b","action":"publish"}
//
// and expects {"allow":true}.
//
// # Failure handling
//
// Authentication always fails closed. There is no safe way to admit a
// connection whose tenant is unknown, because the tenant is the isolation
// boundary — "allow on error" would have to invent one.
//
// Authorization failure is configurable through OnError, because the trade is
// real: denying makes a policy-server outage look like a broker outage, and
// allowing makes it look like a security incident. Denying is the default.
package httpauth

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/mastmq/mast/internal/domain/tenant"
)

// Wire selects the request and response shape used with the auth service.
type Wire string

const (
	// WireMast is mast's own shape: the service names the tenant, which is
	// what multi-tenancy needs.
	WireMast Wire = "mast"

	// WireEMQX is the shape EMQX v5's http authn and authz backends use, so
	// an existing auth service written for EMQX works unchanged.
	//
	// Requests carry {token, username, password, client_id} for
	// authentication and {token, username, password, topic, action} for
	// authorization, and a reply is {"result":"allow"|"deny"} with HTTP 200
	// either way. EMQX has no notion of a tenant, so the service cannot name
	// one and mast falls back to Tenant.
	WireEMQX Wire = "emqx"
)

// Valid reports whether w is a wire format mast knows.
func (w Wire) Valid() bool { return w == WireMast || w == WireEMQX }

// FailMode says what to do when the policy server cannot be reached.
type FailMode string

const (
	// FailDeny refuses the operation. An outage looks like a broker outage.
	FailDeny FailMode = "deny"
	// FailAllow permits the operation. An outage looks like an open door.
	FailAllow FailMode = "allow"
)

// Valid reports whether m is a fail mode mast knows.
func (m FailMode) Valid() bool { return m == FailDeny || m == FailAllow }

// Errors reported by this package.
var (
	// ErrNoAuthnURL is returned when the client is built without an
	// authentication endpoint.
	ErrNoAuthnURL = errors.New("httpauth: authn_url is required")
	// ErrBadFailMode is returned for an unrecognized on_error setting.
	ErrBadFailMode = errors.New("httpauth: on_error must be deny or allow")
	// ErrNoTenant is returned when the service allows a connection but names
	// no tenant, which mast cannot act on.
	ErrNoTenant = errors.New("httpauth: service allowed the connection but named no tenant")
	// ErrDenied is returned when the service refuses a connection.
	ErrDenied = errors.New("httpauth: service denied the connection")
	// ErrBadWire is returned for an unrecognized wire format.
	ErrBadWire = errors.New("httpauth: wire must be mast or emqx")
	// ErrNoTenantConfigured is returned when the emqx wire is selected
	// without a tenant to place connections in.
	ErrNoTenantConfigured = errors.New("httpauth: wire emqx requires a tenant")
)

// Options configures a [Client].
type Options struct {
	// Wire selects the request and response shape. Defaults to mast.
	Wire Wire
	// Tenant is the tenant every connection joins under the emqx wire, which
	// has no field for the service to name one.
	Tenant tenant.ID

	// AuthnURL answers authentication. Required.
	AuthnURL string
	// AuthzURL answers authorization. Empty means every authenticated
	// connection may use any topic within its own tenant, which is a
	// reasonable posture when the tenant mount is the only boundary needed.
	AuthzURL string
	// Timeout bounds a single request.
	Timeout time.Duration
	// OnError decides authorization when the service cannot be reached.
	OnError FailMode
	// CacheTTL and CacheSize bound the authorization decision cache. A zero
	// TTL or size disables caching, at the cost of a round trip per publish.
	CacheTTL  time.Duration
	CacheSize int
	// Headers are sent with every request, for a bearer token or similar.
	Headers map[string]string
}

// Client asks an HTTP service to authenticate and authorize connections. It
// implements both [tenant.Resolver] and [tenant.Policy].
type Client struct {
	opts  Options
	http  *http.Client
	cache *cache
	log   *slog.Logger
}

// New builds a client. It validates eagerly so a misconfiguration is a
// startup failure rather than a flood of denials at runtime.
func New(opts Options, log *slog.Logger) (*Client, error) {
	if opts.AuthnURL == "" {
		return nil, ErrNoAuthnURL
	}

	if opts.Wire == "" {
		opts.Wire = WireMast
	}

	if !opts.Wire.Valid() {
		return nil, fmt.Errorf("%w: got %q", ErrBadWire, opts.Wire)
	}

	if opts.Wire == WireEMQX && opts.Tenant == "" {
		return nil, ErrNoTenantConfigured
	}

	if opts.OnError == "" {
		opts.OnError = FailDeny
	}

	if !opts.OnError.Valid() {
		return nil, fmt.Errorf("%w: got %q", ErrBadFailMode, opts.OnError)
	}

	return &Client{
		opts:  opts,
		http:  &http.Client{Timeout: opts.Timeout}, //nolint:exhaustruct_v5 // defaults are correct for the rest
		cache: newCache(opts.CacheTTL, opts.CacheSize),
		log:   log.With("component", "httpauth"),
	}, nil
}

// authnRequest is the body posted to AuthnURL.
type authnRequest struct {
	ClientID        string `json:"client_id"`
	Username        string `json:"username"`
	Password        string `json:"password"`
	RemoteAddr      string `json:"remote_addr"`
	ProtocolVersion byte   `json:"protocol_version"`
	CleanStart      bool   `json:"clean_start"`
}

// authnResponse is what AuthnURL is expected to return.
type authnResponse struct {
	Allow  bool   `json:"allow"`
	Tenant string `json:"tenant"`
}

// authzRequest is the body posted to AuthzURL.
type authzRequest struct {
	Tenant     string `json:"tenant"`
	ClientID   string `json:"client_id"`
	Username   string `json:"username"`
	RemoteAddr string `json:"remote_addr"`
	Topic      string `json:"topic"`
	Action     string `json:"action"`
}

// authzResponse is what AuthzURL is expected to return.
type authzResponse struct {
	Allow bool `json:"allow"`
}

// emqxAuthnRequest is what EMQX v5's http authentication backend posts.
//
// Token repeats the username because that is how EMQX deployments in the
// wild carry a bearer credential: the device puts its token in the username
// field, and the auth service is written to look in either place.
type emqxAuthnRequest struct {
	Token    string `json:"token"`
	Username string `json:"username"`
	Password string `json:"password"`
	ClientID string `json:"client_id"`
}

// emqxAuthzRequest is what EMQX v5's http authorization backend posts.
type emqxAuthzRequest struct {
	Token    string `json:"token"`
	Username string `json:"username"`
	Password string `json:"password"`
	Topic    string `json:"topic"`
	Action   string `json:"action"`
}

// emqxResponse is what both EMQX backends expect back. The status is 200
// whether the answer is allow or deny; only the body decides.
type emqxResponse struct {
	Result      string `json:"result"`
	IsSuperuser bool   `json:"is_superuser"`
}

// allowed reports whether the service said yes.
func (r emqxResponse) allowed() bool { return r.Result == "allow" }

// Resolve implements [tenant.Resolver]. It always fails closed.
func (c *Client) Resolve(ctx context.Context, creds tenant.Credentials) (tenant.ID, error) {
	if c.opts.Wire == WireEMQX {
		return c.resolveEMQX(ctx, creds)
	}

	var reply authnResponse

	err := c.post(ctx, c.opts.AuthnURL, authnRequest{
		ClientID:        creds.ClientID,
		Username:        creds.Username,
		Password:        string(creds.Password),
		RemoteAddr:      creds.RemoteAddr,
		ProtocolVersion: creds.ProtocolVersion,
		CleanStart:      creds.CleanStart,
	}, &reply)
	if err != nil {
		// Deliberately does not log the credentials.
		c.log.Warn("authentication request failed", "client", creds.ClientID, "error", err)

		return "", err
	}

	if !reply.Allow {
		return "", ErrDenied
	}

	if reply.Tenant == "" {
		return "", ErrNoTenant
	}

	return tenant.ID(reply.Tenant), nil
}

// Allows implements [tenant.Policy].
func (c *Client) Allows(ctx context.Context, access tenant.Access) bool {
	// No authorization endpoint means the tenant mount is the only boundary,
	// which is a deliberate posture rather than an oversight.
	if c.opts.AuthzURL == "" {
		return true
	}

	key := cacheKey(access)
	if allow, ok := c.cache.get(key); ok {
		return allow
	}

	allow, err := c.ask(ctx, access)
	if err != nil {
		c.log.Warn("authorization request failed",
			"tenant", string(access.Tenant), "topic", access.Topic, "error", err)

		// An unreachable service is not a decision, so the result is not
		// cached: caching it would extend a blip into a TTL-long outage.
		return c.opts.OnError == FailAllow
	}

	c.cache.put(key, allow)

	return allow
}

// CacheSize reports how many authorization decisions are held, for metrics.
func (c *Client) CacheSize() int { return c.cache.size() }

// cacheKey identifies an authorization question. The separator cannot appear
// in a client id or a tenant, so two different questions cannot collide.
func cacheKey(a tenant.Access) string {
	var sb strings.Builder

	sb.WriteString(string(a.Tenant))
	sb.WriteByte(0)
	sb.WriteString(a.ClientID)
	sb.WriteByte(0)
	sb.WriteString(a.Action())
	sb.WriteByte(0)
	sb.WriteString(a.Topic)

	return sb.String()
}

// post sends body as JSON and decodes a JSON reply. A non-2xx status is an
// error, so the caller applies its own fail policy rather than reading an
// error page as a decision.
func (c *Client) post(ctx context.Context, url string, body, reply any) error {
	encoded, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("httpauth: encoding request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(encoded))
	if err != nil {
		return fmt.Errorf("httpauth: building request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	for k, v := range c.opts.Headers {
		req.Header.Set(k, v)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("httpauth: calling %s: %w", url, err)
	}

	defer func() {
		if cerr := resp.Body.Close(); cerr != nil {
			c.log.Debug("closing response body", "error", cerr)
		}
	}()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("httpauth: %s returned %w", url, statusError(resp.StatusCode))
	}

	if err := json.NewDecoder(resp.Body).Decode(reply); err != nil {
		return fmt.Errorf("httpauth: decoding reply from %s: %w", url, err)
	}

	return nil
}

// statusError turns an HTTP status into a comparable error value.
type statusError int

func (e statusError) Error() string { return fmt.Sprintf("status %d", int(e)) }

// resolveEMQX authenticates against a service written for EMQX.
func (c *Client) resolveEMQX(ctx context.Context, creds tenant.Credentials) (tenant.ID, error) {
	var reply emqxResponse

	err := c.post(ctx, c.opts.AuthnURL, emqxAuthnRequest{
		Token:    creds.Username,
		Username: creds.Username,
		Password: string(creds.Password),
		ClientID: creds.ClientID,
	}, &reply)
	if err != nil {
		// Deliberately does not log the credentials.
		c.log.Warn("authentication request failed", "client", creds.ClientID, "error", err)

		return "", err
	}

	if !reply.allowed() {
		return "", ErrDenied
	}

	return c.opts.Tenant, nil
}

// ask puts one authorization question to the service in the configured wire
// format.
func (c *Client) ask(ctx context.Context, access tenant.Access) (bool, error) {
	if c.opts.Wire == WireEMQX {
		var reply emqxResponse

		err := c.post(ctx, c.opts.AuthzURL, emqxAuthzRequest{
			Token:    access.Username,
			Username: access.Username,
			Password: "",
			Topic:    access.Topic,
			Action:   access.Action(),
		}, &reply)

		return reply.allowed(), err
	}

	var reply authzResponse

	err := c.post(ctx, c.opts.AuthzURL, authzRequest{
		Tenant:     string(access.Tenant),
		ClientID:   access.ClientID,
		Username:   access.Username,
		RemoteAddr: access.RemoteAddr,
		Topic:      access.Topic,
		Action:     access.Action(),
	}, &reply)

	return reply.Allow, err
}
