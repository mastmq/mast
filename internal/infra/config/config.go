// Package config loads mast's configuration, layering hardcoded defaults, an
// optional file, and environment variables in that order.
package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/knadh/koanf/parsers/toml/v2"
	"github.com/knadh/koanf/providers/env/v2"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/providers/structs"
	"github.com/knadh/koanf/v2"
)

// EnvPrefix is the prefix every mast environment variable carries. Nesting is
// spelled with a double underscore, so MAST__MQTT__ADDR sets mqtt.addr.
const EnvPrefix = "MAST__"

// defaultSessionExpiry is how long a disconnected session and its queued
// messages survive. Long enough for a device on a bad link or a rolling
// deploy, short enough that decommissioned hardware releases storage.
const defaultSessionExpiry = 24 * time.Hour

// defaultAuthTimeout bounds a single call to the auth service. It is short
// on purpose: this call sits in front of every CONNECT.
const defaultAuthTimeout = 2 * time.Second

// defaultAuthCacheTTL is how long an authorization decision is reused.
const defaultAuthCacheTTL = time.Minute

// defaultAuthCacheSize bounds the authorization cache. At roughly a hundred
// bytes an entry this is single-digit megabytes, which is cheap next to
// asking the network on every publish.
const defaultAuthCacheSize = 100_000

// defaultConnectTimeout bounds how long a client may take to complete CONNECT.
const defaultConnectTimeout = 10 * time.Second

// defaultMaxWritesPending bounds the per-client outbound queue by default. It
// is the main driver of memory at high connection counts.
const defaultMaxWritesPending = 1024

// Role selects which job a mast process does in a cluster.
//
// The split exists because the two jobs have opposite operational shapes. An
// edge process scales with connection count, restarts on every deploy, and
// holds no durable state. A core process holds Raft and the KV buckets, wants
// stable peers and a volume, and must not be rescheduled casually. Running
// both in one process is correct for a single site and wrong at scale.
type Role string

const (
	// RoleAllInOne runs both jobs in one process with file-backed storage and
	// no cluster. This is the default, and the right answer for an edge site
	// or a laptop.
	RoleAllInOne Role = "all-in-one"

	// RoleCore holds Raft and the KV buckets. Stable peers, persistent volume,
	// no MQTT listeners.
	RoleCore Role = "core"

	// RoleEdge terminates MQTT and joins the cluster as a leaf node. Stateless
	// and autoscaled.
	RoleEdge Role = "edge"
)

// Valid reports whether r is a role mast knows how to run.
func (r Role) Valid() bool {
	switch r {
	case RoleAllInOne, RoleCore, RoleEdge:
		return true
	default:
		return false
	}
}

// Roles lists every valid role, for help text and validation messages.
func Roles() []Role { return []Role{RoleAllInOne, RoleCore, RoleEdge} }

// Config is the whole configuration tree.
type Config struct {
	Role    Role    `json:"role"    koanf:"role"`
	Log     Log     `json:"log"     koanf:"log"`
	MQTT    MQTT    `json:"mqtt"    koanf:"mqtt"`
	NATS    NATS    `json:"nats"    koanf:"nats"`
	Core    Core    `json:"core"    koanf:"core"`
	Edge    Edge    `json:"edge"    koanf:"edge"`
	Obs     Observ  `json:"obs"     koanf:"obs"`
	Tenant  Tenant  `json:"tenant"  koanf:"tenant"`
	Auth    Auth    `json:"auth"    koanf:"auth"`
	Session Session `json:"session" koanf:"session"`
}

// Session configures what mast keeps for a client between connections.
type Session struct {
	// Expiry is how long a session and its queued messages survive after the
	// client goes away. It bounds what an abandoned device can hold.
	Expiry time.Duration `json:"expiry" koanf:"expiry"`
}

// AuthMode selects how connections are authenticated and authorized.
type AuthMode string

const (
	// AuthStatic puts every connection in one tenant and allows everything.
	// It is the default, and appropriate only where the deployment itself is
	// the security boundary.
	AuthStatic AuthMode = "static"

	// AuthHTTP asks an HTTP service, posting the MQTT fields as JSON.
	AuthHTTP AuthMode = "http"
)

// Valid reports whether m is an authentication mode mast knows.
func (m AuthMode) Valid() bool { return m == AuthStatic || m == AuthHTTP }

// AuthModes lists every valid mode, for help text and error messages.
func AuthModes() []AuthMode { return []AuthMode{AuthStatic, AuthHTTP} }

// Auth configures authentication and authorization.
type Auth struct {
	Mode AuthMode       `json:"mode" koanf:"mode"`
	HTTP AuthHTTPConfig `json:"http" koanf:"http"`
}

// AuthHTTPConfig configures the HTTP backend.
type AuthHTTPConfig struct {
	// Wire selects the request and response shape: "mast", where the service
	// names the tenant, or "emqx", which matches EMQX v5's http backends so
	// an auth service written for EMQX works unchanged.
	Wire string `json:"wire" koanf:"wire"`

	// AuthnURL answers authentication, and is required in http mode.
	AuthnURL string `json:"authn_url" koanf:"authn_url"`

	// AuthzURL answers authorization. Leaving it empty means an
	// authenticated connection may use any topic inside its own tenant,
	// which is a coherent posture when the tenant mount is boundary enough.
	AuthzURL string `json:"authz_url" koanf:"authz_url"`

	Timeout time.Duration `json:"timeout" koanf:"timeout"`

	// OnError decides authorization when the service cannot be reached:
	// "deny" makes an outage look like a broker outage, "allow" makes it look
	// like an open door. Authentication always fails closed regardless.
	OnError string `json:"on_error" koanf:"on_error"`

	// CacheTTL and CacheSize bound the authorization decision cache.
	// Authorization is asked on every publish, so a zero TTL costs a network
	// round trip per message.
	CacheTTL  time.Duration `json:"cache_ttl"  koanf:"cache_ttl"`
	CacheSize int           `json:"cache_size" koanf:"cache_size"`

	// Headers are sent with every request, for a bearer token or similar.
	Headers map[string]string `json:"headers" koanf:"headers"`
}

// Tenant configures how connections are mapped to tenants.
type Tenant struct {
	// Default is the tenant every connection resolves to while mast ships
	// only the static resolver. A real deployment replaces the resolver with
	// one backed by its own identity system.
	Default string `json:"default" koanf:"default"`
}

// Log configures the slog handler.
type Log struct {
	Level  string `json:"level"  koanf:"level"`
	Format string `json:"format" koanf:"format"`
}

// MQTT configures the listeners devices connect to. Only read in the edge and
// all-in-one roles.
type MQTT struct {
	Addr    string `json:"addr"     koanf:"addr"`
	WSAddr  string `json:"ws_addr"  koanf:"ws_addr"`
	TLSCert string `json:"tls_cert" koanf:"tls_cert"`
	TLSKey  string `json:"tls_key"  koanf:"tls_key"`

	// InternalAddr is a second listener that skips authentication and
	// authorization entirely. It exists so trusted in-cluster services can
	// connect without a credential, mirroring the internal listener an EMQX
	// deployment typically runs. It must never be exposed outside the
	// cluster: whatever reaches it is trusted completely.
	InternalAddr string `json:"internal_addr" koanf:"internal_addr"`

	// MaxWritesPending bounds the per-client outbound queue. It is the main
	// driver of memory at high connection counts: budget it times the expected
	// connections per process, not times the fleet.
	MaxWritesPending int           `json:"max_writes_pending" koanf:"max_writes_pending"`
	ConnectTimeout   time.Duration `json:"connect_timeout"    koanf:"connect_timeout"`
}

// NATS configures the embedded nats-server and the in-process client.
type NATS struct {
	// Name is this server's name inside the cluster.
	Name string `json:"name" koanf:"name"`
	// ClientAddr is the client listener. Empty means in-process only, which is
	// what an edge process wants unless you need the nats CLI against it.
	ClientAddr string `json:"client_addr" koanf:"client_addr"`
	// MonitorAddr serves the HTTP monitoring endpoints. Keep it on: without it
	// you lose all visibility into your own data plane.
	MonitorAddr string `json:"monitor_addr" koanf:"monitor_addr"`
}

// Core configures the Raft and storage tier.
type Core struct {
	// Routes are the other core peers to mesh with.
	Routes []string `json:"routes" koanf:"routes"`
	// ListenAddr is the cluster route listener.
	ListenAddr string `json:"listen_addr" koanf:"listen_addr"`
	// LeafAddr accepts leaf-node connections from edge processes.
	LeafAddr string `json:"leaf_addr" koanf:"leaf_addr"`
	// StoreDir holds JetStream state. Must be a persistent volume.
	StoreDir string `json:"store_dir" koanf:"store_dir"`
	// Replicas is the replication factor for the KV buckets.
	Replicas int `json:"replicas" koanf:"replicas"`
}

// Edge configures how an edge process reaches the core tier.
type Edge struct {
	// CoreURLs are the leaf-node endpoints of the core tier.
	CoreURLs []string `json:"core_urls" koanf:"core_urls"`
	// Credentials is a path to a NATS credentials file, if the core tier
	// requires one.
	Credentials string `json:"credentials" koanf:"credentials"`
}

// Observ configures metrics and profiling.
type Observ struct {
	// Addr serves /metrics and, because it costs nothing and pays for itself
	// the first time a goroutine leaks, net/http/pprof.
	Addr string `json:"addr" koanf:"addr"`
}

// Default returns the configuration mast uses when nothing overrides it: a
// single all-in-one process that needs no flags and no cluster.
func Default() Config {
	return Config{
		Role: RoleAllInOne,
		Log: Log{
			Level:  "info",
			Format: "text",
		},
		MQTT: MQTT{
			Addr:             ":1883",
			WSAddr:           "",
			InternalAddr:     "",
			TLSCert:          "",
			TLSKey:           "",
			MaxWritesPending: defaultMaxWritesPending,
			ConnectTimeout:   defaultConnectTimeout,
		},
		NATS: NATS{
			Name:        "",
			ClientAddr:  "",
			MonitorAddr: "127.0.0.1:8222",
		},
		Core: Core{
			Routes:     nil,
			ListenAddr: "",
			LeafAddr:   "",
			StoreDir:   "./data",
			Replicas:   1,
		},
		Edge: Edge{
			CoreURLs:    nil,
			Credentials: "",
		},
		Obs: Observ{
			Addr: "127.0.0.1:9090",
		},
		Tenant: Tenant{
			Default: "default",
		},
		Session: Session{
			Expiry: defaultSessionExpiry,
		},
		Auth: Auth{
			Mode: AuthStatic,
			HTTP: AuthHTTPConfig{
				Wire:      "mast",
				AuthnURL:  "",
				AuthzURL:  "",
				Timeout:   defaultAuthTimeout,
				OnError:   "deny",
				CacheTTL:  defaultAuthCacheTTL,
				CacheSize: defaultAuthCacheSize,
				Headers:   nil,
			},
		},
	}
}

// Load builds the configuration from defaults, then the file at path when it
// is non-empty, then the environment. A missing file at an explicitly given
// path is an error; the defaults alone are a valid configuration.
func Load(path string) (Config, error) {
	k := koanf.New(".")

	if err := k.Load(structs.Provider(Default(), "koanf"), nil); err != nil {
		return Config{}, fmt.Errorf("loading defaults: %w", err)
	}

	if path != "" {
		if err := k.Load(file.Provider(path), toml.Parser()); err != nil {
			return Config{}, fmt.Errorf("loading %s: %w", path, err)
		}
	}

	envProvider := env.Provider(".", env.Opt{
		Prefix:        EnvPrefix,
		TransformFunc: transformEnv,
	})

	if err := k.Load(envProvider, nil); err != nil {
		return Config{}, fmt.Errorf("loading environment: %w", err)
	}

	var cfg Config
	if err := k.Unmarshal("", &cfg); err != nil {
		return Config{}, fmt.Errorf("unmarshalling configuration: %w", err)
	}

	return cfg, nil
}

// isListKey reports whether a setting's value is a list. A comma-separated
// environment variable is split for these and only these: splitting every
// value would corrupt anything that legitimately contains a comma, such as a
// URL with a query parameter or a header.
func isListKey(key string) bool {
	switch key {
	case "core.routes", "edge.core_urls":
		return true
	default:
		return false
	}
}

// headersPrefix is the config path under which every key is an HTTP header
// name rather than a setting of ours.
const headersPrefix = "auth.http.headers."

// transformEnv maps MAST__SECTION__KEY onto section.key, splitting the values
// of list-valued settings on commas and restoring hyphens in header names.
//
// koanf's env provider has no list syntax of its own, so without the split a
// multi-node deployment could not be configured by environment alone — which
// is exactly how a container is usually configured.
func transformEnv(key, value string) (string, any) {
	key = strings.ToLower(strings.TrimPrefix(key, EnvPrefix))
	key = strings.ReplaceAll(key, "__", ".")

	// An environment variable cannot contain a hyphen, so a header like
	// X-Tenant-Hint has to arrive as X_TENANT_HINT. Turn the remaining
	// underscores back into hyphens, or the header would go out on the wire
	// as "x_tenant_hint" and the policy server would never see it. HTTP
	// header names do not conventionally contain underscores, and some
	// servers reject the ones that do, so this direction is unambiguous.
	if name, ok := strings.CutPrefix(key, headersPrefix); ok {
		return headersPrefix + strings.ReplaceAll(name, "_", "-"), value
	}

	if !isListKey(key) {
		return key, value
	}

	if strings.TrimSpace(value) == "" {
		return key, []string{}
	}

	parts := strings.Split(value, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}

	return key, parts
}

// Validate reports the first reason cfg could not be run.
func (c Config) Validate() error {
	if err := c.validateRole(); err != nil {
		return err
	}

	return c.validateAuth()
}

// validateRole checks that the role is one mast knows and that it has what
// that role needs.
func (c Config) validateRole() error {
	if !c.Role.Valid() {
		return fmt.Errorf("%w: %q is not one of %v", ErrInvalidRole, c.Role, Roles())
	}

	if c.Role == RoleEdge && len(c.Edge.CoreURLs) == 0 {
		return ErrEdgeNeedsCore
	}

	if c.Role != RoleEdge && c.Core.StoreDir == "" {
		return ErrCoreNeedsStore
	}

	if c.Role != RoleCore && c.Tenant.Default == "" {
		return ErrNoDefaultTenant
	}

	return nil
}

// validateAuth checks the authentication backend, eagerly, so a broken auth
// setup is a startup failure rather than a flood of denials in production.
func (c Config) validateAuth() error {
	if !c.Auth.Mode.Valid() {
		return fmt.Errorf("%w: %q is not one of %v", ErrInvalidAuthMode, c.Auth.Mode, AuthModes())
	}

	if c.Auth.Mode == AuthHTTP && c.Auth.HTTP.AuthnURL == "" {
		return ErrNoAuthnURL
	}

	return nil
}
