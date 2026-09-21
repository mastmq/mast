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
	Role Role   `json:"role" koanf:"role"`
	Log  Log    `json:"log"  koanf:"log"`
	MQTT MQTT   `json:"mqtt" koanf:"mqtt"`
	NATS NATS   `json:"nats" koanf:"nats"`
	Core Core   `json:"core" koanf:"core"`
	Edge Edge   `json:"edge" koanf:"edge"`
	Obs  Observ `json:"obs"  koanf:"obs"`
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
		Prefix: EnvPrefix,
		TransformFunc: func(key, value string) (string, any) {
			key = strings.ToLower(strings.TrimPrefix(key, EnvPrefix))

			return strings.ReplaceAll(key, "__", "."), value
		},
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

// Validate reports the first reason cfg could not be run.
func (c Config) Validate() error {
	if !c.Role.Valid() {
		return fmt.Errorf("%w: %q is not one of %v", ErrInvalidRole, c.Role, Roles())
	}

	if c.Role == RoleEdge && len(c.Edge.CoreURLs) == 0 {
		return ErrEdgeNeedsCore
	}

	if c.Role != RoleEdge && c.Core.StoreDir == "" {
		return ErrCoreNeedsStore
	}

	return nil
}
