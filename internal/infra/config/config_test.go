package config_test

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/mastmq/mast/internal/infra/config"
)

func TestDefaultIsRunnable(t *testing.T) {
	t.Parallel()

	cfg := config.Default()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("the built-in defaults do not validate: %v", err)
	}

	if cfg.Role != config.RoleAllInOne {
		t.Errorf("default role = %q, want all-in-one", cfg.Role)
	}

	if cfg.Auth.Mode != config.AuthStatic {
		t.Errorf("default auth mode = %q, want static", cfg.Auth.Mode)
	}
}

func TestLoadLayersFileOverDefaults(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "mast.toml")
	if err := os.WriteFile(path, []byte(`
role = "edge"
[mqtt]
addr = "0.0.0.0:8883"
[edge]
core_urls = ["nats-leaf://a:7422", "nats-leaf://b:7422"]
`), 0o600); err != nil {
		t.Fatalf("writing config: %v", err)
	}

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.Role != config.RoleEdge {
		t.Errorf("role = %q, want edge", cfg.Role)
	}

	if cfg.MQTT.Addr != "0.0.0.0:8883" {
		t.Errorf("mqtt.addr = %q", cfg.MQTT.Addr)
	}

	if len(cfg.Edge.CoreURLs) != 2 {
		t.Errorf("edge.core_urls = %v, want two entries", cfg.Edge.CoreURLs)
	}

	// Untouched values must still come from the defaults.
	if cfg.MQTT.ConnectTimeout != 10*time.Second {
		t.Errorf("connect_timeout = %v, want the default 10s", cfg.MQTT.ConnectTimeout)
	}
}

// TestEnvSplitsLists covers the gap found while bringing up a real cluster:
// koanf's env provider has no list syntax, so without splitting there is no
// way to point an edge at its core from the environment alone.
// Not parallel: t.Setenv and t.Parallel are mutually exclusive.
func TestEnvSplitsLists(t *testing.T) {
	t.Setenv("MAST__ROLE", "edge")
	t.Setenv("MAST__EDGE__CORE_URLS", "nats-leaf://a:7422, nats-leaf://b:7422")

	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	want := []string{"nats-leaf://a:7422", "nats-leaf://b:7422"}
	if !slices.Equal(cfg.Edge.CoreURLs, want) {
		t.Errorf("edge.core_urls = %v, want %v", cfg.Edge.CoreURLs, want)
	}

	if err := cfg.Validate(); err != nil {
		t.Errorf("an edge configured purely from the environment does not validate: %v", err)
	}
}

// TestEnvDoesNotSplitScalars guards the other half: a value that legitimately
// contains a comma must survive intact.
// Not parallel: t.Setenv and t.Parallel are mutually exclusive.
func TestEnvDoesNotSplitScalars(t *testing.T) {
	t.Setenv("MAST__MQTT__ADDR", "0.0.0.0:1883")
	t.Setenv("MAST__AUTH__HTTP__AUTHN_URL", "http://policy/auth?tags=a,b,c")

	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.MQTT.Addr != "0.0.0.0:1883" {
		t.Errorf("mqtt.addr = %q", cfg.MQTT.Addr)
	}

	if want := "http://policy/auth?tags=a,b,c"; cfg.Auth.HTTP.AuthnURL != want {
		t.Errorf("authn_url = %q, want %q — a comma in a scalar must not split", cfg.Auth.HTTP.AuthnURL, want)
	}
}

func TestValidate(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		mutit func(*config.Config)
		want  error
	}{
		{"unknown role", func(c *config.Config) { c.Role = "middle" }, config.ErrInvalidRole},
		{"edge without core", func(c *config.Config) { c.Role = config.RoleEdge }, config.ErrEdgeNeedsCore},
		{"core without store", func(c *config.Config) { c.Core.StoreDir = "" }, config.ErrCoreNeedsStore},
		{"no default tenant", func(c *config.Config) { c.Tenant.Default = "" }, config.ErrNoDefaultTenant},
		{"unknown auth mode", func(c *config.Config) { c.Auth.Mode = "ldap" }, config.ErrInvalidAuthMode},
		{
			"http auth without url",
			func(c *config.Config) { c.Auth.Mode = config.AuthHTTP },
			config.ErrNoAuthnURL,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := config.Default()
			tc.mutit(&cfg)

			if err := cfg.Validate(); !errors.Is(err, tc.want) {
				t.Errorf("Validate error = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestMissingConfigFileIsAnError(t *testing.T) {
	t.Parallel()

	// An explicitly named file that is not there is a mistake worth failing
	// on; silently running with defaults would be worse.
	if _, err := config.Load(filepath.Join(t.TempDir(), "absent.toml")); err == nil {
		t.Error("Load accepted a config path that does not exist")
	}
}
