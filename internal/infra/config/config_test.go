package config_test

import (
	"errors"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/knadh/koanf/parsers/toml/v2"
	kfile "github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/providers/structs"
	"github.com/knadh/koanf/v2"
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

// TestEnvHeaderNames covers a bug the Helm chart would otherwise have
// shipped: an environment variable cannot contain a hyphen, so a header has
// to arrive as X_TENANT_HINT and must be turned back into X-Tenant-Hint
// before it goes on the wire.
//
// Not parallel: t.Setenv and t.Parallel are mutually exclusive.
func TestEnvHeaderNames(t *testing.T) {
	t.Setenv("MAST__AUTH__HTTP__HEADERS__AUTHORIZATION", "Bearer tok")
	t.Setenv("MAST__AUTH__HTTP__HEADERS__X_TENANT_HINT", "edge-1")

	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	want := map[string]string{
		"authorization": "Bearer tok",
		"x-tenant-hint": "edge-1",
	}

	if !maps.Equal(cfg.Auth.HTTP.Headers, want) {
		t.Errorf("headers = %v, want %v", cfg.Auth.HTTP.Headers, want)
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
		{
			"http auth without a timeout",
			func(c *config.Config) {
				c.Auth.Mode = config.AuthHTTP
				c.Auth.HTTP.AuthnURL = "http://policy"
				c.Auth.HTTP.Timeout = 0
			},
			config.ErrBadAuthTimeout,
		},
		{
			"jwt with an authz url and a negative timeout",
			func(c *config.Config) {
				c.Auth.Mode = config.AuthJWT
				c.Auth.JWT.Algorithms = []string{"HS256"}
				c.Auth.HTTP.AuthzURL = "http://policy"
				c.Auth.HTTP.Timeout = -time.Second
			},
			config.ErrBadAuthTimeout,
		},
		{
			"static auth ignores the http timeout",
			func(c *config.Config) { c.Auth.HTTP.Timeout = 0 },
			nil,
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

// TestExampleIsExhaustiveDefaults holds configs/config.example.toml to the
// two promises its header makes. It must set every key the broker reads, or
// the file stops being the place to discover a setting; and every value must
// be the built-in default, so an empty file and this one behave the same.
// Both drifted once already, silently, because nothing loaded the file.
func TestExampleIsExhaustiveDefaults(t *testing.T) {
	t.Parallel()

	const example = "../../../configs/config.example.toml"

	file := koanf.New(".")
	if err := file.Load(kfile.Provider(example), toml.Parser()); err != nil {
		t.Fatalf("parsing %s: %v", example, err)
	}

	defaults := koanf.New(".")
	if err := defaults.Load(structs.Provider(config.Default(), "koanf"), nil); err != nil {
		t.Fatalf("loading defaults: %v", err)
	}

	for _, key := range defaults.Keys() {
		// Headers are a map of arbitrary names with nothing to default, so
		// the example can only show one commented out.
		if key == "auth.http.headers" {
			continue
		}

		if !file.Exists(key) {
			t.Errorf("%s does not set %s", example, key)
		}
	}

	for _, key := range file.Keys() {
		if !defaults.Exists(key) {
			t.Errorf("%s sets %s, which the broker does not read", example, key)
		}
	}

	cfg, err := config.Load(example)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	want := config.Default()

	// An empty TOML list decodes as an empty slice where the default is nil.
	// Both mean "none", so only the difference that matters is compared.
	for _, list := range []*[]string{&cfg.Core.Routes, &cfg.Edge.CoreURLs, &cfg.Auth.JWT.Algorithms} {
		if len(*list) == 0 {
			*list = nil
		}
	}

	if !reflect.DeepEqual(cfg, want) {
		t.Errorf("%s differs from the built-in defaults:\n got %+v\nwant %+v", example, cfg, want)
	}
}
