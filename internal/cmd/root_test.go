package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// TestConfigShowHonoursRootFlag is a regression test.
//
// `config show` used to declare a --config flag of its own, which shadowed
// the root's persistent one with an empty default. Writing the flag before
// the subcommand — the way anyone actually types it — silently printed the
// built-in defaults instead of the file, which is the worst possible answer
// from the command whose whole job is to report the effective configuration.
func TestConfigShowHonoursRootFlag(t *testing.T) {
	t.Parallel()

	const conf = `
role = "edge"
[edge]
core_urls = ["nats-leaf://c:7422"]
`

	path := filepath.Join(t.TempDir(), "mast.toml")
	if err := os.WriteFile(path, []byte(conf), 0o600); err != nil {
		t.Fatalf("writing config: %v", err)
	}

	for _, args := range [][]string{
		{"mast", "-c", path, "config", "show"},
		{"mast", "--config", path, "config", "show"},
		{"mast", "config", "show", "-c", path},
	} {
		var out bytes.Buffer

		root := newRoot()
		root.Writer = &out

		if err := root.Run(context.Background(), args); err != nil {
			t.Fatalf("%v: %v", args, err)
		}

		var got struct {
			Role string `json:"role"`
		}

		if err := json.Unmarshal(out.Bytes(), &got); err != nil {
			t.Fatalf("%v: decoding output: %v", args, err)
		}

		if got.Role != "edge" {
			t.Errorf("%v: role = %q, want edge — the config file was ignored", args, got.Role)
		}
	}
}
