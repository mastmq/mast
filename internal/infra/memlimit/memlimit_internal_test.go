package memlimit

import (
	"os"
	"path/filepath"
	"testing"
)

func TestContainerLimit(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	write := func(name, value string) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
			t.Fatal(err)
		}

		return path
	}

	cases := []struct {
		name  string
		paths []string
		want  int64
		ok    bool
	}{
		{"cgroup v2 limit", []string{write("v2", "268435456\n")}, 268435456, true},
		{"cgroup v2 unlimited", []string{write("v2max", "max\n")}, 0, false},
		{"cgroup v1 unlimited", []string{write("v1", "9223372036854771712\n")}, 0, false},
		{"v1 when v2 is absent", []string{filepath.Join(dir, "none"), write("v1lim", "1073741824")}, 1073741824, true},
		{"no cgroup at all", []string{filepath.Join(dir, "none")}, 0, false},
	}

	for _, c := range cases {
		got, ok := container(c.paths)
		if got != c.want || ok != c.ok {
			t.Errorf("%s: container = %d, %v; want %d, %v", c.name, got, ok, c.want, c.ok)
		}
	}
}
