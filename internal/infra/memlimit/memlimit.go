// Package memlimit tells the Go runtime how much memory the container it
// runs in may use.
//
// The runtime does not find out by itself. Go reads the cgroup's CPU quota
// for GOMAXPROCS, but not its memory limit for GOMEMLIMIT, so by default a
// heap is allowed to grow to twice what is live before it is collected,
// whatever the limit. A core holding 150,000 sleeping sessions had about
// 460 MB live and was killed by the kernel at its 1 GiB limit with garbage
// it had not yet collected; the same core at 300,000 sessions sat at
// 600 MB with 260 MB live. With the limit set, the collector runs harder as
// the heap nears it rather than letting the kernel decide.
package memlimit

import (
	"log/slog"
	"math"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
)

// share is the part of the container's limit given to the Go heap. The
// rest is for what the runtime does not count — goroutine stacks, the
// binary, the kernel's page cache charged to the cgroup — and a margin so
// the collector gets its chance before the kernel does.
const share = 0.9

// files are where the container's memory limit is read from: cgroup v2,
// then v1.
var files = []string{ //nolint:gochecknoglobals // overridden only by tests
	"/sys/fs/cgroup/memory.max",
	"/sys/fs/cgroup/memory/memory.limit_in_bytes",
}

// Apply sets the runtime's memory limit from the container's, unless
// GOMEMLIMIT already sets one or no container limit applies. It returns
// the limit set, zero if none.
func Apply(log *slog.Logger) int64 {
	if os.Getenv("GOMEMLIMIT") != "" {
		return 0
	}

	limit, ok := container(files)
	if !ok {
		return 0
	}

	heap := int64(float64(limit) * share)
	debug.SetMemoryLimit(heap)

	log.Info("memory limit from the container", "container_bytes", limit, "go_limit_bytes", heap)

	return heap
}

// container reads the first limit found in paths. cgroup v2 writes "max"
// for none, and v1 a number near the largest int64, which is just as
// unlimited.
func container(paths []string) (int64, bool) {
	for _, path := range paths {
		raw, err := os.ReadFile(path) //nolint:gosec // fixed cgroup paths
		if err != nil {
			continue
		}

		value := strings.TrimSpace(string(raw))
		if value == "max" {
			return 0, false
		}

		limit, err := strconv.ParseInt(value, 10, 64)
		if err != nil || limit <= 0 || limit > math.MaxInt64/2 {
			return 0, false
		}

		return limit, true
	}

	return 0, false
}
