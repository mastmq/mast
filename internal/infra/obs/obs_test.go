package obs_test

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mastmq/mast/internal/infra/obs"
	"github.com/prometheus/client_golang/prometheus"
)

var errCutOff = errors.New("the leaf connection to the core is down")

// TestReadyzFollowsReadinessAndHealthzDoesNot covers the split between the
// two probes.
//
// /healthz has to stay 200 while the node cannot serve: it is the liveness
// probe, and a liveness probe that failed whenever the core tier was away
// would have the kubelet restart every edge at once, which fixes nothing.
func TestReadyzFollowsReadinessAndHealthzDoesNot(t *testing.T) {
	var down atomic.Bool

	ready := func() error {
		if down.Load() {
			return errCutOff
		}

		return nil
	}

	srv, err := obs.Serve(t.Context(), "127.0.0.1:0", prometheus.NewRegistry(), ready, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("serving: %v", err)
	}

	t.Cleanup(srv.Close)

	base := "http://" + srv.Addr()

	expect(t, base+"/readyz", http.StatusOK, "ok")
	expect(t, base+"/healthz", http.StatusOK, "ok")

	down.Store(true)

	expect(t, base+"/readyz", http.StatusServiceUnavailable, errCutOff.Error())
	expect(t, base+"/healthz", http.StatusOK, "ok")

	down.Store(false)

	expect(t, base+"/readyz", http.StatusOK, "ok")
}

// TestReadyzWithoutReadinessIsReady covers a caller that has nothing to
// report, which must not turn into a node that never becomes ready.
func TestReadyzWithoutReadinessIsReady(t *testing.T) {
	srv, err := obs.Serve(t.Context(), "127.0.0.1:0", prometheus.NewRegistry(), nil, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("serving: %v", err)
	}

	t.Cleanup(srv.Close)

	expect(t, "http://"+srv.Addr()+"/readyz", http.StatusOK, "ok")
}

func expect(t *testing.T, url string, status int, body string) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}

	defer func() { _ = resp.Body.Close() }()

	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading %s: %v", url, err)
	}

	if resp.StatusCode != status || !strings.Contains(string(got), body) {
		t.Errorf("GET %s = %d %q, want %d containing %q", url, resp.StatusCode, got, status, body)
	}
}

// TestTenantLabelsAreBounded holds Metrics.Tenant to its promise: the first
// tenants keep their own label for good, and every one past the bound shares
// the overflow, however many goroutines ask at once.
func TestTenantLabelsAreBounded(t *testing.T) {
	m := obs.NewMetrics(prometheus.NewRegistry(), obs.Sources{}, 2)

	if got := m.Tenant("acme"); got != "acme" {
		t.Fatalf("the first tenant was labelled %q", got)
	}

	// Many goroutines racing to be the second tenant: exactly one wins.
	names := []string{"b0", "b1", "b2", "b3", "b4", "b5", "b6", "b7"}
	labels := make(chan string, len(names))

	for _, name := range names {
		go func() { labels <- m.Tenant(name) }()
	}

	own := 0

	for range names {
		if <-labels != obs.OtherTenants {
			own++
		}
	}

	if own != 1 {
		t.Errorf("%d tenants won the one free label, want 1", own)
	}

	if got := m.Tenant("acme"); got != "acme" {
		t.Errorf("the first tenant lost its label to %q", got)
	}
}
