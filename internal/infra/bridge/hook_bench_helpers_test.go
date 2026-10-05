package bridge

import (
	"log/slog"
	"testing"

	"github.com/mastmq/mast/internal/domain/tenant"
	mqtt "github.com/mastmq/mochi/v2"
)

// newTestHook builds a hook with no fabric behind it, enough for the pure
// bookkeeping paths.
func newTestHook(tb testing.TB) *Hook {
	tb.Helper()

	return New(nil, nil, nil, tenant.AllowAll{}, Options{}, slog.New(slog.DiscardHandler))
}

// fakeClient is a mochi client with only the id set, which is all the
// bookkeeping paths read.
func fakeClient(id string) *mqtt.Client {
	return &mqtt.Client{ID: id}
}
