package admin

import (
	"crypto/sha256"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

const testToken = "0123456789abcdef-admin"

func testServer() *Server {
	return &Server{
		http: nil, nc: nil, store: nil, gather: 0,
		digest: sha256.Sum256([]byte(testToken)),
		log:    slog.New(slog.DiscardHandler),
		addr:   "",
	}
}

// TestEveryRouteNeedsTheToken drives the real router. A route registered
// outside the middleware — the easy mistake when adding one — would answer
// without a token here instead of with 401.
func TestEveryRouteNeedsTheToken(t *testing.T) {
	t.Parallel()

	handler := testServer().routes()

	routes := []struct{ method, path string }{
		{http.MethodGet, "/v1/tenants/acme/sessions"},
		{http.MethodGet, "/v1/tenants/acme/sessions/rover"},
		{http.MethodPost, "/v1/tenants/acme/sessions/rover/disconnect"},
		{http.MethodGet, "/v1/tenants/acme/stats"},
	}

	headers := map[string]string{
		"none":         "",
		"wrong token":  "Bearer not-the-token-at-all",
		"prefix only":  "Bearer " + testToken[:8],
		"basic scheme": "Basic " + testToken,
		"bare token":   testToken,
	}

	for _, route := range routes {
		for name, header := range headers {
			req := httptest.NewRequestWithContext(t.Context(), route.method, route.path, nil)
			if header != "" {
				req.Header.Set("Authorization", header)
			}

			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusUnauthorized {
				t.Errorf("%s %s with %s: status %d, want 401", route.method, route.path, name, rec.Code)
			}

			if rec.Header().Get("WWW-Authenticate") == "" {
				t.Errorf("%s %s with %s: no WWW-Authenticate challenge", route.method, route.path, name)
			}
		}
	}
}

func TestTheRightTokenIsAdmitted(t *testing.T) {
	t.Parallel()

	handler := testServer().routes()

	// A path no route serves: past the middleware, the router answers 404,
	// which proves the token was accepted without reaching a handler that
	// needs a cluster behind it.
	for _, header := range []string{"Bearer " + testToken, "bearer " + testToken} {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/v1/nothing", nil)
		req.Header.Set("Authorization", header)

		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusNotFound {
			t.Errorf("with %q: status %d, want 404 from the router", header, rec.Code)
		}
	}
}

func TestLoadToken(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	write := func(name, content string) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}

		return path
	}

	got, err := LoadToken(write("ok", testToken+"\n"))
	if err != nil || string(got) != testToken {
		t.Fatalf("LoadToken = %q, %v; want the token without its newline", got, err)
	}

	if _, err := LoadToken(write("empty", " \n")); !errors.Is(err, ErrEmptyToken) {
		t.Errorf("an empty file: %v, want ErrEmptyToken", err)
	}

	if _, err := LoadToken(write("short", "hunter2")); !errors.Is(err, ErrShortToken) {
		t.Errorf("a short token: %v, want ErrShortToken", err)
	}

	if _, err := LoadToken(filepath.Join(dir, "absent")); err == nil {
		t.Error("a missing file was accepted")
	}
}

func TestCursorRoundTrips(t *testing.T) {
	t.Parallel()

	for _, id := range []string{"rover", "a/b?c=d&e#f", "über", ""} {
		got, err := decodeCursor(encodeCursor(id))
		if err != nil || got != id {
			t.Errorf("cursor for %q came back as %q, %v", id, got, err)
		}
	}

	if _, err := decodeCursor("%%%"); err == nil {
		t.Error("a cursor this api never made was accepted")
	}
}
