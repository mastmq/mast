package jwtauth_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/mastmq/mast/internal/domain/tenant"
	"github.com/mastmq/mast/internal/infra/jwtauth"
)

const secret = "a-shared-secret"

func discard() *slog.Logger { return slog.New(slog.DiscardHandler) }

// sign builds a token with the given claims and algorithm.
func sign(t *testing.T, method jwt.SigningMethod, key any, claims jwt.MapClaims) string {
	t.Helper()

	if _, ok := claims["exp"]; !ok {
		claims["exp"] = time.Now().Add(time.Hour).Unix()
	}

	token, err := jwt.NewWithClaims(method, claims).SignedString(key)
	if err != nil {
		t.Fatalf("signing: %v", err)
	}

	return token
}

func verifier(t *testing.T, mutate func(*jwtauth.Options)) *jwtauth.Verifier {
	t.Helper()

	opts := jwtauth.Options{
		Source:      jwtauth.SourceUsername,
		Algorithms:  []string{"HS512"},
		HMACSecret:  secret,
		TenantClaim: "tenant",
		Tenant:      "fallback",
	}
	if mutate != nil {
		mutate(&opts)
	}

	v, err := jwtauth.New(opts, discard())
	if err != nil {
		t.Fatalf("building verifier: %v", err)
	}

	return v
}

func TestVerifiesAndReadsClaims(t *testing.T) {
	t.Parallel()

	v := verifier(t, func(o *jwtauth.Options) { o.SuperuserClaim = "admin" })

	token := sign(t, jwt.SigningMethodHS512, []byte(secret), jwt.MapClaims{
		"tenant": "acme",
		"admin":  true,
	})

	got, err := v.Resolve(context.Background(), tenant.Credentials{Username: token})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	if got.Tenant != "acme" {
		t.Errorf("tenant = %q, want acme", got.Tenant)
	}

	if !got.Superuser {
		t.Error("superuser claim was not honoured")
	}
}

func TestFallsBackToConfiguredTenant(t *testing.T) {
	t.Parallel()

	v := verifier(t, nil)
	token := sign(t, jwt.SigningMethodHS512, []byte(secret), jwt.MapClaims{})

	got, err := v.Resolve(context.Background(), tenant.Credentials{Username: token})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	if got.Tenant != "fallback" {
		t.Errorf("tenant = %q, want the configured fallback", got.Tenant)
	}
}

// TestRejectsAlgorithmConfusion is the test that matters. Accepting whatever
// algorithm a token asks for is how "alg": "none" and RSA-to-HMAC confusion
// attacks work, so the allowlist is mandatory and must actually be enforced.
func TestRejectsAlgorithmConfusion(t *testing.T) {
	t.Parallel()

	v := verifier(t, nil)

	// Correctly signed, but with an algorithm the verifier does not accept.
	wrongAlg := sign(t, jwt.SigningMethodHS256, []byte(secret), jwt.MapClaims{"tenant": "acme"})
	if _, err := v.Resolve(context.Background(), tenant.Credentials{Username: wrongAlg}); err == nil {
		t.Error("a token signed with an algorithm outside the allowlist was accepted")
	}

	// The unsigned "none" algorithm must never be accepted.
	none, err := jwt.NewWithClaims(jwt.SigningMethodNone, jwt.MapClaims{
		"tenant": "acme",
		"exp":    time.Now().Add(time.Hour).Unix(),
	}).SignedString(jwt.UnsafeAllowNoneSignatureType)
	if err != nil {
		t.Fatalf("building none-signed token: %v", err)
	}

	if _, err := v.Resolve(context.Background(), tenant.Credentials{Username: none}); err == nil {
		t.Error("an unsigned token was accepted")
	}
}

func TestRejectsBadTokens(t *testing.T) {
	t.Parallel()

	v := verifier(t, nil)

	cases := map[string]string{
		"garbage":      "not-a-token",
		"wrong secret": sign(t, jwt.SigningMethodHS512, []byte("different"), jwt.MapClaims{"tenant": "acme"}),
		"expired": sign(t, jwt.SigningMethodHS512, []byte(secret), jwt.MapClaims{
			"tenant": "acme",
			"exp":    time.Now().Add(-time.Hour).Unix(),
		}),
		"empty":     "",
		"no expiry": mustSignWithoutExp(t),
	}

	for name, token := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if _, err := v.Resolve(context.Background(), tenant.Credentials{Username: token}); err == nil {
				t.Errorf("%s was accepted", name)
			}
		})
	}
}

// mustSignWithoutExp builds a token with no expiry. mast requires one: a
// bearer credential that never expires cannot be revoked by a broker that
// only verifies signatures.
func mustSignWithoutExp(t *testing.T) string {
	t.Helper()

	token, err := jwt.NewWithClaims(jwt.SigningMethodHS512, jwt.MapClaims{"tenant": "acme"}).
		SignedString([]byte(secret))
	if err != nil {
		t.Fatalf("signing: %v", err)
	}

	return token
}

// TestVendorPrefix covers the "<vendor>:<token>" shape deployments use to
// namespace a credential, which the parser would otherwise choke on.
func TestVendorPrefix(t *testing.T) {
	t.Parallel()

	token := sign(t, jwt.SigningMethodHS512, []byte(secret), jwt.MapClaims{"tenant": "acme"})

	plain := verifier(t, nil)
	if _, err := plain.Resolve(context.Background(), tenant.Credentials{Username: "internal:" + token}); err == nil {
		t.Error("a prefixed token parsed without the prefix being stripped")
	}

	stripping := verifier(t, func(o *jwtauth.Options) { o.VendorPrefix = true })

	got, err := stripping.Resolve(context.Background(), tenant.Credentials{Username: "internal:" + token})
	if err != nil {
		t.Fatalf("Resolve with prefix: %v", err)
	}

	if got.Tenant != "acme" {
		t.Errorf("tenant = %q, want acme", got.Tenant)
	}
}

func TestSourcePassword(t *testing.T) {
	t.Parallel()

	v := verifier(t, func(o *jwtauth.Options) { o.Source = jwtauth.SourcePassword })
	token := sign(t, jwt.SigningMethodHS512, []byte(secret), jwt.MapClaims{"tenant": "acme"})

	if _, err := v.Resolve(context.Background(), tenant.Credentials{Username: token}); err == nil {
		t.Error("the token was read from the username when password was configured")
	}

	got, err := v.Resolve(context.Background(), tenant.Credentials{Password: []byte(token)})
	if err != nil {
		t.Fatalf("Resolve from password: %v", err)
	}

	if got.Tenant != "acme" {
		t.Errorf("tenant = %q, want acme", got.Tenant)
	}
}

func TestIssuerAndAudience(t *testing.T) {
	t.Parallel()

	v := verifier(t, func(o *jwtauth.Options) {
		o.Issuer = "colony"
		o.Audience = "mast"
	})

	good := sign(t, jwt.SigningMethodHS512, []byte(secret), jwt.MapClaims{
		"tenant": "acme", "iss": "colony", "aud": "mast",
	})
	if _, err := v.Resolve(context.Background(), tenant.Credentials{Username: good}); err != nil {
		t.Fatalf("a matching token was rejected: %v", err)
	}

	wrongIssuer := sign(t, jwt.SigningMethodHS512, []byte(secret), jwt.MapClaims{
		"tenant": "acme", "iss": "someone-else", "aud": "mast",
	})
	if _, err := v.Resolve(context.Background(), tenant.Credentials{Username: wrongIssuer}); err == nil {
		t.Error("a token from the wrong issuer was accepted")
	}
}

func TestNewValidates(t *testing.T) {
	t.Parallel()

	_, err := jwtauth.New(jwtauth.Options{Algorithms: []string{"HS512"}}, discard())
	if !errors.Is(err, jwtauth.ErrNoKey) {
		t.Errorf("missing key error = %v, want ErrNoKey", err)
	}

	_, err = jwtauth.New(jwtauth.Options{HMACSecret: secret}, discard())
	if !errors.Is(err, jwtauth.ErrNoAlgorithms) {
		t.Errorf("missing algorithms error = %v, want ErrNoAlgorithms", err)
	}

	_, err = jwtauth.New(jwtauth.Options{
		HMACSecret: secret, Algorithms: []string{"HS512"}, Source: "elsewhere",
	}, discard())
	if !errors.Is(err, jwtauth.ErrBadSource) {
		t.Errorf("bad source error = %v, want ErrBadSource", err)
	}
}

func TestHMACSecretFromFile(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "secret")
	// A trailing newline is what a mounted Secret or `echo` leaves behind,
	// and silently signing against the wrong key would be miserable to debug.
	if err := os.WriteFile(path, []byte(secret+"\n"), 0o600); err != nil {
		t.Fatalf("writing secret: %v", err)
	}

	v := verifier(t, func(o *jwtauth.Options) {
		o.HMACSecret = ""
		o.HMACSecretFile = path
	})

	token := sign(t, jwt.SigningMethodHS512, []byte(secret), jwt.MapClaims{"tenant": "acme"})
	if _, err := v.Resolve(context.Background(), tenant.Credentials{Username: token}); err != nil {
		t.Errorf("a secret read from file did not verify: %v", err)
	}
}
