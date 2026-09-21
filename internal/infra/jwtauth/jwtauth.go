// Package jwtauth authenticates MQTT connections by verifying a signed token
// locally.
//
// It is the counterpart to the HTTP backend, and the difference that matters
// is where the work happens. An HTTP callback asks a service on every
// CONNECT, which puts that service on the critical path of every reconnect
// storm: 50k devices coming back after a deploy is 50k requests it has to
// survive. Verifying a signature locally costs microseconds and cannot be
// knocked over, at the price of key distribution and of revocation being
// only as fast as token expiry.
//
// Neither is strictly better, so mast supports both and they compose:
// authentication can come from a token while authorization still goes to a
// policy service, which is the arrangement most deployments end up wanting.
package jwtauth

import (
	"context"
	"crypto"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/golang-jwt/jwt/v5"
	"github.com/mastmq/mast/internal/domain/tenant"
)

// Source names where in the CONNECT packet the token is carried.
type Source string

const (
	// SourceUsername reads the token from the username field. This is the
	// convention EMQX deployments use, because it leaves the password free
	// and many clients log passwords.
	SourceUsername Source = "username"

	// SourcePassword reads the token from the password field, which is where
	// the MQTT specification would put a credential.
	SourcePassword Source = "password"
)

// Valid reports whether s names a field mast knows how to read.
func (s Source) Valid() bool { return s == SourceUsername || s == SourcePassword }

// Errors reported by this package.
var (
	// ErrNoKey is returned when no verification key is configured.
	ErrNoKey = errors.New("jwtauth: one of hmac_secret, hmac_secret_file or public_key_file is required")
	// ErrBadSource is returned for an unrecognized token source.
	ErrBadSource = errors.New("jwtauth: source must be username or password")
	// ErrNoAlgorithms is returned when the accepted algorithm list is empty.
	//
	// There is no sensible default here. Accepting whatever a token asks for
	// is how "alg": "none" and RSA-to-HMAC confusion attacks work, so the
	// list is mandatory rather than inferred.
	ErrNoAlgorithms = errors.New("jwtauth: algorithms must list the accepted signing algorithms")
	// ErrNoTenant is returned when a verified token names no tenant.
	ErrNoTenant = errors.New("jwtauth: token carries no tenant claim")
	// ErrInvalidToken is returned when verification fails.
	ErrInvalidToken = errors.New("jwtauth: invalid token")
)

// Options configures a [Verifier].
type Options struct {
	// Source is the CONNECT field carrying the token.
	Source Source

	// VendorPrefix, when true, discards everything up to the first colon
	// before parsing. Deployments commonly namespace a credential as
	// "<vendor>:<token>"; without this the parse fails on the prefix.
	VendorPrefix bool

	// Algorithms is the allowlist of accepted signing algorithms, for
	// example ["HS512"] or ["RS256"]. Required.
	Algorithms []string

	// One of these supplies the verification key.
	HMACSecret     string
	HMACSecretFile string
	PublicKeyFile  string

	// TenantClaim names the claim holding the tenant. When it is empty, or
	// the claim is absent, Tenant is used.
	TenantClaim string
	// Tenant is the fallback tenant.
	Tenant tenant.ID

	// SuperuserClaim names a boolean claim that bypasses authorization, the
	// same way EMQX's is_superuser does.
	SuperuserClaim string

	// Issuer and Audience, when set, must match the token.
	Issuer   string
	Audience string
}

// Verifier authenticates connections from a locally verified token. It
// implements [tenant.Resolver].
type Verifier struct {
	opts   Options
	key    any
	parser *jwt.Parser
	log    *slog.Logger
}

// New builds a verifier, reading key material eagerly so a missing or
// malformed key is a startup failure rather than a flood of denials.
func New(opts Options, log *slog.Logger) (*Verifier, error) {
	if opts.Source == "" {
		opts.Source = SourceUsername
	}

	if !opts.Source.Valid() {
		return nil, fmt.Errorf("%w: got %q", ErrBadSource, opts.Source)
	}

	if len(opts.Algorithms) == 0 {
		return nil, ErrNoAlgorithms
	}

	key, err := loadKey(opts)
	if err != nil {
		return nil, err
	}

	parserOpts := []jwt.ParserOption{
		jwt.WithValidMethods(opts.Algorithms),
		jwt.WithExpirationRequired(),
	}

	if opts.Issuer != "" {
		parserOpts = append(parserOpts, jwt.WithIssuer(opts.Issuer))
	}

	if opts.Audience != "" {
		parserOpts = append(parserOpts, jwt.WithAudience(opts.Audience))
	}

	return &Verifier{
		opts:   opts,
		key:    key,
		parser: jwt.NewParser(parserOpts...),
		log:    log.With("component", "jwtauth"),
	}, nil
}

// Resolve implements [tenant.Resolver].
func (v *Verifier) Resolve(_ context.Context, creds tenant.Credentials) (tenant.Identity, error) {
	raw := creds.Username
	if v.opts.Source == SourcePassword {
		raw = string(creds.Password)
	}

	if v.opts.VendorPrefix {
		if _, after, found := strings.Cut(raw, ":"); found {
			raw = after
		}
	}

	if raw == "" {
		return tenant.Identity{}, tenant.ErrUnauthenticated
	}

	claims := jwt.MapClaims{}

	if _, err := v.parser.ParseWithClaims(raw, claims, func(*jwt.Token) (any, error) {
		return v.key, nil
	}); err != nil {
		// The token itself is never logged: it is a bearer credential.
		v.log.Debug("token rejected", "client", creds.ClientID, "error", err)

		return tenant.Identity{}, fmt.Errorf("%w: %w", ErrInvalidToken, err)
	}

	id, err := v.tenantFrom(claims)
	if err != nil {
		return tenant.Identity{}, err
	}

	return tenant.Identity{Tenant: id, Superuser: v.superuserFrom(claims)}, nil
}

// tenantFrom reads the tenant claim, falling back to the configured tenant.
func (v *Verifier) tenantFrom(claims jwt.MapClaims) (tenant.ID, error) {
	if v.opts.TenantClaim != "" {
		if raw, ok := claims[v.opts.TenantClaim]; ok {
			if s, isString := raw.(string); isString && s != "" {
				return tenant.ID(s), nil
			}
		}
	}

	if v.opts.Tenant == "" {
		return "", ErrNoTenant
	}

	return v.opts.Tenant, nil
}

// superuserFrom reads the superuser claim, which must be a boolean. A claim
// that is present but not a boolean is treated as false rather than as an
// error: mistaking a string for permission is the wrong way to fail.
func (v *Verifier) superuserFrom(claims jwt.MapClaims) bool {
	if v.opts.SuperuserClaim == "" {
		return false
	}

	super, _ := claims[v.opts.SuperuserClaim].(bool)

	return super
}

// loadKey resolves whichever key material was configured.
func loadKey(opts Options) (any, error) {
	switch {
	case opts.HMACSecret != "":
		return []byte(opts.HMACSecret), nil

	case opts.HMACSecretFile != "":
		// Operator-supplied path, as in loadPublicKey.
		secret, err := os.ReadFile(opts.HMACSecretFile)
		if err != nil {
			return nil, fmt.Errorf("jwtauth: reading hmac secret: %w", err)
		}

		return []byte(strings.TrimSpace(string(secret))), nil

	case opts.PublicKeyFile != "":
		return loadPublicKey(opts.PublicKeyFile)

	default:
		return nil, ErrNoKey
	}
}

// loadPublicKey reads a PEM public key, trying the shapes a deployment is
// likely to have on disk.
func loadPublicKey(path string) (crypto.PublicKey, error) {
	// The path comes from this broker's own configuration, not from anything
	// a client can influence.
	pem, err := os.ReadFile(path) //nolint:gosec // operator-supplied path
	if err != nil {
		return nil, fmt.Errorf("jwtauth: reading public key: %w", err)
	}

	if key, rsaErr := jwt.ParseRSAPublicKeyFromPEM(pem); rsaErr == nil {
		return key, nil
	}

	if key, ecErr := jwt.ParseECPublicKeyFromPEM(pem); ecErr == nil {
		return key, nil
	}

	key, edErr := jwt.ParseEdPublicKeyFromPEM(pem)
	if edErr != nil {
		return nil, fmt.Errorf("jwtauth: %s is not an RSA, ECDSA or Ed25519 public key: %w", path, edErr)
	}

	return key, nil
}
