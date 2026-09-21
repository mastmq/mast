// Package auth builds the authentication and authorization backend named in
// configuration.
//
// It is the one place that knows how a config value becomes a
// [tenant.Resolver] and a [tenant.Policy], which keeps that mapping out of
// both the command and the broker.
package auth

import (
	"fmt"
	"log/slog"

	"github.com/mastmq/mast/internal/domain/tenant"
	"github.com/mastmq/mast/internal/infra/config"
	"github.com/mastmq/mast/internal/infra/httpauth"
	"github.com/mastmq/mast/internal/infra/jwtauth"
)

// Build returns the resolver and policy for the configured mode.
//
// Configuration is validated eagerly here so a broken auth setup is a startup
// failure rather than a flood of denied connections in production.
//
// know which backend it got.
//
//nolint:ireturn // returning the interfaces is the point: the caller must not
func Build(cfg config.Config, log *slog.Logger) (tenant.Resolver, tenant.Policy, error) {
	switch cfg.Auth.Mode {
	case config.AuthHTTP:
		client, err := httpauth.New(httpauth.Options{
			Wire:      httpauth.Wire(cfg.Auth.HTTP.Wire),
			Tenant:    tenant.ID(cfg.Tenant.Default),
			AuthnURL:  cfg.Auth.HTTP.AuthnURL,
			AuthzURL:  cfg.Auth.HTTP.AuthzURL,
			Timeout:   cfg.Auth.HTTP.Timeout,
			OnError:   httpauth.FailMode(cfg.Auth.HTTP.OnError),
			CacheTTL:  cfg.Auth.HTTP.CacheTTL,
			CacheSize: cfg.Auth.HTTP.CacheSize,
			Headers:   cfg.Auth.HTTP.Headers,
		}, log)
		if err != nil {
			return nil, nil, fmt.Errorf("auth: %w", err)
		}

		return client, client, nil

	case config.AuthJWT:
		verifier, err := jwtauth.New(jwtauth.Options{
			Source:         jwtauth.Source(cfg.Auth.JWT.Source),
			VendorPrefix:   cfg.Auth.JWT.VendorPrefix,
			Algorithms:     cfg.Auth.JWT.Algorithms,
			HMACSecret:     cfg.Auth.JWT.HMACSecret,
			HMACSecretFile: cfg.Auth.JWT.HMACSecretFile,
			PublicKeyFile:  cfg.Auth.JWT.PublicKeyFile,
			TenantClaim:    cfg.Auth.JWT.TenantClaim,
			Tenant:         tenant.ID(cfg.Tenant.Default),
			SuperuserClaim: cfg.Auth.JWT.SuperuserClaim,
			Issuer:         cfg.Auth.JWT.Issuer,
			Audience:       cfg.Auth.JWT.Audience,
		}, log)
		if err != nil {
			return nil, nil, fmt.Errorf("auth: %w", err)
		}

		policy, err := authorizer(cfg, log)
		if err != nil {
			return nil, nil, err
		}

		return verifier, policy, nil

	case config.AuthStatic:
		return tenant.Static{Tenant: tenant.ID(cfg.Tenant.Default)}, tenant.AllowAll{}, nil

	default:
		return nil, nil, fmt.Errorf("auth: %w: %q", config.ErrInvalidAuthMode, cfg.Auth.Mode)
	}
}

// authorizer returns the policy to pair with a non-HTTP authenticator.
//
// Verifying a token locally answers who a connection is, not what it may
// do, so a deployment can still point authorization at a policy service.
// Without one, a tenant's own subtree is the only boundary, which is a
// coherent posture and not an oversight.
//
//nolint:ireturn // the caller must not know which policy it got.
func authorizer(cfg config.Config, log *slog.Logger) (tenant.Policy, error) {
	if cfg.Auth.HTTP.AuthzURL == "" {
		return tenant.AllowAll{}, nil
	}

	client, err := httpauth.NewAuthorizer(httpauth.Options{
		// AuthnURL is left empty on purpose: this client only answers
		// authorization, and NewAuthorizer fills it in.
		AuthnURL:  "",
		Wire:      httpauth.Wire(cfg.Auth.HTTP.Wire),
		Tenant:    tenant.ID(cfg.Tenant.Default),
		AuthzURL:  cfg.Auth.HTTP.AuthzURL,
		Timeout:   cfg.Auth.HTTP.Timeout,
		OnError:   httpauth.FailMode(cfg.Auth.HTTP.OnError),
		CacheTTL:  cfg.Auth.HTTP.CacheTTL,
		CacheSize: cfg.Auth.HTTP.CacheSize,
		Headers:   cfg.Auth.HTTP.Headers,
	}, log)
	if err != nil {
		return nil, fmt.Errorf("auth: %w", err)
	}

	return client, nil
}
