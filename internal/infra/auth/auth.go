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

	case config.AuthStatic:
		return tenant.Static{Tenant: tenant.ID(cfg.Tenant.Default)}, tenant.AllowAll{}, nil

	default:
		return nil, nil, fmt.Errorf("auth: %w: %q", config.ErrInvalidAuthMode, cfg.Auth.Mode)
	}
}
