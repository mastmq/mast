// Package tenant resolves an MQTT connection to the tenant that owns it and
// decides what that tenant may do.
//
// mast does not own tenant lifecycle. The integrating business names the
// tenant during authentication, exactly as BifroMQ's auth provider does, so a
// deployment can map tenants onto whatever identity system it already has. A
// deployment with no multi-tenancy uses one static tenant and pays nothing.
package tenant

import (
	"context"
	"errors"
)

// ID names a tenant. It must survive as a single NATS subject token, so it is
// constrained to [A-Za-z0-9_-]+ by the topic codec.
type ID string

// ErrUnauthenticated is returned when credentials do not identify a tenant.
var ErrUnauthenticated = errors.New("tenant: unauthenticated")

// Credentials are what an MQTT CONNECT offers about who is connecting.
type Credentials struct {
	ClientID   string
	Username   string
	Password   []byte
	RemoteAddr string
}

// Resolver maps credentials to the tenant that owns the connection.
//
// This runs on every CONNECT, so at 300k devices a deploy can drive tens of
// thousands of calls per second. An implementation that makes a synchronous
// network call per connection will fall over during a reconnect storm; verify
// credentials locally, or cache aggressively.
type Resolver interface {
	Resolve(ctx context.Context, creds Credentials) (ID, error)
}

// Policy decides whether a tenant may publish to or subscribe to a topic.
//
// It is consulted with the tenant resolved at authentication time, never with
// anything the client asserted on the wire.
type Policy interface {
	Allows(tenant ID, topic string, write bool) bool
}

// Static resolves every connection to the same tenant. It is the default, and
// the right answer for a single-tenant deployment or a laptop.
type Static struct {
	Tenant ID
}

// Resolve implements [Resolver].
func (s Static) Resolve(_ context.Context, _ Credentials) (ID, error) {
	if s.Tenant == "" {
		return "", ErrUnauthenticated
	}

	return s.Tenant, nil
}

// AllowAll permits every operation. It pairs with [Static] for single-tenant
// deployments; a multi-tenant deployment must supply a real [Policy].
type AllowAll struct{}

// Allows implements [Policy].
func (AllowAll) Allows(_ ID, _ string, _ bool) bool { return true }
