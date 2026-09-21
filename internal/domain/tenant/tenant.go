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

// Credentials are the CONNECT fields an authenticator may decide on.
//
// Password is the raw CONNECT password. Anything that forwards it off the
// process is responsible for doing so over TLS and for keeping it out of logs.
type Credentials struct {
	ClientID        string
	Username        string
	Password        []byte
	RemoteAddr      string
	ProtocolVersion byte
	CleanStart      bool
}

// Access is one authorization question: may this connection use this topic,
// in this direction.
//
// It carries the connection's identity and not just the topic, because a real
// policy server keys on who is asking. The tenant is the one resolved at
// authentication, never one the client asserted.
type Access struct {
	Tenant     ID
	ClientID   string
	Username   string
	RemoteAddr string
	Topic      string
	// Write is true for a publish and false for a subscribe.
	Write bool
}

// Action names the direction of an [Access] for wire formats and logs.
func (a Access) Action() string {
	if a.Write {
		return "publish"
	}

	return "subscribe"
}

// Identity is what authentication establishes about a connection.
type Identity struct {
	// Tenant owns the connection. Every topic is mounted under it.
	Tenant ID

	// Superuser bypasses authorization entirely for this connection.
	//
	// EMQX works this way, and matching it is not optional for a broker
	// meant to replace one: a service whose token authenticates as a
	// superuser is never asked about individual topics there, so any ACL
	// rule that would deny it has never been exercised. Consulting the
	// policy anyway would enforce rules the old broker ignored and break
	// services on the day of the switch.
	Superuser bool
}

// Resolver maps credentials to the identity behind the connection.
//
// This runs on every CONNECT, so at 300k devices a deploy can drive tens of
// thousands of calls per second. An implementation that makes a synchronous
// network call per connection will fall over during a reconnect storm; verify
// credentials locally, or cache aggressively.
type Resolver interface {
	Resolve(ctx context.Context, creds Credentials) (Identity, error)
}

// Policy decides whether a connection may publish to or subscribe to a topic.
//
// This runs on every publish, which is a far hotter path than authentication.
// An implementation that talks to the network here must cache, or it becomes
// the throughput ceiling of the whole broker.
type Policy interface {
	Allows(ctx context.Context, access Access) bool
}

// Static resolves every connection to the same tenant. It is the default, and
// the right answer for a single-tenant deployment or a laptop.
type Static struct {
	Tenant ID
}

// Resolve implements [Resolver].
func (s Static) Resolve(_ context.Context, _ Credentials) (Identity, error) {
	if s.Tenant == "" {
		return Identity{}, ErrUnauthenticated
	}

	return Identity{Tenant: s.Tenant, Superuser: false}, nil
}

// AllowAll permits every operation. It pairs with [Static] for single-tenant
// deployments; a multi-tenant deployment must supply a real [Policy].
type AllowAll struct{}

// Allows implements [Policy].
func (AllowAll) Allows(_ context.Context, _ Access) bool { return true }
