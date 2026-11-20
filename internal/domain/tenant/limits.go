package tenant

import (
	"errors"
	"fmt"
	"time"
)

// ErrBadLimit is returned for a limit that cannot mean anything: a negative
// count or rate.
var ErrBadLimit = errors.New("tenant: a limit cannot be negative")

// Limits are what one tenant may use. Zero means unlimited for every field
// but OfflineQueue, where it means "as many as the store keeps".
//
// Each limit is enforced where it can be enforced cheaply, and that decides
// its scope. MaxConnections is cluster-wide, from counts the nodes exchange
// on core NATS; PublishRate is per node; OfflineQueue is per session. The
// reasons are in docs/ARCHITECTURE.md under "Tenant limits".
type Limits struct {
	// MaxConnections caps the tenant's open MQTT connections across the
	// cluster.
	MaxConnections int
	// PublishRate is how many PUBLISH packets per second the tenant may send
	// through one node, sustained.
	PublishRate float64
	// PublishBurst is how many it may send at once before PublishRate
	// applies. Zero means one second's worth.
	PublishBurst int
	// OfflineQueue is how many QoS 1 and 2 messages are kept for one absent
	// session of this tenant, newest first.
	OfflineQueue int
}

// Overrides replace some of a tenant's [Limits]. A nil field leaves the
// limit beneath it in place, which is what lets a tenant's configuration
// raise one limit without restating the defaults for the others, and an
// authentication service name only what it knows about.
//
// The JSON names are the wire format an HTTP authentication reply and a JWT
// claim carry them in, so the two backends cannot drift apart.
type Overrides struct {
	MaxConnections *int     `json:"max_connections,omitempty"`
	PublishRate    *float64 `json:"publish_rate,omitempty"`
	PublishBurst   *int     `json:"publish_burst,omitempty"`
	OfflineQueue   *int     `json:"offline_queue,omitempty"`

	// IssuedAt is when the authentication backend stated these overrides:
	// a JWT's iat, or the time an HTTP reply arrived. It is not part of the
	// wire format, and configured overrides leave it zero.
	//
	// It is there because one tenant's devices do not authenticate with one
	// statement. Each JWT carries its own claim, and a device still holding
	// a token from before the tenant's plan changed would otherwise put the
	// old limit back on every node it connected to. Overrides older than
	// the ones in force are ignored; see [Overrides.Supersedes].
	IssuedAt time.Time `json:"-"`
}

// Supersedes reports whether o should replace current as a tenant's
// overrides from authentication.
//
// An empty o never does: a token or reply that says nothing about limits
// says nothing, and is not a statement that the tenant has none, so the
// limits in force stay. Otherwise o replaces current unless it was issued
// before it. An o with no issue time is as old as possible against one that
// has one, so a token without an iat cannot undo a dated one; against
// another without, the later authentication wins.
func (o Overrides) Supersedes(current Overrides) bool {
	return !o.Empty() && !o.IssuedAt.Before(current.IssuedAt)
}

// Validate reports a field o sets to something negative.
func (o Overrides) Validate() error {
	for name, bad := range map[string]bool{
		"max_connections": o.MaxConnections != nil && *o.MaxConnections < 0,
		"publish_rate":    o.PublishRate != nil && *o.PublishRate < 0,
		"publish_burst":   o.PublishBurst != nil && *o.PublishBurst < 0,
		"offline_queue":   o.OfflineQueue != nil && *o.OfflineQueue < 0,
	} {
		if bad {
			return fmt.Errorf("%w: %s", ErrBadLimit, name)
		}
	}

	return nil
}

// Empty reports whether o overrides nothing.
func (o Overrides) Empty() bool {
	return o.MaxConnections == nil && o.PublishRate == nil && o.PublishBurst == nil && o.OfflineQueue == nil
}

// With returns l with every field o sets replaced.
func (l Limits) With(o Overrides) Limits {
	if o.MaxConnections != nil {
		l.MaxConnections = *o.MaxConnections
	}

	if o.PublishRate != nil {
		l.PublishRate = *o.PublishRate
	}

	if o.PublishBurst != nil {
		l.PublishBurst = *o.PublishBurst
	}

	if o.OfflineQueue != nil {
		l.OfflineQueue = *o.OfflineQueue
	}

	return l
}

// Burst is the bucket size PublishRate is enforced with: PublishBurst when
// set, else one second's worth, and never less than one message, or a rate
// below one per second would admit nothing at all.
func (l Limits) Burst() float64 {
	if l.PublishBurst > 0 {
		return float64(l.PublishBurst)
	}

	return max(l.PublishRate, 1)
}
