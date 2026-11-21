package config

import "errors"

// Errors reported when a configuration cannot be run.
var (
	ErrInvalidRole    = errors.New("config: invalid role")
	ErrEdgeNeedsCore  = errors.New("config: role edge requires at least one edge.core_urls entry")
	ErrCoreNeedsStore = errors.New("config: roles core and all-in-one require core.store_dir")

	// ErrNoDefaultTenant guards against a broker that accepts connections it
	// cannot attribute to anyone.
	ErrNoDefaultTenant = errors.New("config: tenant.default must be set for roles that terminate MQTT")

	// ErrInvalidAuthMode guards against a typo silently falling back to the
	// permissive static backend.
	ErrInvalidAuthMode = errors.New("config: invalid auth.mode")

	// ErrNoAuthnURL is returned when http auth is selected without an endpoint.
	ErrNoAuthnURL = errors.New("config: auth.mode http requires auth.http.authn_url")

	// ErrNoJWTAlgorithms guards the classic JWT footgun: accepting whatever
	// algorithm a token asks for.
	ErrNoJWTAlgorithms = errors.New("config: auth.mode jwt requires auth.jwt.algorithms")

	// ErrBadAuthTimeout guards against a zero auth.http.timeout, which to
	// net/http means no timeout: one policy server that stops answering
	// would hang every CONNECT and every uncached publish indefinitely.
	ErrBadAuthTimeout = errors.New("config: auth.http.timeout must be positive")

	// ErrBadDurableMaxAge guards against a durable stream that keeps
	// nothing: with no retention, a node cut off for an instant catches up
	// on nothing, which is the failure the stream exists to prevent.
	ErrBadDurableMaxAge = errors.New("config: fabric.durable_max_age must be positive when fabric.durable_qos is on")

	// ErrAdminNeedsToken guards against an admin API anyone who can reach
	// it may use: it lists every tenant's devices and disconnects them.
	ErrAdminNeedsToken = errors.New("config: admin.addr requires admin.token_file")

	// ErrBadAdminGather guards against a gather window that closes before
	// any other node can answer, which would report a cluster as one node,
	// and one so long that every list and stats request waits it out.
	ErrBadAdminGather = errors.New("config: admin.gather must be positive and at most 5s")

	// ErrInvalidSampler guards against a typo that would otherwise fall
	// back to sampling everything, or nothing, without a word.
	ErrInvalidSampler = errors.New("config: invalid tracing.sampler")

	// ErrBadSamplerArg is returned for a sampling ratio outside [0, 1].
	ErrBadSamplerArg = errors.New("config: tracing.sampler_arg must be a ratio between 0 and 1")
)
