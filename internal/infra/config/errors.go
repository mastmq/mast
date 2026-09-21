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
)
