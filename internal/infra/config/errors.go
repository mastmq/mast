package config

import "errors"

// Errors reported when a configuration cannot be run.
var (
	ErrInvalidRole    = errors.New("config: invalid role")
	ErrEdgeNeedsCore  = errors.New("config: role edge requires at least one edge.core_urls entry")
	ErrCoreNeedsStore = errors.New("config: roles core and all-in-one require core.store_dir")
)
