package cmd

import "errors"

// errNotImplemented marks a code path that is wired but has no runtime yet.
var errNotImplemented = errors.New("runtime not implemented yet")
