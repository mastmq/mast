// Package cmd builds mast's command tree.
package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/carlmjohnson/versioninfo"
	"github.com/mastmq/mast/internal/cmd/configcmd"
	"github.com/urfave/cli/v3"
)

// Execute runs the mast command tree and exits non-zero on failure.
func Execute() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "mast:", err)

		os.Exit(1)
	}
}

// run is separate from [Execute] so the deferred signal cleanup actually runs
// before the process exits.
func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	return newRoot().Run(ctx, os.Args)
}

// newRoot builds the command tree. It is separate from [Execute] so tests can
// drive the real tree rather than a reconstruction of it.
func newRoot() *cli.Command {
	return &cli.Command{
		Name:    "mast",
		Usage:   "multi-tenant MQTT broker built on core NATS",
		Version: versioninfo.Short(),
		Flags:   rootFlags(),
		Action:  serve,
		Commands: []*cli.Command{
			configcmd.Command(),
		},
	}
}

// rootFlags are shared by the root action and every subcommand that needs to
// resolve configuration the same way.
func rootFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:    "config",
			Aliases: []string{"c"},
			Usage:   "path to a TOML config file",
			Sources: cli.EnvVars("MAST_CONFIG"),
		},
		&cli.StringFlag{
			Name:  "role",
			Usage: "which job this process does: all-in-one, core, or edge",
		},
		&cli.StringFlag{
			Name:  "log-level",
			Usage: "debug, info, warn, or error",
		},
	}
}
