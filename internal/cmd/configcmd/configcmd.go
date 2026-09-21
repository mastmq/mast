// Package configcmd implements the "mast config" subcommand group.
package configcmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/mastmq/mast/internal/infra/config"
	"github.com/urfave/cli/v3"
)

// Command returns the "config" subcommand group.
func Command() *cli.Command {
	return &cli.Command{
		Name:  "config",
		Usage: "inspect the resolved configuration",
		Commands: []*cli.Command{
			showCommand(),
		},
	}
}

// showCommand prints the configuration exactly as the broker would see it,
// after defaults, file, and environment have been layered.
func showCommand() *cli.Command {
	return &cli.Command{
		Name:  "show",
		Usage: "print the resolved configuration as JSON",
		// No --config flag of its own. Redeclaring it here shadowed the
		// root's persistent flag with an empty default, so
		// `mast -c file config show` silently ignored the file and printed
		// the built-in defaults instead — the worst possible answer from a
		// command whose entire job is to tell you what the config is.
		Action: func(_ context.Context, cmd *cli.Command) error {
			cfg, err := config.Load(cmd.String("config"))
			if err != nil {
				return err
			}

			out := cmd.Root().Writer
			if out == nil {
				out = os.Stdout
			}

			enc := json.NewEncoder(out)
			enc.SetIndent("", "  ")

			if err := enc.Encode(cfg); err != nil {
				return fmt.Errorf("encoding configuration: %w", err)
			}

			return nil
		},
	}
}
