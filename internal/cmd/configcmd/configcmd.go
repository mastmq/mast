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
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "config",
				Aliases: []string{"c"},
				Usage:   "path to a TOML config file",
				Sources: cli.EnvVars("MAST_CONFIG"),
			},
		},
		Action: func(_ context.Context, cmd *cli.Command) error {
			cfg, err := config.Load(cmd.String("config"))
			if err != nil {
				return err
			}

			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")

			if err := enc.Encode(cfg); err != nil {
				return fmt.Errorf("encoding configuration: %w", err)
			}

			return nil
		},
	}
}
