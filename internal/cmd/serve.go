package cmd

import (
	"context"
	"log/slog"

	"github.com/mastmq/mast/internal/domain/tenant"
	"github.com/mastmq/mast/internal/infra/broker"
	"github.com/mastmq/mast/internal/infra/config"
	"github.com/mastmq/mast/internal/infra/logger"
	"github.com/urfave/cli/v3"
)

// resolve loads configuration and applies the explicitly-set CLI flags on top
// of it. Only flags the user actually passed are applied, so a config file
// value is not clobbered by a flag default.
func resolve(cmd *cli.Command) (config.Config, error) {
	cfg, err := config.Load(cmd.String("config"))
	if err != nil {
		return config.Config{}, err
	}

	if cmd.IsSet("role") {
		cfg.Role = config.Role(cmd.String("role"))
	}

	if cmd.IsSet("log-level") {
		cfg.Log.Level = cmd.String("log-level")
	}

	if err := cfg.Validate(); err != nil {
		return config.Config{}, err
	}

	return cfg, nil
}

// serve runs the broker until the context is cancelled.
func serve(ctx context.Context, cmd *cli.Command) error {
	cfg, err := resolve(cmd)
	if err != nil {
		return err
	}

	log := logger.New(cfg.Log.Level, cfg.Log.Format)
	slog.SetDefault(log)

	log.InfoContext(ctx, "starting mast", "version", cmd.Version, "role", string(cfg.Role))

	node, err := broker.Start(
		cfg,
		tenant.Static{Tenant: tenant.ID(cfg.Tenant.Default)},
		tenant.AllowAll{},
		log,
	)
	if err != nil {
		return err
	}
	defer node.Close()

	log.InfoContext(ctx, "ready",
		"role", string(cfg.Role),
		"jetstream", node.NATS().JetStreamEnabled(),
		"mqtt_addr", cfg.MQTT.Addr,
	)

	<-ctx.Done()

	log.InfoContext(ctx, "shutting down", "nats_subscriptions", node.Subscriptions())

	return nil
}
