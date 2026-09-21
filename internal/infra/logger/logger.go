// Package logger builds the process-wide slog handler.
package logger

import (
	"log/slog"
	"os"
	"strings"
)

// New returns a handler for the given level ("debug", "info", "warn",
// "error") and format ("text" or "json"), falling back to info and text when
// either is unrecognized. Configuration errors must not cost you logging.
func New(level, format string) *slog.Logger {
	var lvl slog.Level

	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn", "warning":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{
		AddSource:   false,
		Level:       lvl,
		ReplaceAttr: nil,
	}

	var handler slog.Handler
	if strings.EqualFold(format, "json") {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}

	return slog.New(handler)
}
