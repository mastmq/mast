package natsd

import (
	"fmt"
	"log/slog"
)

// natsLogger adapts slog to the nats-server logger interface so the embedded
// server's output lands in the same stream, with the same format, as mast's.
type natsLogger struct {
	log *slog.Logger
}

func newLogger(log *slog.Logger) *natsLogger {
	return &natsLogger{log: log.With("component", "nats")}
}

// Noticef implements the nats-server logger interface.
func (l *natsLogger) Noticef(format string, v ...any) { l.log.Info(fmt.Sprintf(format, v...)) }

// Warnf implements the nats-server logger interface.
func (l *natsLogger) Warnf(format string, v ...any) { l.log.Warn(fmt.Sprintf(format, v...)) }

// Fatalf implements the nats-server logger interface. It logs rather than
// exiting: the embedded server's lifetime belongs to mast, not to itself.
func (l *natsLogger) Fatalf(format string, v ...any) { l.log.Error(fmt.Sprintf(format, v...)) }

// Errorf implements the nats-server logger interface.
func (l *natsLogger) Errorf(format string, v ...any) { l.log.Error(fmt.Sprintf(format, v...)) }

// Debugf implements the nats-server logger interface.
func (l *natsLogger) Debugf(format string, v ...any) { l.log.Debug(fmt.Sprintf(format, v...)) }

// Tracef implements the nats-server logger interface. Traces go to debug:
// nats-server's trace output is far too loud for info.
func (l *natsLogger) Tracef(format string, v ...any) { l.log.Debug(fmt.Sprintf(format, v...)) }

// Systemf implements the nats-server logger interface.
func (l *natsLogger) Systemf(format string, v ...any) { l.log.Info(fmt.Sprintf(format, v...)) }
