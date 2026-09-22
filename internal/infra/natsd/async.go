package natsd

import (
	"errors"
	"log/slog"

	"github.com/nats-io/nats.go"
)

// Counts reports what the client connection's asynchronous handlers have
// observed since the process started. Every field is monotonic.
//
// These are the numbers that answer "is this node losing messages". Nothing
// else does: a drop happens after the publish that lost it has already
// returned success, so no synchronous error path ever sees one.
type Counts struct {
	// AsyncErrors is every asynchronous error, slow consumers included.
	AsyncErrors uint64
	// SlowConsumers counts the errors that mean messages were discarded.
	SlowConsumers uint64
	// Disconnects counts losing the connection to the embedded server.
	Disconnects uint64
	// Reconnects counts getting it back.
	Reconnects uint64
}

// Counts returns the asynchronous handler tallies.
func (s *Server) Counts() Counts {
	return Counts{
		AsyncErrors:   s.asyncErrors.Load(),
		SlowConsumers: s.slowConsumers.Load(),
		Disconnects:   s.disconnects.Load(),
		Reconnects:    s.reconnects.Load(),
	}
}

// onAsyncError records and reports an asynchronous error on the client
// connection.
//
// [nats.ErrSlowConsumer] is the one that matters. It is how a NATS client
// says it has dropped messages: the pending queue for a subscription
// overflowed and what did not fit is gone. Nothing upstream can notice on
// its own — the bridge published successfully, the MQTT publisher was
// acknowledged before the message crossed the fabric, and the subscriber
// simply never receives. This handler is the only place that fact surfaces,
// which is why the connection must never be opened without it.
func (s *Server) onAsyncError(_ *nats.Conn, sub *nats.Subscription, err error) {
	s.asyncErrors.Add(1)

	attrs := errAttrs(sub, err)

	if errors.Is(err, nats.ErrSlowConsumer) {
		s.slowConsumers.Add(1)
		s.log.Error("nats slow consumer: messages were dropped", attrs...)

		return
	}

	s.log.Error("nats asynchronous error", attrs...)
}

// errAttrs describes an asynchronous error and the subscription it arrived
// on. Connection-level errors carry no subscription, so nil is expected
// rather than exceptional.
//
// dropped_total is cumulative for that subscription rather than a count for
// this event, and pending against the limit is what says whether the
// overflow is still happening or has already passed.
func errAttrs(sub *nats.Subscription, err error) []any {
	attrs := []any{slog.Any("error", err)}

	if sub == nil {
		return attrs
	}

	attrs = append(attrs, slog.String("subject", sub.Subject))

	if sub.Queue != "" {
		attrs = append(attrs, slog.String("queue", sub.Queue))
	}

	if dropped, derr := sub.Dropped(); derr == nil {
		attrs = append(attrs, slog.Int("dropped_total", dropped))
	}

	if msgs, bytes, perr := sub.Pending(); perr == nil {
		attrs = append(attrs, slog.Int("pending_msgs", msgs), slog.Int("pending_bytes", bytes))
	}

	if maxMsgs, maxBytes, lerr := sub.PendingLimits(); lerr == nil {
		attrs = append(attrs, slog.Int("limit_msgs", maxMsgs), slog.Int("limit_bytes", maxBytes))
	}

	return attrs
}

// onDisconnect reports losing the connection to the embedded server.
//
// For an in-process connection over net.Pipe this should not happen at all
// short of the server going down underneath us, so it is an error rather
// than a warning. Anything published while disconnected is held in the
// client's reconnect buffer and discarded once that fills.
func (s *Server) onDisconnect(_ *nats.Conn, err error) {
	s.disconnects.Add(1)

	if s.closing.Load() {
		s.log.Debug("nats connection closed during shutdown", "error", err)

		return
	}

	s.log.Error("nats connection lost; publishes are buffered and then dropped", "error", err)
}

// onReconnect reports getting the connection back. It is a warning rather
// than an info line because a reconnect that nobody expected is the
// beginning of an incident, and it is much easier to find at warn.
func (s *Server) onReconnect(nc *nats.Conn) {
	s.reconnects.Add(1)
	s.log.Warn("nats connection re-established", "url", nc.ConnectedUrl())
}

// onClosed reports the connection closing for good. Outside shutdown this
// means the node is still accepting MQTT while being unable to route any of
// it, which is the worst state to be in silently.
func (s *Server) onClosed(_ *nats.Conn) {
	if s.closing.Load() {
		s.log.Debug("nats connection closed")

		return
	}

	s.log.Error("nats connection closed permanently; this node can no longer route messages")
}
