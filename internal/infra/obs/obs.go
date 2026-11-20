// Package obs serves the observability endpoints: Prometheus metrics,
// liveness, readiness, and pprof.
//
// Liveness and readiness are different questions and are kept apart on
// purpose. /healthz says the process is running and answering, which is what
// a restart can fix. /readyz says the node can serve, which a restart cannot
// fix: an edge whose core is down is not helped by being killed, and a
// liveness probe that failed on it would restart every edge in the fleet the
// moment the core tier hiccupped.
//
// pprof is on by default rather than behind a flag. It costs nothing until
// something scrapes it, and the first time a broker leaks goroutines under
// a reconnect storm is exactly when nobody can redeploy it with a debug
// build. The endpoint listens on its own address, which is expected to stay
// inside the cluster.
package obs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// namespace prefixes every metric mast publishes.
const namespace = "mast"

// Timeouts for the observability server. It serves a handful of local
// scrapes, so these are deliberately tight.
const (
	readHeaderTimeout = 5 * time.Second
	shutdownTimeout   = 5 * time.Second
)

// Server serves metrics, health and pprof.
type Server struct {
	http *http.Server
	log  *slog.Logger
	// addr is what the listener actually bound, which differs from the
	// configured address whenever that asked for port zero.
	addr string
}

// Metrics are the broker's own counters and gauges.
//
// They are deliberately labelled by tenant and never by client: a fleet has
// hundreds of thousands of clients, and a per-client label would take down
// Prometheus long before it took down the broker.
type Metrics struct {
	ConnectionsTotal *prometheus.CounterVec
	ConnectionsOpen  *prometheus.GaugeVec
	AuthFailures     *prometheus.CounterVec
	MessagesIn       *prometheus.CounterVec
	MessagesOut      *prometheus.CounterVec
	RetainedReplayed *prometheus.CounterVec
	OfflineQueued    *prometheus.CounterVec
	OfflineDelivered *prometheus.CounterVec
	NATSSubs         prometheus.GaugeFunc

	// DurableFailures counts QoS 1 and 2 publishes the durable stream would
	// not store. Each one was refused rather than acknowledged, so any
	// increase is publishers being told to retry.
	DurableFailures prometheus.Counter

	// DurableRead counts messages read off the durable stream, by reader:
	// the bridge delivering to local subscribers, the queuer keeping them
	// for absent ones. Every reader on every node should climb at the same
	// rate as the stream; one that stops has lost its consumer.
	DurableRead *prometheus.CounterVec

	// QueuerAway is how many sessions this node's queuer is keeping
	// messages for. It should match on every core.
	QueuerAway prometheus.Gauge

	// QueuerNodesDead counts nodes this core declared dead because their
	// heartbeats stopped. Each one was killed outright, or was cut off or
	// stalled for longer than the threshold — in which case its devices may
	// receive what was queued for them twice.
	QueuerNodesDead prometheus.Counter
	// QueuerReaped counts sessions this core took over from dead nodes.
	QueuerReaped prometheus.Counter
	// QueuerReplayed counts messages queued by replaying the durable stream
	// for them: what was published between a node's death and its
	// detection.
	QueuerReplayed prometheus.Counter
}

// NATSCounts is what the NATS client's asynchronous handlers have observed.
//
// It is declared here, and converted by whoever assembles the node, so that
// obs and natsd do not have to know about each other.
type NATSCounts struct {
	AsyncErrors   uint64
	SlowConsumers uint64
	Disconnects   uint64
	Reconnects    uint64
}

// Sources are values the metrics read when they are scraped, rather than
// values something elsewhere has to remember to update. A nil field leaves
// its metrics unregistered.
type Sources struct {
	// Subscriptions reports NATS subscriptions held by this node.
	Subscriptions func() float64
	// NATS reports the client connection's asynchronous tallies.
	NATS func() NATSCounts
}

// NewMetrics registers mast's metrics on a registry.
func NewMetrics(reg prometheus.Registerer, src Sources) *Metrics {
	factory := func(name, help string, labels ...string) *prometheus.CounterVec {
		c := prometheus.NewCounterVec(prometheus.CounterOpts{ //nolint:exhaustruct_v5 // defaults are right
			Namespace: namespace,
			Name:      name,
			Help:      help,
		}, labels)
		reg.MustRegister(c)

		return c
	}

	gauge := func(name, help string, labels ...string) *prometheus.GaugeVec {
		g := prometheus.NewGaugeVec(prometheus.GaugeOpts{ //nolint:exhaustruct_v5 // defaults are right
			Namespace: namespace,
			Name:      name,
			Help:      help,
		}, labels)
		reg.MustRegister(g)

		return g
	}

	m := &Metrics{
		ConnectionsTotal: factory("connections_total", "MQTT connections accepted.", "tenant"),
		ConnectionsOpen:  gauge("connections_open", "MQTT connections currently open.", "tenant"),
		AuthFailures:     factory("auth_failures_total", "Connections refused by authentication.", "reason"),
		MessagesIn:       factory("messages_in_total", "Messages accepted from clients.", "tenant"),
		MessagesOut:      factory("messages_out_total", "Messages injected towards subscribers.", "tenant"),
		RetainedReplayed: factory("retained_replayed_total", "Retained messages replayed on subscribe.", "tenant"),
		// How much is being held for clients that are not here. A number
		// that climbs and never falls is devices that never came back,
		// which is a fleet problem long before it is a storage one.
		OfflineQueued:    factory("offline_queued_total", "Messages stored for a client that was absent.", "tenant"),
		OfflineDelivered: factory("offline_delivered_total", "Messages replayed to a client that came back.", "tenant"),
		NATSSubs:         nil,
		DurableFailures: prometheus.NewCounter(prometheus.CounterOpts{ //nolint:exhaustruct_v5 // defaults are right
			Namespace: namespace,
			Name:      "durable_publish_failures_total",
			Help:      "QoS 1 and 2 publishes refused because the durable stream would not store them.",
		}),
		DurableRead: factory("durable_read_total", "Messages read off the durable stream.", "reader"),
		QueuerAway: prometheus.NewGauge(prometheus.GaugeOpts{ //nolint:exhaustruct_v5 // defaults are right
			Namespace: namespace,
			Name:      "queuer_away_sessions",
			Help:      "Sessions this node's queuer keeps messages for, because no connection holds them.",
		}),
		QueuerNodesDead: counter(reg, "queuer_nodes_dead_total",
			"Nodes declared dead because their heartbeats stopped."),
		QueuerReaped: counter(reg, "queuer_reaped_sessions_total",
			"Sessions taken over from nodes declared dead."),
		QueuerReplayed: counter(reg, "queuer_replayed_total",
			"Messages queued by replaying the durable stream after a node was declared dead."),
	}

	if src.Subscriptions != nil {
		m.NATSSubs = prometheus.NewGaugeFunc(prometheus.GaugeOpts{ //nolint:exhaustruct_v5 // defaults are right
			Namespace: namespace,
			Name:      "nats_subscriptions",
			Help:      "NATS subscriptions held by this node. Should track distinct filters, not devices.",
		}, src.Subscriptions)
		reg.MustRegister(m.NATSSubs)
	}

	reg.MustRegister(m.DurableFailures, m.QueuerAway)
	registerNATSCounts(reg, src.NATS)

	return m
}

// counter registers one unlabelled counter.
//
//nolint:ireturn // the client library offers a counter only as an interface
func counter(reg prometheus.Registerer, name, help string) prometheus.Counter {
	c := prometheus.NewCounter(prometheus.CounterOpts{ //nolint:exhaustruct_v5 // defaults are right
		Namespace: namespace,
		Name:      name,
		Help:      help,
	})
	reg.MustRegister(c)

	return c
}

// registerNATSCounts publishes the fabric's asynchronous tallies.
//
// They are counter functions rather than counters the code increments,
// because the values already exist on the connection and a second copy is a
// second thing to get wrong.
//
// mast_nats_slow_consumers_total is the one to alert on. Any value above
// zero means this node discarded messages it had already accepted and
// acknowledged, which no other metric here can show: messages_in_total
// counts them as accepted and messages_out_total never counts them at all,
// so the loss appears only as a gap between two numbers nobody is
// subtracting.
func registerNATSCounts(reg prometheus.Registerer, read func() NATSCounts) {
	if read == nil {
		return
	}

	counter := func(name, help string, field func(NATSCounts) uint64) {
		reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{ //nolint:exhaustruct_v5 // defaults are right
			Namespace: namespace,
			Name:      name,
			Help:      help,
		}, func() float64 { return float64(field(read())) }))
	}

	counter("nats_slow_consumers_total",
		"Times the NATS client dropped messages because a subscription's pending queue overflowed.",
		func(c NATSCounts) uint64 { return c.SlowConsumers })
	counter("nats_async_errors_total",
		"Asynchronous errors on the NATS client connection, slow consumers included.",
		func(c NATSCounts) uint64 { return c.AsyncErrors })
	counter("nats_disconnects_total",
		"Times this node lost its connection to the embedded NATS server.",
		func(c NATSCounts) uint64 { return c.Disconnects })
	counter("nats_reconnects_total",
		"Times this node re-established that connection.",
		func(c NATSCounts) uint64 { return c.Reconnects })
}

// Readiness reports whether the node can serve: nil when it can, an error
// saying why not when it cannot.
type Readiness func() error

// Serve starts the observability server. A blank address disables it. A nil
// ready reports the node always ready.
//
// The listener is opened before returning, and a failure to open it is an
// error rather than a log line. Binding in the background meant a port
// conflict left the process running and reporting ready with nothing on
// /metrics — the same silence this package was written to end, arriving by
// a different route.
func Serve(
	ctx context.Context,
	addr string,
	reg *prometheus.Registry,
	ready Readiness,
	log *slog.Logger,
) (*Server, error) {
	if addr == "" {
		return nil, nil //nolint:nilnil // no address means no server, which is not a failure
	}

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{ //nolint:exhaustruct_v5 // defaults are right
		EnableOpenMetrics: true,
	}))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)

		if _, err := w.Write([]byte("ok\n")); err != nil {
			log.Debug("writing healthz response", "error", err)
		}
	})
	mux.HandleFunc("GET /readyz", readyz(ready, log))
	mux.HandleFunc("GET /debug/pprof/", pprof.Index)
	mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)

	srv := &Server{
		http: &http.Server{ //nolint:exhaustruct_v5 // the rest of net/http's defaults are correct
			Addr:              addr,
			Handler:           mux,
			ReadHeaderTimeout: readHeaderTimeout,
		},
		log:  log.With("component", "obs"),
		addr: addr,
	}

	var lc net.ListenConfig

	listener, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("obs: listening on %s: %w", addr, err)
	}

	srv.addr = listener.Addr().String()

	go func() {
		if err := srv.http.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			srv.log.Error("observability server stopped", "error", err)
		}
	}()

	srv.log.Info("observability listening", "addr", srv.addr, "paths", "/metrics /healthz /readyz /debug/pprof")

	return srv, nil
}

// readyz answers 200 when the node can serve and 503 with the reason when
// it cannot, so that a failing probe says why in the kubelet's event.
func readyz(ready Readiness, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		status, body := http.StatusOK, "ok\n"

		if ready != nil {
			if err := ready(); err != nil {
				status, body = http.StatusServiceUnavailable, err.Error()+"\n"
			}
		}

		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(status)

		if _, err := w.Write([]byte(body)); err != nil {
			log.Debug("writing readyz response", "error", err)
		}
	}
}

// Close stops the server.
func (s *Server) Close() {
	if s == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := s.http.Shutdown(ctx); err != nil {
		s.log.Warn("shutting down observability server", "error", err)
	}
}

// Addr reports the address actually being served.
func (s *Server) Addr() string {
	if s == nil {
		return ""
	}

	return s.addr
}
