// Package obs serves the observability endpoints: Prometheus metrics,
// liveness, and pprof.
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
	}

	if src.Subscriptions != nil {
		m.NATSSubs = prometheus.NewGaugeFunc(prometheus.GaugeOpts{ //nolint:exhaustruct_v5 // defaults are right
			Namespace: namespace,
			Name:      "nats_subscriptions",
			Help:      "NATS subscriptions held by this node. Should track distinct filters, not devices.",
		}, src.Subscriptions)
		reg.MustRegister(m.NATSSubs)
	}

	registerNATSCounts(reg, src.NATS)

	return m
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

// Serve starts the observability server. A blank address disables it.
//
// The listener is opened before returning, and a failure to open it is an
// error rather than a log line. Binding in the background meant a port
// conflict left the process running and reporting ready with nothing on
// /metrics — the same silence this package was written to end, arriving by
// a different route.
func Serve(ctx context.Context, addr string, reg *prometheus.Registry, log *slog.Logger) (*Server, error) {
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

	srv.log.Info("observability listening", "addr", srv.addr, "paths", "/metrics /healthz /debug/pprof")

	return srv, nil
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
