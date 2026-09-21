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
	"log/slog"
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
	NATSSubs         prometheus.GaugeFunc
}

// NewMetrics registers mast's metrics on a registry.
func NewMetrics(reg prometheus.Registerer, natsSubs func() float64) *Metrics {
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
		NATSSubs:         nil,
	}

	if natsSubs != nil {
		m.NATSSubs = prometheus.NewGaugeFunc(prometheus.GaugeOpts{ //nolint:exhaustruct_v5 // defaults are right
			Namespace: namespace,
			Name:      "nats_subscriptions",
			Help:      "NATS subscriptions held by this node. Should track distinct filters, not devices.",
		}, natsSubs)
		reg.MustRegister(m.NATSSubs)
	}

	return m
}

// Serve starts the observability server. A blank address disables it.
func Serve(addr string, reg *prometheus.Registry, log *slog.Logger) *Server {
	if addr == "" {
		return nil
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
		log: log.With("component", "obs"),
	}

	go func() {
		if err := srv.http.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			srv.log.Error("observability server stopped", "error", err)
		}
	}()

	srv.log.Info("observability listening", "addr", addr, "paths", "/metrics /healthz /debug/pprof")

	return srv
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

// Addr reports the address being served, for tests.
func (s *Server) Addr() string {
	if s == nil {
		return ""
	}

	return s.http.Addr
}
