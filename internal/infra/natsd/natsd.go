// Package natsd runs the nats-server that mast embeds, and hands out an
// in-process client connection to it.
//
// Embedding rather than operating a separate cluster is what keeps mast a
// single artifact: the same binary is a standalone broker on a gateway box and
// a node of a large cluster, decided by configuration. The client connection
// is established over net.Pipe through [nats.InProcessServer], so the bridge
// never crosses a socket to reach its own server.
package natsd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/mastmq/mast/internal/infra/config"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// readyTimeout bounds how long Start waits for the embedded server to accept
// connections before giving up.
const readyTimeout = 15 * time.Second

// ErrNotReady is returned when the embedded server does not come up in time.
var ErrNotReady = errors.New("natsd: server did not become ready")

// Server is a running embedded nats-server together with an in-process client
// connection to it.
type Server struct {
	ns  *server.Server
	nc  *nats.Conn
	log *slog.Logger

	// closing tells the asynchronous handlers that a disconnect is the
	// shutdown they were asked for rather than an incident. Without it every
	// clean stop logs an error about losing the fabric.
	closing atomic.Bool

	// What the asynchronous handlers have seen. Read through [Server.Counts].
	asyncErrors   atomic.Uint64
	slowConsumers atomic.Uint64
	disconnects   atomic.Uint64
	reconnects    atomic.Uint64
}

// Conn returns the in-process client connection. It is safe for concurrent
// use, as every nats.Conn is.
func (s *Server) Conn() *nats.Conn { return s.nc }

// JetStreamEnabled reports whether this node carries JetStream. Edge nodes do
// not: they reach the core tier's KV buckets over their leaf connection.
func (s *Server) JetStreamEnabled() bool { return s.ns.JetStreamEnabled() }

// ClusterAddr reports the route listener address, empty when not clustered.
func (s *Server) ClusterAddr() string {
	if addr := s.ns.ClusterAddr(); addr != nil {
		return addr.String()
	}

	return ""
}

// Start builds options for the configured role, boots the embedded server, and
// connects to it in-process.
func Start(cfg config.Config, log *slog.Logger) (*Server, error) {
	opts, err := options(cfg)
	if err != nil {
		return nil, err
	}

	ns, err := server.NewServer(opts)
	if err != nil {
		return nil, fmt.Errorf("natsd: building server: %w", err)
	}

	ns.SetLoggerV2(newLogger(log), opts.Debug, opts.Trace, false)

	go ns.Start()

	if !ns.ReadyForConnections(readyTimeout) {
		ns.Shutdown()

		return nil, ErrNotReady
	}

	s := &Server{
		ns:            ns,
		nc:            nil,
		log:           log,
		closing:       atomic.Bool{},
		asyncErrors:   atomic.Uint64{},
		slowConsumers: atomic.Uint64{},
		disconnects:   atomic.Uint64{},
		reconnects:    atomic.Uint64{},
	}

	// The handlers are not optional instrumentation. A nats.Conn reports a
	// dropped message once, asynchronously, and nowhere else; its default is
	// to report it to nobody. Connecting without them is how a node discards
	// traffic while every counter mast keeps still reads as healthy.
	nc, err := nats.Connect("",
		nats.InProcessServer(ns),
		nats.Name("mast-bridge"),
		nats.ErrorHandler(s.onAsyncError),
		nats.DisconnectErrHandler(s.onDisconnect),
		nats.ReconnectHandler(s.onReconnect),
		nats.ClosedHandler(s.onClosed),
	)
	if err != nil {
		ns.Shutdown()

		return nil, fmt.Errorf("natsd: connecting in-process: %w", err)
	}

	s.nc = nc

	return s, nil
}

// WaitForLeaf blocks until this node has an upstream leaf connection, or ctx
// is done.
//
// An edge carries no JetStream of its own and reaches the core's over that
// link, so anything touching the store before it exists gets "jetstream not
// enabled" from its own server rather than an answer from the core. Opening
// the store is fatal by design, so without this an edge that starts before
// the core is reachable does not degrade — it dies, and in Kubernetes it
// crashloops until the core happens to win the race.
func (s *Server) WaitForLeaf(ctx context.Context) error {
	const poll = 50 * time.Millisecond

	for {
		if s.ns.NumLeafNodes() > 0 {
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("natsd: waiting for leaf connection to the core: %w", ctx.Err())
		case <-time.After(poll):
		}
	}
}

// Shutdown drains the client connection and stops the server.
func (s *Server) Shutdown() {
	s.closing.Store(true)

	if s.nc != nil {
		if err := s.nc.Drain(); err != nil {
			s.log.Warn("draining nats connection", "error", err)
		}
	}

	s.ns.Shutdown()
	s.ns.WaitForShutdown()
}

// options translates mast configuration into nats-server options.
//
// The shape differs per role and the differences are the whole point. A core
// node carries JetStream and accepts routes and leaf connections. An edge node
// carries no JetStream and dials the core tier as a leaf, because a full route
// mesh grows quadratically and an autoscaled MQTT tier is the wrong shape for
// it. An all-in-one node carries JetStream and talks to nobody.
func options(cfg config.Config) (*server.Options, error) {
	opts := &server.Options{
		ServerName: serverName(cfg),
		NoSigs:     true,
		JetStream:  cfg.Role != config.RoleEdge,
	}

	if opts.JetStream {
		opts.StoreDir = cfg.Core.StoreDir
		opts.JetStreamDomain = JetStreamDomain
	}

	if err := applyClientListener(opts, cfg.NATS.ClientAddr); err != nil {
		return nil, err
	}

	if err := applyMonitor(opts, cfg.NATS.MonitorAddr); err != nil {
		return nil, err
	}

	switch cfg.Role {
	case config.RoleCore:
		if err := applyCore(opts, cfg); err != nil {
			return nil, err
		}
	case config.RoleEdge:
		if err := applyEdge(opts, cfg); err != nil {
			return nil, err
		}
	case config.RoleAllInOne:
		// Nothing to join and nobody to accept: an all-in-one node is the
		// whole cluster.
	}

	return opts, nil
}

// applyClientListener configures the TCP client port. An empty address means
// in-process only, which is what an edge node wants unless an operator needs
// the nats CLI against it.
func applyClientListener(opts *server.Options, addr string) error {
	if addr == "" {
		opts.DontListen = true

		return nil
	}

	host, port, err := splitHostPort(addr, "nats.client_addr")
	if err != nil {
		return err
	}

	opts.Host, opts.Port = host, port

	return nil
}

// applyMonitor configures the HTTP monitoring endpoints. Leaving them off
// means having no visibility into your own data plane, so mast defaults them
// on and only an explicit empty value disables them.
func applyMonitor(opts *server.Options, addr string) error {
	if addr == "" {
		return nil
	}

	host, port, err := splitHostPort(addr, "nats.monitor_addr")
	if err != nil {
		return err
	}

	opts.HTTPHost, opts.HTTPPort = host, port

	return nil
}

func applyCore(opts *server.Options, cfg config.Config) error {
	if cfg.Core.ListenAddr != "" {
		host, port, err := splitHostPort(cfg.Core.ListenAddr, "core.listen_addr")
		if err != nil {
			return err
		}

		opts.Cluster = server.ClusterOpts{Name: clusterName, Host: host, Port: port}
	}

	if cfg.Core.LeafAddr != "" {
		host, port, err := splitHostPort(cfg.Core.LeafAddr, "core.leaf_addr")
		if err != nil {
			return err
		}

		opts.LeafNode.Host, opts.LeafNode.Port = host, port
	}

	routes, err := parseURLs(cfg.Core.Routes, "core.routes")
	if err != nil {
		return err
	}

	opts.Routes = routes

	return nil
}

func applyEdge(opts *server.Options, cfg config.Config) error {
	urls, err := parseURLs(cfg.Edge.CoreURLs, "edge.core_urls")
	if err != nil {
		return err
	}

	remote := &server.RemoteLeafOpts{URLs: urls}
	if cfg.Edge.Credentials != "" {
		remote.Credentials = cfg.Edge.Credentials
	}

	opts.LeafNode.Remotes = []*server.RemoteLeafOpts{remote}

	return nil
}

// clusterName is fixed: every mast node belongs to the same logical cluster,
// and tenancy is expressed in subjects and accounts rather than in topology.
const clusterName = "mast"

// JetStreamDomain names the storage tier so an edge can address it.
//
// Without a domain an edge is stuck. Its own embedded server runs with
// JetStream disabled, and a request to the default $JS.API prefix is
// answered by that local server with "jetstream not enabled" rather than
// being forwarded over the leaf connection. Naming the domain gives the
// core's JetStream an address of its own, $JS.mast.API, which the leaf link
// carries like any other subject.
//
// It is a constant for the same reason clusterName is: both sides have to
// agree on it, and a knob whose only correct value is the one the other side
// also chose is not a knob.
const JetStreamDomain = "mast"

func serverName(cfg config.Config) string {
	if cfg.NATS.Name != "" {
		return cfg.NATS.Name
	}

	return "mast-" + string(cfg.Role)
}

func splitHostPort(addr, field string) (string, int, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, fmt.Errorf("natsd: %s %q: %w", field, addr, err)
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", 0, fmt.Errorf("natsd: %s %q: bad port: %w", field, addr, err)
	}

	return host, port, nil
}

func parseURLs(raw []string, field string) ([]*url.URL, error) {
	if len(raw) == 0 {
		return nil, nil
	}

	urls := make([]*url.URL, 0, len(raw))

	for _, s := range raw {
		u, err := url.Parse(s)
		if err != nil {
			return nil, fmt.Errorf("natsd: %s %q: %w", field, s, err)
		}

		urls = append(urls, u)
	}

	return urls, nil
}
