// Package admin serves mast's administrative HTTP API: a tenant's sessions,
// one session, disconnecting a client, and a tenant's counts.
//
// Any node can serve it, a core included, and every answer is for the whole
// cluster. What a node knows itself is only half of it — the connected
// clients of a tenant are spread over every edge — so a request is a
// question put to every node over the control plane, with the stored half
// read from the store, and the two merged by [admin.Merge].
//
// Asking every node is a scatter and gather with a fixed window, because no
// node knows how many others there are: edges come and go with a deploy,
// and the cluster has no registry of them that is not itself eventually
// consistent. A question every node answers waits the whole window; one
// only the holding node answers — a get or a disconnect — returns on the
// first reply, and waits the window only to say nobody has it. Every
// response that depends on the gather says how many nodes answered.
package admin

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/mastmq/mast/internal/domain/admin"
	"github.com/mastmq/mast/internal/infra/config"
	"github.com/mastmq/mast/internal/infra/store"
	"github.com/nats-io/nats.go"
)

// minTokenLen refuses a token short enough to guess. Sixteen bytes is what
// `openssl rand -hex 8` prints; anything a generator makes is longer.
const minTokenLen = 16

// requestTimeout bounds one request's work on the store and the fabric,
// beyond the gather window.
const requestTimeout = 10 * time.Second

// readHeaderTimeout bounds a client that opens a connection and stalls.
const readHeaderTimeout = 5 * time.Second

// shutdownTimeout bounds how long Close waits for requests in flight.
const shutdownTimeout = 5 * time.Second

// Errors reported while starting the API.
var (
	ErrEmptyToken    = errors.New("admin: the token file is empty")
	ErrShortToken    = errors.New("admin: the token is shorter than 16 bytes")
	errStoreNotReady = errors.New("the store is not ready on this node yet")
	errBadLimit      = errors.New("limit must be a positive integer")
)

// Server is a running admin API.
type Server struct {
	http   *http.Server
	nc     *nats.Conn
	store  func() *store.Store
	gather time.Duration
	digest [sha256.Size]byte
	log    *slog.Logger
	addr   string
}

// LoadToken reads the bearer token from a file, trimming the newline an
// editor or `echo` leaves behind.
func LoadToken(path string) ([]byte, error) {
	// The path is the operator's own configuration.
	raw, err := os.ReadFile(path) //nolint:gosec // operator-supplied path
	if err != nil {
		return nil, fmt.Errorf("admin: reading the token: %w", err)
	}

	token := []byte(strings.TrimSpace(string(raw)))

	switch {
	case len(token) == 0:
		return nil, ErrEmptyToken
	case len(token) < minTokenLen:
		return nil, ErrShortToken
	}

	return token, nil
}

// Serve starts the API on cfg.Addr, or does nothing when it is empty.
//
// nc is the node's own connection, which the requests to other nodes go
// out on. st returns the node's store, or nil while it has none yet — a
// core provisions in the background, and its API answers about live
// connections before the store is up.
func Serve(
	ctx context.Context,
	cfg config.Admin,
	nc *nats.Conn,
	st func() *store.Store,
	log *slog.Logger,
) (*Server, error) {
	if cfg.Addr == "" {
		return nil, nil //nolint:nilnil // no address means no server, which is not a failure
	}

	token, err := LoadToken(cfg.TokenFile)
	if err != nil {
		return nil, err
	}

	srv := &Server{
		http:   nil,
		nc:     nc,
		store:  st,
		gather: cfg.Gather,
		// Only the digest is kept. Comparing digests of equal length is
		// what lets the comparison be constant-time without leaking the
		// token's length, which comparing the raw bytes would.
		digest: sha256.Sum256(token),
		log:    log.With("component", "admin"),
		addr:   cfg.Addr,
	}

	srv.http = &http.Server{ //nolint:exhaustruct_v5 // the rest of net/http's defaults are correct
		Addr:              cfg.Addr,
		Handler:           srv.routes(),
		ReadHeaderTimeout: readHeaderTimeout,
	}

	var lc net.ListenConfig

	listener, err := lc.Listen(ctx, "tcp", cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("admin: listening on %s: %w", cfg.Addr, err)
	}

	srv.addr = listener.Addr().String()

	go func() {
		if err := srv.http.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			srv.log.Error("admin api stopped", "error", err)
		}
	}()

	srv.log.Info("admin api listening", "addr", srv.addr)

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
		s.log.Warn("shutting down the admin api", "error", err)
	}
}

// Addr reports the address actually being served.
func (s *Server) Addr() string {
	if s == nil {
		return ""
	}

	return s.addr
}

// routes is the API. The client id is a path segment, so one containing a
// slash has to arrive escaped as %2F; the router matches on the escaped
// path and hands the handler the unescaped value.
func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/tenants/{tenant}/sessions", s.listSessions)
	mux.HandleFunc("GET /v1/tenants/{tenant}/sessions/{client}", s.getSession)
	mux.HandleFunc("POST /v1/tenants/{tenant}/sessions/{client}/disconnect", s.disconnect)
	mux.HandleFunc("GET /v1/tenants/{tenant}/stats", s.stats)

	return s.authenticate(mux)
}

// authenticate admits a request only with the configured bearer token.
func (s *Server) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.authorized(r.Header.Get("Authorization")) {
			s.log.Warn("admin request refused: bad or missing token", "remote", r.RemoteAddr, "path", r.URL.Path)
			w.Header().Set("WWW-Authenticate", `Bearer realm="mast-admin"`)
			s.writeError(w, http.StatusUnauthorized, "a valid bearer token is required")

			return
		}

		next.ServeHTTP(w, r)
	})
}

// authorized compares the presented token with the configured one in
// constant time.
func (s *Server) authorized(header string) bool {
	scheme, token, ok := strings.Cut(header, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return false
	}

	presented := sha256.Sum256([]byte(strings.TrimSpace(token)))

	return subtle.ConstantTimeCompare(presented[:], s.digest[:]) == 1
}

// listSessions is GET /v1/tenants/{tenant}/sessions?limit=&cursor=.
func (s *Server) listSessions(w http.ResponseWriter, r *http.Request) {
	tenantName := r.PathValue("tenant")

	limit, after, err := pageOf(r)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	st := s.store()
	if st == nil {
		s.writeError(w, http.StatusServiceUnavailable, errStoreNotReady.Error())

		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()

	live, nodes, err := s.connectedIn(ctx, tenantName, after, limit)
	if err != nil {
		s.writeError(w, http.StatusBadGateway, err.Error())

		return
	}

	storedIDs, err := st.TenantSessions(ctx, tenantName)
	if err != nil {
		s.writeError(w, http.StatusBadGateway, err.Error())

		return
	}

	stored := make([]admin.Session, 0, len(storedIDs))
	for _, id := range storedIDs {
		stored = append(stored, admin.Session{ //nolint:exhaustruct_v5 // filled from the record below, once paged
			ClientID: id, Persistent: true, Subscriptions: []admin.Subscription{},
		})
	}

	page, more := admin.Merge(live, admin.Window(stored, after, limit), limit)

	if err := s.fillStored(ctx, st, tenantName, page); err != nil {
		s.writeError(w, http.StatusBadGateway, err.Error())

		return
	}

	out := admin.Page{Sessions: page, Next: "", Nodes: nodes}
	if more {
		out.Next = encodeCursor(page[len(page)-1].ClientID)
	}

	s.writeJSON(w, http.StatusOK, out)
}

// connectedIn gathers every node's window of its connected clients in a
// tenant, and how many nodes answered.
func (s *Server) connectedIn(ctx context.Context, tenantName, after string, limit int) ([]admin.Session, int, error) {
	replies, err := s.ask(ctx, admin.SubjectList, listRequest(tenantName, after, limit), false)
	if err != nil {
		return nil, 0, err
	}

	var live []admin.Session

	for _, reply := range replies {
		var window []admin.Session
		if err := json.Unmarshal(reply, &window); err != nil {
			s.log.Warn("undecodable list reply", "error", err)

			continue
		}

		live = append(live, window...)
	}

	return live, len(replies), nil
}

// fillStored completes a page from the store: when each stored record was
// written, what an absent session is subscribed to, and what is queued.
//
// One read per stored session on the page and one count for the whole
// tenant, so a page costs at most a page of reads, whatever the tenant's
// size.
func (s *Server) fillStored(ctx context.Context, st *store.Store, tenantName string, page []admin.Session) error {
	queued, err := st.TenantQueued(ctx, tenantName)
	if err != nil {
		return err
	}

	for i := range page {
		page[i].Queued = queued[page[i].ClientID]

		if !page[i].Persistent {
			continue
		}

		if err := s.fillOne(ctx, st, tenantName, &page[i]); err != nil {
			return err
		}
	}

	return nil
}

// fillOne completes one session from its stored record, if it has one, and
// leaves what a node said about a connected client alone otherwise.
func (s *Server) fillOne(ctx context.Context, st *store.Store, tenantName string, sess *admin.Session) error {
	record, err := st.GetSession(ctx, store.SessionKey(tenantName, sess.ClientID))

	switch {
	case errors.Is(err, store.ErrNotFound):
		// Deleted between the listing and the read: a clean start, or an
		// expiry. What the node said about a live one still stands.
		return nil
	case err != nil:
		return err
	}

	sess.Persistent = true
	updated := record.UpdatedAt
	sess.UpdatedAt = &updated

	// A connected client's own node knows its subscriptions better than
	// the record, which is written only when they change.
	if !sess.Connected {
		sess.Subscriptions = make([]admin.Subscription, 0, len(record.Subscriptions))
		for _, sub := range record.Subscriptions {
			sess.Subscriptions = append(sess.Subscriptions, admin.Subscription{Filter: sub.Filter, QoS: sub.QoS})
		}
	}

	return nil
}

// getSession is GET /v1/tenants/{tenant}/sessions/{client}.
func (s *Server) getSession(w http.ResponseWriter, r *http.Request) {
	tenantName, clientID := r.PathValue("tenant"), r.PathValue("client")

	st := s.store()
	if st == nil {
		s.writeError(w, http.StatusServiceUnavailable, errStoreNotReady.Error())

		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()

	replies, err := s.ask(ctx, admin.SubjectGet, clientRequest(tenantName, clientID), true)
	if err != nil {
		s.writeError(w, http.StatusBadGateway, err.Error())

		return
	}

	sess := admin.Session{ //nolint:exhaustruct_v5 // a stranger until a node or the store says otherwise
		ClientID: clientID, Subscriptions: []admin.Subscription{},
	}

	if len(replies) > 0 {
		if err := json.Unmarshal(replies[0], &sess); err != nil {
			s.writeError(w, http.StatusBadGateway, "undecodable reply from the node holding the client")

			return
		}
	}

	// Read whether connected or not: a connected client may also have a
	// stored record, and an absent one has nothing else.
	if err := s.fillOne(ctx, st, tenantName, &sess); err != nil {
		s.writeError(w, http.StatusBadGateway, err.Error())

		return
	}

	if !sess.Connected && sess.UpdatedAt == nil {
		s.writeError(w, http.StatusNotFound, "no node holds this client and no session is stored for it")

		return
	}

	queued, err := st.Queued(ctx, tenantName, clientID)
	if err != nil {
		s.writeError(w, http.StatusBadGateway, err.Error())

		return
	}

	sess.Queued = queued

	s.writeJSON(w, http.StatusOK, sess)
}

// disconnect is POST /v1/tenants/{tenant}/sessions/{client}/disconnect.
func (s *Server) disconnect(w http.ResponseWriter, r *http.Request) {
	tenantName, clientID := r.PathValue("tenant"), r.PathValue("client")

	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()

	replies, err := s.ask(ctx, admin.SubjectDisconnect, clientRequest(tenantName, clientID), true)
	if err != nil {
		s.writeError(w, http.StatusBadGateway, err.Error())

		return
	}

	if len(replies) == 0 {
		s.writeError(w, http.StatusNotFound, "no node holds a connection for this client")

		return
	}

	var by admin.Session
	if err := json.Unmarshal(replies[0], &by); err != nil {
		s.writeError(w, http.StatusBadGateway, "undecodable reply from the node holding the client")

		return
	}

	s.log.Info("client disconnected through the admin api",
		"tenant", tenantName, "client", clientID, "node", by.Node, "remote", r.RemoteAddr)

	s.writeJSON(w, http.StatusOK, map[string]any{"client_id": clientID, "node": by.Node, "disconnected": true})
}

// stats is GET /v1/tenants/{tenant}/stats.
func (s *Server) stats(w http.ResponseWriter, r *http.Request) {
	tenantName := r.PathValue("tenant")

	st := s.store()
	if st == nil {
		s.writeError(w, http.StatusServiceUnavailable, errStoreNotReady.Error())

		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()

	replies, err := s.ask(ctx, admin.SubjectStats, listRequest(tenantName, "", 0), false)
	if err != nil {
		s.writeError(w, http.StatusBadGateway, err.Error())

		return
	}

	nodes := make([]admin.NodeStats, 0, len(replies))

	for _, reply := range replies {
		var n admin.NodeStats
		if err := json.Unmarshal(reply, &n); err != nil {
			s.log.Warn("undecodable stats reply", "error", err)

			continue
		}

		nodes = append(nodes, n)
	}

	out := admin.Sum(tenantName, nodes)

	stored, err := st.TenantSessions(ctx, tenantName)
	if err != nil {
		s.writeError(w, http.StatusBadGateway, err.Error())

		return
	}

	queued, err := st.TenantQueued(ctx, tenantName)
	if err != nil {
		s.writeError(w, http.StatusBadGateway, err.Error())

		return
	}

	out.StoredSessions = len(stored)
	for _, n := range queued {
		out.QueuedMessages += n
	}

	s.writeJSON(w, http.StatusOK, out)
}

// ask puts a question to every node and gathers the answers for the
// window, or until the first when first is set.
//
// A plain request would take only the first reply and could not tell one
// node's silence from another's, so this subscribes its own inbox. The
// inbox's interest reaches the other nodes before the question does,
// because both leave on the same connection, in order.
func (s *Server) ask(ctx context.Context, subject string, req admin.Request, first bool) ([][]byte, error) {
	data, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("admin: encoding request: %w", err)
	}

	inbox := s.nc.NewRespInbox()

	sub, err := s.nc.SubscribeSync(inbox)
	if err != nil {
		return nil, fmt.Errorf("admin: subscribing for replies: %w", err)
	}

	defer func() { _ = sub.Unsubscribe() }()

	if err := s.nc.PublishRequest(subject, inbox, data); err != nil {
		return nil, fmt.Errorf("admin: asking the cluster: %w", err)
	}

	window, cancel := context.WithTimeout(ctx, s.gather)
	defer cancel()

	var replies [][]byte

	for {
		msg, err := sub.NextMsgWithContext(window)
		if err != nil {
			// The window closing is the normal end of a gather; the
			// request's own context ending is not.
			if ctx.Err() != nil {
				return nil, fmt.Errorf("admin: gathering replies: %w", ctx.Err())
			}

			return replies, nil
		}

		replies = append(replies, msg.Data)

		if first {
			return replies, nil
		}
	}
}

// listRequest asks every node about a tenant.
func listRequest(tenantName, after string, limit int) admin.Request {
	return admin.Request{Tenant: tenantName, ClientID: "", After: after, Limit: limit}
}

// clientRequest asks the node holding one client.
func clientRequest(tenantName, clientID string) admin.Request {
	return admin.Request{Tenant: tenantName, ClientID: clientID, After: "", Limit: 0}
}

// pageOf reads a list request's limit and cursor.
func pageOf(r *http.Request) (int, string, error) {
	limit := admin.DefaultLimit

	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			return 0, "", fmt.Errorf("%w, got %q", errBadLimit, raw)
		}

		limit = admin.ClampLimit(n)
	}

	after, err := decodeCursor(r.URL.Query().Get("cursor"))
	if err != nil {
		return 0, "", err
	}

	return limit, after, nil
}

// encodeCursor makes a client id safe to hand back as a query parameter.
// A client id is whatever the device sent, slashes, ampersands and all.
func encodeCursor(clientID string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(clientID))
}

func decodeCursor(cursor string) (string, error) {
	if cursor == "" {
		return "", nil
	}

	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return "", fmt.Errorf("cursor is not one this api returned: %w", err)
	}

	return string(raw), nil
}

func (s *Server) writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	// The status is already sent, so a failure here is a client that went
	// away, which is worth a debug line and nothing more.
	if err := json.NewEncoder(w).Encode(body); err != nil {
		s.log.Debug("writing an admin response", "error", err)
	}
}

func (s *Server) writeError(w http.ResponseWriter, status int, message string) {
	s.writeJSON(w, status, map[string]string{"error": message})
}
