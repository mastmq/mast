package bridge

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"strings"
	"sync"

	"github.com/mastmq/mast/internal/domain/tenant"
	"github.com/mastmq/mast/internal/domain/topic"
	"github.com/mastmq/mochi/v2/packets"
	"github.com/nats-io/nats.go"
)

// sessionRoot is the control-plane subject prefix.
//
// Deliberately outside the "t." namespace that carries tenant traffic.
// Every filter a client can subscribe to is mounted under its tenant and
// encoded by [topic.EncodeFilter], which always produces "t.<tenant>...."
// — so no client, wildcard or otherwise, can reach this root, and none can
// forge a message on it.
const sessionRoot = "mast.session"

// ownerLen is the byte length of a connection's owner token before hex
// encoding. It only has to be long enough that two live connections never
// collide, which 8 bytes is by a wide margin.
const ownerLen = 8

// sessionNotice announces that a connection has claimed a session.
//
// It carries the claimant rather than the client id, because the client id
// is already in the subject. Owner identifies the connection, not the node:
// the node that publishes a notice is also subscribed to it, so it has to
// recognise its own, and two connections on the same node must still
// displace each other if mochi somehow did not.
type sessionNotice struct {
	Owner string `json:"owner"`
	Node  string `json:"node"`
}

// sessions tracks the claims this node holds: the owner token of each
// mounted client id.
//
// One subscription hears every notice in the cluster, rather than one per
// claim. A claim used to open its own NATS subscription on the client's
// subject, which was the natural shape and the expensive one: nats.go runs
// a goroutine per subscription, each SUBSCRIBE and UNSUBSCRIBE is a round
// trip to the core and an interest update across the cluster, and a
// persistent session keeps its claim for as long as it lives, so an edge
// holding 25k devices held 25k subscriptions and goroutines for them even
// while they were asleep. A connection storm was a subscription storm. The
// wildcard puts every notice in front of every node instead, which is one
// small message per CONNECT anywhere in the cluster, and a map lookup
// decides whether it concerns this one.
type sessions struct {
	mu   sync.Mutex
	held map[string]string
}

func newSessions() *sessions {
	return &sessions{mu: sync.Mutex{}, held: make(map[string]string)}
}

// noticeSubject is the node's one subscription to the control plane.
const noticeSubject = sessionRoot + ".>"

// listenForNotices opens the node's subscription to the control plane.
//
// Failing is not fatal, for the same reason a failed claim never was:
// without it a client can end up live in two places, which is the bug this
// prevents, but refusing every connection would turn duplicate delivery
// into an outage.
func (h *Hook) listenForNotices() {
	sub, err := h.nc.Subscribe(noticeSubject, h.onSessionNotice)
	if err != nil {
		h.log.Error("listening for session notices: cross-node takeover is disabled on this node", "error", err)

		return
	}

	h.noticeSub = sub
}

// sessionSubject is where notices for one client id are published.
//
// Both tokens are escaped. The tenant is already restricted to characters a
// subject accepts, so escaping it changes nothing in practice and costs
// nothing; it is here so that a resolver returning something unexpected
// cannot widen the subject. The client id has no such restriction — it is
// whatever the device sent — and is the reason [topic.EncodeToken] exists.
func sessionSubject(id tenant.ID, clientID string) string {
	return sessionRoot + "." + topic.EncodeToken(string(id)) + "." + topic.EncodeToken(clientID)
}

// newOwner returns a token unique to one connection.
func newOwner() string {
	b := make([]byte, ownerLen)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand does not fail on any supported platform, and a
		// broker that refused connections because of it would be worse
		// than one that reused a token.
		return "unknown"
	}

	return hex.EncodeToString(b)
}

// claimSession announces that this connection now owns the client id.
//
// Recorded before it is published, so the node's own notice finds the claim
// and recognises the owner token rather than taking the session over from
// itself. mochi has already displaced any local client with this id, so a
// claim still held for it belongs to that dead connection and is simply
// overwritten.
func (h *Hook) claimSession(id tenant.ID, bareClientID, mountedClientID string) {
	owner := newOwner()

	h.sessions.mu.Lock()
	h.sessions.held[mountedClientID] = owner
	h.sessions.mu.Unlock()

	notice, err := json.Marshal(sessionNotice{Owner: owner, Node: h.nodeID})
	if err != nil {
		h.log.Error("encoding session notice", "client", mountedClientID, "error", err)

		return
	}

	if err := h.nc.Publish(sessionSubject(id, bareClientID), notice); err != nil {
		h.log.Error("announcing session", "client", mountedClientID, "error", err)
	}
}

// releaseSession drops this node's claim.
func (h *Hook) releaseSession(mountedClientID string) {
	h.sessions.mu.Lock()
	delete(h.sessions.held, mountedClientID)
	h.sessions.mu.Unlock()
}

// holdsSession reports the owner token this node holds for a client, if any.
func (h *Hook) holdsSession(mountedClientID string) (string, bool) {
	h.sessions.mu.Lock()
	defer h.sessions.mu.Unlock()

	owner, ok := h.sessions.held[mountedClientID]

	return owner, ok
}

// onSessionNotice disconnects a local client whose session has been claimed
// somewhere else.
//
// MQTT requires that a second connection with an existing client id
// displaces the first. mochi enforces that within a node by keying its
// client map on the id; across nodes nothing did, so the same device
// reconnecting to a different pod — which is every rolling deploy — left
// two live sessions receiving the same traffic.
func (h *Hook) onSessionNotice(msg *nats.Msg) {
	if h.server == nil {
		return
	}

	id, clientID, err := parseSessionSubject(msg.Subject)
	if err != nil {
		h.log.Warn("undecodable session subject", "subject", msg.Subject, "error", err)

		return
	}

	// Every CONNECT in the cluster lands here, and nearly all of them are
	// for clients this node has never heard of, so that is decided on the
	// subject alone before the notice is decoded.
	mounted := mountClient(id, clientID)

	owner, ok := h.holdsSession(mounted)
	if !ok {
		return
	}

	var notice sessionNotice
	if err := json.Unmarshal(msg.Data, &notice); err != nil {
		h.log.Warn("undecodable session notice", "subject", msg.Subject, "error", err)

		return
	}

	// Our own notice.
	if owner == notice.Owner {
		return
	}

	cl, ok := h.server.Clients.Get(mounted)
	if !ok {
		h.releaseSession(mounted)

		return
	}

	h.log.Info("session taken over elsewhere, disconnecting local client",
		"client", clientID, "tenant", string(id), "node", notice.Node)

	// mochi reports the disconnect from the client's own goroutine, which
	// may run before or after forget below drops the tenant. Released here
	// as well, the gauge comes down exactly once either way; left to mochi
	// alone, it climbed by one on most cross-node takeovers.
	h.onDisconnected(cl)

	if err := h.server.DisconnectClient(cl, packets.ErrSessionTakenOver); err != nil {
		h.log.Warn("disconnecting a taken-over client", "client", mounted, "error", err)
	}

	// Ownership has moved, so release everything this node held for it
	// rather than waiting for a session expiry that may never come.
	h.forget(mounted)

	// And empty mochi's copy of the session, which it keeps for a
	// persistent client until expiry. Left alone, a device that went to
	// another node and came back here would inherit it: subscriptions that
	// no longer have NATS interest behind them, so it received nothing, and
	// an inflight backlog the other node may already have delivered.
	// Emptied, the device arrives here as a stranger and the session is
	// restored from the bucket like on any other node.
	//
	// After forget, deliberately: UnsubscribeClient calls OnUnsubscribed,
	// which would otherwise persist the now-empty subscription list over
	// the session the new owner is relying on.
	h.server.UnsubscribeClient(cl)
	cl.ClearInflights()
}

// parseSessionSubject reverses [sessionSubject].
func parseSessionSubject(subject string) (tenant.ID, string, error) {
	rest, ok := strings.CutPrefix(subject, sessionRoot+".")
	if !ok {
		return "", "", errNotASessionSubject
	}

	tenantToken, clientToken, ok := strings.Cut(rest, ".")
	if !ok {
		return "", "", errNotASessionSubject
	}

	tenantName, err := topic.DecodeToken(tenantToken)
	if err != nil {
		return "", "", err
	}

	clientID, err := topic.DecodeToken(clientToken)
	if err != nil {
		return "", "", err
	}

	return tenant.ID(tenantName), clientID, nil
}
