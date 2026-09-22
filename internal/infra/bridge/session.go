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

// session is this node's claim on one client id.
type session struct {
	sub   *nats.Subscription
	owner string
}

// sessions tracks the claims this node holds, keyed by mounted client id.
type sessions struct {
	mu   sync.Mutex
	held map[string]session
}

func newSessions() *sessions {
	return &sessions{mu: sync.Mutex{}, held: make(map[string]session)}
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

// claimSession announces that this connection now owns the client id, and
// listens for anyone else claiming it later.
//
// Subscribe before publishing: the order makes this node receive its own
// notice, which is the cheapest proof the subscription is live, and the
// owner token tells it to ignore it.
func (h *Hook) claimSession(id tenant.ID, bareClientID, mountedClientID string) {
	subject := sessionSubject(id, bareClientID)
	owner := newOwner()

	sub, err := h.nc.Subscribe(subject, h.onSessionNotice)
	if err != nil {
		// Not fatal. Without the claim a client can end up connected in two
		// places, which is the bug this exists to prevent, but refusing the
		// connection outright would turn a duplicate-delivery problem into
		// an outage.
		h.log.Error("claiming session", "client", mountedClientID, "error", err)

		return
	}

	h.sessions.mu.Lock()
	previous, replaced := h.sessions.held[mountedClientID]
	h.sessions.held[mountedClientID] = session{sub: sub, owner: owner}
	h.sessions.mu.Unlock()

	// mochi has already displaced any local client with this id, so a claim
	// we were still holding belongs to that dead connection.
	if replaced {
		h.unsubscribeSession(previous.sub, mountedClientID)
	}

	notice, err := json.Marshal(sessionNotice{Owner: owner, Node: h.nodeID})
	if err != nil {
		h.log.Error("encoding session notice", "client", mountedClientID, "error", err)

		return
	}

	if err := h.nc.Publish(subject, notice); err != nil {
		h.log.Error("announcing session", "client", mountedClientID, "error", err)
	}
}

// releaseSession drops this node's claim.
func (h *Hook) releaseSession(mountedClientID string) {
	h.sessions.mu.Lock()
	held, ok := h.sessions.held[mountedClientID]
	delete(h.sessions.held, mountedClientID)
	h.sessions.mu.Unlock()

	if ok {
		h.unsubscribeSession(held.sub, mountedClientID)
	}
}

func (h *Hook) unsubscribeSession(sub *nats.Subscription, mountedClientID string) {
	if err := sub.Unsubscribe(); err != nil {
		h.log.Debug("releasing session claim", "client", mountedClientID, "error", err)
	}
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

	var notice sessionNotice
	if err := json.Unmarshal(msg.Data, &notice); err != nil {
		h.log.Warn("undecodable session notice", "subject", msg.Subject, "error", err)

		return
	}

	id, clientID, err := parseSessionSubject(msg.Subject)
	if err != nil {
		h.log.Warn("undecodable session subject", "subject", msg.Subject, "error", err)

		return
	}

	mounted := mountClient(id, clientID)

	h.sessions.mu.Lock()
	held, ok := h.sessions.held[mounted]
	h.sessions.mu.Unlock()

	// Our own notice, or one for a client we do not hold.
	if !ok || held.owner == notice.Owner {
		return
	}

	cl, ok := h.server.Clients.Get(mounted)
	if !ok {
		h.releaseSession(mounted)

		return
	}

	h.log.Info("session taken over elsewhere, disconnecting local client",
		"client", clientID, "tenant", string(id), "node", notice.Node)

	if err := h.server.DisconnectClient(cl, packets.ErrSessionTakenOver); err != nil {
		h.log.Warn("disconnecting a taken-over client", "client", mounted, "error", err)
	}

	// Ownership has moved, so release everything this node held for it
	// rather than waiting for a session expiry that may never come.
	h.forget(mounted)
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
