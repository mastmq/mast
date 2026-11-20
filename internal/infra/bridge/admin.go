package bridge

import (
	"encoding/json"
	"errors"
	"slices"
	"strings"

	"github.com/mastmq/mast/internal/domain/admin"
	"github.com/mastmq/mast/internal/domain/tenant"
	mqtt "github.com/mastmq/mochi/v2"
	"github.com/mastmq/mochi/v2/packets"
	"github.com/nats-io/nats.go"
)

// listenForAdmin opens the node's one subscription to the admin control
// plane, through which the admin API on any node asks this one about the
// clients it holds.
//
// One subscription for every request, as with session notices: a request
// names its tenant and client in the body, and the node answers from its
// own maps. Every node answers a list or a stats request, so the asking
// node can count who replied; only the node holding a client answers a get
// or a disconnect, so a cluster of fifty edges does not send forty-nine
// "not here" replies for each.
//
// Failing is not fatal. This node's clients go missing from the admin API's
// answers, which is an operator's inconvenience; refusing MQTT over it would
// be an outage.
func (h *Hook) listenForAdmin() {
	sub, err := h.nc.Subscribe(admin.SubjectAll, h.onAdminRequest)
	if err != nil {
		h.log.Error("listening for admin requests: this node's clients are invisible to the admin api", "error", err)

		return
	}

	h.adminSub = sub
}

// onAdminRequest answers one admin control-plane request.
func (h *Hook) onAdminRequest(msg *nats.Msg) {
	if h.server == nil || msg.Reply == "" {
		return
	}

	var req admin.Request
	if err := json.Unmarshal(msg.Data, &req); err != nil || req.Tenant == "" {
		h.log.Warn("undecodable admin request", "subject", msg.Subject, "error", err)

		return
	}

	reply, ok := h.answer(msg.Subject, req)
	if !ok {
		return
	}

	data, err := json.Marshal(reply)
	if err != nil {
		h.log.Error("encoding admin reply", "subject", msg.Subject, "error", err)

		return
	}

	if err := msg.Respond(data); err != nil {
		h.log.Warn("answering admin request", "subject", msg.Subject, "error", err)
	}
}

// answer is this node's reply to one request, and false when this node has
// nothing to say: a get or a disconnect for a client it does not hold.
func (h *Hook) answer(subject string, req admin.Request) (any, bool) {
	id := tenant.ID(req.Tenant)

	switch subject {
	case admin.SubjectList:
		return admin.Window(h.connectedIn(id), req.After, admin.ClampLimit(req.Limit)), true
	case admin.SubjectStats:
		return h.statsFor(id), true
	case admin.SubjectGet:
		cl, ok := h.connected(id, req.ClientID)
		if !ok {
			return nil, false
		}

		return h.describe(id, cl), true
	case admin.SubjectDisconnect:
		if !h.disconnect(id, req.ClientID) {
			return nil, false
		}

		//nolint:exhaustruct_v5 // names the node, nothing else
		return admin.Session{ClientID: req.ClientID, Node: h.nodeID}, true
	default:
		return nil, false
	}
}

// openIn returns the connections this node holds for a tenant.
//
// It reads the gauge's map, which holds exactly the connections that are
// open: mochi keeps a persistent client in its own map while it is away,
// and that one is not connected anywhere.
func (h *Hook) openIn(id tenant.ID) []*mqtt.Client {
	h.mu.Lock()
	defer h.mu.Unlock()

	var out []*mqtt.Client

	for cl, owner := range h.open {
		if owner == id {
			out = append(out, cl)
		}
	}

	return out
}

// connected returns the open connection for one client in a tenant.
func (h *Hook) connected(id tenant.ID, clientID string) (*mqtt.Client, bool) {
	cl, ok := h.server.Clients.Get(mountClient(id, clientID))
	if !ok {
		return nil, false
	}

	h.mu.Lock()
	owner, open := h.open[cl]
	h.mu.Unlock()

	return cl, open && owner == id
}

// connectedIn describes every client this node holds for a tenant.
func (h *Hook) connectedIn(id tenant.ID) []admin.Session {
	conns := h.openIn(id)

	out := make([]admin.Session, 0, len(conns))
	for _, cl := range conns {
		out = append(out, h.describe(id, cl))
	}

	return out
}

// describe is one connected client as the admin API reports it.
//
// Filters are shown as the sessions bucket stores them: without the tenant,
// with any share prefix. Inflight comes from mochi, which is also what
// decides what the client still owes an acknowledgement for.
func (h *Hook) describe(id tenant.ID, cl *mqtt.Client) admin.Session {
	subs := cl.State.Subscriptions.GetAll()

	filters := make([]admin.Subscription, 0, len(subs))
	for mounted, sub := range subs {
		filters = append(filters, admin.Subscription{Filter: storedFilter(id, mounted), QoS: sub.Qos})
	}

	slices.SortFunc(filters, func(a, b admin.Subscription) int { return strings.Compare(a.Filter, b.Filter) })

	return admin.Session{
		ClientID:        unmountClient(id, cl.ID),
		Connected:       true,
		Node:            h.nodeID,
		Remote:          cl.Net.Remote,
		ProtocolVersion: cl.Properties.ProtocolVersion,
		Persistent:      outlivesConnection(cl),
		Subscriptions:   filters,
		Inflight:        cl.State.Inflight.Len(),
		Queued:          0,
		UpdatedAt:       nil,
	}
}

// statsFor counts this node's connections and their filters in a tenant.
func (h *Hook) statsFor(id tenant.ID) admin.NodeStats {
	stats := admin.NodeStats{Node: h.nodeID, Connections: 0, Subscriptions: 0}

	for _, cl := range h.openIn(id) {
		stats.Connections++
		stats.Subscriptions += cl.State.Subscriptions.Len()
	}

	return stats
}

// disconnect drops one client by administrative action, and reports
// whether this node held it.
//
// It is mochi's own disconnect with MQTT 5's "administrative action" reason,
// and nothing else. The session is not touched: mochi reports the
// disconnect through OnDisconnect like any other, so a persistent session
// is marked away and queued for, and a clean one is dropped, exactly as if
// the device had gone by itself. Taking the session over instead — sending
// a session notice with a fresh owner, which would also have reached the
// right node — empties mochi's copy and releases the NATS interest on the
// assumption that another connection now holds them, which here none does.
func (h *Hook) disconnect(id tenant.ID, clientID string) bool {
	cl, ok := h.connected(id, clientID)
	if !ok {
		return false
	}

	h.log.Info("disconnecting client by administrative action", "client", clientID, "tenant", string(id))

	// mochi returns the reason code itself as the error for any failing
	// code, which this always is, so only a different error is news.
	if err := h.server.DisconnectClient(cl, packets.ErrAdministrativeAction); err != nil &&
		!errors.Is(err, packets.ErrAdministrativeAction) {
		h.log.Warn("disconnecting a client by administrative action", "client", clientID, "error", err)
	}

	return true
}
