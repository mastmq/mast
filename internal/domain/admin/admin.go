// Package admin is the shape of mast's administrative view of a tenant: what
// the admin API answers with, what nodes tell each other to build that
// answer, and how the answers of many nodes and the store become one page.
//
// No single place knows a tenant's sessions. A connected client is known
// only to the node holding its connection, and a persistent session that is
// away is known only to the sessions bucket. A cluster-wide answer is
// therefore a merge, and the merge is pure, which is why it lives here and
// not beside the network code that gathers its inputs.
package admin

import (
	"cmp"
	"slices"
	"time"
)

// Control-plane subjects the admin API asks nodes on.
//
// They sit under "mast.", outside the "t." namespace every client filter is
// mounted into, for the same reason the session control plane does: no
// MQTT client, wildcard or not, can subscribe to one or publish on one. The
// tenant and client id travel in the body, not the subject, so neither has
// to be escaped into a token and no request can widen its own subject.
const (
	// SubjectAll is the one subscription a node holds for all of them.
	SubjectAll = "mast.admin.>"
	// SubjectList asks every node for its connected clients in a tenant.
	SubjectList = "mast.admin.sessions"
	// SubjectGet asks the node holding one client to describe it.
	SubjectGet = "mast.admin.session"
	// SubjectDisconnect asks the node holding one client to drop it.
	SubjectDisconnect = "mast.admin.disconnect"
	// SubjectStats asks every node for its counts in a tenant.
	SubjectStats = "mast.admin.stats"
)

// Page sizes. The maximum bounds both the HTTP response and each node's
// reply, which crosses the fabric as one NATS message.
const (
	DefaultLimit = 100
	MaxLimit     = 1000
)

// Request is what the admin API sends a node.
type Request struct {
	Tenant   string `json:"tenant"`
	ClientID string `json:"client_id,omitempty"`
	// After and Limit page a list: client ids strictly after After, at most
	// Limit of them.
	After string `json:"after,omitempty"`
	Limit int    `json:"limit,omitempty"`
}

// Subscription is one filter a session holds, as the client wrote it.
type Subscription struct {
	Filter string `json:"filter"`
	QoS    byte   `json:"qos"`
}

// Session is one client's session as the admin API reports it.
type Session struct {
	ClientID string `json:"client_id"`
	// Connected is whether a node answered that it holds a connection for
	// the client. A node that did not answer within the gather window is
	// not counted, so false means "no node said so", not "proved absent".
	Connected bool `json:"connected"`
	// Node is the embedded NATS server id of the node holding it.
	Node            string `json:"node,omitempty"`
	Remote          string `json:"remote,omitempty"`
	ProtocolVersion byte   `json:"protocol_version,omitempty"`
	// Persistent is whether the session outlives its connection.
	Persistent    bool           `json:"persistent"`
	Subscriptions []Subscription `json:"subscriptions"`
	// Inflight counts QoS 1 and 2 messages the node is waiting for the
	// client to acknowledge.
	Inflight int `json:"inflight"`
	// Queued counts messages kept for the session while it was away and
	// not yet delivered.
	Queued uint64 `json:"queued"`
	// UpdatedAt is when the stored record was last written; nil for a
	// session that is not stored.
	UpdatedAt *time.Time `json:"updated_at,omitempty"`
}

// Page is one page of a tenant's sessions.
type Page struct {
	Sessions []Session `json:"sessions"`
	// Next is the cursor for the following page, empty on the last one.
	Next string `json:"next,omitempty"`
	// Nodes is how many nodes answered. A node missing from it is a node
	// whose connected clients are missing from the page.
	Nodes int `json:"nodes"`
}

// NodeStats is one node's counts for a tenant.
type NodeStats struct {
	Node          string `json:"node"`
	Connections   int    `json:"connections"`
	Subscriptions int    `json:"subscriptions"`
}

// Stats is a tenant's counts across the cluster.
type Stats struct {
	Tenant string `json:"tenant"`
	// Connections and Subscriptions are summed over the nodes that answered:
	// live MQTT connections and the filters they hold.
	Connections   int `json:"connections"`
	Subscriptions int `json:"subscriptions"`
	// StoredSessions counts the sessions bucket's records, connected or not.
	StoredSessions int `json:"stored_sessions"`
	// QueuedMessages counts what is waiting for absent sessions.
	QueuedMessages uint64 `json:"queued_messages"`
	Nodes          int    `json:"nodes"`
}

// Sum adds up what each node reported.
func Sum(tenantName string, nodes []NodeStats) Stats {
	s := Stats{
		Tenant: tenantName, Connections: 0, Subscriptions: 0,
		StoredSessions: 0, QueuedMessages: 0, Nodes: len(nodes),
	}

	for _, n := range nodes {
		s.Connections += n.Connections
		s.Subscriptions += n.Subscriptions
	}

	return s
}

// ClampLimit turns a requested page size into one that is served: the
// default for none, the maximum for too many.
func ClampLimit(limit int) int {
	switch {
	case limit <= 0:
		return DefaultLimit
	case limit > MaxLimit:
		return MaxLimit
	default:
		return limit
	}
}

// Window is what one source contributes to a page: its sessions after the
// cursor, in client id order, at most limit+1 of them.
//
// The one extra is what lets [Merge] tell a last page from a full one
// without asking again. If every source returns at most limit+1, and the
// union of them holds more than limit, there is another page; if it does
// not, no source was cut short, because each holds no more than the union.
func Window(sessions []Session, after string, limit int) []Session {
	out := make([]Session, 0, min(len(sessions), limit+1))

	for _, s := range sessions {
		if s.ClientID > after {
			out = append(out, s)
		}
	}

	slices.SortFunc(out, byClientID)

	if len(out) > limit+1 {
		out = out[:limit+1]
	}

	return out
}

// Merge combines the windows nodes returned for their connected clients
// with the window of stored sessions into one page of at most limit, and
// reports whether there is another.
//
// A client both connected and stored appears once: the connection says
// where it is and what it holds right now, the record that it is
// persistent and when it was last written. Two nodes answering for one
// client — a takeover caught half way — keep the first answer; the other
// node is about to drop it.
func Merge(live, stored []Session, limit int) ([]Session, bool) {
	byID := make(map[string]Session, len(live)+len(stored))

	for _, s := range live {
		if _, seen := byID[s.ClientID]; !seen {
			byID[s.ClientID] = s
		}
	}

	for _, s := range stored {
		current, seen := byID[s.ClientID]
		if !seen {
			byID[s.ClientID] = s

			continue
		}

		current.Persistent = true
		current.UpdatedAt = s.UpdatedAt
		byID[s.ClientID] = current
	}

	out := make([]Session, 0, len(byID))
	for _, s := range byID {
		out = append(out, s)
	}

	slices.SortFunc(out, byClientID)

	if len(out) > limit {
		return out[:limit], true
	}

	return out, false
}

func byClientID(a, b Session) int { return cmp.Compare(a.ClientID, b.ClientID) }
