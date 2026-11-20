// Package liveness decides, from heartbeats, which nodes of a cluster have
// died without saying so.
//
// A node that shuts down cleanly records each of its sessions as away, and
// the queuer starts keeping their messages. A node that is killed outright —
// SIGKILL, an OOM kill, a lost VM — records nothing, so its sessions stayed
// on record as held by a connection that no longer existed, and everything
// published to them was dropped until each device reconnected somewhere.
// Every node that serves MQTT therefore announces itself on [Subject] every
// [Interval], and a [Tracker] on the cores declares a node dead once it has
// been silent for longer than its threshold.
//
// The threshold is the trade-off. Too short, and a slow node — a long GC
// pause, a leaf reconnect, a CPU-starved pod — is declared dead while its
// devices are still connected, and its sessions are queued for as well as
// delivered to live: duplicates at their next reconnect. Too long, and more
// messages wait in the gap before anything is queued. [DeadAfter] is ten
// heartbeats, longer than a leaf reconnect and far longer than any pause the
// collector makes, and short enough that the edge a device moves to still
// looks at its queue again after the cores have filled it.
//
// Nothing here touches the network or the clock: the caller passes the time
// in, which is what lets the decision be tested to the millisecond.
package liveness

import (
	"strings"
	"time"
)

// Interval is how often a node announces that it is alive.
const Interval = time.Second

// DeadAfter is how long a node may be silent before it is declared dead.
const DeadAfter = 10 * Interval

// forgetAfter is how long a dead node is remembered. A node's id is its
// embedded server's, which a restart replaces, so a dead id never comes
// back and only has to be kept long enough for late session records that
// still name it to be recognised.
const forgetAfter = 30 * time.Minute

// subjectPrefix is where heartbeats are published. It is outside the "t."
// namespace that carries tenant traffic, like every control subject, so no
// client filter can reach it or forge a heartbeat.
const subjectPrefix = "mast.node."

// Subject is where node announces itself.
func Subject(node string) string { return subjectPrefix + node }

// Wildcard is the one subscription that hears every node.
const Wildcard = subjectPrefix + "*"

// Goodbye is the body of a node's last beat, sent as it shuts down cleanly,
// after it has recorded its sessions as away. Every other beat is empty.
const Goodbye = "bye"

// NodeOf reverses [Subject].
func NodeOf(subject string) (string, bool) {
	node, ok := strings.CutPrefix(subject, subjectPrefix)
	if !ok || node == "" || strings.Contains(node, ".") {
		return "", false
	}

	return node, true
}

// Death is a node declared dead, and when it was last heard from.
type Death struct {
	Node     string
	LastSeen time.Time
}

// node is what a tracker knows about one node.
type node struct {
	name     string
	lastSeen time.Time
	dead     bool
	diedAt   time.Time
}

// Tracker follows the nodes of a cluster. It is not safe for concurrent
// use; the caller holds its own lock around it.
type Tracker struct {
	deadAfter time.Duration
	nodes     map[string]*node

	// checked is when [Tracker.Check] last ran, so that a check arriving
	// late — the caller itself stalled — is recognised as such.
	checked time.Time
}

// NewTracker returns a tracker that declares a node dead after deadAfter
// without a heartbeat.
func NewTracker(deadAfter time.Duration) *Tracker {
	return &Tracker{deadAfter: deadAfter, nodes: make(map[string]*node), checked: time.Time{}}
}

// Beat records a heartbeat from a node, and reports whether the node had
// been declared dead: a false positive, which the caller has to undo.
func (t *Tracker) Beat(name string, now time.Time) bool {
	n := t.lookup(name, now)
	n.lastSeen = later(n.lastSeen, now)

	revived := n.dead
	n.dead, n.diedAt = false, time.Time{}

	return revived
}

// Heard notes a node named by something other than its heartbeat — a
// session record that says the node holds it — and returns the tracker's
// own copy of the name, so that a caller holding the name for many
// sessions holds one string rather than one per session.
//
// A node never heard before starts its clock now. That is what gives a
// freshly started core the full threshold to hear every live node before it
// judges any, and what still catches a node that died before the core
// started: its id never beats again.
func (t *Tracker) Heard(name string, now time.Time) string {
	return t.lookup(name, now).name
}

// Dead reports whether a node has been declared dead, and when it was last
// heard from.
func (t *Tracker) Dead(name string) (time.Time, bool) {
	n, ok := t.nodes[name]
	if !ok || !n.dead {
		return time.Time{}, false
	}

	return n.lastSeen, true
}

// Check declares dead every node silent for longer than the threshold and
// returns them. Each death is returned once.
//
// A check that comes much later than the one before is not trusted. Every
// node's silence is measured on the caller's clock, so a caller that was
// itself stalled — a GC pause, a starved process, a laptop lid — would see
// the whole cluster silent at once and declare everyone dead. Instead it
// restarts every node's clock: the heartbeats that piled up in the meantime
// are what will tell the truth, a moment later.
func (t *Tracker) Check(now time.Time) []Death {
	late := !t.checked.IsZero() && now.Sub(t.checked) > t.deadAfter/2
	t.checked = now

	var deaths []Death

	for name, n := range t.nodes {
		switch {
		case n.dead:
			if now.Sub(n.diedAt) > forgetAfter {
				delete(t.nodes, name)
			}
		case late:
			n.lastSeen = later(n.lastSeen, now)
		case now.Sub(n.lastSeen) > t.deadAfter:
			n.dead, n.diedAt = true, now
			deaths = append(deaths, Death{Node: n.name, LastSeen: n.lastSeen})
		}
	}

	return deaths
}

// Overdue reports whether some live node has been silent past the
// threshold: whether a [Tracker.Check] now would declare a death. A caller
// that wants to confirm its own health before acting on one asks this
// first.
func (t *Tracker) Overdue(now time.Time) bool {
	if !t.checked.IsZero() && now.Sub(t.checked) > t.deadAfter/2 {
		return false
	}

	for _, n := range t.nodes {
		if !n.dead && now.Sub(n.lastSeen) > t.deadAfter {
			return true
		}
	}

	return false
}

// Excuse restarts every live node's clock without declaring anyone dead:
// what a caller does when it cannot trust its own view of the cluster.
func (t *Tracker) Excuse(now time.Time) {
	t.checked = now

	for _, n := range t.nodes {
		if !n.dead {
			n.lastSeen = later(n.lastSeen, now)
		}
	}
}

func (t *Tracker) lookup(name string, now time.Time) *node {
	n, ok := t.nodes[name]
	if !ok {
		n = &node{name: name, lastSeen: now, dead: false, diedAt: time.Time{}}
		t.nodes[name] = n
	}

	return n
}

func later(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}

	return a
}
