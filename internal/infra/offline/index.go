package offline

import "strings"

// index matches a topic to the away sessions subscribed to it.
//
// It is not mochi's TopicsIndex, which the queuer used first, because of
// what that costs held for a fleet. mochi builds every node of its tree
// with three maps already made — subscriptions, shared and inline — and
// device topics are per device, so nearly every node belongs to one session.
// A core held 2.8 KB per away session in the index alone, and with 150,000
// seeded sessions of two filters each the cores were killed at their 1 GiB
// limit. At the 300k target the index alone would have been over 800 MB on
// every core.
//
// A node here keeps its children and subscribers in short slices and turns
// either into a map only past bigLimit, which is what keeps a node shared
// by a whole fleet — "devices" above every device id — from being scanned
// linearly. Tokens are slices of the filter string the queuer already
// keeps, so they cost no copy.
//
// Matching mirrors mochi's exactly, quirks included, because mochi's index
// on the edge is what decides live delivery, and a message the device would
// not have been sent live must not be kept for it either. The property test
// holds the two to the same answers: this index under a tenant mount against
// mochi's with none, since the bridge corrects mochi's answer for the mount
// on the edge (excludeDollar).
type index struct {
	root node
}

// bigLimit is how many children or subscribers a node scans linearly
// before it switches to a map.
const bigLimit = 16

type node struct {
	kids    []*node
	bigKids map[string]*node
	subs    []indexed
	bigSubs map[string]byte
	token   string
}

// indexed is one session's subscription at a node.
type indexed struct {
	key string
	qos byte
}

func newIndex() *index {
	return &index{root: node{kids: nil, bigKids: nil, subs: nil, bigSubs: nil, token: ""}}
}

// subscribe adds key's subscription to filter at qos.
func (x *index) subscribe(filter, key string, qos byte) {
	n := &x.root

	for level := range levels(filter) {
		n = n.child(level, true)
	}

	n.add(key, qos)
}

// unsubscribe removes key's subscription to filter, and any node it leaves
// empty.
func (x *index) unsubscribe(filter, key string) {
	path := []*node{&x.root}

	for level := range levels(filter) {
		next := path[len(path)-1].child(level, false)
		if next == nil {
			return
		}

		path = append(path, next)
	}

	path[len(path)-1].remove(key)

	for i := len(path) - 1; i > 0; i-- {
		if !path[i].empty() {
			return
		}

		path[i-1].drop(path[i].token)
	}
}

// match calls fn with every session subscribed to a filter that matches
// topic under mount, once per session, at the highest QoS among its
// matching filters.
//
// Every filter here is mounted under its tenant, and the mount is matched
// apart from the topic because the rule that keeps a filter starting with a
// wildcard from a topic starting with '$' applies to the topic's first
// level, not the tenant's. Matching the mounted string whole, as mochi does
// on the edge, never applied it (#35).
func (x *index) match(mount, topic string, fn func(key string, qos byte)) {
	if topic == "" {
		return
	}

	top := x.root.child(mount, false)
	if top == nil {
		return
	}

	found := map[string]byte{}

	x.scan(topic, top, top, strings.HasPrefix(topic, "$"), false, found)

	for key, qos := range found {
		fn(key, qos)
	}
}

// scan is mochi's scanSubscribers over this node type, from root, the
// mount's node. dollar is whether the topic starts with "$", which no
// filter whose first level is a wildcard may match [MQTT-4.7.2-1]; wildTop
// is whether the filter being followed has such a first level.
//
// topic here is what is left of it, and may be empty: "a/" has an empty
// last level, which a filter can name. Stopping on an empty remainder
// missed it.
func (x *index) scan(topic string, n, root *node, dollar, wildTop bool, found map[string]byte) {
	key, rest, hasNext := strings.Cut(topic, "/")
	top := n == root

	for _, part := range [2]string{key, "+"} {
		next := n.child(part, false)
		if next == nil {
			continue
		}

		wild := wildTop || (top && part == "+")

		if hasNext {
			x.scan(rest, next, root, dollar, wild, found)
		} else {
			next.gatherLast(part, dollar && wild, found)
		}
	}

	if hash := n.child("#", false); hash != nil {
		hash.gather(dollar && (wildTop || top), found)
	}
}

// gatherLast adds the subscribers of the node the topic's last level
// reached, which part named.
func (n *node) gatherLast(part string, excluded bool, found map[string]byte) {
	n.gather(excluded, found)

	// "filter/#" also matches "filter" itself [MQTT-4.7.1-2]; as in mochi,
	// not when the level was matched by "+".
	if hash := n.child("#", false); hash != nil && part != "+" {
		hash.gather(excluded, found)
	}
}

// gather adds the node's subscribers, unless the topic is one their
// filters may not match.
func (n *node) gather(excluded bool, found map[string]byte) {
	if excluded {
		return
	}

	n.each(func(key string, qos byte) {
		found[key] = max(found[key], qos)
	})
}

// child returns the child for token, adding it if create is set.
func (n *node) child(token string, create bool) *node {
	if c := n.find(token); c != nil || !create {
		return c
	}

	c := &node{kids: nil, bigKids: nil, subs: nil, bigSubs: nil, token: token}

	switch {
	case n.bigKids != nil:
		n.bigKids[token] = c
	case len(n.kids) < bigLimit:
		n.kids = append(n.kids, c)
	default:
		n.bigKids = make(map[string]*node, len(n.kids)+1)
		for _, k := range n.kids {
			n.bigKids[k.token] = k
		}

		n.bigKids[token] = c
		n.kids = nil
	}

	return c
}

func (n *node) find(token string) *node {
	if n.bigKids != nil {
		return n.bigKids[token]
	}

	for _, c := range n.kids {
		if c.token == token {
			return c
		}
	}

	return nil
}

func (n *node) drop(token string) {
	if n.bigKids != nil {
		delete(n.bigKids, token)

		return
	}

	for i, c := range n.kids {
		if c.token == token {
			last := len(n.kids) - 1
			n.kids[i], n.kids[last] = n.kids[last], nil
			n.kids = n.kids[:last]

			return
		}
	}
}

func (n *node) add(key string, qos byte) {
	if n.bigSubs != nil {
		n.bigSubs[key] = qos

		return
	}

	for i := range n.subs {
		if n.subs[i].key == key {
			n.subs[i].qos = qos

			return
		}
	}

	if len(n.subs) < bigLimit {
		n.subs = append(n.subs, indexed{key: key, qos: qos})

		return
	}

	n.bigSubs = make(map[string]byte, len(n.subs)+1)
	for _, s := range n.subs {
		n.bigSubs[s.key] = s.qos
	}

	n.bigSubs[key] = qos
	n.subs = nil
}

func (n *node) remove(key string) {
	if n.bigSubs != nil {
		delete(n.bigSubs, key)

		return
	}

	for i := range n.subs {
		if n.subs[i].key == key {
			last := len(n.subs) - 1
			n.subs[i], n.subs[last] = n.subs[last], indexed{key: "", qos: 0}
			n.subs = n.subs[:last]

			return
		}
	}
}

func (n *node) each(fn func(key string, qos byte)) {
	for _, s := range n.subs {
		fn(s.key, s.qos)
	}

	for key, qos := range n.bigSubs {
		fn(key, qos)
	}
}

func (n *node) empty() bool {
	return len(n.kids) == 0 && len(n.bigKids) == 0 && len(n.subs) == 0 && len(n.bigSubs) == 0
}

// levels yields a filter's levels, as substrings of it.
func levels(filter string) func(func(string) bool) {
	return func(yield func(string) bool) {
		for {
			level, rest, more := strings.Cut(filter, "/")
			if !yield(level) || !more {
				return
			}

			filter = rest
		}
	}
}
