package quota

import "time"

// Census is what this node has heard about every other node's connections,
// per tenant.
//
// Each node reports its own counts every interval and the census keeps the
// latest of each, so a tenant's connections across the cluster are this
// node's own plus [Census.Remote]. A report that is not renewed within ttl
// stops counting: a node that died took its connections with it, and the
// devices that held them are about to reconnect somewhere else and be
// counted there. Holding on to its last report would count them twice.
type Census struct {
	ttl   time.Duration
	nodes map[string]map[string]sighting
}

// sighting is one node's count for one tenant and when it was reported.
type sighting struct {
	count int
	at    time.Time
}

// NewCensus returns an empty census that forgets a report after ttl.
func NewCensus(ttl time.Duration) *Census {
	return &Census{ttl: ttl, nodes: make(map[string]map[string]sighting)}
}

// Observe records counts that node reported at now. A tenant missing from
// counts keeps whatever the node said about it before, until that ages out;
// a node reports a tenant whose count fell to zero once, as zero, which is
// what clears it promptly.
func (c *Census) Observe(node string, counts map[string]int, now time.Time) {
	seen, ok := c.nodes[node]
	if !ok {
		seen = make(map[string]sighting, len(counts))
		c.nodes[node] = seen
	}

	for tenant, n := range counts {
		if n <= 0 {
			delete(seen, tenant)

			continue
		}

		seen[tenant] = sighting{count: n, at: now}
	}
}

// Remote is how many connections every other node holds for tenant, as of
// their latest reports that are still fresh at now.
func (c *Census) Remote(tenant string, now time.Time) int {
	total := 0

	for _, seen := range c.nodes {
		if s, ok := seen[tenant]; ok && now.Sub(s.at) <= c.ttl {
			total += s.count
		}
	}

	return total
}

// Prune drops every report older than ttl, and every node left with none,
// so a cluster that replaces its edges on every deploy does not accumulate
// the ids of every edge it ever ran.
func (c *Census) Prune(now time.Time) {
	for node, seen := range c.nodes {
		for tenant, s := range seen {
			if now.Sub(s.at) > c.ttl {
				delete(seen, tenant)
			}
		}

		if len(seen) == 0 {
			delete(c.nodes, node)
		}
	}
}
