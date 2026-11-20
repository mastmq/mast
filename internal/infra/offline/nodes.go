package offline

import (
	"context"
	"sync"
	"time"

	"github.com/mastmq/mast/internal/domain/liveness"
	"github.com/mastmq/mast/internal/infra/store"
	"github.com/nats-io/nats.go"
)

// Taking over the sessions of a node that died without a word.
//
// A node that closes records each session it holds as away, and the queuer
// starts keeping its messages. A node killed outright records nothing: its
// sessions stayed on record as held, the queuer kept nothing for them, and
// everything published to those devices until each reconnected somewhere
// was lost. Every node that serves MQTT now beats on [liveness.Subject],
// every held record names its node, and a node silent for
// [liveness.DeadAfter] has its sessions queued for as if each had
// disconnected.
//
// Three choices shape it, each against the failure the alternative had.
//
// The takeover is in this queuer's memory only; no record is rewritten.
// Writing the sessions away in the bucket was the obvious shape, and it
// makes a false positive — a slow node, a leaf that dropped for longer than
// the threshold — a cluster-wide fact: the node's own later writes would
// have to win back every record, while devices still connected to it were
// recorded as gone. Held in memory, a false positive costs only queue writes
// for devices that also got their messages live, which their next reconnect
// delivers twice, and it ends the moment the node beats again. Each core
// decides for itself, as each already queues for itself, and the queue
// stream stores what they all write once.
//
// What was published between the node's last beat and the moment it was
// declared dead is replayed from the durable stream. The queuer read those
// messages live, while the sessions still looked held, and kept nothing; a
// takeover that only queued from then on would lose exactly the seconds
// after a crash, which is when a device is most likely to be sent
// something. The replay starts [replayLead] before the last beat, because a
// node can be behind the stream by a moment when it dies.
//
// A session that moved off the node in that window is replayed too, up to
// the moment it moved. A device on a killed edge reconnects within a second
// or two, nearly always before the threshold has passed; it is restored on
// another node with nothing queued, and the messages published while it was
// between the two were queued by nobody. The queuer remembers where each
// session was last held for long enough to recognise that, and the edge's
// straggler check thirty seconds after a connect delivers what the replay
// queued.
//
// The memory this costs is a node name and a key per held session: about
// a hundred bytes each, thirty megabytes per core at the 300k-device target.

// replayLead is how far before a dead node's last heartbeat the replay
// starts. Whatever the node had delivered in that time is delivered again
// at reconnect, which at-least-once allows.
const replayLead = 5 * time.Second

// departureMemory is how long the queuer remembers the node a session moved
// off. The node is declared dead at most DeadAfter and a check after its
// last beat, and a move matters only if it came after the replay's start.
const departureMemory = 2*liveness.DeadAfter + replayLead

// reachTimeout bounds asking the sessions stream's leader whether this core
// can reach it, before declaring a node dead.
const reachTimeout = 2 * time.Second

// replayTimeout bounds a dead node's replay: reading the stream from its
// last beat and writing what matches.
const replayTimeout = 2 * time.Minute

// fetchWorkers bounds the session records read at once when a node dies.
const fetchWorkers = 16

// replayFlush is how many queue writes a replay gathers before it makes
// them.
const replayFlush = 1024

// departure is where a session was held before it moved, and when it moved.
type departure struct {
	node string
	at   time.Time
}

// reaping is the sessions to take over from one dead node.
type reaping struct {
	node  string
	since time.Time // the replay starts here
	keys  []reapTarget
}

// reapTarget is one session to take over. until is zero for a session the
// dead node still holds, and otherwise when the session moved off it: a
// message stored after that was the new node's to deliver.
type reapTarget struct {
	key   string
	until time.Time
}

// replayTarget is a reap target with the record that says what it
// subscribed to.
type replayTarget struct {
	reapTarget

	sess store.Session
}

// hold records that key is held by node. It reports false for a node
// already declared dead, whose record is taken over at the next check.
// Called with q.mu held.
func (q *Queuer) hold(key, node string, at time.Time) bool {
	prev, had := q.held[key]

	delete(q.orphans, key)

	// A move off a node that said goodbye is not remembered: it delivered
	// until it closed. Its goodbye may well arrive before the record of the
	// move does — they travel different paths.
	if _, bye := q.farewell[prev]; had && prev != node && !bye {
		q.left[key] = departure{node: prev, at: at}
	}

	if node == "" {
		delete(q.held, key)

		return true
	}

	name := q.live.Heard(node, time.Now())
	q.held[key] = name

	if _, dead := q.live.Dead(name); dead {
		q.orphans[key] = name

		return false
	}

	return true
}

// unhold forgets where key is held. Where it was held before stays known:
// a session that moved and then went away still lost what was published
// while it moved. Called with q.mu held.
func (q *Queuer) unhold(key string) {
	delete(q.held, key)
	delete(q.orphans, key)
}

// onBeat records a node's heartbeat. A beat from a node already declared
// dead means it never was, and the sessions taken from it are handed back.
func (q *Queuer) onBeat(msg *nats.Msg) {
	node, ok := liveness.NodeOf(msg.Subject)
	if !ok {
		return
	}

	now := time.Now()

	q.mu.Lock()
	defer q.mu.Unlock()

	// A node leaving cleanly left no gap behind its sessions that moved: it
	// delivered until it closed them. It is not counted as a beat, so that a
	// session it still holds on record is taken over once it falls silent.
	if string(msg.Data) == liveness.Goodbye {
		q.farewell[q.live.Heard(node, now)] = now

		for key, dep := range q.left {
			if dep.node == node {
				delete(q.left, key)
			}
		}

		return
	}

	if !q.live.Beat(node, now) {
		return
	}

	returned := q.reviveLocked(node, now)

	q.log.Warn("a node declared dead is beating again; its sessions are held after all, "+
		"and what was queued for them meanwhile may be delivered twice", "node", node, "sessions", returned)
}

// reviveLocked hands back the sessions taken from node, as if each had come
// back at now. Called with q.mu held.
func (q *Queuer) reviveLocked(node string, now time.Time) int {
	returned := 0

	for key, away := range q.absent {
		if away.reapedFrom != node || !away.back.IsZero() {
			continue
		}

		away.back = now
		q.absent[key] = away

		if _, held := q.held[key]; !held {
			q.held[key] = q.live.Heard(node, now)
		}

		returned++
	}

	return returned
}

// watchNodes checks every node's heartbeat once an interval.
func (q *Queuer) watchNodes(ctx context.Context) {
	ticker := time.NewTicker(liveness.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-q.done:
			return
		case now := <-ticker.C:
			for _, r := range q.checkNodes(ctx, now) {
				go q.reap(ctx, r)
			}
		}
	}
}

// checkNodes declares dead the nodes that stopped beating and returns what
// to take over from them.
func (q *Queuer) checkNodes(ctx context.Context, now time.Time) []reaping {
	q.mu.Lock()
	overdue := q.live.Overdue(now)
	q.mu.Unlock()

	// A core cut off from the cluster hears no heartbeat from anyone, and
	// would take over the whole fleet. Its opinion is not trusted until it
	// can reach the sessions stream's leader, which a core in a minority
	// cannot; until then every node's clock starts again.
	if overdue {
		reach, cancel := context.WithTimeout(ctx, reachTimeout)
		err := q.reachable(reach)

		cancel()

		if err != nil {
			q.log.Warn("nodes look silent, but this core cannot reach the cluster; judging none of them", "error", err)

			q.mu.Lock()
			q.live.Excuse(now)
			q.mu.Unlock()

			return nil
		}
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	var work []reaping

	for _, d := range q.live.Check(now) {
		r := q.collectLocked(d)

		q.log.Warn("a node stopped beating without recording its sessions as away; queueing for them",
			"node", d.Node, "last_seen", d.LastSeen, "sessions", len(r.keys))

		if q.metrics != nil {
			q.metrics.QueuerNodesDead.Inc()
		}

		work = append(work, r)
	}

	work = append(work, q.orphansLocked()...)

	for key, dep := range q.left {
		if now.Sub(dep.at) > departureMemory {
			delete(q.left, key)
		}
	}

	for node, at := range q.farewell {
		if now.Sub(at) > departureMemory {
			delete(q.farewell, node)
		}
	}

	return work
}

// collectLocked lists the sessions a dead node held, and those that moved
// off it after the replay's start. Called with q.mu held.
func (q *Queuer) collectLocked(d liveness.Death) reaping {
	r := reaping{node: d.Node, since: d.LastSeen.Add(-replayLead), keys: nil}

	for key, node := range q.held {
		if node == d.Node {
			r.keys = append(r.keys, reapTarget{key: key, until: time.Time{}})
		}
	}

	for key, dep := range q.left {
		if dep.node == d.Node && dep.at.After(r.since) {
			r.keys = append(r.keys, reapTarget{key: key, until: dep.at})
		}
	}

	return r
}

// orphansLocked takes the held records that named a node after it was
// declared dead. Called with q.mu held.
func (q *Queuer) orphansLocked() []reaping {
	byNode := make(map[string]*reaping)

	for key, node := range q.orphans {
		delete(q.orphans, key)

		last, dead := q.live.Dead(node)
		if !dead {
			continue
		}

		r, ok := byNode[node]
		if !ok {
			r = &reaping{node: node, since: last.Add(-replayLead), keys: nil}
			byNode[node] = r
		}

		r.keys = append(r.keys, reapTarget{key: key, until: time.Time{}})
	}

	work := make([]reaping, 0, len(byNode))
	for _, r := range byNode {
		work = append(work, *r)
	}

	return work
}

// reap takes over one dead node's sessions: indexes those it still holds as
// away, so that the live path queues for them from now on, and replays what
// the stream stored since the node's last beat for all of them.
func (q *Queuer) reap(ctx context.Context, r reaping) {
	if len(r.keys) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(ctx, replayTimeout)
	defer cancel()

	records := q.fetch(ctx, r.keys)

	q.mu.Lock()
	targets, adopted := q.adoptLocked(r, records)
	q.gauge()
	q.mu.Unlock()

	if q.metrics != nil {
		q.metrics.QueuerReaped.Add(float64(adopted))
	}

	q.replay(ctx, r, targets)
}

// adoptLocked indexes as away every target the dead node still holds and
// returns everything to replay for. A node that beat again while its
// records were being read was never dead, and nothing is taken from it.
// Called with q.mu held.
func (q *Queuer) adoptLocked(r reaping, records map[string]store.Session) ([]replayTarget, int) {
	if _, dead := q.live.Dead(r.node); !dead {
		return nil, 0
	}

	targets := make([]replayTarget, 0, len(r.keys))
	adopted := 0

	for _, t := range r.keys {
		sess, ok := records[t.key]
		if !ok {
			continue
		}

		if t.until.IsZero() {
			switch dep, moved := q.left[t.key]; {
			case q.held[t.key] == r.node:
				delete(q.held, t.key)
				delete(q.orphans, t.key)
				q.removeLocked(t.key)
				q.indexAway(t.key, sess, sess.UpdatedAt, r.node)

				adopted++
			case moved && dep.node == r.node:
				// It moved while its record was being read.
				t.until = dep.at
			default:
				// Away on record, or gone: the record's own path has it.
				continue
			}
		}

		targets = append(targets, replayTarget{reapTarget: t, sess: sess})
	}

	return targets, adopted
}

// fetch reads the records of the sessions to take over.
//
// With the bucket's Get, which any replica answers and which may be a
// moment behind. That was a real loss once, for MarkAway reading a record
// written a moment before; here the newest write to a dead node's record
// is at least DeadAfter old, far past any replica's lag.
func (q *Queuer) fetch(ctx context.Context, keys []reapTarget) map[string]store.Session {
	var (
		mu      sync.Mutex
		records = make(map[string]store.Session, len(keys))
		work    = make(chan string)
		wg      sync.WaitGroup
	)

	for range min(fetchWorkers, len(keys)) {
		wg.Go(func() {
			for key := range work {
				sess, err := q.store.GetSession(ctx, key)
				if err != nil {
					q.log.Warn("reading a dead node's session; it is not queued for", "key", key, "error", err)

					continue
				}

				mu.Lock()
				records[key] = sess
				mu.Unlock()
			}
		})
	}

	for _, t := range keys {
		work <- t.key
	}

	close(work)
	wg.Wait()

	return records
}

// replay queues, for each target, what the durable stream stored since the
// dead node's last beat that matches it — for a session that moved off the
// node, only what was stored before it moved.
//
// It overlaps the live path for the sessions just indexed as away, which
// is what leaves no gap between the two; both name a message the same way,
// so the queue stream stores it once.
func (q *Queuer) replay(ctx context.Context, r reaping, targets []replayTarget) {
	if len(targets) == 0 {
		return
	}

	w := newReplayWriter(q, r.node, targets)

	read, err := q.store.ReplayDurable(ctx, r.since, func(subject string, header nats.Header, data []byte) {
		if msg, ok := q.decode(subject, header, data); ok {
			w.add(ctx, msg, header)
		}
	})
	if err != nil {
		q.log.Error("replaying the durable stream for a dead node; messages published since it was last heard "+
			"from may be missing from its sessions' queues", "node", r.node, "error", err)
	}

	w.write(ctx)

	if w.stored > 0 && q.metrics != nil {
		// Not by tenant: a replay spans every tenant the node served, and
		// the per-tenant counter is the live path's.
		q.metrics.QueuerReplayed.Add(float64(w.stored))
	}

	q.log.Info("replayed the durable stream for a dead node's sessions",
		"node", r.node, "since", r.since, "sessions", len(targets), "read", read, "queued", w.stored)
}

// replayWriter matches replayed messages against a dead node's sessions
// and writes what matches in batches.
type replayWriter struct {
	q      *Queuer
	node   string
	idx    *index
	until  map[string]time.Time
	items  []store.Queued
	stored int
}

func newReplayWriter(q *Queuer, node string, targets []replayTarget) *replayWriter {
	w := &replayWriter{
		q: q, node: node, idx: newIndex(), until: make(map[string]time.Time, len(targets)), items: nil, stored: 0,
	}

	for _, t := range targets {
		w.until[t.key] = t.until

		for _, sub := range t.sess.Subscriptions {
			if !isShared(sub.Filter) {
				w.idx.subscribe(t.sess.Tenant+"/"+sub.Filter, t.key, sub.QoS)
			}
		}
	}

	return w
}

// add queues msg for every target it matches.
func (w *replayWriter) add(ctx context.Context, msg fabricMessage, header nats.Header) {
	at, id := storedAt(header), header.Get(headerID)

	w.idx.match(msg.tenant+"/"+msg.stored.Topic, func(key string, qos byte) {
		if u := w.until[key]; !u.IsZero() && !at.Before(u.Add(clockSlack)) {
			return
		}

		kept := msg.stored
		kept.QoS = min(kept.QoS, qos)

		if kept.QoS > 0 {
			w.items = append(w.items, store.Queued{Key: key, Msg: kept, ID: store.QueueID(id, key)})
		}
	})

	if len(w.items) >= replayFlush {
		w.write(ctx)
	}
}

// write stores what has gathered.
func (w *replayWriter) write(ctx context.Context) {
	if len(w.items) == 0 {
		return
	}

	n, err := w.q.store.EnqueueMany(ctx, w.items)
	if err != nil {
		w.q.log.Error("queueing a dead node's backlog", "node", w.node, "error", err)
	}

	w.stored += n
	w.items = w.items[:0]
}
