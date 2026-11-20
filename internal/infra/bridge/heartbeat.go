package bridge

import (
	"time"

	"github.com/mastmq/mast/internal/domain/liveness"
)

// beat announces that this node is alive, every [liveness.Interval], for as
// long as the hook runs.
//
// The cores listen, and a node that falls silent for [liveness.DeadAfter]
// has the sessions it holds queued for as if each had disconnected. That is
// the only way they learn of a node killed outright: it records nothing on
// its way down, and its sessions stayed marked held by connections that no
// longer existed.
//
// A goroutine and a timer of its own, publishing on the in-process
// connection, so that a node busy with MQTT still beats on time; a node so
// starved it cannot is close enough to dead that queueing for its devices
// as well costs only duplicates. An empty body: the subject names the node,
// and the beat's arrival is the whole message.
func (h *Hook) beat() {
	subject := liveness.Subject(h.nodeID)

	ticker := time.NewTicker(liveness.Interval)
	defer ticker.Stop()

	for {
		if err := h.nc.Publish(subject, nil); err != nil {
			h.log.Debug("announcing this node", "error", err)
		}

		select {
		case <-h.stop:
			return
		case <-ticker.C:
		}
	}
}

// sayGoodbye tells the cores this node is shutting down cleanly. mochi stops
// the hook once every client is closed, so each lasting session has been
// recorded as away by then, or the write failed.
//
// What it spares is the replay for sessions that moved off this node. A
// rolling deploy closes every connection and its devices reconnect to
// another edge at once, often before this node's record of them as away
// lands, so the cores see them move from one held record to another — which
// is also what a device leaving a killed edge looks like, and which they
// replay once this node falls silent. A node that said goodbye left nothing
// in that gap. Sessions still recorded as held by it are taken over as
// before: the goodbye says the node is leaving, not that every write made it.
func (h *Hook) sayGoodbye() {
	if h.nodeID == "" {
		return
	}

	if err := h.nc.Publish(liveness.Subject(h.nodeID), []byte(liveness.Goodbye)); err != nil {
		h.log.Debug("saying goodbye to the cores", "error", err)
	}
}
