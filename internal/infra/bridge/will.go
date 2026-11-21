package bridge

import (
	"context"
	"strconv"
	"sync"
	"time"

	mqtt "github.com/mastmq/mochi/v2"
	"github.com/mastmq/mochi/v2/packets"
	"github.com/nats-io/nats.go"
)

// delayedWills holds the wills whose MQTT 5 Will Delay Interval has not yet
// passed, by mounted client id, so that a new connection to the session can
// cancel one before it is sent [MQTT-3.1.3-9].
//
// mochi has a delay of its own, but it calls OnWill when the connection
// drops, not when the delay ends, and the bridge forwarded the will to the
// fabric right there, which is why every will went out at once whatever
// delay the client asked for (#38). mochi's own delayed publish is still
// handed the dropped topic and goes nowhere.
//
// The timer is in this node's memory. A will whose node dies or shuts down
// inside its delay is not sent, which is no worse than before for a node
// that dies (a SIGKILLed node never ran OnWill at all) but is a gap for a
// graceful restart; see the README's "Not yet" line.
type delayedWills struct {
	mu      sync.Mutex
	pending map[string]*time.Timer
}

func newDelayedWills() *delayedWills {
	return &delayedWills{mu: sync.Mutex{}, pending: make(map[string]*time.Timer)}
}

// schedule runs send after delay unless the will for clientID is cancelled
// or replaced first.
func (w *delayedWills) schedule(clientID string, delay time.Duration, send func()) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if old, ok := w.pending[clientID]; ok {
		old.Stop()
	}

	var timer *time.Timer

	timer = time.AfterFunc(delay, func() {
		w.mu.Lock()

		current := w.pending[clientID] == timer
		if current {
			delete(w.pending, clientID)
		}
		w.mu.Unlock()

		// Cancelled after the timer fired but before it took the lock.
		if current {
			send()
		}
	})

	w.pending[clientID] = timer
}

// cancel drops the will waiting for clientID, if there is one, and reports
// whether there was.
func (w *delayedWills) cancel(clientID string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	timer, ok := w.pending[clientID]
	if ok {
		timer.Stop()
		delete(w.pending, clientID)
	}

	return ok
}

// stop drops every waiting will, for a node that is shutting down.
func (w *delayedWills) stop() {
	w.mu.Lock()
	defer w.mu.Unlock()

	for id, timer := range w.pending {
		timer.Stop()
		delete(w.pending, id)
	}
}

// forwardWill publishes a will onto the fabric, and stores it when it is
// retained.
func (h *Hook) forwardWill(clientID, subject string, will mqtt.Will) {
	// mochi's Will keeps only the user properties of the will's own
	// property set, so those are all there is to carry.
	props := packets.Properties{User: will.User}

	if will.Retain {
		h.storeRetained(context.Background(), subject, will.TopicName, packets.Packet{
			FixedHeader: packets.FixedHeader{Qos: will.Qos, Retain: true},
			Payload:     will.Payload,
			Properties:  props,
		})
	}

	msg := nats.NewMsg(subject)
	msg.Data = will.Payload
	msg.Header.Set(headerQoS, strconv.Itoa(int(will.Qos)))
	msg.Header.Set(headerID, h.newMessageID())

	if err := setPropsHeader(msg, messageProps(props)); err != nil {
		h.log.Warn("forwarding will without its properties", "subject", subject, "error", err)
	}

	// Nobody is waiting on an acknowledgement for a will, so a failed store
	// falls back to the plain fabric rather than losing the will outright.
	if will.Qos > 0 && h.durable.Load() {
		if err := h.publishDurable(context.Background(), msg); err != nil {
			h.log.Warn("storing a will on the durable stream", "client", clientID, "error", err)
		} else {
			msg.Header.Set(headerDurable, "1")
		}
	}

	if err := h.nc.PublishMsg(msg); err != nil {
		h.log.Error("forwarding will to nats", "subject", subject, "error", err)
	}

	h.log.Debug("will forwarded", "client", clientID, "topic", will.TopicName)
}
