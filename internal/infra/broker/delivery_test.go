package broker_test

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/mastmq/mast/internal/domain/tenant"
)

// TestOverlappingFiltersDeliverOnce is the regression test for a node that
// delivered one message once per overlapping filter it held.
//
// A node keeps one NATS subscription per distinct filter, so "a/#" and
// "a/b/c" held by two different clients put two subscriptions on it, and a
// publish to "a/b/c" arrives twice. Each copy used to go through mochi's
// whole local fan-out, so both clients received it twice — and the count
// grew with every overlapping filter any client on the node happened to hold.
func TestOverlappingFiltersDeliverOnce(t *testing.T) {
	t.Parallel()

	addr := start(t, tenant.Static{Tenant: "acme"})

	wide := connect(t, addr, "wide", "acme")
	wide.subscribe(t, "a/#")

	narrow := connect(t, addr, "narrow", "acme")
	narrow.subscribe(t, "a/b/c")

	pub := connect(t, addr, "pub", "acme")
	pub.publish(t, "a/b/c", "once")

	if !waitFor(func() bool { return len(wide.messages()) > 0 && len(narrow.messages()) > 0 }) {
		t.Fatalf("a subscriber received nothing: wide=%v narrow=%v", wide.messages(), narrow.messages())
	}

	// A duplicate arrives microseconds after the original, but give it
	// every chance before declaring there is none.
	time.Sleep(settle)

	if got := len(wide.messages()); got != 1 {
		t.Errorf("a/# received %d copies, want 1: %v", got, wide.messages())
	}

	if got := len(narrow.messages()); got != 1 {
		t.Errorf("a/b/c received %d copies, want 1: %v", got, narrow.messages())
	}
}

// TestSharedGroupBesidePlainSubscriber holds the shared-subscription promise
// when a plain subscription to the same topic lives on the same node.
//
// The node then receives two copies: a plain one, and a queue one NATS
// picked this node for. Both used to run the full local fan-out, and mochi
// picks a group member on every fan-out, so the group received each message
// twice — and in a cluster the plain copy would reach a member here while
// the queue copy reached a member on another node.
func TestSharedGroupBesidePlainSubscriber(t *testing.T) {
	t.Parallel()

	addr := start(t, tenant.Static{Tenant: "acme"})

	var group atomic.Int64

	for i := range 2 {
		c := connect(t, addr, fmt.Sprintf("worker-%d", i), "acme")

		tok := c.client.Subscribe("$share/workers/jobs/new", 0, func(_ paho.Client, _ paho.Message) {
			group.Add(1)
		})
		if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
			t.Fatalf("subscribing worker %d: %v", i, tok.Error())
		}
	}

	audit := connect(t, addr, "audit", "acme")
	audit.subscribe(t, "jobs/#")

	pub := connect(t, addr, "producer", "acme")

	const sent = 6
	for i := range sent {
		pub.publish(t, "jobs/new", fmt.Sprintf("job-%d", i))
	}

	if !waitFor(func() bool { return group.Load() >= sent && len(audit.messages()) >= sent }) {
		t.Fatalf("group received %d and audit %d of %d", group.Load(), len(audit.messages()), sent)
	}

	time.Sleep(settle)

	if got := group.Load(); got != sent {
		t.Errorf("the group received %d messages, want exactly %d", got, sent)
	}

	if got := len(audit.messages()); got != sent {
		t.Errorf("the plain subscriber received %d messages, want exactly %d", got, sent)
	}
}
