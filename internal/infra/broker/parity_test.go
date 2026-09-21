package broker_test

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/mastmq/mast/internal/domain/tenant"
)

// This file is the definition of done for replacing an EMQX cluster. Each
// test here is a behaviour an MQTT client is entitled to assume, and the
// skipped ones are the gaps that remain. Deleting a t.Skip is how a gap is
// proven closed — not a changelog entry.
//
// Gaps are tracked at github.com/mastmq/mast/issues with the "parity" label.

// connectAs dials with an explicit clean-session flag, which the parity tests
// need and the happy-path helpers do not expose.
//
//nolint:ireturn // paho.Client is an interface in the library, not a type.
func connectAs(t *testing.T, addr, clientID string, clean bool) paho.Client {
	t.Helper()

	opts := paho.NewClientOptions().
		AddBroker("tcp://" + addr).
		SetClientID(clientID).
		SetUsername("acme").
		SetCleanSession(clean).
		SetConnectTimeout(5 * time.Second).
		SetAutoReconnect(false)

	c := paho.NewClient(opts)

	tok := c.Connect()
	if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("connecting %s: %v", clientID, tok.Error())
	}

	t.Cleanup(func() { c.Disconnect(200) })

	return c
}

// TestParityQoS checks that a subscriber receives at the QoS it negotiated.
//
// The failure mode here is the dangerous kind: nothing errors, the client
// believes it has at-least-once delivery, and it does not.
func TestParityQoS(t *testing.T) {
	t.Parallel()

	addr := start(t, tenant.Static{Tenant: "acme"})

	for _, level := range []byte{0, 1, 2} {
		t.Run(fmt.Sprintf("qos%d", level), func(t *testing.T) {
			got := make(chan byte, 4)
			topic := fmt.Sprintf("parity/qos%d", level)

			sub := connectAs(t, addr, fmt.Sprintf("qos%d-sub", level), true)
			if tok := sub.Subscribe(topic, level, func(_ paho.Client, m paho.Message) {
				got <- m.Qos()
			}); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
				t.Fatalf("subscribing: %v", tok.Error())
			}

			pub := connectAs(t, addr, fmt.Sprintf("qos%d-pub", level), true)
			if tok := pub.Publish(topic, level, false, "x"); !tok.WaitTimeout(5 * time.Second) {
				t.Fatal("publish timed out")
			}

			select {
			case delivered := <-got:
				if delivered != level {
					t.Errorf("subscriber negotiated QoS %d but received QoS %d", level, delivered)
				}
			case <-time.After(settle):
				t.Fatalf("QoS %d message never arrived", level)
			}
		})
	}
}

// TestParityRetained checks that a subscriber arriving after the publish
// still gets the last known value.
func TestParityRetained(t *testing.T) {
	t.Parallel()

	addr := start(t, tenant.Static{Tenant: "acme"})

	pub := connectAs(t, addr, "retain-pub", true)
	if tok := pub.Publish("parity/retained", 0, true, "kept"); !tok.WaitTimeout(5 * time.Second) {
		t.Fatal("publish timed out")
	}

	time.Sleep(500 * time.Millisecond)

	got := make(chan string, 4)

	sub := connectAs(t, addr, "retain-sub", true)
	if tok := sub.Subscribe("parity/retained", 0, func(_ paho.Client, m paho.Message) {
		got <- string(m.Payload())
	}); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribing: %v", tok.Error())
	}

	select {
	case v := <-got:
		if v != "kept" {
			t.Errorf("retained payload = %q, want kept", v)
		}
	case <-time.After(settle):
		t.Error("a subscriber arriving after the publish received no retained message")
	}
}

// TestParityPersistentSession checks that a device with clean_start=false
// receives what it missed while it was away.
func TestParityPersistentSession(t *testing.T) {
	t.Parallel()

	addr := start(t, tenant.Static{Tenant: "acme"})

	away := connectAs(t, addr, "persistent", false)
	if tok := away.Subscribe("parity/persist", 1, func(paho.Client, paho.Message) {}); !tok.WaitTimeout(5 * time.Second) {
		t.Fatal("subscribe timed out")
	}

	away.Disconnect(200)
	time.Sleep(500 * time.Millisecond)

	pub := connectAs(t, addr, "persist-pub", true)
	if tok := pub.Publish("parity/persist", 1, false, "while-away"); !tok.WaitTimeout(5 * time.Second) {
		t.Fatal("publish timed out")
	}

	time.Sleep(500 * time.Millisecond)

	var received atomic.Int64

	opts := paho.NewClientOptions().
		AddBroker("tcp://" + addr).
		SetClientID("persistent").
		SetUsername("acme").
		SetCleanSession(false).
		SetConnectTimeout(5 * time.Second).
		SetAutoReconnect(false).
		SetDefaultPublishHandler(func(paho.Client, paho.Message) { received.Add(1) })

	back := paho.NewClient(opts)
	if tok := back.Connect(); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("reconnecting: %v", tok.Error())
	}

	t.Cleanup(func() { back.Disconnect(200) })

	if !waitFor(func() bool { return received.Load() > 0 }) {
		t.Error("a resumed session received none of the messages published while it was away")
	}
}

// TestParityWill checks that a will reaches subscribers when a client drops
// without a DISCONNECT.
func TestParityWill(t *testing.T) {
	t.Parallel()
	t.Skip("gap: the will topic is never mounted, so it matches no subscriber (issue #6)")

	addr := start(t, tenant.Static{Tenant: "acme"})

	got := make(chan string, 4)

	watcher := connectAs(t, addr, "will-watcher", true)
	if tok := watcher.Subscribe("parity/will", 0, func(_ paho.Client, m paho.Message) {
		got <- string(m.Payload())
	}); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribing: %v", tok.Error())
	}

	opts := paho.NewClientOptions().
		AddBroker("tcp://"+addr).
		SetClientID("will-dying").
		SetUsername("acme").
		SetWill("parity/will", "gone", 0, false).
		SetConnectTimeout(5 * time.Second).
		SetAutoReconnect(false)

	dying := paho.NewClient(opts)
	if tok := dying.Connect(); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("connecting: %v", tok.Error())
	}

	// A clean DISCONNECT suppresses the will by design, so the socket has to
	// go away underneath the client.
	forceClose(t, dying)

	select {
	case v := <-got:
		if v != "gone" {
			t.Errorf("will payload = %q, want gone", v)
		}
	case <-time.After(settle):
		t.Error("no will message was delivered after an ungraceful disconnect")
	}
}
