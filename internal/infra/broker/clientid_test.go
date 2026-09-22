package broker_test

import (
	"testing"
	"time"
)

// takeoverSettle is how long a displaced client is given to notice. mochi
// closes the old connection during the new one's CONNECT, so this only has
// to cover the client library noticing the socket went away.
const takeoverSettle = 2 * time.Second

// TestClientIDIsScopedToTenant is the regression test for a cross-tenant
// denial of service.
//
// MQTT requires that a second connection using an existing client id
// disconnects the first, and mochi implements that by keying its client map
// on the id alone. Before client ids were mounted under their tenant, any
// authenticated tenant could disconnect another tenant's device by reusing
// its client id — and keep it disconnected by reconnecting in a loop.
// Tenant isolation is the product, so this is the test that matters most.
func TestClientIDIsScopedToTenant(t *testing.T) {
	addr := start(t, byUsername{})

	acme := connect(t, addr, "device-1", "acme")
	acme.subscribe(t, "own/topic")

	// The same client id, in a different tenant. It must be a different
	// client, not the same one reconnecting.
	other := connect(t, addr, "device-1", "other")
	other.subscribe(t, "own/topic")

	time.Sleep(takeoverSettle)

	if !acme.client.IsConnected() {
		t.Fatal("another tenant reusing the client id disconnected acme's device")
	}

	if !other.client.IsConnected() {
		t.Fatal("the second tenant's client is not connected")
	}

	// Still connected is not enough: it has to still be receiving.
	pub := connect(t, addr, "acme-pub", "acme")
	if !waitFor(func() bool {
		pub.publish(t, "own/topic", "hello")

		return len(acme.messages()) > 0
	}) {
		t.Errorf("acme's device stopped receiving; got %v", acme.messages())
	}

	// And the boundary must hold in the other direction: acme's traffic is
	// not other's, however identical the client ids.
	if got := other.messages(); len(got) != 0 {
		t.Errorf("the other tenant received acme's traffic: %v", got)
	}
}

// TestTakeoverWithinATenantStillWorks is the other half. Scoping ids must
// not cost us the behaviour MQTT actually requires: inside one tenant, a
// second connection with the same id still displaces the first.
func TestTakeoverWithinATenantStillWorks(t *testing.T) {
	addr := start(t, byUsername{})

	first := connect(t, addr, "device-1", "acme")
	first.subscribe(t, "own/topic")

	second := connect(t, addr, "device-1", "acme")

	displaced := waitFor(func() bool { return !first.client.IsConnected() })

	if !displaced {
		t.Error("a second connection with the same id in the same tenant did not displace the first")
	}

	if !second.client.IsConnected() {
		t.Error("the displacing client is not connected")
	}
}
