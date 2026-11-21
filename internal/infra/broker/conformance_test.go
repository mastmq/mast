package broker_test

import (
	"context"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/mastmq/mast/internal/domain/tenant"
)

// The tests in this file each pin one gap the Eclipse Paho interoperability
// suite found (test/conformance), so that the gap cannot come back without
// a test here failing first. The suite runs in its own workflow and takes
// minutes; these run with every go test.

// TestV5AckCarriesNoPublisherProperties covers #39. mochi copied a PUBLISH's
// properties into the PUBACK or PUBREC answering it, so a publisher was sent
// its own user properties back, and the Paho client dropped the connection
// on an ack it did not expect to carry any.
func TestV5AckCarriesNoPublisherProperties(t *testing.T) {
	t.Parallel()

	addr := start(t, tenant.Static{Tenant: "acme"})
	pub := dialV5(t, addr, "publisher")

	for _, qos := range []byte{1, 2} {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)

		resp, err := pub.client.Publish(ctx, &paho.Publish{
			Topic: "acks/props", QoS: qos, Payload: []byte("x"), Properties: richProperties(),
		})

		cancel()

		if err != nil {
			t.Fatalf("publishing at QoS %d: %v", qos, err)
		}

		if resp.Properties != nil && len(resp.Properties.User) > 0 {
			t.Errorf("the QoS %d ack carried the publisher's user properties: %v", qos, resp.Properties.User)
		}
	}
}

// subscribeWith subscribes with whatever options a test needs, which the
// v5client's own subscribe fixes at QoS 1 and no options.
func (c *v5client) subscribeWith(t *testing.T, opts ...paho.SubscribeOptions) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	ack, err := c.client.Subscribe(ctx, &paho.Subscribe{Subscriptions: opts})
	if err != nil {
		t.Fatalf("subscribing to %v: %v", opts, err)
	}

	for i, opt := range opts {
		if i < len(ack.Reasons) && ack.Reasons[i] >= 0x80 {
			t.Fatalf("subscribing to %s: reason code %#x", opt.Topic, ack.Reasons[i])
		}
	}
}

// topicsOf lists the topics of what a client received, in order.
func topicsOf(got []*paho.Publish) []string {
	topics := make([]string, 0, len(got))
	for _, p := range got {
		topics = append(topics, p.Topic)
	}

	return topics
}

// TestDollarTopicsEscapeLeadingWildcards covers #35: a filter whose first
// level is a wildcard must not match a topic beginning with '$'
// [MQTT-4.7.2-1]. mochi applies the rule to the whole topic, which mast
// mounts under its tenant, and NATS matches encoded subjects without it, so
// "+/+" used to receive "$TopicA/B".
//
// Each case publishes the '$' topic and then a plain one on the same path,
// and judges once the plain one has arrived, rather than after a sleep.
func TestDollarTopicsEscapeLeadingWildcards(t *testing.T) {
	t.Parallel()

	addr := start(t, tenant.Static{Tenant: "acme"})
	pub := dialV5(t, addr, "publisher")

	wild := dialV5(t, addr, "wild")
	wild.subscribeWith(t,
		paho.SubscribeOptions{Topic: "+/+", QoS: 1},
		paho.SubscribeOptions{Topic: "#", QoS: 1},
		paho.SubscribeOptions{Topic: "$share/g/+/+", QoS: 1},
	)

	// Holds a wildcard filter and one that names the '$' level, so mochi
	// merges both into one entry for the client; the message is owed once,
	// through the filter that does match.
	both := dialV5(t, addr, "both")
	both.subscribeWith(t,
		paho.SubscribeOptions{Topic: "+/+", QoS: 1},
		paho.SubscribeOptions{Topic: "$TopicA/#", QoS: 1},
	)

	pub.publish(t, &paho.Publish{Topic: "$TopicA/B", QoS: 1, Payload: []byte("dollar")})
	pub.publish(t, &paho.Publish{Topic: "TopicA/B", QoS: 1, Payload: []byte("plain")})

	if !waitFor(func() bool { return len(wild.received()) >= 2 && len(both.received()) >= 2 }) {
		t.Fatalf("the plain message never arrived: wild %v, both %v",
			topicsOf(wild.received()), topicsOf(both.received()))
	}

	// The plain topic reaches "wild" twice, once through its merged "#"
	// and "+/+" and once through the shared group; the '$' one never.
	for _, got := range topicsOf(wild.received()) {
		if got == "$TopicA/B" {
			t.Errorf("a filter starting with a wildcard received a '$' topic: %v", topicsOf(wild.received()))
		}
	}

	if got := topicsOf(both.received()); !slices.Equal(got, []string{"$TopicA/B", "TopicA/B"}) {
		t.Errorf("a client also subscribed to \"$TopicA/#\": got %v, want the '$' topic once, then the plain one", got)
	}
}

// TestDollarRetainedEscapesLeadingWildcards is #35 for a retained message,
// which is replayed from the bucket on subscribe and matched there on
// encoded subjects, not by mochi.
func TestDollarRetainedEscapesLeadingWildcards(t *testing.T) {
	t.Parallel()

	addr := start(t, tenant.Static{Tenant: "acme"})
	pub := dialV5(t, addr, "publisher")

	pub.publish(t, &paho.Publish{Topic: "$state/door", QoS: 1, Retain: true, Payload: []byte("dollar")})
	pub.publish(t, &paho.Publish{Topic: "state/door", QoS: 1, Retain: true, Payload: []byte("plain")})

	wild := dialV5(t, addr, "wild")
	wild.subscribeWith(t, paho.SubscribeOptions{Topic: "+/door", QoS: 1})

	named := dialV5(t, addr, "named")
	named.subscribeWith(t, paho.SubscribeOptions{Topic: "$state/+", QoS: 1})

	if !waitFor(func() bool { return len(wild.received()) >= 1 && len(named.received()) >= 1 }) {
		t.Fatalf("the retained messages were not replayed: wild %v, named %v",
			topicsOf(wild.received()), topicsOf(named.received()))
	}

	time.Sleep(200 * time.Millisecond) // the replay is one burst; let it finish

	if got := topicsOf(wild.received()); !slices.Equal(got, []string{"state/door"}) {
		t.Errorf("\"+/door\" was replayed %v, want only state/door", got)
	}

	if got := topicsOf(named.received()); !slices.Equal(got, []string{"$state/door"}) {
		t.Errorf("\"$state/+\" was replayed %v, want only $state/door", got)
	}
}

// payloadsOf lists the payloads of what a client received, in order.
func payloadsOf(got []*paho.Publish) []string {
	payloads := make([]string, 0, len(got))
	for _, p := range got {
		payloads = append(payloads, string(p.Payload))
	}

	return payloads
}

// TestV5NoLocal covers #37: with No Local set, a client is not sent its own
// publications [MQTT-3.8.3-3]. Every message reaches local subscribers from
// the bridge's inline client, so mochi's check against the packet's origin
// never matched, and the publisher was sent its own message back.
func TestV5NoLocal(t *testing.T) {
	t.Parallel()

	addr := start(t, tenant.Static{Tenant: "acme"})

	a := dialV5(t, addr, "a")
	a.subscribeWith(t, paho.SubscribeOptions{Topic: "chat/room", QoS: 1, NoLocal: true})

	b := dialV5(t, addr, "b")
	b.subscribeWith(t, paho.SubscribeOptions{Topic: "chat/room", QoS: 1, NoLocal: true})

	a.publish(t, &paho.Publish{Topic: "chat/room", QoS: 1, Payload: []byte("from a")})

	// b has it, so the one fan-out that would have reached a is over.
	if !waitFor(func() bool { return len(b.received()) == 1 }) {
		t.Fatalf("b never received a's message: %v", payloadsOf(b.received()))
	}

	b.publish(t, &paho.Publish{Topic: "chat/room", QoS: 1, Payload: []byte("from b")})

	if !waitFor(func() bool { return len(a.received()) >= 1 }) {
		t.Fatal("a never received b's message")
	}

	if got := payloadsOf(a.received()); !slices.Equal(got, []string{"from b"}) {
		t.Errorf("a received %q, want only b's message", got)
	}

	if got := payloadsOf(b.received()); !slices.Equal(got, []string{"from a"}) {
		t.Errorf("b received %q, want only a's message", got)
	}
}

// TestV5RetainAsPublished covers #42, the second part of
// test_subscribe_options, which #37 hid: with Retain As Published set, a live message keeps the
// retain flag it was published with [MQTT-3.3.1-13]. The bridge injects
// every live message with the flag cleared.
func TestV5RetainAsPublished(t *testing.T) {
	t.Parallel()

	addr := start(t, tenant.Static{Tenant: "acme"})

	asPublished := dialV5(t, addr, "as-published")
	asPublished.subscribeWith(t, paho.SubscribeOptions{Topic: "rap/+", QoS: 1, RetainAsPublished: true})

	cleared := dialV5(t, addr, "cleared")
	cleared.subscribeWith(t, paho.SubscribeOptions{Topic: "rap/+", QoS: 1})

	pub := dialV5(t, addr, "publisher")
	pub.publish(t, &paho.Publish{Topic: "rap/a", QoS: 1, Payload: []byte("plain")})
	pub.publish(t, &paho.Publish{Topic: "rap/a", QoS: 1, Retain: true, Payload: []byte("retained")})

	if !waitFor(func() bool { return len(asPublished.received()) >= 2 && len(cleared.received()) >= 2 }) {
		t.Fatalf("the messages did not arrive: %v, %v",
			payloadsOf(asPublished.received()), payloadsOf(cleared.received()))
	}

	for _, p := range asPublished.received() {
		if want := string(p.Payload) == "retained"; p.Retain != want {
			t.Errorf("Retain As Published: %q arrived with retain %v, want %v", p.Payload, p.Retain, want)
		}
	}

	// MQTT says a subscriber that was already there gets retain 0.
	for _, p := range cleared.received() {
		if p.Retain {
			t.Errorf("without Retain As Published, %q arrived with retain set", p.Payload)
		}
	}
}

// TestV5RetainHandling covers the rest of #42 and test_subscribe_options: Retain
// Handling 1 sends retained messages only for a new subscription, and 2
// never [MQTT-3.3.1-10] [MQTT-3.3.1-11]. A resubscribe with 1 was sent them
// again, because mast replays the bucket and mochi told it nothing about
// which subscriptions existed.
func TestV5RetainHandling(t *testing.T) {
	t.Parallel()

	addr := start(t, tenant.Static{Tenant: "acme"})

	pub := dialV5(t, addr, "publisher")
	pub.publish(t, &paho.Publish{Topic: "rh/a", QoS: 1, Retain: true, Payload: []byte("retained")})

	ifNew := dialV5(t, addr, "if-new")
	ifNew.subscribeWith(t, paho.SubscribeOptions{Topic: "rh/+", QoS: 1, RetainHandling: 1})

	if !waitFor(func() bool { return len(ifNew.received()) == 1 }) {
		t.Fatalf("a new subscription with Retain Handling 1 was not sent the retained message: %v",
			payloadsOf(ifNew.received()))
	}

	ifNew.subscribeWith(t, paho.SubscribeOptions{Topic: "rh/+", QoS: 1, RetainHandling: 1})

	never := dialV5(t, addr, "never")
	never.subscribeWith(t, paho.SubscribeOptions{Topic: "rh/+", QoS: 1, RetainHandling: 2})

	// The replay is written before the SUBACK returns, so a live message
	// published now arrives after anything the resubscribe replayed.
	pub.publish(t, &paho.Publish{Topic: "rh/b", QoS: 1, Payload: []byte("live")})

	if !waitFor(func() bool { return len(ifNew.received()) >= 2 && len(never.received()) >= 1 }) {
		t.Fatalf("the live message did not arrive: %v, %v", payloadsOf(ifNew.received()), payloadsOf(never.received()))
	}

	if got := payloadsOf(ifNew.received()); !slices.Equal(got, []string{"retained", "live"}) {
		t.Errorf("Retain Handling 1: received %q, want the retained message once, then the live one", got)
	}

	if got := payloadsOf(never.received()); !slices.Equal(got, []string{"live"}) {
		t.Errorf("Retain Handling 2: received %q, want only the live message", got)
	}
}

// TestV5ExpiredMessageIsNotResent covers #36: a message held for an absent
// session whose expiry interval passes before the session returns is
// deleted, not delivered [MQTT-3.3.2-5]. The session reconnects to the node
// it left, so it is mochi's inflight that holds the messages, and mochi
// expired only packets injected by an MQTT 5 client, which the bridge's
// inline client was not.
func TestV5ExpiredMessageIsNotResent(t *testing.T) {
	t.Parallel()

	addr := start(t, tenant.Static{Tenant: "acme"})

	sub := dialV5Persistent(t, addr, "sleeper")
	sub.subscribe(t, "expiry/t")
	_ = sub.client.Disconnect(&paho.Disconnect{ReasonCode: 0, Properties: nil})

	pub := dialV5(t, addr, "publisher")
	short, long := uint32(1), uint32(60)

	pub.publish(t, &paho.Publish{
		Topic: "expiry/t", QoS: 1, Payload: []byte("expires"),
		Properties: &paho.PublishProperties{MessageExpiry: &short},
	})
	pub.publish(t, &paho.Publish{
		Topic: "expiry/t", QoS: 1, Payload: []byte("keeps"),
		Properties: &paho.PublishProperties{MessageExpiry: &long},
	})

	// Expiry is in whole seconds and mochi sweeps once a second, so a
	// one-second interval is certainly past after three.
	time.Sleep(3 * time.Second)

	back := dialV5Persistent(t, addr, "sleeper")
	t.Cleanup(func() { _ = back.client.Disconnect(&paho.Disconnect{ReasonCode: 0, Properties: nil}) })

	// The held messages are resent straight after the CONNACK, before a
	// message published now can arrive.
	pub.publish(t, &paho.Publish{Topic: "expiry/t", QoS: 1, Payload: []byte("live")})

	if !waitFor(func() bool { return slices.Contains(payloadsOf(back.received()), "live") }) {
		t.Fatalf("the live message never arrived: %v", payloadsOf(back.received()))
	}

	if got := payloadsOf(back.received()); !slices.Equal(got, []string{"keeps", "live"}) {
		t.Fatalf("the returning session received %q, want the unexpired message, then the live one", got)
	}

	// And it is told what is left of the interval, not the whole of it.
	if left := back.received()[0].Properties.MessageExpiry; left == nil || *left >= long {
		t.Errorf("the held message's expiry interval: got %v, want less than %d", left, long)
	}
}

// dialWithWill connects an MQTT 5 client with a will and the given delay
// and session expiry, and returns the connection so a test can drop it
// without a DISCONNECT, which is what makes a will go out.
func dialWithWill(t *testing.T, addr, clientID string, delay, expiry uint32) net.Conn {
	t.Helper()

	var d net.Dialer

	conn, err := d.DialContext(t.Context(), "tcp", addr)
	if err != nil {
		t.Fatalf("dialling %s: %v", addr, err)
	}

	t.Cleanup(func() { _ = conn.Close() })

	client := paho.NewClient(paho.ClientConfig{Conn: conn})

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	ack, err := client.Connect(ctx, &paho.Connect{
		ClientID:       clientID,
		Username:       "acme",
		UsernameFlag:   true,
		CleanStart:     true,
		KeepAlive:      30,
		Properties:     &paho.ConnectProperties{SessionExpiryInterval: &expiry},
		WillMessage:    &paho.WillMessage{Topic: "wills/" + clientID, QoS: 1, Payload: []byte("gone")},
		WillProperties: &paho.WillProperties{WillDelayInterval: &delay},
	})
	if err != nil {
		t.Fatalf("connecting %s: %v", clientID, err)
	}

	if ack.ReasonCode != 0 {
		t.Fatalf("connecting %s: reason code %d", clientID, ack.ReasonCode)
	}

	return conn
}

// TestV5WillDelay covers #38: a will is held for its Will Delay Interval
// [MQTT-3.1.3-9]. The bridge forwarded it the moment the connection dropped,
// whatever delay the client had asked for.
func TestV5WillDelay(t *testing.T) {
	t.Parallel()

	addr := start(t, tenant.Static{Tenant: "acme"})

	watcher := dialV5(t, addr, "watcher")
	watcher.subscribe(t, "wills/+")

	conn := dialWithWill(t, addr, "delayed", 2, 60)
	dropped := time.Now()

	_ = conn.Close()

	deadline := time.Now().Add(5 * time.Second)
	for len(watcher.received()) == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}

	if len(watcher.received()) == 0 {
		t.Fatal("the delayed will was never sent")
	}

	if waited := time.Since(dropped); waited < 1500*time.Millisecond {
		t.Errorf("the will arrived %v after the connection dropped, before its 2s delay", waited)
	}
}

// TestV5WillDelayIsCancelledByReconnecting is the other half of #38: a
// device that is back inside the delay is not reported gone at all.
func TestV5WillDelayIsCancelledByReconnecting(t *testing.T) {
	t.Parallel()

	addr := start(t, tenant.Static{Tenant: "acme"})

	watcher := dialV5(t, addr, "watcher")
	watcher.subscribe(t, "wills/+")

	conn := dialWithWill(t, addr, "flaky", 1, 60)
	_ = conn.Close()

	back := dialV5Persistent(t, addr, "flaky")
	t.Cleanup(func() { _ = back.client.Disconnect(&paho.Disconnect{ReasonCode: 0, Properties: nil}) })

	// Past the delay, then a message on the same path to prove the
	// watcher is still receiving.
	time.Sleep(2 * time.Second)

	pub := dialV5(t, addr, "publisher")
	pub.publish(t, &paho.Publish{Topic: "wills/probe", QoS: 1, Payload: []byte("probe")})

	if !waitFor(func() bool { return len(watcher.received()) > 0 }) {
		t.Fatal("the probe never arrived")
	}

	if got := payloadsOf(watcher.received()); !slices.Equal(got, []string{"probe"}) {
		t.Errorf("the watcher received %q after the device came back inside its will delay, want only the probe", got)
	}
}

// TestClusterWillDelayIsCancelledElsewhere is #38 across nodes: the device
// whose connection dropped comes back on another edge inside its will
// delay, which every rolling deploy and every mobile handover does. Its old
// node holds the timer, and only the session notice can tell it.
func TestClusterWillDelayIsCancelledElsewhere(t *testing.T) {
	c := startCluster(t)

	watcher := dialV5(t, c.addrs[1], "watcher")
	watcher.subscribe(t, "wills/+")

	conn := dialWithWill(t, c.addrs[0], "roamer", 2, 60)
	_ = conn.Close()

	back := dialV5Persistent(t, c.addrs[1], "roamer")
	t.Cleanup(func() { _ = back.client.Disconnect(&paho.Disconnect{ReasonCode: 0, Properties: nil}) })

	time.Sleep(3 * time.Second) // past the delay

	// A probe from the old edge, so it crosses the same path the will
	// would have, and publish until interest has crossed the leaf.
	pub := dialV5(t, c.addrs[0], "publisher")

	if !waitForLeaf(func() bool {
		pub.publish(t, &paho.Publish{Topic: "wills/probe", QoS: 1, Payload: []byte("probe")})

		return len(watcher.received()) > 0
	}) {
		t.Fatal("the probe never crossed the fabric")
	}

	for _, got := range payloadsOf(watcher.received()) {
		if got != "probe" {
			t.Errorf("the watcher received %q after the device came back elsewhere inside its will delay", got)
		}
	}
}
