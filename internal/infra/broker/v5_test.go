package broker_test

import (
	"context"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/mastmq/mast/internal/domain/tenant"
)

// v5client is an MQTT 5 client that records every PUBLISH it receives,
// properties included. The v3 collector cannot see properties at all, which
// is how their loss on the fabric went unnoticed.
type v5client struct {
	client *paho.Client

	mu  sync.Mutex
	got []*paho.Publish
}

func (c *v5client) received() []*paho.Publish {
	c.mu.Lock()
	defer c.mu.Unlock()

	return slices.Clone(c.got)
}

// dialV5 connects a clean-start MQTT 5 client as tenant acme. Every node in
// these tests resolves the tenant from the username, so it is fixed.
func dialV5(t *testing.T, addr, clientID string) *v5client {
	t.Helper()

	var d net.Dialer

	conn, err := d.DialContext(t.Context(), "tcp", addr)
	if err != nil {
		t.Fatalf("dialling %s: %v", addr, err)
	}

	c := &v5client{client: nil, mu: sync.Mutex{}, got: nil}

	c.client = paho.NewClient(paho.ClientConfig{
		Conn: conn,
		OnPublishReceived: []func(paho.PublishReceived) (bool, error){
			func(pr paho.PublishReceived) (bool, error) {
				c.mu.Lock()
				defer c.mu.Unlock()

				c.got = append(c.got, pr.Packet)

				return true, nil
			},
		},
	})

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	ack, err := c.client.Connect(ctx, &paho.Connect{
		ClientID:     clientID,
		Username:     "acme",
		UsernameFlag: true,
		CleanStart:   true,
		KeepAlive:    30,
	})
	if err != nil {
		t.Fatalf("connecting %s: %v", clientID, err)
	}

	if ack.ReasonCode != 0 {
		t.Fatalf("connecting %s: reason code %d", clientID, ack.ReasonCode)
	}

	t.Cleanup(func() { _ = c.client.Disconnect(&paho.Disconnect{ReasonCode: 0, Properties: nil}) })

	return c
}

// subscribe asks for QoS 1, the lowest level a message is kept for an absent
// session at, so every test here exercises the path that stores properties.
func (c *v5client) subscribe(t *testing.T, filter string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	_, err := c.client.Subscribe(ctx, &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{{Topic: filter, QoS: 1}},
	})
	if err != nil {
		t.Fatalf("subscribing to %s: %v", filter, err)
	}
}

func (c *v5client) publish(t *testing.T, pub *paho.Publish) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	if _, err := c.client.Publish(ctx, pub); err != nil {
		t.Fatalf("publishing to %s: %v", pub.Topic, err)
	}
}

// richProperties is every PUBLISH property a publisher may set that a
// subscriber is meant to receive unchanged.
func richProperties() *paho.PublishProperties {
	format := byte(1)

	return &paho.PublishProperties{
		PayloadFormat:   &format,
		ContentType:     "application/json",
		ResponseTopic:   "replies/42",
		CorrelationData: []byte{0x00, 0xff, 'r', 'q'},
		User: paho.UserProperties{
			{Key: "trace", Value: "abc"},
			{Key: "trace", Value: "def"}, // repeated keys are legal and ordered
			{Key: "non-ascii", Value: "دما"},
		},
	}
}

// assertProperties compares what a subscriber received with what was sent.
func assertProperties(t *testing.T, got *paho.PublishProperties, want *paho.PublishProperties) {
	t.Helper()

	if got == nil {
		t.Fatal("the message arrived with no properties at all")
	}

	if got.PayloadFormat == nil || *got.PayloadFormat != *want.PayloadFormat {
		t.Errorf("payload format: got %v, want %d", got.PayloadFormat, *want.PayloadFormat)
	}

	if got.ContentType != want.ContentType {
		t.Errorf("content type: got %q, want %q", got.ContentType, want.ContentType)
	}

	if got.ResponseTopic != want.ResponseTopic {
		t.Errorf("response topic: got %q, want %q", got.ResponseTopic, want.ResponseTopic)
	}

	if string(got.CorrelationData) != string(want.CorrelationData) {
		t.Errorf("correlation data: got %x, want %x", got.CorrelationData, want.CorrelationData)
	}

	if !slices.Equal(got.User, want.User) {
		t.Errorf("user properties: got %v, want %v", got.User, want.User)
	}
}

// TestV5PropertiesSurviveTheFabric covers the MQTT 5 properties a publisher
// sets. Every publish leaves the node for NATS and comes back, and only the
// payload, the QoS and the retain flag used to make the trip: a request
// with a response topic and correlation data arrived as a bare message,
// which breaks MQTT 5 request/response entirely.
func TestV5PropertiesSurviveTheFabric(t *testing.T) {
	t.Parallel()

	addr := start(t, tenant.Static{Tenant: "acme"})

	sub := dialV5(t, addr, "responder")
	sub.subscribe(t, "requests/+")

	pub := dialV5(t, addr, "requester")
	want := richProperties()

	pub.publish(t, &paho.Publish{Topic: "requests/7", QoS: 1, Payload: []byte(`{}`), Properties: want})

	if !waitFor(func() bool { return len(sub.received()) > 0 }) {
		t.Fatal("the request never arrived")
	}

	assertProperties(t, sub.received()[0].Properties, want)
}

// TestV5PropertiesSurviveRetention covers a retained message, which MQTT 5
// requires to be replayed with the properties it was published with.
func TestV5PropertiesSurviveRetention(t *testing.T) {
	t.Parallel()

	addr := start(t, tenant.Static{Tenant: "acme"})

	pub := dialV5(t, addr, "publisher")
	want := richProperties()

	pub.publish(t, &paho.Publish{Topic: "state/door", QoS: 1, Retain: true, Payload: []byte("open"), Properties: want})

	sub := dialV5(t, addr, "late")
	sub.subscribe(t, "state/#")

	if !waitFor(func() bool { return len(sub.received()) > 0 }) {
		t.Fatal("the retained message was not replayed")
	}

	assertProperties(t, sub.received()[0].Properties, want)
}

// TestV5ExpiredRetainedMessageIsNotReplayed covers the message expiry
// interval on a retained message. MQTT 5 says a retained message whose
// expiry has passed must not be sent, and the bucket used to keep and
// replay it forever.
func TestV5ExpiredRetainedMessageIsNotReplayed(t *testing.T) {
	t.Parallel()

	addr := start(t, tenant.Static{Tenant: "acme"})

	pub := dialV5(t, addr, "publisher")
	expiry := uint32(1)

	pub.publish(t, &paho.Publish{
		Topic: "flash/sale", QoS: 1, Retain: true, Payload: []byte("now"),
		Properties: &paho.PublishProperties{MessageExpiry: &expiry},
	})

	// The interval is in whole seconds, and "has passed" needs a clear
	// margin over one.
	time.Sleep(2500 * time.Millisecond)

	sub := dialV5(t, addr, "too-late")
	sub.subscribe(t, "flash/#")

	time.Sleep(settle)

	if got := sub.received(); len(got) != 0 {
		t.Errorf("an expired retained message was replayed: %q", got[0].Payload)
	}
}

// dialV5Persistent connects an MQTT 5 client whose session outlives the
// connection, which for v5 means a non-zero session expiry rather than the
// v3 clean flag.
func dialV5Persistent(t *testing.T, addr, clientID string) *v5client {
	t.Helper()

	var d net.Dialer

	conn, err := d.DialContext(t.Context(), "tcp", addr)
	if err != nil {
		t.Fatalf("dialling %s: %v", addr, err)
	}

	c := &v5client{client: nil, mu: sync.Mutex{}, got: nil}

	c.client = paho.NewClient(paho.ClientConfig{
		Conn: conn,
		OnPublishReceived: []func(paho.PublishReceived) (bool, error){
			func(pr paho.PublishReceived) (bool, error) {
				c.mu.Lock()
				defer c.mu.Unlock()

				c.got = append(c.got, pr.Packet)

				return true, nil
			},
		},
	})

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	expiry := uint32(3600)

	if _, err := c.client.Connect(ctx, &paho.Connect{
		ClientID:     clientID,
		Username:     "acme",
		UsernameFlag: true,
		CleanStart:   false,
		KeepAlive:    30,
		Properties:   &paho.ConnectProperties{SessionExpiryInterval: &expiry},
	}); err != nil {
		t.Fatalf("connecting %s: %v", clientID, err)
	}

	return c
}

// TestClusterV5PropertiesCrossTheLeaf is the fabric test across a real
// socket: the header has to survive the leaf connection between two edges,
// not only an in-process round trip.
func TestClusterV5PropertiesCrossTheLeaf(t *testing.T) {
	c := startCluster(t)

	sub := dialV5(t, c.addrs[0], "responder")
	sub.subscribe(t, "requests/+")

	pub := dialV5(t, c.addrs[1], "requester")
	want := richProperties()

	// Interest crosses the leaf asynchronously, so publish until it lands.
	delivered := waitForLeaf(func() bool {
		pub.publish(t, &paho.Publish{Topic: "requests/7", QoS: 1, Payload: []byte(`{}`), Properties: want})

		return len(sub.received()) > 0
	})
	if !delivered {
		t.Fatal("the request never crossed the fabric")
	}

	assertProperties(t, sub.received()[0].Properties, want)
}

// TestClusterV5PropertiesSurviveTheOfflineQueue covers a message queued for
// an absent MQTT 5 session and replayed on another node, which is the path
// that goes through the bucket rather than through mochi's own inflight.
func TestClusterV5PropertiesSurviveTheOfflineQueue(t *testing.T) {
	c := startCluster(t)

	first := dialV5Persistent(t, c.addrs[0], "field-unit")
	first.subscribe(t, "orders/field-unit")

	// awaitInterest needs a v3 collector on the same node and tenant; its
	// probe proves the edge's interest has reached the core.
	probe := connect(t, c.addrs[0], "interest-holder", "acme")
	c.awaitInterest(t, probe, "acme")

	_ = first.client.Disconnect(&paho.Disconnect{ReasonCode: 0, Properties: nil})

	pub := dialV5(t, c.addrs[1], "dispatcher")
	want := richProperties()

	pub.publish(t, &paho.Publish{Topic: "orders/field-unit", QoS: 1, Payload: []byte("go"), Properties: want})

	if !awaitMetric(t, c.obs[0], `mast_offline_queued_total{tenant="acme"} `, 1) {
		t.Fatal("nothing was queued for the absent session")
	}

	back := dialV5Persistent(t, c.addrs[1], "field-unit")
	t.Cleanup(func() { _ = back.client.Disconnect(&paho.Disconnect{ReasonCode: 0, Properties: nil}) })

	if !waitForLeaf(func() bool { return len(back.received()) > 0 }) {
		t.Fatal("the queued message did not follow the session")
	}

	assertProperties(t, back.received()[0].Properties, want)
}
