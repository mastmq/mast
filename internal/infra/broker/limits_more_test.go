package broker_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	paho5 "github.com/eclipse/paho.golang/paho"
	"github.com/mastmq/mast/internal/infra/auth"
	"github.com/mastmq/mast/internal/infra/broker"
	"github.com/mastmq/mast/internal/infra/config"
)

// TestPublishRateV5QoS2Disconnects covers the one MQTT 5 case that cannot
// be answered with a reason code on the acknowledgement: mochi writes a
// failing code as a PUBACK even for QoS 2, which a client waiting for a
// PUBREC cannot read, so the client is disconnected with 0x96 instead.
func TestPublishRateV5QoS2Disconnects(t *testing.T) {
	addr, _ := startLimited(t, func(l *config.Limits) {
		l.PublishRate = 0.01
		l.PublishBurst = 1
	})

	var d net.Dialer

	conn, err := d.DialContext(t.Context(), "tcp", addr)
	if err != nil {
		t.Fatalf("dialling %s: %v", addr, err)
	}

	disconnected := make(chan byte, 1)

	client := paho5.NewClient(paho5.ClientConfig{
		Conn: conn,
		OnServerDisconnect: func(d *paho5.Disconnect) {
			disconnected <- d.ReasonCode
		},
	})

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	ack, err := client.Connect(ctx, &paho5.Connect{
		ClientID: "chatty-qos2", Username: "acme", UsernameFlag: true, CleanStart: true, KeepAlive: 30,
	})
	if err != nil || ack.ReasonCode != 0 {
		t.Fatalf("connecting: %v, %v", ack, err)
	}

	if _, err := client.Publish(ctx, &paho5.Publish{Topic: "m", QoS: 2, Payload: []byte("first")}); err != nil {
		t.Fatalf("the first publish, inside the burst, failed: %v", err)
	}

	// The second never completes; the disconnect is the answer.
	go func() { _, _ = client.Publish(ctx, &paho5.Publish{Topic: "m", QoS: 2, Payload: []byte("second")}) }()

	select {
	case code := <-disconnected:
		if code != reasonRateTooHigh {
			t.Errorf("a v5 QoS 2 publish past the rate was disconnected with %#x, want %#x", code, reasonRateTooHigh)
		}
	case <-ctx.Done():
		t.Fatal("a v5 QoS 2 publish past the rate was neither refused nor disconnected")
	}
}

// TestPublishRateDropsQoS0 covers QoS 0 past the rate: nothing acknowledges
// it, so it is dropped, counted, and the publisher keeps its connection.
func TestPublishRateDropsQoS0(t *testing.T) {
	addr, obsAddr := startLimited(t, func(l *config.Limits) {
		l.PublishRate = 0.01
		l.PublishBurst = 2
	})

	acme := connect(t, addr, "acme-listener", "acme")
	acme.subscribe(t, "m")

	pub := connect(t, addr, "chatty-qos0", "acme")
	for i := range 3 {
		pub.publish(t, "m", strconv.Itoa(i))
	}

	waitFor(func() bool { return len(acme.messages()) >= 2 })
	time.Sleep(200 * time.Millisecond)

	if got := acme.messages(); len(got) != 2 {
		t.Errorf("a burst of 2 then one more delivered %v, want the first two", got)
	}

	if got := readMetric(t, obsAddr, `mast_limit_exceeded_total{limit="publish_rate",tenant="acme"} `); got != 1 {
		t.Errorf("mast_limit_exceeded_total for publish_rate = %v, want 1", got)
	}

	if !pub.client.IsConnectionOpen() {
		t.Error("a QoS 0 publisher past its rate lost its connection")
	}
}

// TestAuthReplyLimitsAreEnforced follows a limit from an HTTP
// authentication reply all the way to a refused CONNECT. The parsing and
// the layering have unit tests of their own; this is the one that fails if
// the bridge stops handing the identity's limits to the limiter.
func TestAuthReplyLimitsAreEnforced(t *testing.T) {
	service := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decoding request: %v", err)
		}

		reply := map[string]any{"allow": true}
		if _, isAuthz := req["action"]; !isAuthz {
			reply["tenant"] = "acme"
			reply["limits"] = map[string]any{"max_connections": 1}
		}

		w.Header().Set("Content-Type", "application/json")

		if err := json.NewEncoder(w).Encode(reply); err != nil {
			t.Errorf("encoding reply: %v", err)
		}
	}))
	t.Cleanup(service.Close)

	cfg := config.Default()
	cfg.MQTT.Addr = freeAddr(t)
	cfg.NATS.MonitorAddr = ""
	cfg.Obs.Addr = anyPort
	cfg.Core.StoreDir = t.TempDir()
	cfg.Auth.Mode = config.AuthHTTP
	cfg.Auth.HTTP.AuthnURL = service.URL
	cfg.Auth.HTTP.AuthzURL = service.URL

	log := slog.New(slog.DiscardHandler)

	resolver, policy, err := auth.Build(cfg, log)
	if err != nil {
		t.Fatalf("building auth: %v", err)
	}

	node, err := broker.Start(t.Context(), cfg, resolver, policy, log)
	if err != nil {
		t.Fatalf("starting broker: %v", err)
	}

	t.Cleanup(node.Close)

	// The configuration sets no limit at all; only the reply does.
	if ack := connectV5(t, cfg.MQTT.Addr, "first"); ack.ReasonCode != 0 {
		t.Fatalf("the first connection got CONNACK %#x", ack.ReasonCode)
	}

	if ack := connectV5(t, cfg.MQTT.Addr, "second"); ack.ReasonCode != reasonQuotaExceeded {
		t.Errorf("a second connection under a reply's limit of 1 got CONNACK %#x, want %#x",
			ack.ReasonCode, reasonQuotaExceeded)
	}
}
