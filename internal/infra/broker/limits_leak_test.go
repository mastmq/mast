package broker_test

import (
	"bytes"
	"encoding/binary"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/mastmq/mast/internal/infra/config"
)

// resumeRaw opens a raw MQTT 3.1.1 connection resuming clientID's
// persistent session, reads the CONNACK and nothing more, so the broker is
// still resending the session's backlog for as long as the test likes.
//
// No client library can do this on purpose: each reads what the broker
// sends. That moment is what the tests are about, because mochi returns
// from a failed resend without calling OnDisconnect.
func resumeRaw(t *testing.T, addr, clientID, username string) net.Conn {
	t.Helper()

	var dialer net.Dialer

	conn, err := dialer.DialContext(t.Context(), "tcp", addr)
	if err != nil {
		t.Fatalf("dialing %s: %v", addr, err)
	}

	const (
		usernameFlag = 0x80
		protocolL311 = 4
		keepAlive    = 60
	)

	var variable []byte

	variable = appendString(variable, "MQTT")
	variable = append(variable, protocolL311, usernameFlag) // no clean session
	variable = binary.BigEndian.AppendUint16(variable, keepAlive)
	variable = appendString(variable, clientID)
	variable = appendString(variable, username)

	packet := append([]byte{0x10}, appendVarint(nil, len(variable))...)
	packet = append(packet, variable...)

	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("setting deadline: %v", err)
	}

	if _, err := conn.Write(packet); err != nil {
		t.Fatalf("writing CONNECT: %v", err)
	}

	ack := make([]byte, 4)
	if _, err := conn.Read(ack); err != nil {
		t.Fatalf("reading CONNACK: %v", err)
	}

	// Session present, accepted.
	if !bytes.Equal(ack, []byte{0x20, 0x02, 0x01, 0x00}) {
		t.Fatalf("CONNACK for a resumed session = % x", ack)
	}

	t.Cleanup(func() { _ = conn.Close() })

	return conn
}

// drop resets conn rather than closing it, so the broker's next write fails
// at once instead of filling the socket buffer.
func drop(conn net.Conn) {
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetLinger(0)
	}

	_ = conn.Close()
}

// withBacklog leaves acme's persistent client "sleeper" away with a backlog
// far larger than a socket buffer, so resending it takes many writes.
func withBacklog(t *testing.T, addr string) {
	t.Helper()

	const backlog, size = 60, 200 << 10

	sleeper := persistent(t, addr, "sleeper", "acme")
	sleeper.subscribeQoS(t, "fleet/sleeper/cmd", 1)
	sleeper.client.Disconnect(100)

	pub := connect(t, addr, "dispatcher", "acme")
	payload := bytes.Repeat([]byte("x"), size)

	for i := range backlog {
		tok := pub.client.Publish("fleet/sleeper/cmd", 1, false, payload)
		if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
			t.Fatalf("publish %d: %v", i, tok.Error())
		}
	}

	pub.client.Disconnect(100)
}

// requireRoom fails the test unless n more of acme's connections fit.
func requireRoom(t *testing.T, addr string, n int) {
	t.Helper()

	for i := range n {
		name := "after-" + strconv.Itoa(i)

		if !waitFor(func() bool {
			_, err := tryV3(t, addr, name, "acme")

			return err == nil
		}) {
			t.Fatalf("connection %d of %d was refused with nothing else of acme's open: a slot leaked", i+1, n)
		}
	}
}

// TestConnectionLimitSurvivesADropDuringResend is the leak a review found:
// a device that resumes a session with a backlog and drops while mochi is
// resending it never reached OnDisconnect, so its slot was never released
// and the tenant, at its limit, was locked out until the node restarted.
func TestConnectionLimitSurvivesADropDuringResend(t *testing.T) {
	addr, obsAddr := startLimited(t, func(l *config.Limits) { l.MaxConnections = 2 })

	withBacklog(t, addr)
	drop(resumeRaw(t, addr, "sleeper", "acme"))

	// The periodic reap, with no connection asking: this is what stops the
	// leaked slot being reported to every other node. The gauge is
	// released with the slot, so it shows the reap ran. The reap runs every
	// five seconds.
	gauge := `mast_connections_open{tenant="acme"} `

	deadline := time.Now().Add(12 * time.Second)
	for readMetric(t, obsAddr, gauge) != 0 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}

	if got := readMetric(t, obsAddr, gauge); got != 0 {
		t.Errorf("%s= %v after the only connection dropped, want 0", gauge, got)
	}

	// And the slot itself, on the refusal path.
	requireRoom(t, addr, 2)
}
