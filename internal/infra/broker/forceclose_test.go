package broker_test

import (
	"context"
	"encoding/binary"
	"math"
	"net"
	"testing"
	"time"
)

// connectWithWill opens a raw MQTT 3.1.1 connection carrying a will and
// returns the socket.
//
// It speaks the protocol by hand because the test needs a connection that
// dies without a DISCONNECT, and no client library offers that: sending
// DISCONNECT is precisely what tells a broker to discard the will. Closing
// the returned net.Conn is the ungraceful loss a will exists for.
func connectWithWill(t *testing.T, addr, clientID, username, willTopic, willPayload string) net.Conn {
	t.Helper()

	var dialer net.Dialer

	conn, err := dialer.DialContext(context.Background(), "tcp", addr)
	if err != nil {
		t.Fatalf("dialing %s: %v", addr, err)
	}

	const (
		cleanSession = 0x02
		willFlag     = 0x04
		usernameFlag = 0x80
		protocolL311 = 4
		keepAlive    = 60
	)

	var variable []byte

	variable = appendString(variable, "MQTT")
	variable = append(variable, protocolL311, cleanSession|willFlag|usernameFlag)
	variable = binary.BigEndian.AppendUint16(variable, keepAlive)

	// Payload order is fixed by the spec: client id, will topic, will
	// message, username, password.
	variable = appendString(variable, clientID)
	variable = appendString(variable, willTopic)
	variable = appendString(variable, willPayload)
	variable = appendString(variable, username)

	packet := append([]byte{0x10}, appendVarint(nil, len(variable))...)
	packet = append(packet, variable...)

	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("setting deadline: %v", err)
	}

	if _, err := conn.Write(packet); err != nil {
		t.Fatalf("writing CONNECT: %v", err)
	}

	// CONNACK is four bytes; the last is the return code.
	ack := make([]byte, 4)
	if _, err := conn.Read(ack); err != nil {
		t.Fatalf("reading CONNACK: %v", err)
	}

	if ack[0] != 0x20 || ack[3] != 0 {
		t.Fatalf("CONNACK refused the connection: % x", ack)
	}

	return conn
}

func appendString(b []byte, s string) []byte {
	// Every string this test writes is a short literal; MQTT caps them at
	// 65535 anyway.
	b = binary.BigEndian.AppendUint16(b, uint16(min(len(s), math.MaxUint16)))

	return append(b, s...)
}

// appendVarint writes MQTT's remaining-length encoding.
func appendVarint(b []byte, n int) []byte {
	const radix = 128

	for {
		//nolint:gosec // n%128 is 0-127 by construction and fits a byte.
		digit := byte(n % radix)
		n /= radix

		if n > 0 {
			digit |= 0x80
		}

		b = append(b, digit)

		if n == 0 {
			return b
		}
	}
}
