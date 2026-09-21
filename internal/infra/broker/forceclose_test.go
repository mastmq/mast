package broker_test

import (
	"testing"

	paho "github.com/eclipse/paho.mqtt.golang"
)

// forceClose drops a client's TCP connection without sending DISCONNECT, so
// the broker treats it as an ungraceful loss and fires the will.
//
// paho has no API for this, so the test reaches for the rudest equivalent it
// can: disconnecting with a zero quiesce leaves the broker to notice the
// closed socket. When the will work lands this may need a raw net.Conn
// instead.
func forceClose(t *testing.T, c paho.Client) {
	t.Helper()

	c.Disconnect(0)
}
