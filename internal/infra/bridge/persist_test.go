package bridge

import (
	"testing"

	mqtt "github.com/mastmq/mochi/v2"
	"github.com/mastmq/mochi/v2/packets"
)

// TestQueuesForAbsenceOnlyKeepsSessionsThatOutliveTheConnection covers the
// clean-session subscriber caught mid-disconnect: absent, so mochi cannot
// write to it, but with no session anything could ever be replayed into.
//
// Every client here is absent — no connection and never opened — which is
// the state OnQosPublish sees in that window.
func TestQueuesForAbsenceOnlyKeepsSessionsThatOutliveTheConnection(t *testing.T) {
	cases := []struct {
		name  string
		props mqtt.ClientProperties
		want  bool
	}{
		{
			name:  "v3 clean session",
			props: mqtt.ClientProperties{ProtocolVersion: 4, Clean: true},
			want:  false,
		},
		{
			name:  "v3 persistent session",
			props: mqtt.ClientProperties{ProtocolVersion: 4, Clean: false},
			want:  true,
		},
		{
			name:  "v5 with no expiry",
			props: mqtt.ClientProperties{ProtocolVersion: 5, Props: packets.Properties{SessionExpiryInterval: 0}},
			want:  false,
		},
		{
			name:  "v5 with an expiry",
			props: mqtt.ClientProperties{ProtocolVersion: 5, Props: packets.Properties{SessionExpiryInterval: 3600}},
			want:  true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cl := fakeClient("absent")
			cl.Properties = tc.props

			if got := queuesForAbsence(cl); got != tc.want {
				t.Errorf("queuesForAbsence = %v, want %v", got, tc.want)
			}
		})
	}
}
