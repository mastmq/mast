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

// TestStoredFilterKeepsTheShare covers a persistent shared subscriber that
// moves to another node. The stored filter dropped the share prefix with the
// tenant, so the restored subscription was a plain one and received every
// message its group was meant to divide.
func TestStoredFilterKeepsTheShare(t *testing.T) {
	for filter, want := range map[string]string{
		"a/b":                   "a/b",
		"a/#":                   "a/#",
		"$share/workers/jobs/+": "$share/workers/jobs/+",
		"$SHARE/g/x":            "$share/g/x", // mochi matches the prefix in any case
		"/lead/":                "/lead/",
	} {
		mounted := mountFilter("acme", filter)

		stored := storedFilter("acme", mounted)
		if stored != want {
			t.Errorf("stored %q as %q, want %q", filter, stored, want)
		}

		if back := mountFilter("acme", stored); back != mounted {
			t.Errorf("%q restored as %q, want %q", filter, back, mounted)
		}
	}
}

// TestRouteRoundTrip pins the route the bridge hands itself through a
// packet field mochi never encodes.
func TestRouteRoundTrip(t *testing.T) {
	for _, r := range []route{
		{id: "abc", tenant: "", queue: "", subject: ""},
		{id: "", tenant: "", queue: "", subject: ""},
		{id: "abc", tenant: "acme", queue: "acme.workers", subject: "t.acme.jobs.*"},
	} {
		got, ok := parseRoute(r.String())
		if !ok || got != r {
			t.Errorf("route %+v came back as %+v (ok=%v)", r, got, ok)
		}
	}

	if _, ok := parseRoute(""); ok {
		t.Error("an empty server reference parsed as a route")
	}
}
