package store

import (
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func replica(active time.Duration, offline bool) *jetstream.PeerInfo {
	return &jetstream.PeerInfo{Name: "r", Current: false, Offline: offline, Active: active, Lag: 0}
}

// A core cut off with its edge may lead the sessions stream itself, and
// answer its own request for the stream's info well inside the twenty
// seconds Raft lets it keep leading alone. Asking only whether there is a
// leader let it declare every other edge dead; the leader's own view of its
// replicas is what gives it away.
func TestQuorateNeedsAMajorityOfReplicasInTouch(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		cluster  *jetstream.ClusterInfo
		reachable bool
	}{
		{"not replicated", nil, true},
		{"no leader", &jetstream.ClusterInfo{Leader: ""}, false},
		{"single replica", &jetstream.ClusterInfo{Leader: "core-0"}, true},
		{"R3, both replicas heard", &jetstream.ClusterInfo{
			Leader: "core-0", Replicas: []*jetstream.PeerInfo{replica(time.Second, false), replica(200*time.Millisecond, false)},
		}, true},
		{"R3, one replica heard", &jetstream.ClusterInfo{
			Leader: "core-0", Replicas: []*jetstream.PeerInfo{replica(time.Second, false), replica(15*time.Second, false)},
		}, true},
		{"R3, leader alone in a minority", &jetstream.ClusterInfo{
			Leader: "core-0", Replicas: []*jetstream.PeerInfo{replica(11*time.Second, false), replica(11*time.Second, false)},
		}, false},
		{"R3, replicas never heard", &jetstream.ClusterInfo{
			Leader: "core-0", Replicas: []*jetstream.PeerInfo{replica(0, false), replica(0, true)},
		}, false},
		{"R3, replica offline", &jetstream.ClusterInfo{
			Leader: "core-0", Replicas: []*jetstream.PeerInfo{replica(time.Second, true), replica(20*time.Second, false)},
		}, false},
	}

	for _, c := range cases {
		err := quorate(c.cluster)
		if c.reachable && err != nil {
			t.Errorf("%s: %v", c.name, err)
		}

		if !c.reachable && !errors.Is(err, errNoQuorum) {
			t.Errorf("%s: err = %v, want errNoQuorum", c.name, err)
		}
	}
}
