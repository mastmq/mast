package store

import (
	"errors"
	"testing"

	"github.com/nats-io/nats.go"
)

// TestDirectGetNoticesAMissingReply covers a batched read that lost a reply
// in transit. Read as a short batch, a drain would have trimmed the message
// it never delivered.
func TestDirectGetNoticesAMissingReply(t *testing.T) {
	t.Parallel()

	reply := func(prev string) *nats.Msg {
		m := nats.NewMsg("_INBOX.x")
		m.Header.Set(nats.JSLastSequence, prev)

		return m
	}

	if err := continuous(reply("0"), 0); err != nil {
		t.Errorf("the first reply of a batch: %v", err)
	}

	if err := continuous(reply("7"), 7); err != nil {
		t.Errorf("a reply following the last: %v", err)
	}

	if err := continuous(reply("9"), 7); !errors.Is(err, errDirectGap) {
		t.Errorf("a reply after a missing one = %v, want errDirectGap", err)
	}

	if err := continuous(nats.NewMsg("_INBOX.x"), 7); err != nil {
		t.Errorf("a reply without the header is taken on trust: %v", err)
	}
}
