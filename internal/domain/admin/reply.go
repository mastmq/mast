package admin

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
)

// ErrTooSmall is a byte budget that cannot hold even an empty reply.
var ErrTooSmall = errors.New("admin: the reply budget cannot hold an empty reply")

// ListReply is one node's answer to a list request.
//
// A node's answer crosses the fabric as one NATS message, which the server
// refuses past its max_payload — 1 MB unless configured. A window of a
// thousand clients that each hold a few dozen filters passes that, and a
// reply that is refused is indistinguishable from a node that is not there:
// its clients would be missing from the page, and the cursor built from the
// other sources would skip them on every later page. So a node fills its
// reply up to the budget and says where it stopped, and [Merge] cuts the
// page there.
type ListReply struct {
	Node     string    `json:"node"`
	Sessions []Session `json:"sessions"`
	// Truncated says the node stopped short of its window: it holds more
	// clients after the last one in Sessions than it sent.
	Truncated bool `json:"truncated,omitempty"`
}

// Failure is what a node sends back instead of an answer it could not send,
// so the asking node reports an error rather than a smaller cluster.
type Failure struct {
	Node  string `json:"node"`
	Error string `json:"error"`
}

// FailureOf reports whether a reply is a [Failure], and its message.
//
// No answer has an "error" field, so its presence is the whole test.
func FailureOf(data []byte) (Failure, bool) {
	var f Failure
	if err := json.Unmarshal(data, &f); err != nil || f.Error == "" {
		return Failure{Node: "", Error: ""}, false
	}

	return f, true
}

// EncodeList encodes a node's window as a [ListReply] of at most budget
// bytes.
//
// Sessions are taken in order until the next would not fit; the reply is
// then marked truncated. The first session always goes in, if need be with
// fewer of its filters (see [EncodeSession]), so a reply always makes
// progress: a node whose every page was cut before its first client would
// stall the cursor for good.
func EncodeList(node string, window []Session, budget int) ([]byte, error) {
	nodeJSON, err := json.Marshal(node)
	if err != nil {
		return nil, fmt.Errorf("admin: encoding the node id: %w", err)
	}

	const (
		head      = `{"node":`
		sessions  = `,"sessions":[`
		truncated = `],"truncated":true}`
		tail      = `]}`
	)

	// The fixed part of a reply, sized for the longer of the two endings,
	// so whichever ending is written still fits.
	fixed := len(head) + len(nodeJSON) + len(sessions) + len(truncated)
	if fixed > budget {
		return nil, ErrTooSmall
	}

	var buf bytes.Buffer

	buf.WriteString(head)
	buf.Write(nodeJSON)
	buf.WriteString(sessions)

	cut, err := appendWindow(&buf, window, budget-fixed)
	if err != nil {
		return nil, err
	}

	if cut {
		buf.WriteString(truncated)
	} else {
		buf.WriteString(tail)
	}

	return buf.Bytes(), nil
}

// appendWindow writes as many of the window's sessions as room allows,
// separated by commas, and reports whether it stopped short.
func appendWindow(buf *bytes.Buffer, window []Session, room int) (bool, error) {
	for i, s := range window {
		sep := min(i, 1)

		encoded, err := json.Marshal(s)
		if err != nil {
			return false, fmt.Errorf("admin: encoding session %q: %w", s.ClientID, err)
		}

		if len(encoded)+sep > room {
			if i > 0 {
				return true, nil
			}

			encoded, err = EncodeSession(s, room)
			if err != nil {
				return false, err
			}
		}

		if i > 0 {
			buf.WriteByte(',')
		}

		buf.Write(encoded)
		room -= len(encoded) + sep
	}

	return false, nil
}

// EncodeSession encodes one session in at most budget bytes, dropping its
// last filters when all of them do not fit, and marking it when it does.
//
// One client can hold enough filters to pass max_payload alone. A reply
// that is refused would make the API report a connected client as unknown,
// which is worse than reporting it with the first of its filters and saying
// so; the stats count every filter regardless.
func EncodeSession(s Session, budget int) ([]byte, error) {
	encoded, err := json.Marshal(s)
	if err != nil {
		return nil, fmt.Errorf("admin: encoding session %q: %w", s.ClientID, err)
	}

	if len(encoded) <= budget {
		return encoded, nil
	}

	trimmed := s
	trimmed.SubscriptionsTruncated = true
	trimmed.Subscriptions = []Subscription{}

	bare, err := json.Marshal(trimmed)
	if err != nil {
		return nil, fmt.Errorf("admin: encoding session %q: %w", s.ClientID, err)
	}

	if len(bare) > budget {
		return nil, fmt.Errorf("%w: session %q needs %d bytes with no filters, budget %d",
			ErrTooSmall, s.ClientID, len(bare), budget)
	}

	keep, err := filtersThatFit(s.Subscriptions, budget-len(bare))
	if err != nil {
		return nil, fmt.Errorf("admin: encoding a filter of %q: %w", s.ClientID, err)
	}

	if keep > 0 {
		trimmed.Subscriptions = s.Subscriptions[:keep]
	}

	out, err := json.Marshal(trimmed)
	if err != nil {
		return nil, fmt.Errorf("admin: encoding session %q: %w", s.ClientID, err)
	}

	return out, nil
}

// filtersThatFit is how many of the first filters fit in room bytes, each
// costing its own encoding and, after the first, a comma.
func filtersThatFit(subs []Subscription, room int) (int, error) {
	for i, sub := range subs {
		one, err := json.Marshal(sub)
		if err != nil {
			return 0, err //nolint:wrapcheck // the caller names the session
		}

		need := len(one) + min(i, 1)
		if need > room {
			return i, nil
		}

		room -= need
	}

	return len(subs), nil
}
