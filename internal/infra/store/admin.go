package store

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/mastmq/mast/internal/domain/topic"
	"github.com/nats-io/nats.go/jetstream"
)

// SessionKey is where a client's durable state lives: its record in the
// sessions bucket, and its queue under "o.<key>" on the offline stream.
//
// Both tokens are escaped, because the codec's character set was chosen so
// that an encoded token is also a valid JetStream key and subject token. A
// client id arrives from the wire; unescaped it could collide with another
// client's key, or carry a "." that splits it into two tokens and moves it
// under another tenant's prefix.
func SessionKey(tenantName, clientID string) string {
	return topic.EncodeToken(tenantName) + "." + topic.EncodeToken(clientID)
}

// TenantSessions lists the client ids that have a stored session in a
// tenant, in no particular order.
//
// Listing keys is an ordered consumer on the sessions stream, created for
// the call and gone after it. That is one per administrative request, not
// one per session or device, and it reads headers only; but it does visit
// every stored session of the tenant, so a tenant with a hundred thousand
// of them costs a hundred thousand small messages per call. It is an
// operator's tool, and is priced like one.
func (s *Store) TenantSessions(ctx context.Context, tenantName string) ([]string, error) {
	prefix := topic.EncodeToken(tenantName) + "."

	lister, err := s.sessions.ListKeysFiltered(ctx, prefix+"*")
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return nil, nil
		}

		return nil, fmt.Errorf("store: listing sessions of %s: %w", tenantName, err)
	}

	// The lister stops its own watcher once it has read everything; this is
	// for a context that ends first, and a second stop has nothing to say.
	defer func() { _ = lister.Stop() }()

	// A key written during the listing can be reported twice.
	seen := make(map[string]struct{})

	var clients []string

	for key := range lister.Keys() {
		encoded, ok := strings.CutPrefix(key, prefix)
		if !ok {
			continue
		}

		clientID, err := topic.DecodeToken(encoded)
		if err != nil {
			// Not a key mast wrote; skipping it costs one row of a report.
			continue
		}

		if _, dup := seen[clientID]; dup {
			continue
		}

		seen[clientID] = struct{}{}
		clients = append(clients, clientID)
	}

	// The keys channel also closes when ctx ends, which is a short list,
	// not a complete one.
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("store: listing sessions of %s: %w", tenantName, err)
	}

	return clients, nil
}

// TenantQueued counts what is queued for each absent session of a tenant,
// by client id.
//
// One stream info request with a subject filter, answered from the stream's
// own per-subject index: no consumer, and no message is read. What an
// older version left in the legacy bucket is not counted.
func (s *Store) TenantQueued(ctx context.Context, tenantName string) (map[string]uint64, error) {
	if s.offline == nil {
		return map[string]uint64{}, nil
	}

	prefix := offlinePrefix + topic.EncodeToken(tenantName) + "."

	info, err := s.offline.Info(ctx, jetstream.WithSubjectFilter(prefix+"*"))
	if err != nil {
		return nil, fmt.Errorf("store: counting queues of %s: %w", tenantName, err)
	}

	counts := make(map[string]uint64, len(info.State.Subjects))

	for subject, n := range info.State.Subjects {
		encoded, ok := strings.CutPrefix(subject, prefix)
		if !ok {
			continue
		}

		clientID, err := topic.DecodeToken(encoded)
		if err != nil {
			continue
		}

		counts[clientID] = n
	}

	return counts, nil
}

// Queued counts what is waiting for one absent session.
func (s *Store) Queued(ctx context.Context, tenantName, clientID string) (uint64, error) {
	if s.offline == nil {
		return 0, nil
	}

	subject := offlinePrefix + SessionKey(tenantName, clientID)

	info, err := s.offline.Info(ctx, jetstream.WithSubjectFilter(subject))
	if err != nil {
		return 0, fmt.Errorf("store: counting the queue of %s: %w", subject, err)
	}

	return info.State.Subjects[subject], nil
}
