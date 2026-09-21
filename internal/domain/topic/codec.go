// Package topic converts MQTT topic names and filters into NATS subjects and
// back again.
//
// The conversion is total and reversible: every byte sequence that is a legal
// MQTT topic name round-trips through [EncodeTopic] and [DecodeTopic]
// unchanged. This is the property that makes the mapping safe to bake into
// stored subjects, authorization rules, and retained-message keys, and it is
// the main reason mast does not reuse the mapping nats-server applies to its
// own MQTT listener — that one is lossy for topics containing '.' and for
// leading '/'.
//
// A subject has the shape
//
//	t.<tenant>.<level>.<level>...
//
// The leading "t" token keeps MQTT traffic in a namespace of its own so a
// tenant's native NATS subjects cannot collide with its MQTT topics. Every
// MQTT level is percent-escaped so that the characters NATS reserves ('.',
// '*', '>' and whitespace) can never appear in a token, and an empty level is
// written as a bare "%" because NATS has no empty token.
package topic

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

// Prefix is the first token of every subject mast derives from an MQTT topic.
const Prefix = "t"

// MaxTopicLen is the maximum length in bytes of an MQTT topic name or filter,
// matching the MQTT 5.0 limit on the UTF-8 string that carries it.
const MaxTopicLen = 65535

// emptyLevel is the encoded form of an empty MQTT topic level. A non-empty
// level can never encode to a bare "%" because a literal '%' escapes to "%25",
// so the sentinel is unambiguous.
const emptyLevel = "%"

const hexDigits = "0123456789ABCDEF"

// headerTokens is the number of subject tokens that precede the topic levels:
// the prefix and the tenant. Each also contributes one '.' separator.
const headerTokens = 2

// hexLetterOffset converts 'A'..'F' and 'a'..'f' to their numeric value.
const hexLetterOffset = 10

// Errors reported by this package. They are joined with the offending value
// through %w so callers can both match on the sentinel and log the input.
var (
	ErrEmptyTopic    = errors.New("topic: empty topic")
	ErrTopicTooLong  = errors.New("topic: topic exceeds 65535 bytes")
	ErrInvalidUTF8   = errors.New("topic: topic is not valid UTF-8")
	ErrNullChar      = errors.New("topic: topic contains a null character")
	ErrWildcard      = errors.New("topic: wildcard in a topic name")
	ErrBadWildcard   = errors.New("topic: wildcard must occupy a whole level")
	ErrTrailingHash  = errors.New("topic: '#' must be the last level")
	ErrInvalidTenant = errors.New("topic: tenant must match [A-Za-z0-9_-]+")
	ErrBadSubject    = errors.New("topic: not a mast subject")
	ErrBadEscape     = errors.New("topic: malformed percent escape")
)

// EncodeTopic converts an MQTT topic name into the NATS subject mast publishes
// it on. Topic names may not contain wildcards; use [EncodeFilter] for the
// subscribe path.
func EncodeTopic(tenant, topic string) (string, error) {
	if err := checkTenant(tenant); err != nil {
		return "", err
	}

	if err := checkTopic(topic); err != nil {
		return "", err
	}

	if strings.ContainsAny(topic, "+#") {
		return "", fmt.Errorf("%w: %q", ErrWildcard, topic)
	}

	var sb strings.Builder

	sb.Grow(len(Prefix) + len(tenant) + len(topic) + headerTokens)
	sb.WriteString(Prefix)
	sb.WriteByte('.')
	sb.WriteString(tenant)

	for level := range strings.SplitSeq(topic, "/") {
		sb.WriteByte('.')
		encodeLevel(level, &sb)
	}

	return sb.String(), nil
}

// EncodeFilter converts an MQTT topic filter into a NATS subject filter,
// translating '+' to '*' and a trailing '#' to '>'.
//
// It does not account for '#' also matching its own parent level; callers on
// the subscribe path want [FilterSubjects] instead.
func EncodeFilter(tenant, filter string) (string, error) {
	if err := checkTenant(tenant); err != nil {
		return "", err
	}

	if err := checkTopic(filter); err != nil {
		return "", err
	}

	var sb strings.Builder

	sb.Grow(len(Prefix) + len(tenant) + len(filter) + headerTokens)
	sb.WriteString(Prefix)
	sb.WriteByte('.')
	sb.WriteString(tenant)

	levels := strings.Split(filter, "/")
	for i, level := range levels {
		sb.WriteByte('.')

		switch level {
		case "+":
			sb.WriteByte('*')
		case "#":
			if i != len(levels)-1 {
				return "", fmt.Errorf("%w: %q", ErrTrailingHash, filter)
			}

			sb.WriteByte('>')
		default:
			if strings.ContainsAny(level, "+#") {
				return "", fmt.Errorf("%w: %q", ErrBadWildcard, filter)
			}

			encodeLevel(level, &sb)
		}
	}

	return sb.String(), nil
}

// FilterSubjects returns the NATS subjects a broker must subscribe to in order
// to receive exactly the messages an MQTT filter selects.
//
// MQTT's '#' matches the parent level as well as its descendants, so "foo/#"
// matches "foo" itself. NATS's '>' does not: "foo.>" never matches "foo". A
// filter ending in '#' therefore needs two subscriptions, and this is the
// function that knows it.
func FilterSubjects(tenant, filter string) ([]string, error) {
	subject, err := EncodeFilter(tenant, filter)
	if err != nil {
		return nil, err
	}

	if !strings.HasSuffix(subject, ".>") {
		return []string{subject}, nil
	}

	parent := strings.TrimSuffix(subject, ".>")

	// A filter of "#" alone selects the whole tenant tree; its parent is the
	// tenant token itself, which is not a topic and must not be subscribed to.
	if parent == Prefix+"."+tenant {
		return []string{subject}, nil
	}

	return []string{subject, parent}, nil
}

// DecodeTopic reverses [EncodeTopic], recovering the tenant and the original
// MQTT topic name from a subject.
func DecodeTopic(subject string) (string, string, error) {
	tokens := strings.Split(subject, ".")
	if len(tokens) < headerTokens+1 || tokens[0] != Prefix {
		return "", "", fmt.Errorf("%w: %q", ErrBadSubject, subject)
	}

	tenant := tokens[1]
	if err := checkTenant(tenant); err != nil {
		return "", "", err
	}

	levels := make([]string, 0, len(tokens)-headerTokens)

	for _, token := range tokens[headerTokens:] {
		level, err := decodeLevel(token)
		if err != nil {
			return "", "", fmt.Errorf("%w: in %q", err, subject)
		}

		levels = append(levels, level)
	}

	return tenant, strings.Join(levels, "/"), nil
}

// encodeLevel appends one escaped MQTT level to sb.
func encodeLevel(level string, sb *strings.Builder) {
	if level == "" {
		sb.WriteString(emptyLevel)

		return
	}

	for i := range len(level) {
		b := level[i]
		if !mustEscape(b) {
			sb.WriteByte(b)

			continue
		}

		sb.WriteByte('%')
		sb.WriteByte(hexDigits[b>>4])
		sb.WriteByte(hexDigits[b&0x0F])
	}
}

// decodeLevel reverses encodeLevel for a single subject token.
func decodeLevel(token string) (string, error) {
	if token == emptyLevel {
		return "", nil
	}

	if !strings.Contains(token, "%") {
		return token, nil
	}

	var sb strings.Builder

	sb.Grow(len(token))

	for i := 0; i < len(token); {
		if token[i] != '%' {
			sb.WriteByte(token[i])
			i++

			continue
		}

		if i+2 >= len(token) {
			return "", ErrBadEscape
		}

		hi, hiOK := unhex(token[i+1])

		lo, loOK := unhex(token[i+2])
		if !hiOK || !loOK {
			return "", ErrBadEscape
		}

		sb.WriteByte(hi<<4 | lo)

		i += 3
	}

	return sb.String(), nil
}

// mustEscape reports whether b has to be percent-escaped to sit inside a NATS
// subject token.
//
// Three groups are escaped. NATS reserves '.', '*' and '>', and '%' introduces
// an escape sequence. Everything below 0x21 covers whitespace and control
// bytes, which NATS rejects outright. Everything from 0x7F up covers DEL and
// all non-ASCII, which is escaped not because NATS forbids it but because
// keeping subjects pure ASCII is what makes them safe to paste into a metric
// label, a KV bucket key, a log line, or a filesystem path without a second
// encoding step. A topic of "دما/۱" costs more bytes this way; correctness at
// every downstream boundary is worth more than the bytes.
func mustEscape(b byte) bool {
	switch b {
	case '.', '*', '>', '%':
		return true
	}

	return b < 0x21 || b >= 0x7F
}

func unhex(b byte) (byte, bool) {
	switch {
	case b >= '0' && b <= '9':
		return b - '0', true
	case b >= 'A' && b <= 'F':
		return b - 'A' + hexLetterOffset, true
	case b >= 'a' && b <= 'f':
		return b - 'a' + hexLetterOffset, true
	}

	return 0, false
}

// checkTenant rejects tenant identifiers that would not survive as a single
// NATS token. The character set is deliberately narrower than NATS allows so
// that tenant ids stay safe in metric labels, file paths, and KV bucket keys.
func checkTenant(tenant string) error {
	if tenant == "" {
		return fmt.Errorf("%w: empty", ErrInvalidTenant)
	}

	for i := range len(tenant) {
		c := tenant[i]

		switch {
		case c >= 'a' && c <= 'z',
			c >= 'A' && c <= 'Z',
			c >= '0' && c <= '9',
			c == '_', c == '-':
		default:
			return fmt.Errorf("%w: %q", ErrInvalidTenant, tenant)
		}
	}

	return nil
}

// checkTopic applies the validity rules MQTT 5.0 puts on a topic name or
// filter, independent of wildcards.
func checkTopic(topic string) error {
	switch {
	case topic == "":
		return ErrEmptyTopic
	case len(topic) > MaxTopicLen:
		return fmt.Errorf("%w: %d bytes", ErrTopicTooLong, len(topic))
	case !utf8.ValidString(topic):
		return ErrInvalidUTF8
	case strings.IndexByte(topic, 0) >= 0:
		return ErrNullChar
	}

	return nil
}
