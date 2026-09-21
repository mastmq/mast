package topic_test

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/mastmq/mast/internal/domain/topic"
)

func TestEncodeTopic(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		topic string
		want  string
	}{
		{"plain", "a/b/c", "t.acme.a.b.c"},
		{"single level", "status", "t.acme.status"},

		// The two cases nats-server's own MQTT mapping gets wrong: it turns a
		// '.' into "//" and gives a leading '/' an empty token.
		{"dot in level", "sensor/temp.1", "t.acme.sensor.temp%2E1"},
		{"leading slash", "/foo/bar", "t.acme.%.foo.bar"},

		{"trailing slash", "foo/", "t.acme.foo.%"},
		{"empty middle level", "a//b", "t.acme.a.%.b"},
		{"only slash", "/", "t.acme.%.%"},
		{"space", "a b/c", "t.acme.a%20b.c"},
		{"percent", "100%/x", "t.acme.100%25.x"},

		// NATS wildcards appearing literally in an MQTT topic name.
		{"literal star", "a/*/b", "t.acme.a.%2A.b"},
		{"literal gt", "a/>/b", "t.acme.a.%3E.b"},

		// '$' is printable ASCII and not reserved by NATS, so $SYS survives.
		{"dollar", "$SYS/broker/uptime", "t.acme.$SYS.broker.uptime"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := topic.EncodeTopic("acme", tc.topic)
			if err != nil {
				t.Fatalf("EncodeTopic(%q) error: %v", tc.topic, err)
			}

			if got != tc.want {
				t.Errorf("EncodeTopic(%q) = %q, want %q", tc.topic, got, tc.want)
			}

			_, back, err := topic.DecodeTopic(got)
			if err != nil {
				t.Fatalf("DecodeTopic(%q) error: %v", got, err)
			}

			if back != tc.topic {
				t.Errorf("round trip %q -> %q -> %q", tc.topic, got, back)
			}
		})
	}
}

func TestEncodeTopicKeepsSubjectsASCII(t *testing.T) {
	t.Parallel()

	// Non-ASCII topics are common in the field; the subject must stay ASCII so
	// it is safe as a metric label, a KV key, or a path.
	for _, in := range []string{"دما/۱", "温度/传感器", "температура"} {
		subject, err := topic.EncodeTopic("acme", in)
		if err != nil {
			t.Fatalf("EncodeTopic(%q) error: %v", in, err)
		}

		for i := range len(subject) {
			if subject[i] >= 0x7F || subject[i] < 0x21 {
				t.Fatalf("EncodeTopic(%q) = %q: non-ASCII byte %#x at %d", in, subject, subject[i], i)
			}
		}

		_, back, err := topic.DecodeTopic(subject)
		if err != nil {
			t.Fatalf("DecodeTopic(%q) error: %v", subject, err)
		}

		if back != in {
			t.Errorf("round trip %q -> %q -> %q", in, subject, back)
		}
	}
}

func TestEncodeFilter(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		filter string
		want   string
	}{
		{"single level wildcard", "a/+/c", "t.acme.a.*.c"},
		{"leading plus", "+/b", "t.acme.*.b"},
		{"trailing hash", "a/#", "t.acme.a.>"},
		{"hash alone", "#", "t.acme.>"},
		{"plus then hash", "a/+/#", "t.acme.a.*.>"},
		{"no wildcard", "a/b", "t.acme.a.b"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := topic.EncodeFilter("acme", tc.filter)
			if err != nil {
				t.Fatalf("EncodeFilter(%q) error: %v", tc.filter, err)
			}

			if got != tc.want {
				t.Errorf("EncodeFilter(%q) = %q, want %q", tc.filter, got, tc.want)
			}
		})
	}
}

func TestFilterSubjects(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		filter string
		want   []string
	}{
		// MQTT's "a/#" matches "a" itself; NATS's "a.>" does not, so the
		// parent needs its own subscription.
		{"hash adds parent", "a/#", []string{"t.acme.a.>", "t.acme.a"}},
		{"deep hash adds parent", "a/b/#", []string{"t.acme.a.b.>", "t.acme.a.b"}},

		// "#" alone would make the parent the tenant token, which is not a
		// topic and must not be subscribed to.
		{"bare hash has no parent", "#", []string{"t.acme.>"}},

		{"plus is single", "a/+", []string{"t.acme.a.*"}},
		{"literal is single", "a/b", []string{"t.acme.a.b"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := topic.FilterSubjects("acme", tc.filter)
			if err != nil {
				t.Fatalf("FilterSubjects(%q) error: %v", tc.filter, err)
			}

			if !slices.Equal(got, tc.want) {
				t.Errorf("FilterSubjects(%q) = %v, want %v", tc.filter, got, tc.want)
			}
		})
	}
}

func TestEncodeErrors(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		tenant string
		topic  string
		want   error
	}{
		{"empty topic", "acme", "", topic.ErrEmptyTopic},
		{"plus in name", "acme", "a/+/c", topic.ErrWildcard},
		{"hash in name", "acme", "a/#", topic.ErrWildcard},
		{"null byte", "acme", "a\x00b", topic.ErrNullChar},
		{"empty tenant", "", "a", topic.ErrInvalidTenant},
		{"dot in tenant", "ac.me", "a", topic.ErrInvalidTenant},
		{"slash in tenant", "ac/me", "a", topic.ErrInvalidTenant},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if _, err := topic.EncodeTopic(tc.tenant, tc.topic); !errors.Is(err, tc.want) {
				t.Errorf("EncodeTopic(%q, %q) error = %v, want %v", tc.tenant, tc.topic, err, tc.want)
			}
		})
	}
}

func TestEncodeFilterErrors(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		filter string
		want   error
	}{
		{"hash not last", "a/#/b", topic.ErrTrailingHash},
		{"plus inside level", "a+/b", topic.ErrBadWildcard},
		{"hash inside level", "a/b#", topic.ErrBadWildcard},
		{"empty filter", "", topic.ErrEmptyTopic},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if _, err := topic.EncodeFilter("acme", tc.filter); !errors.Is(err, tc.want) {
				t.Errorf("EncodeFilter(%q) error = %v, want %v", tc.filter, err, tc.want)
			}
		})
	}
}

func TestDecodeErrors(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		subject string
		want    error
	}{
		{"wrong prefix", "x.acme.a", topic.ErrBadSubject},
		{"no levels", "t.acme", topic.ErrBadSubject},
		{"truncated escape", "t.acme.%2", topic.ErrBadEscape},
		{"non hex escape", "t.acme.%ZZ", topic.ErrBadEscape},
		{"bad tenant", "t.ac me.a", topic.ErrInvalidTenant},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if _, _, err := topic.DecodeTopic(tc.subject); !errors.Is(err, tc.want) {
				t.Errorf("DecodeTopic(%q) error = %v, want %v", tc.subject, err, tc.want)
			}
		})
	}
}

// FuzzRoundTrip is the test that matters: the codec is baked into stored
// subjects and authorization rules, so any topic it accepts must come back
// byte-identical. A failure here is a data-loss bug, not a cosmetic one.
func FuzzRoundTrip(f *testing.F) {
	seeds := []string{
		"a/b/c",
		"/",
		"//",
		"a//b",
		"sensor/temp.1",
		"/leading",
		"trailing/",
		"a b/c",
		"100%/x",
		"a/*/b",
		"a/>/b",
		"$SYS/x",
		"دما/۱",
		"温度",
		"%",
		"%25",
		"\x01\x02",
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, in string) {
		subject, err := topic.EncodeTopic("acme", in)
		if err != nil {
			return // not a legal MQTT topic name; nothing to round-trip
		}

		if strings.Count(subject, ".")+1 != strings.Count(in, "/")+3 {
			t.Fatalf("EncodeTopic(%q) = %q: token count does not match level count", in, subject)
		}

		tenant, back, err := topic.DecodeTopic(subject)
		if err != nil {
			t.Fatalf("DecodeTopic(%q) from %q: %v", subject, in, err)
		}

		if tenant != "acme" {
			t.Errorf("DecodeTopic(%q) tenant = %q, want acme", subject, tenant)
		}

		if back != in {
			t.Errorf("round trip %q -> %q -> %q", in, subject, back)
		}
	})
}
