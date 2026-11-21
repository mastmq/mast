package topic_test

import (
	"testing"

	"github.com/mastmq/mast/internal/domain/topic"
)

func TestMatches(t *testing.T) {
	t.Parallel()

	cases := []struct {
		filter, name string
		want         bool
	}{
		{"a/b", "a/b", true},
		{"a/b", "a/c", false},
		{"a/b", "a/b/c", false},
		{"a/b/c", "a/b", false},
		{"a/+", "a/b", true},
		{"a/+", "a/", true}, // an empty level is still a level
		{"a/+", "a", false},
		{"a/+", "a/b/c", false},
		{"+/+", "a/b", true},
		{"+", "/", false},
		{"+/+", "/", true},
		{"a/#", "a", true}, // '#' includes its parent [MQTT-4.7.1-2]
		{"a/#", "a/b/c", true},
		{"a/#", "ab", false},
		{"+/#", "a", false}, // mochi's quirk, kept so every path agrees with it
		{"+/#", "a/b", true},
		{"#", "a/b", true},
		{"/#", "/a", true},
		{"/#", "a", false},
		// [MQTT-4.7.2-1]: no first-level wildcard selects a "$" topic.
		{"#", "$SYS/x", false},
		{"+/b", "$a/b", false},
		{"+/+", "$TopicA/B", false},
		{"$a/+", "$a/b", true},
		{"$a/#", "$a/b", true},
		{"a/+", "a/$b", true}, // only the first level is special
	}

	for _, c := range cases {
		if got := topic.Matches(c.filter, c.name); got != c.want {
			t.Errorf("Matches(%q, %q) = %v, want %v", c.filter, c.name, got, c.want)
		}
	}
}
