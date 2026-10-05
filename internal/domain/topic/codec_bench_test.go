package topic_test

import (
	"testing"

	"github.com/mastmq/mast/internal/domain/topic"
)

// benchTopics are the shapes real fleets send: a plain ASCII topic, which
// is nearly all of them, one that needs escaping, and a deep one.
func benchTopics() []struct{ name, topic string } {
	return []struct{ name, topic string }{
		{"plain", "devices/rover-7/telemetry/gps"},
		{"escaped", "devices/rover 7/temp °C"},
		{"deep", "a/b/c/d/e/f/g/h/i/j/k/l/m/n/o/p"},
	}
}

// The codec runs once per publish on the ingress node and once per delivery
// on the owning node, so it is on the hot path twice.
func BenchmarkEncodeTopic(b *testing.B) {
	for _, tc := range benchTopics() {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()

			for b.Loop() {
				if _, err := topic.EncodeTopic("acme", tc.topic); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkDecodeTopic(b *testing.B) {
	for _, tc := range benchTopics() {
		b.Run(tc.name, func(b *testing.B) {
			subject, err := topic.EncodeTopic("acme", tc.topic)
			if err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()

			for b.Loop() {
				if _, _, err := topic.DecodeTopic(subject); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkFilterSubjects(b *testing.B) {
	b.ReportAllocs()

	for b.Loop() {
		if _, err := topic.FilterSubjects("acme", "devices/+/telemetry/#"); err != nil {
			b.Fatal(err)
		}
	}
}
