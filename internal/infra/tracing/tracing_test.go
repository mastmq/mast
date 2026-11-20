package tracing_test

import (
	"log/slog"
	"testing"

	"github.com/mastmq/mast/internal/infra/config"
	"github.com/mastmq/mast/internal/infra/tracing"
)

// TestSamplerSpeaksOTELTracesSampler holds the config to the vocabulary of
// OTEL_TRACES_SAMPLER, so a value means the same in either place.
func TestSamplerSpeaksOTELTracesSampler(t *testing.T) {
	t.Parallel()

	cases := []struct {
		sampler, arg, want string
	}{
		{"always_on", "", "AlwaysOnSampler"},
		{"always_off", "", "AlwaysOffSampler"},
		{"traceidratio", "0.25", "TraceIDRatioBased{0.25}"},
		{"traceidratio", "", "TraceIDRatioBased{1}"},
		{
			"parentbased_traceidratio", "0.01",
			"ParentBased{root:TraceIDRatioBased{0.01},remoteParentSampled:AlwaysOnSampler," +
				"remoteParentNotSampled:AlwaysOffSampler,localParentSampled:AlwaysOnSampler," +
				"localParentNotSampled:AlwaysOffSampler}",
		},
	}

	for _, tc := range cases {
		t.Run(tc.sampler+"/"+tc.arg, func(t *testing.T) {
			t.Parallel()

			s, ok := tracing.Sampler(config.Tracing{Enabled: true, Sampler: tc.sampler, SamplerArg: tc.arg})
			if !ok {
				t.Fatalf("no sampler for %q", tc.sampler)
			}

			if got := s.Description(); got != tc.want {
				t.Errorf("sampler = %s, want %s", got, tc.want)
			}
		})
	}
}

// TestEmptySamplerDefersToTheEnvironment is the other half: with nothing
// configured, no sampler is imposed, and the SDK reads OTEL_TRACES_SAMPLER.
func TestEmptySamplerDefersToTheEnvironment(t *testing.T) {
	t.Parallel()

	if s, ok := tracing.Sampler(config.Default().Tracing); ok {
		t.Errorf("the default config imposes %s over OTEL_TRACES_SAMPLER", s.Description())
	}
}

// TestStartDialsNothing covers a collector that is down when the broker
// starts: building the exporter must not fail, or tracing would turn a
// monitoring outage into a broker outage.
func TestStartDialsNothing(t *testing.T) {
	t.Parallel()

	p, err := tracing.Start(t.Context(), config.Tracing{
		Enabled: true, Endpoint: "http://127.0.0.1:1", Sampler: "", SamplerArg: "",
	}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("Start with an unreachable collector: %v", err)
	}

	// Nothing was traced, so there is nothing to fail to flush.
	t.Cleanup(func() { _ = p.Shutdown(t.Context()) })

	if p.TracerProvider() == nil {
		t.Fatal("no tracer provider")
	}
}
