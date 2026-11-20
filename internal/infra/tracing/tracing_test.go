package tracing_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/mastmq/mast/internal/infra/config"
	"github.com/mastmq/mast/internal/infra/tracing"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// env is a getenv over a fixed set of variables, so the tests can run in
// parallel where t.Setenv cannot.
func env(vars map[string]string) func(string) string {
	return func(key string) string { return vars[key] }
}

// noEnv is an environment with no OTEL_* variable set.
func noEnv(string) string { return "" }

// wrapped is the description of a configured sampler once ingressSampler
// wraps it.
func wrapped(root, configured string) string {
	return "MQTTIngress{client:" + root + ",otherwise:" + configured + "}"
}

// parentBased is the SDK's description of ParentBased(root) with the
// default options.
func parentBased(root string) string {
	return "ParentBased{root:" + root + ",remoteParentSampled:AlwaysOnSampler," +
		"remoteParentNotSampled:AlwaysOffSampler,localParentSampled:AlwaysOnSampler," +
		"localParentNotSampled:AlwaysOffSampler}"
}

// TestSamplerSpeaksOTELTracesSampler holds the config to the vocabulary of
// OTEL_TRACES_SAMPLER, so a value means the same in either place.
func TestSamplerSpeaksOTELTracesSampler(t *testing.T) {
	t.Parallel()

	cases := []struct {
		sampler, arg, want string
	}{
		{"always_on", "", wrapped("AlwaysOnSampler", "AlwaysOnSampler")},
		{"always_off", "", wrapped("AlwaysOffSampler", "AlwaysOffSampler")},
		{"traceidratio", "0.25", wrapped("TraceIDRatioBased{0.25}", "TraceIDRatioBased{0.25}")},
		{"traceidratio", "", wrapped("TraceIDRatioBased{1}", "TraceIDRatioBased{1}")},
		{"parentbased_traceidratio", "0.01", wrapped("TraceIDRatioBased{0.01}", parentBased("TraceIDRatioBased{0.01}"))},
		{"", "", wrapped("AlwaysOnSampler", parentBased("AlwaysOnSampler"))},
	}

	for _, tc := range cases {
		t.Run(tc.sampler+"/"+tc.arg, func(t *testing.T) {
			t.Parallel()

			s, err := tracing.Sampler(config.Tracing{Enabled: true, Sampler: tc.sampler, SamplerArg: tc.arg}, noEnv)
			if err != nil {
				t.Fatalf("Sampler(%q, %q): %v", tc.sampler, tc.arg, err)
			}

			if got := s.Description(); got != tc.want {
				t.Errorf("sampler = %s, want %s", got, tc.want)
			}
		})
	}
}

// TestEachEmptyKeyDefersToItsVariable covers each key reading its own
// variable. The SDK reads OTEL_TRACES_SAMPLER and OTEL_TRACES_SAMPLER_ARG
// only as a pair, so leaving it to the SDK sampled 100% whenever the file
// set one of the two and the environment the other.
func TestEachEmptyKeyDefersToItsVariable(t *testing.T) {
	t.Parallel()

	ratio := wrapped("TraceIDRatioBased{0.01}", parentBased("TraceIDRatioBased{0.01}"))

	cases := []struct {
		name    string
		cfg     config.Tracing
		vars    map[string]string
		want    string
		wantErr bool
	}{
		{
			name: "sampler in the file, ratio in the environment",
			cfg:  config.Tracing{Sampler: "parentbased_traceidratio"},
			vars: map[string]string{"OTEL_TRACES_SAMPLER_ARG": "0.01"},
			want: ratio,
		},
		{
			name: "ratio in the file, sampler in the environment",
			cfg:  config.Tracing{SamplerArg: "0.01"},
			vars: map[string]string{"OTEL_TRACES_SAMPLER": "parentbased_traceidratio"},
			want: ratio,
		},
		{
			name: "both in the environment",
			vars: map[string]string{"OTEL_TRACES_SAMPLER": "parentbased_traceidratio", "OTEL_TRACES_SAMPLER_ARG": "0.01"},
			want: ratio,
		},
		{
			name: "the file wins",
			cfg:  config.Tracing{Sampler: "parentbased_traceidratio", SamplerArg: "0.01"},
			vars: map[string]string{"OTEL_TRACES_SAMPLER": "always_off", "OTEL_TRACES_SAMPLER_ARG": "0.5"},
			want: ratio,
		},
		{
			name:    "an unknown sampler in the environment",
			vars:    map[string]string{"OTEL_TRACES_SAMPLER": "jaeger_remote"},
			wantErr: true,
		},
		{
			name:    "a ratio in the environment that is not one",
			cfg:     config.Tracing{Sampler: "traceidratio"},
			vars:    map[string]string{"OTEL_TRACES_SAMPLER_ARG": "1%"},
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s, err := tracing.Sampler(tc.cfg, env(tc.vars))
			if tc.wantErr {
				if !errors.Is(err, tracing.ErrSamplerEnv) {
					t.Fatalf("err = %v, want %v", err, tracing.ErrSamplerEnv)
				}

				return
			}

			if err != nil {
				t.Fatalf("Sampler: %v", err)
			}

			if got := s.Description(); got != tc.want {
				t.Errorf("sampler = %s, want %s", got, tc.want)
			}
		})
	}
}

// clientParent is the context a client's "traceparent" user property
// extracts to: remote, with the trace id and the flag the client chose.
func clientParent(t *testing.T, traceID string, sampled bool) context.Context {
	t.Helper()

	tid, err := trace.TraceIDFromHex(traceID)
	if err != nil {
		t.Fatal(err)
	}

	sid, err := trace.SpanIDFromHex("00f067aa0ba902b7")
	if err != nil {
		t.Fatal(err)
	}

	var flags trace.TraceFlags
	if sampled {
		flags = trace.FlagsSampled
	}

	return trace.ContextWithRemoteSpanContext(t.Context(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: tid, SpanID: sid, TraceFlags: flags, TraceState: trace.TraceState{}, Remote: true,
	}))
}

// TestClientDoesNotDecideSampling covers a client's PUBLISH under the
// recommended parentbased_traceidratio: the client's flag, and the trace id
// it chose, must not be what decides. Before, a flag of 01 made every one
// of its messages traced on every node whatever the ratio, and 00 hid them.
func TestClientDoesNotDecideSampling(t *testing.T) {
	t.Parallel()

	// The lowest trace id there is, which any ratio above zero samples
	// when it decides on the trace id.
	const lowTraceID = "00000000000000000000000000000001"

	cases := []struct {
		name    string
		ratio   string
		traceID string
		sampled bool
		want    bool
	}{
		{"ratio 0, flag 01", "0", "4bf92f3577b34da6a3ce929d0e0e4736", true, false},
		{"ratio 1, flag 00", "1", "4bf92f3577b34da6a3ce929d0e0e4736", false, true},
		{"ratio 1e-9, a trace id picked to pass", "0.000000001", lowTraceID, true, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s, err := tracing.Sampler(config.Tracing{Sampler: "parentbased_traceidratio", SamplerArg: tc.ratio}, noEnv)
			if err != nil {
				t.Fatal(err)
			}

			tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(s))

			t.Cleanup(func() { _ = tp.Shutdown(t.Context()) })

			// With the low trace id the decision is a random roll that
			// passes one time in a billion, so a hundred tries tell it
			// from a forced 100% and flake about once in ten million runs.
			tries := 1
			if tc.traceID == lowTraceID {
				tries = 100
			}

			for range tries {
				_, span := tp.Tracer("t").Start(clientParent(t, tc.traceID, tc.sampled), "mqtt.publish",
					trace.WithSpanKind(trace.SpanKindServer))
				span.End()

				if got := span.SpanContext().IsSampled(); got != tc.want {
					t.Fatalf("sampled = %v, want %v", got, tc.want)
				}

				if got := span.SpanContext().TraceID().String(); got != tc.traceID {
					t.Fatalf("trace id = %s, want the client's %s", got, tc.traceID)
				}
			}
		})
	}
}

// TestDeliveryFollowsTheFabric is the other side: a consumer span's remote
// parent is the publish span the ingress put on the fabric message, and its
// decision must be followed, or every delivering node would roll its own
// ratio and a trace would arrive in pieces.
func TestDeliveryFollowsTheFabric(t *testing.T) {
	t.Parallel()

	s, err := tracing.Sampler(config.Tracing{Sampler: "parentbased_traceidratio", SamplerArg: "0"}, noEnv)
	if err != nil {
		t.Fatal(err)
	}

	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(s))

	t.Cleanup(func() { _ = tp.Shutdown(t.Context()) })

	_, span := tp.Tracer("t").Start(clientParent(t, "4bf92f3577b34da6a3ce929d0e0e4736", true), "mqtt.deliver",
		trace.WithSpanKind(trace.SpanKindConsumer))
	span.End()

	if !span.SpanContext().IsSampled() {
		t.Error("a delivery under a sampled publish was not sampled")
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
