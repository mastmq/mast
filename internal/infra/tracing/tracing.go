// Package tracing builds the OpenTelemetry tracer provider a node exports
// its message-path spans through.
//
// It owns only construction and shutdown. What gets traced, and how a trace
// crosses the fabric, is the bridge's business; the bridge sees nothing but
// a [trace.TracerProvider], which keeps the SDK and the exporter out of every
// package that does not build one.
//
// # Why OTLP over HTTP and nothing else
//
// Every collector accepts OTLP over HTTP on 4318, and one exporter means one
// set of OTEL_EXPORTER_OTLP_* variables that does what it says, rather than
// a protocol switch whose other half is never tested. It does not keep gRPC
// out of the binary: the exporter's shared config and the OTLP collector
// types import it, though nothing here dials it. The whole exporter costs
// about 4.8 MB of a 35 MB linux build, and the alternative — encoding the
// request by hand — means owning a copy of the SDK's span conversion and its
// reading of the OTEL_EXPORTER_OTLP_* variables. docs/ARCHITECTURE.md has the
// longer version.
package tracing

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/carlmjohnson/versioninfo"
	"github.com/mastmq/mast/internal/infra/config"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	"go.opentelemetry.io/otel/trace"
)

// serviceName is what a span says it came from unless OTEL_SERVICE_NAME or
// OTEL_RESOURCE_ATTRIBUTES says otherwise.
const serviceName = "mast"

// Provider is a running tracer provider and the exporter behind it.
type Provider struct {
	tp *sdktrace.TracerProvider
}

// Start builds a provider from cfg, whether or not cfg.Enabled is set: the
// caller decides that, because only it knows whether there is anything on
// the node to trace. attrs describe the node, such as its role, and are
// added to every span's resource.
//
// Nothing is dialled here. The exporter connects on its first batch, so a
// collector that is down at startup costs dropped spans, never a broker
// that will not start.
func Start(ctx context.Context, cfg config.Tracing, log *slog.Logger, attrs ...attribute.KeyValue) (*Provider, error) {
	var opts []otlptracehttp.Option
	if cfg.Endpoint != "" {
		opts = append(opts, otlptracehttp.WithEndpointURL(cfg.Endpoint))
	}

	exporter, err := otlptracehttp.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("tracing: building the otlp exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithTelemetrySDK(),
		resource.WithAttributes(append([]attribute.KeyValue{
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion(versioninfo.Short()),
		}, attrs...)...),
		// Last, so the environment overrides the defaults above. That is
		// the order every other OpenTelemetry SDK applies them in.
		resource.WithFromEnv(),
	)
	if err != nil {
		return nil, fmt.Errorf("tracing: describing the node: %w", err)
	}

	providerOpts := []sdktrace.TracerProviderOption{
		// Batched: a span is handed to a bounded queue and dropped when it
		// is full, so a slow or absent collector can never push back onto
		// the publish path. OTEL_BSP_* tune it.
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	}

	// With no sampler of our own the SDK reads OTEL_TRACES_SAMPLER and
	// OTEL_TRACES_SAMPLER_ARG itself, which is the point of leaving it empty.
	if sampler, ok := Sampler(cfg); ok {
		providerOpts = append(providerOpts, sdktrace.WithSampler(sampler))
	}

	// The SDK reports export failures through a process-wide handler, which
	// otherwise writes to stderr in its own format. Routed through slog
	// they land in the same place as everything else.
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		log.Warn("exporting traces", "error", err)
	}))

	return &Provider{tp: sdktrace.NewTracerProvider(providerOpts...)}, nil
}

// TracerProvider is what the bridge creates its tracer from.
//
//nolint:ireturn // the interface is the point: the bridge must not see the SDK
func (p *Provider) TracerProvider() trace.TracerProvider { return p.tp }

// Shutdown flushes the spans still queued and stops the exporter.
func (p *Provider) Shutdown(ctx context.Context) error {
	if err := p.tp.Shutdown(ctx); err != nil {
		return fmt.Errorf("tracing: shutting down: %w", err)
	}

	return nil
}

// Sampler builds the sampler cfg names. ok is false when cfg names none, in
// which case the SDK's own reading of OTEL_TRACES_SAMPLER applies.
//
// The names and their meaning are OTEL_TRACES_SAMPLER's, so one vocabulary
// serves both the file and the environment. config.Validate has already
// refused anything else, and a ratio it could not parse.
//
//nolint:ireturn // sdktrace.Sampler is an interface by design
func Sampler(cfg config.Tracing) (sdktrace.Sampler, bool) {
	ratio := 1.0

	if cfg.SamplerArg != "" {
		if parsed, err := strconv.ParseFloat(cfg.SamplerArg, 64); err == nil {
			ratio = parsed
		}
	}

	switch cfg.Sampler {
	case "always_on":
		return sdktrace.AlwaysSample(), true
	case "always_off":
		return sdktrace.NeverSample(), true
	case "traceidratio":
		return sdktrace.TraceIDRatioBased(ratio), true
	case "parentbased_always_on":
		return sdktrace.ParentBased(sdktrace.AlwaysSample()), true
	case "parentbased_always_off":
		return sdktrace.ParentBased(sdktrace.NeverSample()), true
	case "parentbased_traceidratio":
		return sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio)), true
	default:
		return nil, false
	}
}
