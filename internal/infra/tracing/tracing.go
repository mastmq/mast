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
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/url"
	"os"
	"strconv"
	"strings"

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

// tracesPath is where an OTLP/HTTP collector takes spans.
const tracesPath = "/v1/traces"

// TracesURL is the URL spans are posted to for a configured endpoint.
//
// tracing.endpoint is documented, here and in the chart and the Ansible
// role, as the collector's address — "http://otel-collector:4318" — which
// is what OTEL_EXPORTER_OTLP_ENDPOINT holds and what collectors print. The
// exporter's WithEndpointURL takes its URL as the whole traces URL, path
// included, so that address posted to "/" and every collector answered
// 404: tracing turned on as documented exported nothing, and said so only
// in a warning per batch. An endpoint with no path gets the standard one,
// as the SDK gives OTEL_EXPORTER_OTLP_ENDPOINT; one with a path is a full
// URL, as OTEL_EXPORTER_OTLP_TRACES_ENDPOINT is, and is used as it is. A
// string that does not parse is passed on for the exporter to refuse.
func TracesURL(endpoint string) string {
	u, err := url.Parse(endpoint)
	if err != nil || (u.Path != "" && u.Path != "/") {
		return endpoint
	}

	u.Path = tracesPath

	return u.String()
}

// Provider is a running tracer provider and the exporter behind it.
type Provider struct {
	tp      *sdktrace.TracerProvider
	sampler string
}

// Sampler describes the sampler in use, for the startup log: with the keys
// empty it came from the environment, which the config does not show.
func (p *Provider) Sampler() string { return p.sampler }

// Start builds a provider from cfg, whether or not cfg.Enabled is set: the
// caller decides that, because only it knows whether there is anything on
// the node to trace. attrs describe the node, such as its role, and are
// added to every span's resource.
//
// Nothing is dialled here. The exporter connects on its first batch, so a
// collector that is down at startup costs dropped spans, never a broker
// that will not start.
func Start(ctx context.Context, cfg config.Tracing, log *slog.Logger, attrs ...attribute.KeyValue) (*Provider, error) {
	// Always one of ours, never the SDK's reading of the environment:
	// Sampler reads the same variables, key by key, and wraps the result so
	// that a client cannot choose to be sampled.
	sampler, err := Sampler(cfg, os.Getenv)
	if err != nil {
		return nil, err
	}

	var opts []otlptracehttp.Option
	if cfg.Endpoint != "" {
		opts = append(opts, otlptracehttp.WithEndpointURL(TracesURL(cfg.Endpoint)))
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
		sdktrace.WithSampler(sampler),
	}

	// The SDK reports export failures through a process-wide handler, which
	// otherwise writes to stderr in its own format. Routed through slog
	// they land in the same place as everything else.
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		log.Warn("exporting traces", "error", err)
	}))

	return &Provider{tp: sdktrace.NewTracerProvider(providerOpts...), sampler: sampler.Description()}, nil
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

// The standard variables a sampler key left empty defers to.
const (
	envSampler    = "OTEL_TRACES_SAMPLER"
	envSamplerArg = "OTEL_TRACES_SAMPLER_ARG"
)

// defaultSampler is the SDK's own default, kept so that a node with nothing
// configured samples exactly as it would have had the SDK chosen.
const defaultSampler = "parentbased_always_on"

// ErrSamplerEnv is returned for an OTEL_TRACES_SAMPLER or
// OTEL_TRACES_SAMPLER_ARG that config.Validate never saw. The SDK would log
// it and sample everything; a typo in a ratio meant to be 0.01 should not
// be found by the collector falling over.
var ErrSamplerEnv = errors.New("tracing: invalid sampler in the environment")

// Sampler builds the sampler for cfg. A key cfg leaves empty is read from
// its standard variable through getenv, each key on its own: a sampler
// named in the file takes its ratio from OTEL_TRACES_SAMPLER_ARG, and a
// ratio in the file applies to a sampler named by OTEL_TRACES_SAMPLER.
//
// It used to hand the environment to the SDK whenever the sampler key was
// empty, and the SDK reads the two variables only as a pair: a sampler from
// the file ignored the ratio in the environment, and a ratio from the file
// was ignored under a sampler from the environment. Both sampled 100%.
//
// The names and their meaning are OTEL_TRACES_SAMPLER's, so one vocabulary
// serves both the file and the environment. config.Validate has already
// refused a bad value in the file; a bad one in the environment is refused
// here.
//
// Whatever is configured is wrapped by [ingressSampler], so that no client
// decides what is sampled.
//
//nolint:ireturn // sdktrace.Sampler is an interface by design
func Sampler(cfg config.Tracing, getenv func(string) string) (sdktrace.Sampler, error) {
	name, nameFrom := cfg.Sampler, "tracing.sampler"
	if name == "" {
		name, nameFrom = getenv(envSampler), envSampler
	}

	if name == "" {
		name = defaultSampler
	}

	ratio, err := samplerRatio(cfg, getenv)
	if err != nil {
		return nil, err
	}

	var root sdktrace.Sampler

	switch name {
	case "always_on", "parentbased_always_on":
		root = sdktrace.AlwaysSample()
	case "always_off", "parentbased_always_off":
		root = sdktrace.NeverSample()
	case "traceidratio", "parentbased_traceidratio":
		root = sdktrace.TraceIDRatioBased(ratio)
	default:
		return nil, fmt.Errorf("%w: %s = %q is not one of %v", ErrSamplerEnv, nameFrom, name, config.Samplers())
	}

	configured := root
	if strings.HasPrefix(name, "parentbased_") {
		configured = sdktrace.ParentBased(root)
	}

	return ingressSampler{configured: configured, root: root}, nil
}

// samplerRatio is the ratio cfg or else OTEL_TRACES_SAMPLER_ARG names, and 1
// when neither does.
func samplerRatio(cfg config.Tracing, getenv func(string) string) (float64, error) {
	arg, argFrom := cfg.SamplerArg, "tracing.sampler_arg"
	if arg == "" {
		arg, argFrom = getenv(envSamplerArg), envSamplerArg
	}

	if arg == "" {
		return 1, nil
	}

	ratio, err := strconv.ParseFloat(arg, 64)
	if err != nil || ratio < 0 || ratio > 1 {
		return 0, fmt.Errorf("%w: %s = %q is not a ratio between 0 and 1", ErrSamplerEnv, argFrom, arg)
	}

	return ratio, nil
}

// ingressSampler keeps the sampling decision on the broker.
//
// A span of kind server with a remote parent is a client's PUBLISH that
// named a trace in its "traceparent" user property: the bridge opens no
// other server span. A parent-based sampler would let that client's sampled
// flag decide, and the decision then rides the fabric to every node that
// delivers the message. So any MQTT 5 client could have every one of its
// messages traced on every node — the +97 allocations of a traced publish,
// times every edge, and a full span queue that drops other tenants' spans —
// or hide its traffic from tracing with a flag of 00. For those spans the
// configured root sampler decides instead, as if the span had no parent.
//
// It decides on a trace id of its own rather than the client's: a ratio
// sampler compares the trace id with its ratio, and a client picks its
// trace id as freely as its flag. The span itself keeps the client's trace
// id, so the trace still joins up.
//
// Every other span goes to the configured sampler. A delivery span's remote
// parent is the publish span, whose context the ingress wrote onto the
// fabric message, and following it is what keeps a trace whole across
// nodes.
type ingressSampler struct {
	configured sdktrace.Sampler
	root       sdktrace.Sampler
}

// ShouldSample implements [sdktrace.Sampler].
func (s ingressSampler) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	if p.Kind == trace.SpanKindServer {
		if psc := trace.SpanContextFromContext(p.ParentContext); psc.IsValid() && psc.IsRemote() {
			p.TraceID = randomTraceID()

			return s.root.ShouldSample(p)
		}
	}

	return s.configured.ShouldSample(p)
}

// Description implements [sdktrace.Sampler].
func (s ingressSampler) Description() string {
	return "MQTTIngress{client:" + s.root.Description() + ",otherwise:" + s.configured.Description() + "}"
}

// randomTraceID is the id a client's publish is sampled on. It never names
// a span, so it needs to be uniform, not unguessable.
func randomTraceID() trace.TraceID {
	var id trace.TraceID

	binary.BigEndian.PutUint64(id[:8], rand.Uint64()) //nolint:gosec // a sampling roll, not a secret
	binary.BigEndian.PutUint64(id[8:], rand.Uint64()) //nolint:gosec // a sampling roll, not a secret

	return id
}
