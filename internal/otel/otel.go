package otel

import (
	"context"
	"errors"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/dtcenter/METjson2db/pkg/telemetry"
)

// defaultServiceName is used only when OTEL_SERVICE_NAME isn't set — resource.Default() already
// reads that env var, so setting this unconditionally would silently override it on every merge.
const defaultServiceName = "metjson2db-sqsworker"

// serviceNameKey is the stable "service.name" resource attribute key from the OTel resource
// semantic conventions. Using the literal key instead of a pinned semconv package (e.g.
// semconv.ServiceName from go.opentelemetry.io/otel/semconv/vX.Y.Z) avoids having to track which
// schema version resource.Default() uses internally — see the NewSchemaless comment below.
const serviceNameKey = attribute.Key("service.name")

func InitOTel(ctx context.Context) (shutdown func(context.Context) error, err error) {
	var shutdownFuncs []func(context.Context) error

	shutdown = func(ctx context.Context) error {
		var errs []error
		for _, fn := range shutdownFuncs {
			errs = append(errs, fn(ctx))
		}
		return errors.Join(errs...)
	}

	handleErr := func(inErr error) {
		err = errors.Join(inErr, shutdown(ctx))
	}

	// resource.Default() always carries a service.name — if the operator hasn't configured one,
	// its own built-in fallback detector sets "unknown_service:<executable>" before the env
	// detector even runs, so checking defaultRes here is never unset and our default would never
	// apply. resource.Environment() runs only the env detector (OTEL_SERVICE_NAME or
	// OTEL_RESOURCE_ATTRIBUTES), with no fallback baked in, so it's what actually tells us whether
	// the operator configured one.
	defaultRes := resource.Default()
	var resourceAttrs []attribute.KeyValue
	if _, ok := resource.Environment().Set().Value(serviceNameKey); !ok {
		resourceAttrs = append(resourceAttrs, serviceNameKey.String(defaultServiceName))
	}
	// NewSchemaless (no schema URL) instead of NewWithAttributes(semconv.SchemaURL, ...): Merge
	// errors if both sides carry a non-empty, differing schema URL, and resource.Default()'s
	// schema URL tracks whatever semconv version the SDK itself was built against — pinning our
	// own semconv import here would silently break on the next unrelated SDK upgrade (as it did
	// before this fix). A schemaless resource can never conflict; Merge just adopts the other
	// side's schema URL.
	res, err := resource.Merge(
		defaultRes,
		resource.NewSchemaless(resourceAttrs...),
	)
	if err != nil {
		return shutdown, err
	}

	// Traces
	if signalEnabled("OTEL_TRACES_EXPORTER") {
		traceExporter, err := otlptracegrpc.New(ctx)
		if err != nil {
			handleErr(err)
			return shutdown, err
		}
		tp := sdktrace.NewTracerProvider(
			sdktrace.WithBatcher(traceExporter),
			sdktrace.WithResource(res),
		)
		shutdownFuncs = append(shutdownFuncs, tp.Shutdown)
		otel.SetTracerProvider(tp)
	}
	// otel.Tracer reads the global provider set above, or the API package's built-in no-op
	// provider if traces are disabled — so callers never need to check whether tracing is on.
	telemetry.Tracer = otel.Tracer("metjson2db")

	// Metrics
	if signalEnabled("OTEL_METRICS_EXPORTER") {
		metricExporter, err := otlpmetricgrpc.New(ctx)
		if err != nil {
			handleErr(err)
			return shutdown, err
		}
		mp := sdkmetric.NewMeterProvider(
			sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter, sdkmetric.WithInterval(30*time.Second))),
			sdkmetric.WithResource(res),
		)
		shutdownFuncs = append(shutdownFuncs, mp.Shutdown)
		otel.SetMeterProvider(mp)

		if err := runtime.Start(runtime.WithMinimumReadMemStatsInterval(15 * time.Second)); err != nil {
			handleErr(err)
			return shutdown, err
		}
	}
	telemetry.InitMetrics(otel.Meter("metjson2db"))

	// Logs
	if signalEnabled("OTEL_LOGS_EXPORTER") {
		logExporter, err := otlploggrpc.New(ctx)
		if err != nil {
			handleErr(err)
			return shutdown, err
		}
		lp := sdklog.NewLoggerProvider(
			sdklog.WithProcessor(sdklog.NewBatchProcessor(logExporter)),
			sdklog.WithResource(res),
		)
		shutdownFuncs = append(shutdownFuncs, lp.Shutdown)
		SetLoggerProvider(lp)
	}

	return shutdown, nil
}

// signalEnabled reports whether the given OTEL_*_EXPORTER env var permits creating that signal's
// exporter — the OTel spec's convention is that "none" disables it and any other value (including
// unset, which defaults to "otlp") leaves it enabled. This is the only exporter selection value
// this package implements.
func signalEnabled(envVar string) bool {
	return !strings.EqualFold(os.Getenv(envVar), "none")
}
