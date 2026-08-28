package otel

import (
	"context"
	"errors"
	"os"
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
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	"github.com/dtcenter/METjson2db/pkg/telemetry"
)

// defaultServiceName is used only when OTEL_SERVICE_NAME isn't set — resource.Default() already
// reads that env var, so setting this unconditionally would silently override it on every merge.
const defaultServiceName = "metjson2db-sqsworker"

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

	// resource.Default() already honors OTEL_SERVICE_NAME via its env detector; only fall back to
	// our default when the operator hasn't set one, so the env var actually takes effect.
	var resourceAttrs []attribute.KeyValue
	if os.Getenv("OTEL_SERVICE_NAME") == "" {
		resourceAttrs = append(resourceAttrs, semconv.ServiceName(defaultServiceName))
	}
	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(semconv.SchemaURL, resourceAttrs...),
	)
	if err != nil {
		return shutdown, err
	}

	// Traces
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
	telemetry.Tracer = tp.Tracer("metjson2db")

	// Metrics
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
	telemetry.InitMetrics(mp.Meter("metjson2db"))

	if err := runtime.Start(runtime.WithMinimumReadMemStatsInterval(15 * time.Second)); err != nil {
		handleErr(err)
		return shutdown, err
	}

	// Logs
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

	return shutdown, nil
}
