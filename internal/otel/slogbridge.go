package otel

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	sdklog "go.opentelemetry.io/otel/sdk/log"
)

var loggerProvider *sdklog.LoggerProvider

func SetLoggerProvider(lp *sdklog.LoggerProvider) {
	loggerProvider = lp
}

func NewFanoutHandler(level slog.Level) slog.Handler {
	jsonHandler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: true,
		Level:     level,
	})
	if loggerProvider == nil {
		return jsonHandler
	}
	otelHandler := otelslog.NewHandler("metjson2db", otelslog.WithLoggerProvider(loggerProvider))
	return &fanoutHandler{level: level, handlers: []slog.Handler{jsonHandler, otelHandler}}
}

type fanoutHandler struct {
	level    slog.Level
	handlers []slog.Handler
	attrs    []slog.Attr
	groups   []string
}

// Enabled applies the worker's configured level uniformly across every sink. Delegating to each
// sub-handler's own Enabled instead would let an unfiltered one (the OTel bridge has no level of
// its own) bypass the configured level for the whole fanout.
func (h *fanoutHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level
}

func (h *fanoutHandler) Handle(ctx context.Context, r slog.Record) error {
	for _, handler := range h.handlers {
		if err := handler.Handle(ctx, r.Clone()); err != nil {
			// A sink failing (e.g. the OTLP log exporter can't reach the collector) shouldn't
			// break logging as a whole, but it shouldn't vanish silently either.
			fmt.Fprintf(os.Stderr, "slog fanout: handler failed: %v\n", err)
		}
	}
	return nil
}

func (h *fanoutHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	handlers := make([]slog.Handler, len(h.handlers))
	for i, handler := range h.handlers {
		handlers[i] = handler.WithAttrs(attrs)
	}
	return &fanoutHandler{level: h.level, handlers: handlers, attrs: append(h.attrs, attrs...), groups: h.groups}
}

func (h *fanoutHandler) WithGroup(name string) slog.Handler {
	handlers := make([]slog.Handler, len(h.handlers))
	for i, handler := range h.handlers {
		handlers[i] = handler.WithGroup(name)
	}
	return &fanoutHandler{level: h.level, handlers: handlers, attrs: h.attrs, groups: append(h.groups, name)}
}
