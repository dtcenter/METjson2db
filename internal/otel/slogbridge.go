package otel

import (
	"context"
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
	return &fanoutHandler{handlers: []slog.Handler{jsonHandler, otelHandler}}
}

type fanoutHandler struct {
	handlers []slog.Handler
	attrs    []slog.Attr
	groups   []string
}

func (h *fanoutHandler) Enabled(ctx context.Context, level slog.Level) bool {
	for _, handler := range h.handlers {
		if handler.Enabled(ctx, level) {
			return true
		}
	}
	return false
}

func (h *fanoutHandler) Handle(ctx context.Context, r slog.Record) error {
	for _, handler := range h.handlers {
		if handler.Enabled(ctx, r.Level) {
			_ = handler.Handle(ctx, r.Clone())
		}
	}
	return nil
}

func (h *fanoutHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	handlers := make([]slog.Handler, len(h.handlers))
	for i, handler := range h.handlers {
		handlers[i] = handler.WithAttrs(attrs)
	}
	return &fanoutHandler{handlers: handlers, attrs: append(h.attrs, attrs...), groups: h.groups}
}

func (h *fanoutHandler) WithGroup(name string) slog.Handler {
	handlers := make([]slog.Handler, len(h.handlers))
	for i, handler := range h.handlers {
		handlers[i] = handler.WithGroup(name)
	}
	return &fanoutHandler{handlers: handlers, attrs: h.attrs, groups: append(h.groups, name)}
}
