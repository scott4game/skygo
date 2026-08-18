// Package skylog is the logging seam for the framework's transport and actor
// packages. It defines a minimal interface and a log/slog-backed default so
// those packages carry no dependency on any particular logging library.
//
// Embedders replace the default with SetDefault, forwarding to whatever logger
// the host application already uses.
package skylog

import (
	"context"
	"log/slog"
	"sync/atomic"
)

// Logger is the four-level, context-carrying sink used by framework packages.
// The context is passed through untouched so an adapter can recover values the
// host attached to it, such as a trace ID.
type Logger interface {
	Debugf(ctx context.Context, format string, args ...any)
	Infof(ctx context.Context, format string, args ...any)
	Warnf(ctx context.Context, format string, args ...any)
	Errorf(ctx context.Context, format string, args ...any)
}

var current atomic.Pointer[Logger]

// SetDefault installs the process-wide logger. Passing nil restores the
// log/slog-backed default. It is safe to call concurrently with logging.
func SetDefault(l Logger) {
	if l == nil {
		current.Store(nil)
		return
	}
	current.Store(&l)
}

// Default returns the logger currently installed.
func Default() Logger {
	if p := current.Load(); p != nil {
		return *p
	}
	return slogLogger{}
}

// Debugf writes a formatted debug-level record to the default logger.
func Debugf(ctx context.Context, format string, args ...any) {
	Default().Debugf(ctx, format, args...)
}

// Infof writes a formatted info-level record to the default logger.
func Infof(ctx context.Context, format string, args ...any) {
	Default().Infof(ctx, format, args...)
}

// Warnf writes a formatted warning-level record to the default logger.
func Warnf(ctx context.Context, format string, args ...any) {
	Default().Warnf(ctx, format, args...)
}

// Errorf writes a formatted error-level record to the default logger.
func Errorf(ctx context.Context, format string, args ...any) {
	Default().Errorf(ctx, format, args...)
}

// slogLogger is the zero-configuration default. It formats eagerly only when
// the level is enabled, so disabled levels stay cheap.
type slogLogger struct{}

func (slogLogger) log(ctx context.Context, level slog.Level, format string, args []any) {
	logger := slog.Default()
	if !logger.Enabled(ctx, level) {
		return
	}
	logger.Log(ctx, level, sprintf(format, args))
}

func (l slogLogger) Debugf(ctx context.Context, format string, args ...any) {
	l.log(ctx, slog.LevelDebug, format, args)
}

func (l slogLogger) Infof(ctx context.Context, format string, args ...any) {
	l.log(ctx, slog.LevelInfo, format, args)
}

func (l slogLogger) Warnf(ctx context.Context, format string, args ...any) {
	l.log(ctx, slog.LevelWarn, format, args)
}

func (l slogLogger) Errorf(ctx context.Context, format string, args ...any) {
	l.log(ctx, slog.LevelError, format, args)
}

// FuncLogger adapts four plain functions to Logger. A nil field discards that
// level, which makes it convenient for tests that only assert on one level.
type FuncLogger struct {
	DebugFunc func(ctx context.Context, format string, args ...any)
	InfoFunc  func(ctx context.Context, format string, args ...any)
	WarnFunc  func(ctx context.Context, format string, args ...any)
	ErrorFunc func(ctx context.Context, format string, args ...any)
}

func (f FuncLogger) Debugf(ctx context.Context, format string, args ...any) {
	if f.DebugFunc != nil {
		f.DebugFunc(ctx, format, args...)
	}
}

func (f FuncLogger) Infof(ctx context.Context, format string, args ...any) {
	if f.InfoFunc != nil {
		f.InfoFunc(ctx, format, args...)
	}
}

func (f FuncLogger) Warnf(ctx context.Context, format string, args ...any) {
	if f.WarnFunc != nil {
		f.WarnFunc(ctx, format, args...)
	}
}

func (f FuncLogger) Errorf(ctx context.Context, format string, args ...any) {
	if f.ErrorFunc != nil {
		f.ErrorFunc(ctx, format, args...)
	}
}
