// Package logging is ForgeDB's Phase 12 structured logging layer: a thin
// configuration wrapper around the standard library's log/slog, plus a
// catalog of event names (events.go) every other package logs under.
//
// It intentionally does not reinvent what slog already provides --
// levels, structured key/value fields, and concurrency-safe output are
// all slog's job. What this package adds is ForgeDB-specific
// conventions: parsing the project's own LogLevel configuration value
// (see internal/config.Config.LogLevel) into a slog.Level, and a single
// process-wide Default logger every package logs through, mirroring the
// global-registry pattern internal/metrics already uses -- neither
// package makes a caller thread a logger/registry through every
// constructor just to record one event.
package logging

import (
	"io"
	"log/slog"
	"os"
	"strings"
)

// level is the dynamic level Default's handler reads on every log call;
// slog.LevelVar is safe for concurrent use, so SetLevel never races a
// concurrent log call. It defaults to Info, the sensible level for
// normal operation.
var level slog.LevelVar

// Default is the process-wide logger every ForgeDB package logs through.
// SetLevel and SetOutput reconfigure it in place (by replacing the
// handler it wraps), so packages that captured Default before
// configuration still log through the same, now-reconfigured logger.
var Default = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: &level}))

// ParseLevel converts a case-insensitive level name ("debug", "info",
// "warn"/"warning", "error") into a slog.Level, defaulting to Info for
// an empty or unrecognized value -- the sensible default for normal
// operation (see docs/observability/phase12-observability.md). This is
// what internal/config.Config.LogLevel is parsed through.
func ParseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// SetLevel sets Default's minimum log level. Records below this level
// are filtered before any formatting work happens, so lowering
// verbosity is also the cheapest way to reduce logging overhead on a hot
// path.
func SetLevel(l slog.Level) { level.Set(l) }

// SetOutput replaces Default with a new logger writing to w at the
// currently configured level, preserving the same dynamic level variable
// (a later SetLevel call still affects it). It exists mainly for tests
// that need to capture log output into a buffer.
func SetOutput(w io.Writer) {
	Default = slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: &level}))
}

// With returns a logger that annotates every record with the given
// key/value pairs in addition to Default's own -- e.g.
// logging.With("node_id", id) once at node startup, so every subsequent
// call through the returned logger carries node_id without repeating it.
func With(args ...any) *slog.Logger {
	return Default.With(args...)
}
