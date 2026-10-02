package logging

import (
	"bytes"
	"log/slog"
	"strings"
	"sync"
	"testing"
)

func TestParseLevel(t *testing.T) {
	cases := map[string]slog.Level{
		"":        slog.LevelInfo,
		"info":    slog.LevelInfo,
		"INFO":    slog.LevelInfo,
		"debug":   slog.LevelDebug,
		"warn":    slog.LevelWarn,
		"warning": slog.LevelWarn,
		"error":   slog.LevelError,
		"bogus":   slog.LevelInfo,
	}
	for in, want := range cases {
		if got := ParseLevel(in); got != want {
			t.Errorf("ParseLevel(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestSetLevel_FiltersBelowMinimum(t *testing.T) {
	defer SetLevel(slog.LevelInfo)

	var buf bytes.Buffer
	SetOutput(&buf)
	SetLevel(slog.LevelWarn)

	Default.Debug("should not appear")
	Default.Info("should not appear either")
	Default.Warn("should appear", "k", "v")

	out := buf.String()
	if strings.Contains(out, "should not appear") {
		t.Fatalf("filtered levels leaked into output: %s", out)
	}
	if !strings.Contains(out, "should appear") || !strings.Contains(out, "k=v") {
		t.Fatalf("expected record missing or malformed: %s", out)
	}
}

func TestWith_AttachesFields(t *testing.T) {
	var buf bytes.Buffer
	SetOutput(&buf)
	SetLevel(slog.LevelInfo)

	logger := With("node_id", "node-1", "component", "raft")
	logger.Info(EventRaftLeaderChanged, "term", uint64(7))

	out := buf.String()
	for _, want := range []string{"node_id=node-1", "component=raft", "term=7", EventRaftLeaderChanged} {
		if !strings.Contains(out, want) {
			t.Fatalf("output missing %q: %s", want, out)
		}
	}
}

func TestConcurrentLogging_DoesNotPanicOrRace(t *testing.T) {
	var buf bytes.Buffer
	SetOutput(&buf)
	SetLevel(slog.LevelDebug)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			Default.Info(EventStoragePut, "n", n)
		}(i)
	}
	wg.Wait()
}
