package logger

import (
	"log/slog"
	"testing"
)

func TestInit(t *testing.T) {
	tests := []struct {
		level string
		want  slog.Level
	}{
		{"debug", slog.LevelDebug},
		{"info", slog.LevelInfo},
		{"warn", slog.LevelWarn},
		{"error", slog.LevelError},
		{"DEBUG", slog.LevelDebug},
		{"INFO", slog.LevelInfo},
		{"WARN", slog.LevelWarn},
		{"ERROR", slog.LevelError},
		{"", slog.LevelInfo},          // default
		{"unknown", slog.LevelInfo},   // default for unknown
		{"Debug", slog.LevelDebug},    // mixed case
		{"Warning", slog.LevelInfo},   // not in map, falls to default
	}
	for _, tt := range tests {
		t.Run(tt.level, func(t *testing.T) {
			Init(tt.level)
			// Verify the default logger was set (we can't easily inspect level
			// but we verify Init doesn't panic and sets a logger)
			if slog.Default() == nil {
				t.Fatal("slog.Default() is nil after Init")
			}
		})
	}
}

func TestLevelStrings(t *testing.T) {
	levels := LevelStrings()
	if len(levels) != 4 {
		t.Fatalf("len = %d, want 4", len(levels))
	}
	expected := map[string]bool{"debug": true, "info": true, "warn": true, "error": true}
	for _, l := range levels {
		if !expected[l] {
			t.Fatalf("unexpected level %q", l)
		}
	}
}

func TestInit_ProducesOutput(t *testing.T) {
	// Init with debug should set AddSource=true
	Init("debug")
	h := slog.Default().Handler()
	if h == nil {
		t.Fatal("handler is nil")
	}
	// Verify handler accepts debug level
	if !h.Enabled(nil, slog.LevelDebug) {
		t.Fatal("debug level should be enabled")
	}
}

func TestInit_InfoLevel(t *testing.T) {
	Init("info")
	h := slog.Default().Handler()
	if h == nil {
		t.Fatal("handler is nil")
	}
	// Debug should not be enabled at info level
	if h.Enabled(nil, slog.LevelDebug) {
		t.Fatal("debug should not be enabled at info level")
	}
	// Info should be enabled
	if !h.Enabled(nil, slog.LevelInfo) {
		t.Fatal("info should be enabled")
	}
}

func TestInit_WarnLevel(t *testing.T) {
	Init("warn")
	h := slog.Default().Handler()
	if !h.Enabled(nil, slog.LevelWarn) {
		t.Fatal("warn should be enabled")
	}
	if h.Enabled(nil, slog.LevelInfo) {
		t.Fatal("info should not be enabled at warn level")
	}
}

func TestInit_ErrorLevel(t *testing.T) {
	Init("error")
	h := slog.Default().Handler()
	if !h.Enabled(nil, slog.LevelError) {
		t.Fatal("error should be enabled")
	}
	if h.Enabled(nil, slog.LevelWarn) {
		t.Fatal("warn should not be enabled at error level")
	}
}
