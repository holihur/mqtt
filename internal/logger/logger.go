package logger

import (
	"log/slog"
	"os"
	"strings"
)

var levelMap = map[string]slog.Level{
	"debug": slog.LevelDebug,
	"info":  slog.LevelInfo,
	"warn":  slog.LevelWarn,
	"error": slog.LevelError,
}

func Init(levelStr string) {
	lvl := slog.LevelInfo
	if v, ok := levelMap[strings.ToLower(levelStr)]; ok {
		lvl = v
	}
	h := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level:     lvl,
		AddSource: lvl == slog.LevelDebug,
	})
	slog.SetDefault(slog.New(h))
}

func LevelStrings() []string { return []string{"debug", "info", "warn", "error"} }
