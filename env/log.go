package env

import (
	"strconv"
	"strings"
	"time"
)

// Level controls structured log verbosity.
type Level uint8

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
)

// String returns the lowercase name of l.
func (l Level) String() string {
	switch l {
	case LevelDebug:
		return "debug"
	case LevelInfo:
		return "info"
	case LevelWarn:
		return "warn"
	case LevelError:
		return "error"
	default:
		return "unknown"
	}
}

// Attr is a deterministic, typed key-value pair for structured logging.
type Attr struct {
	Key   string
	Value string
}

// Str constructs a string log attribute.
func Str(key, val string) Attr { return Attr{Key: key, Value: val} }

// Int constructs an int log attribute.
func Int(key string, val int) Attr { return Attr{Key: key, Value: strconv.Itoa(val)} }

// Int64 constructs an int64 log attribute.
func Int64(key string, val int64) Attr { return Attr{Key: key, Value: strconv.FormatInt(val, 10)} }

// Bool constructs a bool log attribute.
func Bool(key string, val bool) Attr { return Attr{Key: key, Value: strconv.FormatBool(val)} }

// Dur constructs a duration log attribute formatted in integer nanoseconds.
func Dur(key string, val time.Duration) Attr {
	return Attr{Key: key, Value: strconv.FormatInt(int64(val), 10) + "ns"}
}

// Err constructs an error log attribute ("<nil>" when err is nil).
func Err(err error) Attr {
	if err == nil {
		return Attr{Key: "err", Value: "<nil>"}
	}
	return Attr{Key: "err", Value: err.Error()}
}

// Logger is a narrow, deterministic structured logger interface usable inside
// both the simulator (stamped with virtual time) and production (backed by slog).
type Logger interface {
	Log(level Level, msg string, attrs ...Attr)
	Debug(msg string, attrs ...Attr)
	Info(msg string, attrs ...Attr)
	Warn(msg string, attrs ...Attr)
	Error(msg string, attrs ...Attr)
}

// NopLogger discards all log records with zero allocations.
type NopLogger struct{}

func (NopLogger) Log(Level, string, ...Attr) {}
func (NopLogger) Debug(string, ...Attr)      {}
func (NopLogger) Info(string, ...Attr)       {}
func (NopLogger) Warn(string, ...Attr)       {}
func (NopLogger) Error(string, ...Attr)      {}

// FormatLogAttrs renders msg and attrs as a single deterministic key=value
// string suitable for simulator traces and structured log lines.
func FormatLogAttrs(msg string, attrs ...Attr) string {
	var b strings.Builder
	b.WriteString("msg=")
	writeLogVal(&b, msg)
	for _, a := range attrs {
		if a.Key == "" {
			continue
		}
		b.WriteByte(' ')
		b.WriteString(a.Key)
		b.WriteByte('=')
		writeLogVal(&b, a.Value)
	}
	return b.String()
}

func writeLogVal(b *strings.Builder, s string) {
	if s == "" || strings.ContainsAny(s, " \t\r\n=\"") {
		b.WriteString(strconv.Quote(s))
		return
	}
	b.WriteString(s)
}
