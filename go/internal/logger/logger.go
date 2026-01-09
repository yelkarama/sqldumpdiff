package logger

import (
	"fmt"
	"io"
	"log"
	"os"
	"time"
)

// Logger provides structured logging with debug levels
type Logger struct {
	debugEnabled bool
	infoLog      *log.Logger
	debugLog     *log.Logger
	errorLog     *log.Logger
}

var defaultLogger *Logger

// Init initializes the default logger
func Init(debugEnabled bool) {
	defaultLogger = New(os.Stderr, debugEnabled)
}

// New creates a new logger
func New(w io.Writer, debugEnabled bool) *Logger {
	flags := log.Ldate | log.Ltime | log.Lshortfile
	return &Logger{
		debugEnabled: debugEnabled,
		infoLog:      log.New(w, "[INFO] ", flags),
		debugLog:     log.New(w, "[DEBUG] ", flags),
		errorLog:     log.New(w, "[ERROR] ", flags),
	}
}

// Info logs an info message
func Info(msg string, args ...interface{}) {
	if defaultLogger != nil {
		defaultLogger.Info(msg, args...)
	}
}

// Info logs an info message
func (l *Logger) Info(msg string, args ...interface{}) {
	l.infoLog.Printf(msg, args...)
}

// Debug logs a debug message (only shown if debug enabled)
func Debug(msg string, args ...interface{}) {
	if defaultLogger != nil {
		defaultLogger.Debug(msg, args...)
	}
}

// Debug logs a debug message (only shown if debug enabled)
func (l *Logger) Debug(msg string, args ...interface{}) {
	if l.debugEnabled {
		l.debugLog.Printf(msg, args...)
	}
}

// Error logs an error message
func Error(msg string, args ...interface{}) {
	if defaultLogger != nil {
		defaultLogger.Error(msg, args...)
	}
}

// Error logs an error message
func (l *Logger) Error(msg string, args ...interface{}) {
	l.errorLog.Printf(msg, args...)
}

// Timing logs a timed operation
func Timing(name string, start time.Time) {
	if defaultLogger != nil {
		defaultLogger.Timing(name, start)
	}
}

// Timing logs a timed operation
func (l *Logger) Timing(name string, start time.Time) {
	elapsed := time.Since(start)
	l.debugLog.Printf("%s took %v", name, elapsed)
}

// IsDebugEnabled returns whether debug logging is enabled
func IsDebugEnabled() bool {
	if defaultLogger != nil {
		return defaultLogger.debugEnabled
	}
	return false
}

// Printf is a convenience function for formatted output
func Printf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format, args...)
}
