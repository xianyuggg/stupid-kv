/*
Copyright (c) 2020, pigeonligh.
*/

package log

import (
	"io"
	"log"
	"os"
)

// Level is log level type
type Level = uint

type modeType = string

const (
	modeDebug   modeType = "debug"
	modeRelease modeType = "release"
)

var (
	loggers *Logger

	logLevel Level = 1 // block the logs

	mode modeType = modeDebug
)

func init() {
	loggers = &Logger{
		debugLogger:   log.New(os.Stderr, "\x1b[32m [DEBUG] \x1b[0m", log.LstdFlags|log.Lmicroseconds),
		infoLogger:    log.New(os.Stdout, "\x1b[34m [INFO] \x1b[0m", log.LstdFlags|log.Lmicroseconds),
		warningLogger: log.New(os.Stdout, "\x1b[33m [WARNING] \x1b[0m", log.LstdFlags|log.Lmicroseconds),
		errorLogger:   log.New(os.Stderr, "\x1b[31m [ERROR] \x1b[0m", log.LstdFlags|log.Lmicroseconds),
	}
}

// SetDebugMode sets debug mode
func SetDebugMode(debug bool) {
	if debug {
		mode = modeDebug
	} else {
		mode = modeRelease
	}
}

// SetOutput sets log's output
func SetOutput(debugOutput, infoOutput, warningOutput, errorOutput io.Writer) {
	loggers.debugLogger.SetOutput(debugOutput)
	loggers.infoLogger.SetOutput(infoOutput)
	loggers.warningLogger.SetOutput(warningOutput)
	loggers.errorLogger.SetOutput(errorOutput)
}

// SetLevel sets log's level
func SetLevel(level Level) {
	logLevel = level
}

// V gets logger by level
func V(level Level) *Logger {
	return get(level, 2)
}

func get(level Level, depth int) *Logger {
	if level&logLevel == 0 {
		return &Logger{}
	}
	return &Logger{
		debugLogger:   loggers.debugLogger,
		infoLogger:    loggers.infoLogger,
		warningLogger: loggers.warningLogger,
		errorLogger:   loggers.errorLogger,

		depth: depth,
	}
}

// Debug logs important message
func Debug(v ...interface{}) {
	get(logLevel, 3).Debug(v...)
}

// Debugf logs important message
func Debugf(format string, v ...interface{}) {
	get(logLevel, 3).Debugf(format, v...)
}

// Info logs important message
func Info(v ...interface{}) {
	get(logLevel, 3).Info(v...)
}

// Infof logs important message
func Infof(format string, v ...interface{}) {
	get(logLevel, 3).Infof(format, v...)
}

// Warning logs warning message
func Warning(v ...interface{}) {
	get(logLevel, 3).Warning(v...)
}

// Warningf logs important message
func Warningf(format string, v ...interface{}) {
	get(logLevel, 3).Warningf(format, v...)
}

// Error logs error message
func Error(v ...interface{}) {
	get(logLevel, 3).Error(v...)
	os.Exit(1)
}

// Errorf logs important message
func Errorf(format string, v ...interface{}) {
	get(logLevel, 3).Errorf(format, v...)
	os.Exit(1)
}
