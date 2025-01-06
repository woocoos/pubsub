package rocketmqv4

import (
	"github.com/tsingsun/woocoo/pkg/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var logger = log.Component("pubsub")

// 替换rocketmq的logger
type apacheLogger struct {
	level zapcore.Level
}

func (al apacheLogger) Debug(msg string, fields map[string]interface{}) {
	if al.level.Enabled(zapcore.DebugLevel) {
		logger.Debug(msg, zap.Any("fields", fields))
	}
}

func (al apacheLogger) Info(msg string, fields map[string]interface{}) {
	if al.level.Enabled(zapcore.InfoLevel) {
		logger.Info(msg, zap.Any("fields", fields))
	}
}

func (al apacheLogger) Warning(msg string, fields map[string]interface{}) {
	if al.level.Enabled(zapcore.WarnLevel) {
		logger.Warn(msg, zap.Any("fields", fields))
	}
}

func (al apacheLogger) Error(msg string, fields map[string]interface{}) {
	if al.level.Enabled(zapcore.ErrorLevel) {
		logger.Error(msg, zap.Any("fields", fields))
	}
}

func (al apacheLogger) Fatal(msg string, fields map[string]interface{}) {
	if al.level.Enabled(zapcore.FatalLevel) {
		logger.Fatal(msg, zap.Any("fields", fields))
	}
}

func (al apacheLogger) Level(level string) {
	// TODO implement me
	panic("implement me")
}

func (al apacheLogger) OutputPath(path string) (err error) {
	// TODO implement me
	panic("implement me")
}
