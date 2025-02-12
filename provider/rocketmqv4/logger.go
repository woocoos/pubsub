package rocketmqv4

import (
	"github.com/tsingsun/woocoo/pkg/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var logger = log.Component("pubsub")

// 替换rocketmq的logger
type apacheLogger struct {
	level  zapcore.Level
	logger log.ComponentLogger
}

func (al apacheLogger) Debug(msg string, fields map[string]any) {
	if al.level.Enabled(zapcore.DebugLevel) {
		al.logger.Debug(msg, zap.Any("fields", fields))
	}
}

func (al apacheLogger) Info(msg string, fields map[string]any) {
	if al.level.Enabled(zapcore.InfoLevel) {
		al.logger.Info(msg, zap.Any("fields", fields))
	}
}

func (al apacheLogger) Warning(msg string, fields map[string]any) {
	if al.level.Enabled(zapcore.WarnLevel) {
		al.logger.Warn(msg, zap.Any("fields", fields))
	}
}

func (al apacheLogger) Error(msg string, fields map[string]any) {
	if al.level.Enabled(zapcore.ErrorLevel) {
		al.logger.Error(msg, zap.Any("fields", fields))
	}
}

func (al apacheLogger) Fatal(msg string, fields map[string]any) {
	if al.level.Enabled(zapcore.FatalLevel) {
		al.logger.Fatal(msg, zap.Any("fields", fields))
	}
}

func (al apacheLogger) Level(level string) {
	ll, err := zapcore.ParseLevel(level)
	if err != nil {
		panic(err)
	}
	al.level = ll
}

func (al apacheLogger) OutputPath(path string) (err error) {
	// TODO implement me
	panic("implement me")
}
