package rocketmqv4

import (
	"github.com/tsingsun/woocoo/pkg/conf"
	"github.com/tsingsun/woocoo/pkg/log"
	"go.uber.org/zap"
)

var logger *apacheLogger

// 替换rocketmq的logger
type apacheLogger struct {
	logger *log.Logger
}

func newApacheLogger(cfg *conf.Configuration) *apacheLogger {
	l := log.New(nil)
	l.Apply(cfg)
	return &apacheLogger{
		logger: l,
	}
}

func (al apacheLogger) Debug(msg string, fields map[string]any) {
	al.logger.Debug(msg, zap.Any("fields", fields))
}

func (al apacheLogger) Info(msg string, fields map[string]any) {
	al.logger.Info(msg, zap.Any("fields", fields))
}

func (al apacheLogger) Warning(msg string, fields map[string]any) {
	al.logger.Warn(msg, zap.Any("fields", fields))
}

func (al apacheLogger) Error(msg string, fields map[string]any) {
	al.logger.Error(msg, zap.Any("fields", fields))
}

func (al apacheLogger) Fatal(msg string, fields map[string]any) {
	al.logger.Fatal(msg, zap.Any("fields", fields))
}

func (al apacheLogger) Level(level string) {
	panic("implement me")
}

func (al apacheLogger) OutputPath(path string) (err error) {
	// TODO implement me
	panic("implement me")
}
