package aliyunmq

import (
	"github.com/tsingsun/woocoo/pkg/conf"
	"github.com/tsingsun/woocoo/pkg/log"
	"go.uber.org/zap"
)

var logger *logWrapper

type logWrapper struct {
	logger *log.Logger
}

func newWrapperLogger(cfg *conf.Configuration) *logWrapper {
	l := log.New(nil)
	l.Apply(cfg)
	return &logWrapper{
		logger: l,
	}
}

func (al logWrapper) Debug(msg string, fields ...zap.Field) {
	al.logger.Debug(msg, fields...)
}

func (al logWrapper) Info(msg string, fields ...zap.Field) {
	al.logger.Info(msg, fields...)
}

func (al logWrapper) Warn(msg string, fields ...zap.Field) {
	al.logger.Warn(msg, fields...)
}

func (al logWrapper) Error(msg string, fields ...zap.Field) {
	al.logger.Error(msg, fields...)
}

func (al logWrapper) Fatal(msg string, fields ...zap.Field) {
	al.logger.Fatal(msg, fields...)
}
