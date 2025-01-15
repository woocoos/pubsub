package pubsub

import (
	"context"
	"fmt"
	"github.com/tsingsun/woocoo/pkg/log"
	"go.uber.org/zap"
)

var logger log.ComponentLogger

// SetLogger sets the logger for the package.
func SetLogger(l log.ComponentLogger) {
	logger = l
}

// Logger returns the logger for the package.
func Logger() log.ComponentLogger {
	if logger == nil {
		logger = log.Global()
	}
	return logger
}

type LoggerMiddleware struct {
	Logger log.ComponentLogger
}

func (o LoggerMiddleware) SubscribeInterceptor(opts *HandlerOptions, next MessageHandler) MessageHandler {
	if o.Logger == nil {
		o.Logger = Logger()
	}
	return func(ctx context.Context, m *Message) error {
		err := next(ctx, m)
		if err != nil {
			o.Logger.Error("failed processing subscriber", zap.String("serviceName", opts.ServiceName),
				zap.Error(err), zap.Any("msg", m))
		} else {
			o.Logger.Debug(fmt.Sprintf("success processing subscriber, serviceName:%s,msg:%s", opts.ServiceName, m.ID))
		}
		return nil
	}
}

func (o LoggerMiddleware) PublishInterceptor(ctx context.Context, serviceName string, next PublishHandler) PublishHandler {
	if o.Logger == nil {
		o.Logger = Logger()
	}
	return func(ctx context.Context, m *Message) error {
		err := next(ctx, m)
		if err != nil {
			o.Logger.Ctx(ctx).Error("failed processing publish", zap.String("serviceName", serviceName),
				zap.Error(err), zap.Any("msg", m))
		} else {
			o.Logger.Ctx(ctx).Debug(fmt.Sprintf("success processing publisher, serviceName:%s,msg:%s", serviceName, m.ID))
		}
		return next(ctx, m)
	}
}
