package pubsub

import (
	"context"
	"github.com/pkg/errors"
)

// RecoveryMiddleware is middleware for recovering from panics
type RecoveryMiddleware struct {
	RecoveryHandlerFunc RecoveryHandlerFunc
}

// RecoveryHandlerFunc is a function that recovers from the panic `p` by returning an `error`.
type RecoveryHandlerFunc func(p interface{}) (err error)

// SubscribeInterceptor returns a subscriber middleware with added logging via Zap
func (o RecoveryMiddleware) SubscribeInterceptor(next MessageHandler) MessageHandler {
	return func(ctx context.Context, m Message) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = recoverFrom(r, "pubsub: subscriber panic \n", o.RecoveryHandlerFunc)
			}
		}()
		err = next(ctx, m)
		return
	}
}

// PublishInterceptor adds recovery to the publisher
func (o RecoveryMiddleware) PublishInterceptor(ctx context.Context, next PublishHandler) PublishHandler {
	return func(ctx context.Context, m *Message) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = recoverFrom(r, "pubsub: publisher panic \n", o.RecoveryHandlerFunc)
			}
		}()
		err = next(ctx, m)
		return
	}
}

func recoverFrom(p interface{}, wrap string, r RecoveryHandlerFunc) error {
	if r == nil {
		var e error
		switch val := p.(type) {
		case string:
			e = errors.New(val)
		case error:
			e = val
		default:
			e = errors.New("unknown error occurred")
		}
		return errors.Wrap(e, wrap)
	}
	return r(p)
}
