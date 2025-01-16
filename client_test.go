package pubsub

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

// MockProvider 模拟 Provider 接口
type MockProvider struct {
}

func (mp *MockProvider) Start(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (mp *MockProvider) Stop(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (mp *MockProvider) GetMQType() string {
	//TODO implement me
	panic("implement me")
}

func (mp *MockProvider) Publish(ctx context.Context, opts PublishOptions, m *Message) error {
	//TODO implement me
	panic("implement me")
}

func (mp *MockProvider) Subscribe(opts HandlerOptions, handler MessageHandler) error {
	return handler(context.Background(), &Message{})
}

func TestOnRawdw(t *testing.T) {
	t.Run("missHandler", func(t *testing.T) {
		client := &Client{
			Provider: &MockProvider{},
		}

		opts := HandlerOptions{
			Handler: nil,
		}

		err := client.OnRaw(opts)
		assert.Equal(t, ErrMissHandler, err)

	})
	t.Run("not messageHandler", func(t *testing.T) {
		client := &Client{
			Provider: &MockProvider{},
		}

		opts := HandlerOptions{
			Handler: func() {},
		}

		err := client.OnRaw(opts)
		assert.Error(t, err)
		assert.ErrorContains(t, err, "handler should be MessageHandler")
	})
	t.Run("ok", func(t *testing.T) {
		client := &Client{
			Provider: &MockProvider{},
		}
		cb := func(ctx context.Context, m *Message) error {
			return nil
		}
		opts := HandlerOptions{
			Handler: cb,
		}
		err := client.OnRaw(opts)
		assert.NoError(t, err)
	})
}
