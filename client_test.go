package pubsub

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tsingsun/woocoo/pkg/log"
)

// MockProvider 模拟 Provider 接口
type MockProvider struct {
	wg         sync.WaitGroup
	consumer   chan string
	handlerErr error
}

func NewMockProvider() *MockProvider {
	return &MockProvider{
		consumer: make(chan string),
	}
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
	go func() {
		for {
			select {
			case msg := <-mp.consumer:
				func() {
					defer mp.wg.Done()
					err := handler(context.Background(), &Message{
						Data: []byte(msg),
					})
					if err != nil {
						mp.handlerErr = err
					}
				}()
			}
		}
	}()
	return nil
}

func (mp *MockProvider) SendReceiveMessage(sig string) {
	select {
	case mp.consumer <- sig:
	case <-time.After(time.Second):
		log.Error("SendReceiveMessage:timeout")
	}
}

func TestOnRaw(t *testing.T) {
	t.Run("missHandler", func(t *testing.T) {
		client := &Client{
			Provider: NewMockProvider(),
		}

		opts := HandlerOptions{
			Handler: nil,
		}

		err := client.OnRaw(opts)
		assert.Equal(t, ErrMissHandler, err)

	})
	t.Run("not messageHandler", func(t *testing.T) {
		client := &Client{
			Provider: NewMockProvider(),
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
			Provider: NewMockProvider(),
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
	t.Run("panic", func(t *testing.T) {
		provider := NewMockProvider()
		client := &Client{
			Provider:   provider,
			Middleware: defaults,
		}
		provider.wg.Add(1)
		cb := func(ctx context.Context, m *Message) error {
			panic(string(m.Data))
		}
		opts := HandlerOptions{
			Handler: cb,
		}

		err := client.OnRaw(opts)
		require.NoError(t, err)

		provider.wg.Add(1)
		go func() {
			provider.wg.Done()
			provider.SendReceiveMessage("panic")
		}()
		provider.wg.Wait()
		assert.Error(t, provider.handlerErr)
	})
}
