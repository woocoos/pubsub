package pubsub

import (
	"context"
	"github.com/tsingsun/woocoo"
)

// Subscriber is an interface for message queue subscriber.
type Subscriber interface {
}

// Publisher is an interface for message queue publisher.
type Publisher interface {
	PublishMessage(ctx context.Context, msg *Message) error
}

// Provider is an interface for message queue provider.
// this is also a wrapper for the sdk of mq officer client.
type Provider interface {
	woocoo.Server
	// GetMQType get the type name of message queue provider
	GetMQType() string
	Publish(ctx context.Context, opts PublishOptions, m *Message) error
	Subscribe(opts HandlerOptions, handler MessageHandler) error
}

// Handler is custom callback for subscriber.
// The handler function format is.
// func(ctx context.Context, custom CustomModel,m *Message) error
type Handler any

// HandlerOptions is the options for handler
type HandlerOptions struct {
	Name string
	// ServiceName identify the service of Provider, will use this to find the service or Config
	ServiceName string
	// The function to invoke
	Handler Handler
	// Decode JSON objects from message body instead of protobuf.
	JSON bool
}

// MessageHandler is the internal or raw message handler
type MessageHandler func(ctx context.Context, m Message) error

// PublishHandler wraps a call to publish, for interception
type PublishHandler func(ctx context.Context, m *Message) error

type PublishOptions struct {
	// ServiceName identify the service of Provider, will use this to find the service or Config
	ServiceName string
	// Metadata is a key-value pair to pass to the Provider
	Metadata map[string]string
	// Decode JSON objects from message body instead of protobuf.
	JSON bool
}

// Middleware is an interface to provide subscriber and publisher interceptors
type Middleware interface {
	SubscribeInterceptor(next MessageHandler) MessageHandler
	PublishInterceptor(ctx context.Context, next PublishHandler) PublishHandler
}
