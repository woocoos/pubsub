package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/tsingsun/woocoo/pkg/conf"
	"google.golang.org/protobuf/proto"
	"reflect"
)

// providerBuildFunc is the function to build a provider.
type providerBuildFunc func(*conf.Configuration) (Provider, error)

var providerBuilder = make(map[string]providerBuildFunc)

// RegisterProvider register a provider builder.
func RegisterProvider(name string, builder providerBuildFunc) {
	providerBuilder[name] = builder
}

// TopicKind is the kind of topic.
type TopicKind string

const (
	TopicKindCommon TopicKind = "common"
	TopicKindOrder  TopicKind = "orderly"
	TopicKindTiming TopicKind = "timing"
	TopicKindTrans  TopicKind = "trans"
)

type Client struct {
	ServiceName string
	Provider    Provider
	Middleware  []Middleware
}

// New creates a new pubsub client.
func New(cfg *conf.Configuration) (*Client, error) {
	ptype := cfg.String("type")
	if ptype == "" {
		return nil, fmt.Errorf("pubsub type is required")
	}
	bf, ok := providerBuilder[ptype]
	if !ok {
		return nil, fmt.Errorf("pubsub type %s is not registered", ptype)
	}
	provider, err := bf(cfg)
	if err != nil {
		return nil, err
	}
	client := &Client{
		Provider:   provider,
		Middleware: defaults,
	}

	return client, nil
}

func (c *Client) On(opts HandlerOptions) error {
	if opts.Handler == nil {
		return fmt.Errorf("handler cannot be nil")
	}
	// Reflection is slow, but this is done only once on subscriber setup
	hndlr := reflect.TypeOf(opts.Handler)
	if hndlr.Kind() != reflect.Func {
		return fmt.Errorf("handler must be a function")
	}
	errtpl := `handler should be of format like：
func(ctx context.Context, obj *proto.Message, msg *Message) error
%s
`
	if hndlr.NumIn() != 3 {
		return fmt.Errorf(errtpl, `but first arg was not context.Context`)
	}
	if hndlr.In(0) != reflect.TypeOf((*context.Context)(nil)).Elem() {
		return fmt.Errorf("handler's first argument must be context.Context")
	}
	if !opts.JSON {
		if !hndlr.In(1).Implements(reflect.TypeOf((*proto.Message)(nil)).Elem()) {
			return fmt.Errorf(errtpl, `but second arg does not implement proto.Message interface`)
		}
	}

	if hndlr.In(2) != reflect.TypeOf(&Message{}) {
		return fmt.Errorf(errtpl, `but third arg was not pubsub.Message`)
	}

	if !hndlr.Out(0).Implements(reflect.TypeOf((*error)(nil)).Elem()) {
		return fmt.Errorf(errtpl, `but output type is not error`)
	}

	fn := reflect.ValueOf(opts.Handler)
	cb := func(ctx context.Context, m Message) error {
		var err error
		obj := reflect.New(hndlr.In(1).Elem()).Interface()
		if opts.JSON {
			err = json.Unmarshal(m.Data, obj)
		} else {
			err = proto.Unmarshal(m.Data, obj.(proto.Message))
		}

		if err != nil {
			return fmt.Errorf("pubsub failed to unmarshal message:%w", err)
		}

		rtrn := fn.Call([]reflect.Value{
			reflect.ValueOf(ctx),
			reflect.ValueOf(obj),
			reflect.ValueOf(&m),
		})
		if len(rtrn) == 0 {
			return nil
		}

		erri := rtrn[0].Interface()
		if erri != nil {
			err = erri.(error)
		}

		return err
	}
	mw := chainSubscriberMiddleware(c.Middleware...)
	return c.Provider.Subscribe(opts, mw(cb))
}

func chainSubscriberMiddleware(mw ...Middleware) func(next MessageHandler) MessageHandler {
	return func(final MessageHandler) MessageHandler {
		return func(ctx context.Context, m Message) error {
			last := final
			for i := len(mw) - 1; i >= 0; i-- {
				last = mw[i].SubscribeInterceptor(last)
			}
			return last(ctx, m)
		}
	}
}

func chainPublisherMiddleware(mw ...Middleware) func(next PublishHandler) PublishHandler {
	return func(final PublishHandler) PublishHandler {
		return func(ctx context.Context, m *Message) error {
			last := final
			for i := len(mw) - 1; i >= 0; i-- {
				last = mw[i].PublishInterceptor(ctx, last)
			}
			return last(ctx, m)
		}
	}
}

func (c *Client) Publish(ctx context.Context, opts PublishOptions, data any) (err error) {
	var b []byte
	if opts.JSON {
		b, err = json.Marshal(data)
	} else {
		b, err = proto.Marshal(data.(proto.Message))
	}

	if err != nil {
		return err
	}

	m := &Message{Data: b}

	if opts.ServiceName == "" {
		opts.ServiceName = c.ServiceName
	}
	mw := chainPublisherMiddleware(c.Middleware...)
	return mw(func(ctx context.Context, m *Message) error {
		return c.Provider.Publish(ctx, opts, m)
	})(ctx, m)
}

func (c *Client) Stop(ctx context.Context) error {
	return c.Provider.Stop(ctx)
}
