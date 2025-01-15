package rocketmqv4

import (
	"context"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/apache/rocketmq-client-go/v2/rlog"
	"github.com/tsingsun/woocoo/pkg/conf"
	"github.com/tsingsun/woocoo/pkg/gds"
	"github.com/tsingsun/woocoo/pkg/log"
	"github.com/woocoos/pubsub"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"sync"
	"sync/atomic"
	"time"
)

const mqTypeName = "rocketmq-v4"

func init() {
	pubsub.RegisterProvider(mqTypeName, New)
}

type TopicKind string

const (
	// TopicKindCommon 普通消息
	TopicKindCommon TopicKind = "common"
	// TopicKindOrderly 顺序消息
	TopicKindOrderly TopicKind = "orderly"
	// TopicKindTiming 定时消息
	TopicKindTiming TopicKind = "timing"
	// TopicKindTrans 事务消息
	TopicKindTrans TopicKind = "trans"
)

type ProviderConfig struct {
	EndPoint   string
	AccessKey  string
	SecretKey  string
	InstanceID string
	LogLevel   string
	Consumers  map[string]struct {
		Topic      string
		Group      string
		MessageTag string
		Kind       TopicKind
	}
	Producers map[string]struct {
		Topic string
		Group string
		Kind  TopicKind
	}
}

type Provider struct {
	ProviderConfig
	ctx       context.Context
	cancel    context.CancelFunc
	producers map[string]any
	mu        sync.RWMutex
}

func New(cfg *conf.Configuration) (pubsub.Provider, error) {
	var pc ProviderConfig
	err := cfg.Unmarshal(&pc)
	if err != nil {
		return nil, err
	}
	p := &Provider{
		ProviderConfig: pc,
		producers:      make(map[string]any),
	}
	p.ctx, p.cancel = context.WithCancel(context.Background())
	sync.OnceFunc(func() {
		ll, err := zapcore.ParseLevel(pc.LogLevel)
		if err != nil {
			panic(err)
		}
		rlog.SetLogger(&apacheLogger{
			level:  ll,
			logger: pubsub.Logger(),
		})
	})()
	return p, nil
}

func (p *Provider) Start(ctx context.Context) error {
	return nil
}

func (p *Provider) Stop(ctx context.Context) error {
	p.cancel()
	return nil
}

func (p *Provider) GetMQType() string {
	return mqTypeName
}

func (p *Provider) Publish(ctx context.Context, opts pubsub.PublishOptions, m *pubsub.Message) (err error) {
	// find producer
	pc, ok := p.Producers[opts.ServiceName]
	if !ok {
		return fmt.Errorf("no producer config for serviceName: %s", opts.ServiceName)
	}
	logger.Info(fmt.Sprintf("%s publish topic %s", p.GetMQType(), pc.Topic))

	msg := primitive.NewMessage(pc.Topic, m.Data)

	if opts.Metadata != nil {
		if v, ok := opts.Metadata["key"]; ok {
			msg.WithKeys([]string{v})
		}
		if v, ok := opts.Metadata["tag"]; ok {
			msg.WithTag(v)
		}
	}

	pdtmp, ok := p.producers[opts.ServiceName]
	switch pc.Kind {
	case TopicKindTrans:
		var pd rocketmq.TransactionProducer
		if !ok {
			pd, err = rocketmq.NewTransactionProducer(
				newListener(),
				producer.WithNameServer([]string{p.ProviderConfig.EndPoint}),
			)
			if err != nil {
				return err
			}
			p.mu.Lock()
			p.producers[opts.ServiceName] = pd
			p.mu.Unlock()
			if err = pd.Start(); err != nil {
				return err
			}
			go func() {
				select {
				case <-p.ctx.Done():
					pd.Shutdown()
				}
			}()
		} else {
			pd = pdtmp.(rocketmq.TransactionProducer)
		}

		// TODO SendInTransaction
	default:
		var pd rocketmq.Producer
		if !ok {
			pd, err = rocketmq.NewProducer(producer.WithNameServer([]string{p.ProviderConfig.EndPoint}))
			if err != nil {
				return err
			}
			p.mu.Lock()
			p.producers[opts.ServiceName] = pd
			p.mu.Unlock()
			if err = pd.Start(); err != nil {
				return err
			}
			go func() {
				select {
				case <-p.ctx.Done():
					pd.Shutdown()
				}
			}()
		} else {
			pd = pdtmp.(rocketmq.Producer)
		}
		_, err = pd.SendSync(ctx, msg)
	}
	return
}

func (p *Provider) Subscribe(opts pubsub.HandlerOptions, handler pubsub.MessageHandler) error {
	// find consumer
	cc, ok := p.Consumers[opts.ServiceName]
	if !ok {
		return fmt.Errorf("no consumer config for serviceName: %s", opts.ServiceName)
	}

	cs, err := rocketmq.NewPushConsumer(consumer.WithNameServer([]string{p.ProviderConfig.EndPoint}),
		consumer.WithConsumerModel(consumer.Clustering),
		consumer.WithGroupName(cc.Group),
		consumer.WithConsumerOrder(cc.Kind == TopicKindOrderly))
	if err != nil {
		return err
	}
	err = cs.Subscribe(cc.Topic,
		consumer.MessageSelector{Type: consumer.TAG, Expression: cc.MessageTag},
		func(ctx context.Context, ext ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
			for _, v := range ext {
				msg := pubsub.Message{
					Data: v.Body,
					ID:   v.MsgId,
					Metadata: map[string]string{
						"key": v.GetKeys(),
						"tag": v.GetTags(),
					},
					PublishTime: gds.Ptr(time.UnixMilli(v.BornTimestamp)),
				}
				if err := handler(context.Background(), &msg); err != nil {
					logger.Error("messageHandler error", zap.Any("entity", v), zap.Error(err))
				}
			}
			return consumer.ConsumeSuccess, nil
		},
	)
	if err != nil {
		return err
	}

	log.Infof("subscribe topic %s", cc.Topic)
	if err = cs.Start(); err != nil {
		return err
	}

	go func() {
		select {
		case <-p.ctx.Done():
			cs.Shutdown()
		}
	}()
	return nil
}

type listener struct {
	localTrans       *sync.Map
	transactionIndex int32
}

func newListener() *listener {
	return &listener{
		localTrans: new(sync.Map),
	}
}

func (dl *listener) ExecuteLocalTransaction(msg *primitive.Message) primitive.LocalTransactionState {
	nextIndex := atomic.AddInt32(&dl.transactionIndex, 1)
	status := nextIndex % 3
	dl.localTrans.Store(msg.TransactionId, primitive.LocalTransactionState(status+1))
	return primitive.UnknowState
}

func (dl *listener) CheckLocalTransaction(msg *primitive.MessageExt) primitive.LocalTransactionState {
	v, existed := dl.localTrans.Load(msg.TransactionId)
	if !existed {
		return primitive.CommitMessageState
	}
	state := v.(primitive.LocalTransactionState)
	switch state {
	case 1:
		return primitive.CommitMessageState
	case 2:
		return primitive.RollbackMessageState
	case 3:
		return primitive.UnknowState
	default:
		return primitive.CommitMessageState
	}
}
