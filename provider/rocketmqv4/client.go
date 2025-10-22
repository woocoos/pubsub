package rocketmqv4

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

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
	// 单次消费消息数量, 默认1
	MaxRecMsgNum int
	Consumers    map[string]struct {
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
	pc := ProviderConfig{
		MaxRecMsgNum: 1,
	}
	err := cfg.Unmarshal(&pc)
	if err != nil {
		return nil, err
	}
	p := &Provider{
		ProviderConfig: pc,
		producers:      make(map[string]any),
	}
	p.ctx, p.cancel = context.WithCancel(context.Background())
	if cfg.IsSet("log") {
		sync.OnceFunc(func() {
			logger = newApacheLogger(cfg.Sub("log"))
			rlog.SetLogger(logger)
		})()
	} else {
		logger = &apacheLogger{
			logger: log.Global().Logger(),
		}
	}
	return p, nil
}

func (p *Provider) Start(ctx context.Context) error {
	return nil
}

// Stop consumers and producers.
func (p *Provider) Stop(ctx context.Context) error {
	p.cancel()
	// leaves to UnCommit
	time.Sleep(time.Second)
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
	rlog.Info(fmt.Sprintf("rocketmq publish topic %s", pc.Topic), nil)

	msg := primitive.NewMessage(pc.Topic, m.Data)

	if opts.Metadata != nil {
		key, ok := opts.Metadata["key"]
		if ok {
			msg.WithKeys([]string{key})
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
			pruOpts := []producer.Option{
				parseProducerEndpoint(p.ProviderConfig.EndPoint),
				producer.WithQueueSelector(producer.NewHashQueueSelector()),
			}
			pd, err = rocketmq.NewTransactionProducer(
				newListener(),
				pruOpts...,
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
	case TopicKindOrderly:
		if v, ok := opts.Metadata["shardingKey"]; ok {
			msg.WithShardingKey(v)
		} else {
			msg.WithShardingKey(msg.GetKeys())
		}
		fallthrough
	default:
		var pd rocketmq.Producer
		if !ok {
			pd, err = rocketmq.NewProducer(
				producer.WithNameServer([]string{p.ProviderConfig.EndPoint}),
				producer.WithQueueSelector(producer.NewHashQueueSelector()),
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
	consumerOpts := []consumer.Option{
		consumer.WithConsumerModel(consumer.Clustering),
		consumer.WithGroupName(cc.Group),
		consumer.WithConsumerOrder(cc.Kind == TopicKindOrderly),
		consumer.WithConsumeMessageBatchMaxSize(p.MaxRecMsgNum),
		parseConsumerEndpoint(p.ProviderConfig.EndPoint),
	}
	cs, err := rocketmq.NewPushConsumer(consumerOpts...)
	if err != nil {
		return err
	}
	err = cs.Subscribe(cc.Topic,
		consumer.MessageSelector{Type: consumer.TAG, Expression: cc.MessageTag},
		func(ctx context.Context, ext ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
			log.Println(ext)
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
					logger.logger.Error("messageHandler error", zap.Any("entity", v), zap.Error(err))
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

const (
	endPointIP     = "ip"
	endPointDomain = "domain"
)

func parseEndpoint(endpoint string) ([]string, string) {
	ns := strings.Split(endpoint, ";")
	if len(ns) > 1 {
		return ns, endPointIP
	}
	if uri, err := url.Parse(endpoint); err == nil && uri.Host != "" {
		return []string{endpoint}, endPointDomain
	}
	if ip := net.ParseIP(endpoint); ip == nil {
		addr, err := net.ResolveTCPAddr("tcp", endpoint)
		if err != nil {
			return []string{endpoint}, endPointIP
		} else if addr.IP != nil {
			return []string{addr.String()}, endPointIP
		}
	}
	return []string{endpoint}, endPointIP
}

// parseConsumerEndpoint
func parseConsumerEndpoint(endpoint string) consumer.Option {
	ns, kind := parseEndpoint(endpoint)
	switch kind {
	case endPointDomain:
		return consumer.WithNameServerDomain(endpoint)
	default:
		return consumer.WithNameServer(ns)
	}
}

func parseProducerEndpoint(endpoint string) producer.Option {
	ns, kind := parseEndpoint(endpoint)
	switch kind {
	case endPointDomain:
		return producer.WithNameServerDomain(endpoint)
	default:
		return producer.WithNameServer(ns)
	}
}
