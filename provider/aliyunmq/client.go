package aliyunmq

import (
	"context"
	"fmt"
	mqhttpsdk "github.com/aliyunmq/mq-http-go-sdk"
	"github.com/gogap/errors"
	"github.com/tsingsun/woocoo/pkg/conf"
	"github.com/tsingsun/woocoo/pkg/gds"
	"github.com/woocoos/pubsub"
	"go.uber.org/zap"
	"sync"
	"time"
)

const mqTypeName = "rocketmq-aliyun-v4"

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

// ProviderConfig 配置
type ProviderConfig struct {
	EndPoint   string
	AccessKey  string
	SecretKey  string
	InstanceID string
	// 消费等待时长,默认3秒
	ConsumerWaitSeconds int
	// 单次消费消息数量, 默认3
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

// Provider 消息队列实现
type Provider struct {
	ProviderConfig
	client mqhttpsdk.MQClient
	ctx    context.Context
	cancel context.CancelFunc
}

// New create a new aliyunmq provider
func New(cfg *conf.Configuration) (pubsub.Provider, error) {
	var pc = ProviderConfig{
		MaxRecMsgNum:        3,
		ConsumerWaitSeconds: 3,
	}
	err := cfg.Unmarshal(&pc)
	if err != nil {
		return nil, err
	}
	p := &Provider{
		ProviderConfig: pc,
		client:         mqhttpsdk.NewAliyunMQClient(pc.EndPoint, pc.AccessKey, pc.SecretKey, ""),
	}
	p.ctx, p.cancel = context.WithCancel(context.Background())
	sync.OnceFunc(func() {
		logger = newWrapperLogger(cfg.Sub("log"))
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

func (p *Provider) Subscribe(opts pubsub.HandlerOptions, handler pubsub.MessageHandler) error {
	// find consumer
	cc, ok := p.Consumers[opts.ServiceName]
	if !ok {
		return fmt.Errorf("no consumer config for serviceName: %s", opts.ServiceName)
	}
	logger.Info(fmt.Sprintf("aliyunmq subscribe topic %s", cc.Topic))

	mqConsumer := p.client.GetConsumer(p.InstanceID, cc.Topic, cc.Group, cc.MessageTag)
	go p.consumeMessages(mqConsumer, cc.Kind, handler)
	return nil
}

func (p *Provider) consumeMessages(consumer mqhttpsdk.MQConsumer, kind TopicKind, handler pubsub.MessageHandler) {
	for {
		select {
		case <-p.ctx.Done():
			return
		default:
			p.processMessages(consumer, kind, handler)
		}
	}
}

func (p *Provider) processMessages(consumer mqhttpsdk.MQConsumer, kind TopicKind, handler pubsub.MessageHandler) {
	endChan := make(chan int)
	respChan := make(chan mqhttpsdk.ConsumeMessageResponse)
	errChan := make(chan error)
	seconds := p.ConsumerWaitSeconds
	if seconds == 0 {
		seconds = 3
	}
	if p.MaxRecMsgNum == 0 {
		p.MaxRecMsgNum = 3
	}
	go func() {
		select {
		case resp := <-respChan:
			var handles []string
			for _, v := range resp.Messages {
				msg := pubsub.Message{
					ID:   v.MessageId,
					Data: []byte(v.MessageBody),
					Metadata: map[string]string{
						"key": v.MessageKey,
						"tag": v.MessageTag,
					},
					PublishTime: gds.Ptr(time.UnixMilli(v.PublishTime)),
				}
				if err := handler(context.Background(), &msg); err != nil {
					logger.Error("aliyunmq: messageHandler error", zap.Any("entity", v), zap.Error(err))
				}
				handles = append(handles, v.ReceiptHandle)
			}
			// NextConsumeTime前若不确认消息消费成功，则消息会被重复消费。
			// 消息句柄有时间戳，同一条消息每次消费拿到的都不一样。
			if err := consumer.AckMessage(handles); err != nil {
				logger.Error("aliyunmq: ack error", zap.Error(err))
				time.Sleep(time.Duration(seconds) * time.Second)
			}
			endChan <- 1
		case err := <-errChan:
			aderr := err.(errors.ErrCode)
			switch aderr.Code() {
			case 101:
				// no message
				logger.Debug("aliyunmq: no more message", zap.String("message", err.Error()))
			default:
				logger.Warn("aliyunmq: message error", zap.Error(err))
				time.Sleep(time.Duration(seconds) * time.Second)
			}
			endChan <- 1
		case <-time.After(35 * time.Second):
			logger.Warn(fmt.Sprintf("aliyunmq:timeout of consumer message on topic:%s", consumer.TopicName()))
			endChan <- 1
		}
	}()

	if kind == TopicKindOrderly {
		consumer.ConsumeMessageOrderly(respChan, errChan, int32(p.MaxRecMsgNum), int64(seconds))
	} else {
		consumer.ConsumeMessage(respChan, errChan, int32(p.MaxRecMsgNum), int64(seconds))
	}

	<-endChan
}

func (p *Provider) Publish(ctx context.Context, opts pubsub.PublishOptions, m *pubsub.Message) error {
	// find producer
	cc, ok := p.Producers[opts.ServiceName]
	if !ok {
		return fmt.Errorf("no producer config for serviceName: %s", opts.ServiceName)
	}
	req := mqhttpsdk.PublishMessageRequest{
		MessageBody: string(m.Data),
		MessageTag:  opts.Metadata["tag"],
		MessageKey:  opts.Metadata["key"],
	}

	var producer mqhttpsdk.MQProducer
	switch cc.Kind {
	case TopicKindOrderly:
		producer = p.client.GetTransProducer(p.InstanceID, cc.Topic, cc.Group)
		req.ShardingKey = opts.Metadata["shardingKey"]
	case TopicKindTrans:
		producer = p.client.GetTransProducer(p.InstanceID, cc.Topic, cc.Group)
	default:
		producer = p.client.GetProducer(p.InstanceID, cc.Topic)
	}

	_, err := producer.PublishMessage(req)
	return err
}
