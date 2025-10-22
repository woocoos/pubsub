package aliyunmq

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/gogap/errors"
	"github.com/stretchr/testify/suite"
	"github.com/tsingsun/woocoo/pkg/conf"
	"github.com/tsingsun/woocoo/pkg/gds"
	"github.com/tsingsun/woocoo/pkg/log"
	"github.com/woocoos/pubsub"
)

type customer struct {
	ID   string
	Name string
}

type testsuite struct {
	suite.Suite
	provider *Provider
	client   *pubsub.Client
}

func TestSuite(t *testing.T) {
	if os.Getenv("TEST_WIP") != "" {
		t.Skip()
		return
	}
	suite.Run(t, new(testsuite))
}

func (t *testsuite) SetupSuite() {
	cfg := conf.New(conf.WithBaseDir("testdata"), conf.WithLocalPath("testdata/app.yaml")).Load()
	var err error
	t.client, err = pubsub.New(cfg.Sub("aliyun"))
	t.Require().NoError(err)
	t.provider = t.client.Provider.(*Provider)
	time.Sleep(time.Second * 2)
}

func (t *testsuite) TearDownSuite() {
	t.NoError(t.client.Stop(context.Background()))
}

// 顺序消费,支持ACK后面的消息, 阿里云会将失败的消息重新投递,最多288次.
// 客户端根据错误重试次数,如果超过将确认该消息已被消费.
func (t *testsuite) TestOrderlyConsumer() {
	ch := make(chan *pubsub.Message)
	key1 := gds.RandomString(8)
	count := 0
	opts := pubsub.HandlerOptions{
		ServiceName: "service1",
		JSON:        true,
		Handler: func(ctx context.Context, message *customer, msg *pubsub.Message) error {
			var cus customer
			err := json.Unmarshal(msg.Data, &cus)
			t.Require().NoError(err)
			if cus.ID == "2" {
				// 让该消息一直失败
				return errors.New("key2")
			}
			ch <- msg
			return nil
		},
	}
	t.Require().NoError(t.client.On(opts))
	t.publish(context.Background(), "service1", key1, "orderly", customer{
		ID:   "1",
		Name: "testname1",
	})
	t.publish(context.Background(), "service1", key1, "orderly", customer{
		ID:   "2",
		Name: "testname2",
	})
	time.Sleep(time.Millisecond * 200)
	t.publish(context.Background(), "service1", key1, "orderly", customer{
		ID:   "3",
		Name: "testname3",
	})
	// 失败1分钟重发,至少需要2分钟才能识别.
	tc := time.NewTicker(time.Minute * 2)
	for {
		select {
		case <-tc.C:
			t.Equal(2, count)
			return
		case msg := <-ch:
			log.Debug(msg)
			count++
		}
	}
}

func (t *testsuite) TestCommonConsumer() {
	ch := make(chan *pubsub.Message)
	key1 := gds.RandomString(8)
	count := 0
	opts := pubsub.HandlerOptions{
		ServiceName: "service2",
		JSON:        true,
		Handler: func(ctx context.Context, message *customer, msg *pubsub.Message) error {
			var cus customer
			err := json.Unmarshal(msg.Data, &cus)
			t.Require().NoError(err)
			if cus.ID == "2" {
				// 让该消息一直失败
				return errors.New("make retry")
			}
			ch <- msg
			return nil
		},
	}
	t.Require().NoError(t.client.On(opts))
	t.publish(context.Background(), "service2", key1, "common", customer{
		ID:   "1",
		Name: "testname1",
	})
	t.publish(context.Background(), "service2", key1, "common", customer{
		ID:   "2",
		Name: "testname2",
	})
	time.Sleep(time.Millisecond * 200)
	t.publish(context.Background(), "service2", key1, "common", customer{
		ID:   "3",
		Name: "testname3",
	})
	// 失败5分钟重发,至少需要6分钟才能识别.
	tc := time.NewTicker(time.Minute * 6)
	for {
		select {
		case <-tc.C:
			t.Equal(2, count)
			return
		case msg := <-ch:
			log.Debug(msg)
			count++
		}
	}
}

func (t *testsuite) TestCommonConsumerRetry() {
	ch := make(chan *pubsub.Message)
	key1 := gds.RandomString(8)
	count, retry := 0, 0
	opts := pubsub.HandlerOptions{
		ServiceName: "service3",
		JSON:        true,
		Handler: func(ctx context.Context, message *customer, msg *pubsub.Message) error {
			var cus customer
			err := json.Unmarshal(msg.Data, &cus)
			t.Require().NoError(err)
			if cus.ID == "2" {
				retry++
				// 让该消息一直失败
				return errors.New("make retry")
			}
			ch <- msg
			return nil
		},
	}
	tag := "commonRetry"
	t.Require().NoError(t.client.On(opts))
	t.publish(context.Background(), "service2", key1, tag, customer{
		ID:   "1",
		Name: "testname1",
	})
	t.publish(context.Background(), "service2", key1, tag, customer{
		ID:   "2",
		Name: "testname2",
	})
	time.Sleep(time.Millisecond * 200)
	t.publish(context.Background(), "service2", key1, tag, customer{
		ID:   "3",
		Name: "testname3",
	})
	// 失败5分钟重发,至少需要6分钟才能识别.
	tc := time.NewTicker(time.Minute * 6)
	for {
		select {
		case <-tc.C:
			t.Equal(2, count)
			t.Equal(2, retry)
			return
		case msg := <-ch:
			log.Debug(msg)
			count++
		}
	}
}

func (t *testsuite) publish(ctx context.Context, service, key, tag string, data customer) {
	err := t.client.Publish(ctx,
		pubsub.PublishOptions{
			ServiceName: service,
			JSON:        true,
			Metadata: map[string]string{
				"tag": tag,
				"key": key,
			},
		},
		data,
	)
	t.Require().NoError(err)
}

// 本方法抽取所有消息, 作用测试工具,实际CI时,可以做SKIP.
func (t *testsuite) TestService1_DrillSubs() {
	ch := make(chan *pubsub.Message)
	count := 0
	for _, s := range []string{"service1", "service2", "service3"} {
		opts := pubsub.HandlerOptions{
			ServiceName: s,
			JSON:        true,
			Handler: func(ctx context.Context, message *customer, msg *pubsub.Message) error {
				ch <- msg
				return nil
			},
		}
		t.Require().NoError(t.client.On(opts))
	}
	tc := time.NewTicker(time.Second * 10)
	for {
		select {
		case <-tc.C:
			return
		case <-ch:
			count++
		}
	}
}
