package rocketmq

import (
	"context"
	"github.com/stretchr/testify/suite"
	"github.com/tsingsun/woocoo/pkg/conf"
	"github.com/woocoos/pubsub"
	"os"
	"testing"
	"time"

	_ "github.com/woocoos/pubsub/provider/aliyunmq"
	_ "github.com/woocoos/pubsub/provider/rocketmqv4"
)

type customModel struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type tradeMsgHandler func(ctx context.Context, custom *customModel, m *pubsub.Message) error

type testSuite struct {
	suite.Suite
	service *pubsub.Client
	mockMQ  *MockV4Server
	handler map[string]tradeMsgHandler
}

func TestSuite(t *testing.T) {
	if os.Getenv("TEST_WIP") != "" {
		t.Skip()
		return
	}
	suite.Run(t, &testSuite{
		handler: make(map[string]tradeMsgHandler),
	})
}

func (t *testSuite) SetupSuite() {
	var err error
	cnf := conf.New(conf.WithLocalPath("testdata/etc/app.yaml")).Load()
	// 启动 RocketMQ 容器
	t.mockMqServer(cnf)

	t.service, err = pubsub.New(cnf.Sub("rocketMQ"))
	t.Require().NoError(err)
}

func (t *testSuite) mockMqServer(cnf *conf.Configuration) {
	var err error
	t.mockMQ, err = NewMockV4Server()
	t.Require().NoError(err)
	err = t.mockMQ.Start(context.Background())
	t.Require().NoError(err)
	// 更新配置文件中的 RocketMQ 地址
	cnf.Parser().Set("rocketMQ.endPoint", t.mockMQ.EndPoint)
}

func (t *testSuite) TearDownSuite() {
	err := t.service.Stop(context.Background())
	if err != nil {
		t.T().Log(err)
	}
	if err = t.mockMQ.Stop(context.Background()); err != nil {
		t.T().Log(err)
	}
}

func (t *testSuite) TestStart() {
	var ch = make(chan struct{})
	t.handler["0AF4B8160001070DEA4E880ED13B0005"] = func(ctx context.Context, custom *customModel, m *pubsub.Message) error {
		ch <- struct{}{}
		return nil
	}
	err := t.service.On(pubsub.HandlerOptions{
		ServiceName: "tradingNotify",
		JSON:        true,
		Handler: func(ctx context.Context, custom *customModel, m *pubsub.Message) error {
			h, ok := t.handler[m.Metadata["key"]]
			if !ok {
				return nil
			}
			return h(ctx, custom, m)
		}})
	t.Require().NoError(err)

	err = t.mockMQ.SendMsg(context.Background(),
		"Trading_Notify", "tag", "0AF4B8160001070DEA4E880ED13B0005", "{}")
	t.Require().NoError(err)

	select {
	case <-ch:
	case <-time.After(time.Second * 5):
		t.T().Fatal("timeout")
	}
}
