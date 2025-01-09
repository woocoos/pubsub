package aliyunmq

import (
	"context"
	"github.com/stretchr/testify/suite"
	"github.com/tsingsun/woocoo/pkg/conf"
	"github.com/woocoos/pubsub"
	"os"
	"testing"
	"time"
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

	time.Sleep(time.Second * 2)
}

func (t *testsuite) TearDownSuite() {
	t.NoError(t.client.Stop(context.Background()))
}

func (t *testsuite) TestService1() {
	ch := make(chan *pubsub.Message)
	opts := pubsub.HandlerOptions{
		ServiceName: "service1",
		JSON:        true,
		Handler: func(ctx context.Context, message *customer, msg *pubsub.Message) error {
			ch <- msg
			return nil
		},
	}
	t.Require().NoError(t.client.On(opts))
	err := t.client.Publish(context.Background(),
		pubsub.PublishOptions{
			ServiceName: "service1",
			JSON:        true,
			Metadata: map[string]string{
				"tag": "test",
				"key": "1",
			},
		},
		customer{
			ID:   "1",
			Name: "test",
		})
	t.Require().NoError(err)
	select {
	case <-time.After(time.Second * 5):
		t.Fail("timeout")
	case <-ch:
	}
}
