package rocketmqv4

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/gogap/errors"
	"github.com/stretchr/testify/suite"
	"github.com/tsingsun/woocoo/pkg/conf"
	"github.com/tsingsun/woocoo/pkg/gds"
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
	t.client, err = pubsub.New(cfg.Sub("rocketmq-v4"))
	t.Require().NoError(err)

	time.Sleep(time.Second * 2)
}

func (t *testsuite) TearDownSuite() {
	t.NoError(t.client.Stop(context.Background()))
}

func (t *testsuite) TestPublish() {
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
				"tag":         "test",
				"key":         "useSameKeyToOrderly",
				"shardingKey": "useSameKeyToOrderly",
			},
		},
		customer{
			ID:   "1",
			Name: "test1",
		})
	t.Require().NoError(err)
	err = t.client.Publish(context.Background(),
		pubsub.PublishOptions{
			ServiceName: "service1",
			JSON:        true,
			Metadata: map[string]string{
				"tag":         "test",
				"key":         "useSameKeyToOrderly",
				"shardingKey": "useSameKeyToOrderly",
			},
		},
		customer{
			ID:   "2",
			Name: "test2",
		})
	t.Require().NoError(err)
	count := 0
	tm := time.NewTimer(time.Second * 10)
	for {
		select {
		case <-tm.C:
			// wait for message ack
			t.Equal(2, count)
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			switch count {
			case 0:
				t.Contains(string(msg.Data), `"1"`)
			case 1:
				t.Contains(string(msg.Data), `"2"`)
			}
			t.T().Log(fmt.Sprintf("count:%d", count), msg)
			count++
		}
	}
	// wait for message ack
}

func (t *testsuite) TestRetryCommonConsume() {
	ch := make(chan *pubsub.Message)

	key := gds.RandomString(16)
	opts := pubsub.HandlerOptions{
		ServiceName: "service2",
		JSON:        true,
		Handler: func(ctx context.Context, message *customer, msg *pubsub.Message) error {
			if msg.Metadata["key"] != key {
				return nil
			}
			ch <- msg
			return errors.New("make error")
		},
	}
	t.Require().NoError(t.client.On(opts))
	time.Sleep(time.Second * 2)
	err := t.client.Publish(context.Background(),
		pubsub.PublishOptions{
			ServiceName: "service1",
			JSON:        true,
			Metadata: map[string]string{
				"tag":         "retry",
				"key":         key,
				"shardingKey": "TestRetryConsume",
			},
		},
		customer{
			ID:   "1",
			Name: "test1",
		})
	t.Require().NoError(err)
	count := 0
	// mq的重试机制 10秒,30秒
	tm := time.NewTimer(time.Second * 60)
	for {
		select {
		case <-tm.C:
			// wait for message ack, 1+ retry times 2
			t.Equal(3, count)
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			t.T().Log(fmt.Sprintf("count:%d", count), msg)
			count++
		}
	}
}

func (t *testsuite) TestRetryOrderlyConsume() {
	ch := make(chan *pubsub.Message)

	key := gds.RandomString(16)
	opts := pubsub.HandlerOptions{
		ServiceName: "service3",
		JSON:        true,
		Handler: func(ctx context.Context, message *customer, msg *pubsub.Message) error {
			if msg.Metadata["key"] != key {
				return nil
			}
			ch <- msg
			return errors.New("make error")
		},
	}
	t.Require().NoError(t.client.On(opts))
	time.Sleep(time.Second * 2)
	err := t.client.Publish(context.Background(),
		pubsub.PublishOptions{
			ServiceName: "service1",
			JSON:        true,
			Metadata: map[string]string{
				"tag":         "orderlyretry",
				"key":         key,
				"shardingKey": "TestRetryConsume",
			},
		},
		customer{
			ID:   "1",
			Name: "test1",
		})
	t.Require().NoError(err)
	count := 0
	// mq的重试机制 10秒,30秒
	tm := time.NewTimer(time.Second * 60)
	for {
		select {
		case <-tm.C:
			// wait for message ack, 1+ retry times 3 + 1(reconsume)
			t.Equal(6, count)
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			t.T().Log(fmt.Sprintf("count:%d", count), msg)
			count++
		}
	}
}

func TestParseEndpoint(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantList []string
		wantType string
	}{
		{
			name:     "multiple semicolon separated",
			input:    "ns1;ns2",
			wantList: []string{"ns1", "ns2"},
			wantType: endPointIP,
		},
		{
			name:     "valid domain url",
			input:    "http://example.com:8080/path",
			wantList: []string{"http://example.com:8080/path"},
			wantType: endPointDomain,
		},
		{
			name:     "valid ip address",
			input:    "192.168.1.1",
			wantList: []string{"192.168.1.1"},
			wantType: endPointIP,
		},
		{
			name:  "valid tcp address",
			input: "localhost:8080",
			wantList: []string{func() string {
				ip, _ := net.ResolveTCPAddr("tcp", "localhost:8080")
				return ip.String()
			}()},
			wantType: endPointIP,
		},
		{
			name:     "invalid endpoint",
			input:    "invalid$endpoint",
			wantList: []string{"invalid$endpoint"},
			wantType: endPointIP,
		},
		{
			name:     "empty input",
			input:    "",
			wantList: []string{""},
			wantType: endPointIP,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotList, gotType := parseEndpoint(tt.input)
			if !equalStringSlice(gotList, tt.wantList) {
				t.Errorf("parseEndpoint() gotList = %v, want %v", gotList, tt.wantList)
			}
			if gotType != tt.wantType {
				t.Errorf("parseEndpoint() gotType = %v, want %v", gotType, tt.wantType)
			}
		})
	}
}

// Helper function to compare string slices
func equalStringSlice(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
