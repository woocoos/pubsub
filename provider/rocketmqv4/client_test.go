package rocketmqv4

import (
	"context"
	"github.com/stretchr/testify/suite"
	"github.com/tsingsun/woocoo/pkg/conf"
	"github.com/woocoos/pubsub"
	"net"
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
	t.client, err = pubsub.New(cfg.Sub("rocketmq-v4"))
	t.Require().NoError(err)

	time.Sleep(time.Second * 2)
}

func (t *testsuite) TearDownSuite() {
	t.NoError(t.client.Stop(context.Background()))
}

func (t *testsuite) TestPublish() {
	ch := make(chan *pubsub.Message)
	count := 0
	opts := pubsub.HandlerOptions{
		ServiceName: "service1",
		JSON:        true,
		Handler: func(ctx context.Context, message *customer, msg *pubsub.Message) error {
			count++
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
	t.Equal(1, count)
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
