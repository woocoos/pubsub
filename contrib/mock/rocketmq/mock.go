package rocketmq

import (
	"bytes"
	"context"
	"fmt"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
	"io"
	"strings"
)

const (
	name    = "rocketmq"
	version = "4.9.7"
)

var mqadmin = fmt.Sprintf("/home/rocketmq/rocketmq-%s/bin/mqadmin", version)

type MockV4Server struct {
	rmqNameSrvContainer testcontainers.Container
	rmqBrokerContainer  testcontainers.Container
	dashboardContainer  testcontainers.Container
	EndPoint            string
}

// NewMockV4Server creates a new Apache RocketMQ server
func NewMockV4Server() (*MockV4Server, error) {
	// 禁止自动删除容器
	//os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
	return &MockV4Server{
		EndPoint: "127.0.0.1:9876",
	}, nil
}

func (m *MockV4Server) Start(ctx context.Context) error {
	hostIp := "127.0.0.1"
	var err error
	net, err := network.New(ctx)
	// 创建自定义网络
	m.rmqNameSrvContainer, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "apache/rocketmq:" + version,
			ExposedPorts: []string{"9876/tcp"},
			WaitingFor:   wait.ForListeningPort("9876/tcp"),
			Name:         "rmqnamesrv",
			Networks:     []string{net.Name},
			Cmd:          []string{"sh", "mqnamesrv"},
			HostConfigModifier: func(hc *container.HostConfig) {
				hc.PortBindings = map[nat.Port][]nat.PortBinding{
					"9876/tcp": {{HostIP: "127.0.0.1", HostPort: "9876"}},
				}
			},
		},
		Started: true,
		Reuse:   true,
	})
	if err != nil {
		return err
	}
	//ip, _ := m.rmqNameSrvContainer.Host(ctx)
	mappedPort, err := m.rmqNameSrvContainer.MappedPort(ctx, "9876/tcp")
	if err != nil {
		return err
	}
	m.EndPoint = fmt.Sprintf("%s:%s", hostIp, mappedPort.Port())
	// broker
	r := bytes.NewReader([]byte(`
brokerClusterName = DefaultCluster
brokerName = broker-a
brokerId = 0
deleteWhen = 04
fileReservedTime = 48
brokerRole = ASYNC_MASTER
flushDiskType = ASYNC_FLUSH
brokerIP1=localhost
`))
	m.rmqBrokerContainer, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "apache/rocketmq:" + version,
			ExposedPorts: []string{"10912/tcp", "10911/tcp", "10909/tcp", "8080/tcp"},
			WaitingFor:   wait.ForListeningPort("10909/tcp"),
			Name:         "rmqbroker",
			Networks:     []string{net.Name},
			Env: map[string]string{
				"NAMESRV_ADDR": "rmqnamesrv:9876",
			},
			Cmd: []string{"sh", "mqbroker", "-c /home/broker.conf"},
			HostConfigModifier: func(hc *container.HostConfig) {
				hc.PortBindings = map[nat.Port][]nat.PortBinding{
					"10912/tcp": {{HostIP: "127.0.0.1", HostPort: "10912"}},
					"10911/tcp": {{HostIP: "127.0.0.1", HostPort: "10911"}},
					"10909/tcp": {{HostIP: "127.0.0.1", HostPort: "10909"}},
					"8080/tcp":  {{HostIP: "127.0.0.1", HostPort: "18080"}},
				}
			},
			Files: []testcontainers.ContainerFile{
				{
					Reader:            r,
					ContainerFilePath: "/home/broker.conf",
					FileMode:          0o777,
				},
			},
		},
		Started: true,
		Reuse:   true,
	})
	if err != nil {
		return err
	}
	m.dashboardContainer, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:    "apacherocketmq/rocketmq-dashboard:latest",
			Name:     "dashboard",
			Networks: []string{net.Name},
			Env: map[string]string{
				"JAVA_OPTS": "-Drocketmq.namesrv.addr=rmqnamesrv:9876",
			},
			HostConfigModifier: func(hc *container.HostConfig) {
				hc.NetworkMode = "container:rmqbroker"
			},
		},
		Started: true,
		Reuse:   true,
	})
	if err != nil {
		return err
	}

	if err := m.CreateTopic(ctx, "Trading_Notify"); err != nil {
		return err
	}
	return nil
}

func (m *MockV4Server) Stop(ctx context.Context) error {
	if m.rmqBrokerContainer != nil {
		m.rmqBrokerContainer.Terminate(ctx)
	}
	if m.rmqNameSrvContainer != nil {
		m.rmqNameSrvContainer.Terminate(ctx)
	}
	if m.dashboardContainer != nil {
		m.dashboardContainer.Terminate(ctx)
	}
	return nil
}

func (m *MockV4Server) CreateTopic(ctx context.Context, topicName string) error {
	c, _, err := m.rmqBrokerContainer.Exec(ctx, []string{
		"sh", "-c", fmt.Sprintf("%s updateTopic -c DefaultCluster -p 6 -n rmqnamesrv:9876 -t %s", mqadmin, topicName)})
	if err != nil {
		return fmt.Errorf("exec command failed: %v", c)
	}
	if c != 0 {
		return fmt.Errorf("exec command failed: %v", c)
	}
	return nil
}

func (m *MockV4Server) SendMsg(ctx context.Context, topic, tag, key, body string) error {
	c, reader, err := m.rmqBrokerContainer.Exec(ctx, []string{
		"sh", "-c", fmt.Sprintf(`%s sendMessage -t %s -p '%s' -k %s -c %s -n rmqnamesrv:9876`, mqadmin, topic, body, key, tag),
	})
	if err != nil {
		return fmt.Errorf("exec command failed: %v", c)
	}
	buf := new(strings.Builder)
	_, err = io.Copy(buf, reader)
	if !strings.Contains(buf.String(), "SEND_OK") {
		return fmt.Errorf("exec command failed: %s", buf)
	}
	return nil
}
