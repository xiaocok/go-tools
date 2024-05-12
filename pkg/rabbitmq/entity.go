package rabbitmq

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type URL struct {
	UserName string
	Password string
	Host     string
	Port     int
	Vhost    string
}

func (url *URL) Get() string {
	return fmt.Sprintf("amqp://%s:%s@%s:%d/%s", url.UserName, url.Password, url.Host, url.Port, url.Vhost)
}

type ExchangeType string

const (
	Direct  ExchangeType = "direct"  // 直连模式
	Fanout  ExchangeType = "fanout"  // 广播模式
	Topic   ExchangeType = "topic"   // 主题模式
	Headers ExchangeType = "headers" // 头模式/match匹配模式
)

type ProducerConfig struct {
	URL          URL
	ExchangeName string
	ExchangeType ExchangeType
	QueueName    string
	RoutingKey   string
	Headers      amqp.Table
}

type ConsumerConfig struct {
	URL          URL
	ExchangeName string
	ExchangeType ExchangeType
	QueueName    string
	RoutingKey   string
	ConsumerTag  string
	Headers      amqp.Table
}
