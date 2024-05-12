package rabbitmq

import (
	"context"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

// 没有交换机，只有队列
func TestSampleConsumer(t *testing.T) {
	config := ConsumerConfig{
		URL: URL{
			UserName: "guest",
			Password: "guest",
			Host:     "127.0.0.1",
			Port:     5672,
			Vhost:    "/",
		},
		ExchangeName: "",
		QueueName:    "my_sample_queue",
		RoutingKey:   "my_sample_queue", // 简单模式：没有路由器，路邮键为队列名称
	}

	consumer, err := NewConsumer(config)
	if err != nil {
		t.Error(err)
	}

	ctx := context.Background()
	consumer.Consume(ctx, func(ctx context.Context, delivery amqp.Delivery) (bool, error) {
		t.Log("Received message: ", string(delivery.Body))
		return false, nil
	})

	exit := make(chan bool)
	<-exit
}

// Publish/Subscribe 发布订阅模式 广播模式
func TestFanoutConsumer(t *testing.T) {
	config := ConsumerConfig{
		URL: URL{
			UserName: "guest",
			Password: "guest",
			Host:     "127.0.0.1",
			Port:     5672,
			Vhost:    "/",
		},
		ExchangeName: "my_fanout_exchange",
		ExchangeType: Fanout,
		QueueName:    "my_fanout_queue",
		RoutingKey:   "", // 广播模式：绑定到该路由的所有队列都会收到，任何RoutingKey匹配的队列都会收到
	}

	consumer, err := NewConsumer(config)
	if err != nil {
		t.Error(err)
	}

	ctx := context.Background()
	consumer.Consume(ctx, func(ctx context.Context, delivery amqp.Delivery) (bool, error) {
		t.Log("Received message: ", string(delivery.Body))
		return false, nil
	})

	exit := make(chan bool)
	<-exit
}

// Routing直连模式
func TestDirectConsumer(t *testing.T) {
	config := ConsumerConfig{
		URL: URL{
			UserName: "guest",
			Password: "guest",
			Host:     "127.0.0.1",
			Port:     5672,
			Vhost:    "/",
		},
		ExchangeName: "my_direct_exchange",
		ExchangeType: Direct,
		QueueName:    "my_direct_queue",
		RoutingKey:   "my_routing.key", // 直连模式：生产者和消费者的路由键要一致
	}

	consumer, err := NewConsumer(config)
	if err != nil {
		t.Error(err)
	}

	ctx := context.Background()
	consumer.Consume(ctx, func(ctx context.Context, delivery amqp.Delivery) (bool, error) {
		t.Log("Received message: ", string(delivery.Body))
		return false, nil
	})

	exit := make(chan bool)
	<-exit
}

// topic模式：RoutingKey通配符模式
// RoutingKey由一个或者多个单词组成，多个单词使用.分割，例如：item.insert
// 通配符：
// * 刚好匹配一个
// # 匹配一个或者多个词
// 例如：
// fan.# 匹配fan.one.two 或者fan.one 等
// fan.* 只能匹配fan.one
func TestTopicConsumer(t *testing.T) {
	config := ConsumerConfig{
		URL: URL{
			UserName: "guest",
			Password: "guest",
			Host:     "127.0.0.1",
			Port:     5672,
			Vhost:    "/",
		},
		ExchangeName: "my_topic_exchange",
		ExchangeType: Topic,
		QueueName:    "my_topic_queue",
		RoutingKey:   "my_routing.#",
	}

	consumer, err := NewConsumer(config)
	if err != nil {
		t.Error(err)
	}

	ctx := context.Background()
	consumer.Consume(ctx, func(ctx context.Context, delivery amqp.Delivery) (bool, error) {
		t.Log("Received message: ", string(delivery.Body))
		return false, nil
	})

	exit := make(chan bool)
	<-exit
}

// headers模式
func TestHeadersConsumer(t *testing.T) {
	config := ConsumerConfig{
		URL: URL{
			UserName: "guest",
			Password: "guest",
			Host:     "127.0.0.1",
			Port:     5672,
			Vhost:    "/",
		},
		ExchangeName: "my_headers_exchange",
		ExchangeType: Headers,
		QueueName:    "my_headers_queue",
		RoutingKey:   "", // 路由键为空，避免Topic规则路由匹配
		Headers: amqp.Table{
			"x-match": "any", // 匹配规则标识，any表示任意一个匹配规则满足都接收消息
			"foo":     "bar", // 需要校验的匹配项
		},
	}
	// all（所有匹配）:
	//   当消费者指定了匹配规则为all时，消息的headers字段中的所有键值对都必须和消费者设置的键值对完全匹配，消费者才会接收到这条消息。
	// any （任意匹配）:
	//   当消费者指定了匹配规则为any时，消息的headers字段中只要存在任意一个键值对和消费者设置的键值对匹配，消费者就会接收到这条消息。

	consumer, err := NewConsumer(config)
	if err != nil {
		t.Error(err)
	}

	ctx := context.Background()
	consumer.Consume(ctx, func(ctx context.Context, delivery amqp.Delivery) (bool, error) {
		t.Log("Received message: ", string(delivery.Body))
		return false, nil
	})

	exit := make(chan bool)
	<-exit
}

// Trace模式
