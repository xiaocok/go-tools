package rabbitmq

import (
	"context"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

// 没有交换机，只有队列
func TestSampleProducer(t *testing.T) {
	ctx := context.Background()
	config := ProducerConfig{
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
	producer, err := NewProducer(config)
	if err != nil {
		t.Error(err)
	}
	err = producer.Publish(ctx, []byte("SampleProducer: Hello RabbitMQ!"))
	if err != nil {
		t.Error(err)
	}
	err = producer.Publish(ctx, []byte("SampleProducer: Hello RabbitMQ two!"))
	if err != nil {
		t.Error(err)
	}
}

// Publish/Subscribe 发布订阅模式 广播模式
func TestFanoutProducer(t *testing.T) {
	ctx := context.Background()
	config := ProducerConfig{
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
		RoutingKey:   "", // 广播模式：没有路由键
	}
	producer, err := NewProducer(config)
	if err != nil {
		t.Error(err)
	}
	err = producer.Publish(ctx, []byte("FanoutProducer: Hello RabbitMQ!"))
	if err != nil {
		t.Error(err)
	}
	err = producer.Publish(ctx, []byte("FanoutProducer: Hello RabbitMQ two!"))
	if err != nil {
		t.Error(err)
	}
}

// Routing直连模式
func TestDirectProducer(t *testing.T) {
	ctx := context.Background()
	config := ProducerConfig{
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
	producer, err := NewProducer(config)
	if err != nil {
		t.Error(err)
	}
	err = producer.Publish(ctx, []byte("DirectProducer: Hello RabbitMQ!"))
	if err != nil {
		t.Error(err)
	}
	err = producer.Publish(ctx, []byte("DirectProducer: Hello RabbitMQ two!"))
	if err != nil {
		t.Error(err)
	}
}

// topic模式：RoutingKey通配符模式
// RoutingKey由一个或者多个单词组成，多个单词使用.分割，例如：item.insert
// 通配符：
// * 刚好匹配一个
// # 匹配一个或者多个词
// 例如：
// fan.# 匹配fan.one.two 或者fan.one 等
// fan.* 只能匹配fan.one
func TestTopicProducer(t *testing.T) {
	ctx := context.Background()
	config := ProducerConfig{
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
		RoutingKey:   "my_routing.key",
	}
	producer, err := NewProducer(config)
	if err != nil {
		t.Error(err)
	}
	err = producer.Publish(ctx, []byte("TopicProducer: Hello RabbitMQ!"))
	if err != nil {
		t.Error(err)
	}
	err = producer.Publish(ctx, []byte("TopicProducer: Hello RabbitMQ two!"))
	if err != nil {
		t.Error(err)
	}
}

// headers模式
// headers 模式是一种用于消息路由的交换机类型，它允许发送方根据消息的自定义头部属性来进行消息的路由。
// 在 headers 模式下，发送方可以为每条消息定义一组键值对的头部属性，接收方则通过判断这些头部属性来决定是否接收该消息。
// 这种方式提供了一种非常灵活的方法，可以根据消息中的任意属性来进行路由，而不仅仅局限于路由键。

// 介绍：
// headers exchange与 direct、topic、fanout不同，它是通过匹配 AMQP 协议消息的 header 而非路由键，有点像HTTP的Headers；
// headers exchange与 direct Exchange类似，性能方面比后者差很多，所以在实际项目中用的很少。
// 在绑定Queue与Exchange时指定一组header键值对；当消息发送到Exchange时，RabbitMQ会取到该消息的headers（也是一个键值对的形式），
// 对比其中的键值对是否完全匹配Queue与Exchange绑定时指定的键值对。如果完全匹配则消息会路由到该Queue，否则不会路由到该Queue。
//
// headers属性是一个键值对，键值对的值可以是任何类型。而fanout，direct，topic 的路由键都需要要字符串形式的。
// 键值对要求携带一个键“x-match”，这个键的Value可以是any或者all，这代表消息携带的header是需要全部匹配(all)，还是仅匹配一个键(any)就可以了。
func TestHeadersProducer(t *testing.T) {
	ctx := context.Background()
	config := ProducerConfig{
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
			"baz":     "qux",
		},
	}
	producer, err := NewProducer(config)
	if err != nil {
		t.Error(err)
	}
	err = producer.AmqpPublishing(ctx, &amqp.Publishing{
		Headers: amqp.Table{
			"foo": "bar",
			"baz": "qux",
		},
		ContentType:  "text/plain",                               // 内容类型
		DeliveryMode: amqp.Persistent,                            // 持久化：RabbitMQ重启后消息仍然存在
		Body:         []byte("HeadersProducer: Hello RabbitMQ!"), // 消息体
	})
	if err != nil {
		t.Error(err)
	}
	err = producer.AmqpPublishing(ctx, &amqp.Publishing{
		Headers: amqp.Table{
			"foo": "bar",
			"baz": "qux",
		},
		ContentType:  "text/plain",                                   // 内容类型
		DeliveryMode: amqp.Persistent,                                // 持久化：RabbitMQ重启后消息仍然存在
		Body:         []byte("HeadersProducer: Hello RabbitMQ two!"), // 消息体
	})
	if err != nil {
		t.Error(err)
	}
}

// Trace模式
// RabbitMQ的Trace交换机是一种特殊的交换机类型，用于在RabbitMQ中实现消息跟踪和调试。
// Trace交换机会将发送到指定交换机的消息发送到预定义的跟踪队列，以便进行消息跟踪和监控。
