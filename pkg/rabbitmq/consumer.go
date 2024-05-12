package rabbitmq

import (
	"context"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type ConsumerHandler func(ctx context.Context, delivery amqp.Delivery) (requeue bool, err error)

type Consumer struct {
	config      *ConsumerConfig
	connection  *amqp.Connection
	channel     *amqp.Channel
	queue       *amqp.Queue
	consumerTag string
	delivery    <-chan amqp.Delivery
	done        chan error
}

func NewConsumer(config ConsumerConfig) (*Consumer, error) {
	// 链接RabbitMQ服务器
	conn, err := amqp.Dial(config.URL.Get())
	if err != nil {
		return nil, fmt.Errorf("Failed to connect to RabbitMQ: %s", err.Error())
	}

	// 创建一个通道
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("Failed to open a channel: %s", err.Error())
	}

	// 交换机名字为空，则不需要创建交换机
	if config.ExchangeName != "" {
		// 声明一个持久化的交换机
		err = ch.ExchangeDeclare(
			config.ExchangeName,         // 交换机名称
			string(config.ExchangeType), // 交换机类型： topic(topics模式)/direct(routing模式)/fanout(publish/subscribe发布订阅模式)/headers/match/rabbitmq.trace
			true,                        // 是否持久化
			false,                       // 是否自动删除
			false,                       // 是否内部使用
			false,                       // 是否等待服务器相应
			nil,                         // 其它参数属性
		)
		if err != nil {
			return nil, fmt.Errorf("Failed to declare an exchange: %s", err.Error())
		}
	}

	// 申明一个持久化的队列
	queue, err := ch.QueueDeclare(
		config.QueueName, // 队列名称：如果为空则表示由RabbitMQ自动生成
		true,             // 是否持久化
		false,            // 是否自动删除：当没有任何消费者链接时删除
		false,            // 是否排他队列：只能当前程序链接
		false,            // 是否等待服务器相应
		nil,              // 其它参数属性
	)
	if err != nil {
		return nil, fmt.Errorf("Failed to declare a queue: %s", err.Error())
	}

	// 都存在时，才绑定交换机
	if config.ExchangeName != "" && config.QueueName != "" {
		// 绑定队列到交换机上
		// 将队列绑定到交换机上
		// topic模式：RoutingKey支持通配符，根据RoutingKey将消息路由至不同的队列中。
		// RoutingKey由一个或者多个单词组成，多个单词使用.分割，例如：item.insert
		// 通配符：
		// * 刚好匹配一个
		// # 匹配一个或者多个词
		// 例如：
		// fan.# 匹配fan.one.two 或者fan.one 等
		// fan.* 只能匹配fan.one
		err = ch.QueueBind(
			queue.Name,          // 队列名称
			config.RoutingKey,   // 路由键：可以使用通配符#匹配多个单词
			config.ExchangeName, // 交换机名称
			false,               // 是否等待服务器相应
			config.Headers,      // 其它参数属性
		)
	}

	// 创建一个消费者通道
	delivery, err := ch.Consume(
		queue.Name,         // 队列名称
		config.ConsumerTag, // 消费者标识符：为空则表示由RabbitMQ自动生成
		false,              // 是否自动确认：为false则需要手动提交：delivery.Ack()，delivery.Nack()，delivery.Reject()
		false,              // 是否排他消费
		false,              // 是否阻塞
		false,              // 是否等待服务器相应
		nil,                // 其它参数属性
	)
	if err != nil {
		return nil, fmt.Errorf("Failed to register a consumer: %s", err.Error())
	}

	consumer := &Consumer{
		config:     &config,
		connection: conn,
		channel:    ch,
		queue:      &queue,
		delivery:   delivery,
		done:       make(chan error),
	}
	return consumer, nil
}

func (c *Consumer) Consume(ctx context.Context, handler ConsumerHandler) {

	go func() {
		cleanup := func() {
			log.Printf("handle: deliveries channel closed")
			c.done <- nil
		}

		defer cleanup()

		go func() {
			err := <-c.connection.NotifyClose(make(chan *amqp.Error))
			log.Printf("revceived connection close: %s", err.Error())
		}()

		for delivery := range c.delivery {
			/*
				log.Printf("Reveived message: %s\n", string(delivery.Body))
				time.Sleep(time.Millisecond)
				// 手动提交确认：multiple 参数表示是否确认多个消息：false表示确认单个消息。true表示确认多个消息。
				_ = delivery.Ack(false)
				// 手动拒绝确认： requeue：false表示不重新入队，true表示重新入队
				// 【拒绝当前特定的一个消息】
				_ = delivery.Reject(false)
				// 手动拒绝确认：multiple 参数表示是否将一次性拒绝多个消息。requeue 参数表示是否重新将消息放回队列中。
				// 【除了拒绝当前特定的一个消息之外，还可以反悔/拒绝之前已确认的消息，从而重新消费之前的消息】
				_ = delivery.Nack(false, true)
			*/
			if requeue, err := handler(ctx, delivery); err != nil {
				_ = delivery.Reject(requeue)
			} else {
				_ = delivery.Ack(false)
			}
		}
	}()
}

func (c *Consumer) Close() error {
	// will close() the deliveries channel
	if err := c.channel.Cancel(c.consumerTag, true); err != nil {
		return fmt.Errorf("Consumer cancel failed: %s", err.Error())
	}
	if err := c.connection.Close(); err != nil {
		return fmt.Errorf("AMQP connection close error: %s", err.Error())
	}

	defer log.Printf("AMQP Close OK")

	// wait for handle() to exit
	return <-c.done
}
