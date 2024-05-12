package rabbitmq

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Producer struct {
	config     ProducerConfig
	connection *amqp.Connection
	channel    *amqp.Channel
	queue      *amqp.Queue
	confirms   chan amqp.Confirmation
}

func NewProducer(config ProducerConfig) (*Producer, error) {
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

	// 开启发布确认模式: 确认消息是否到达交换机
	err = ch.Confirm(false)
	if err != nil {
		return nil, fmt.Errorf("Failed to Confirm for channel: %s", err.Error())
	}

	// 声明一个持久化的交换机：交换机不存在是，则报错
	//ch.ExchangeDeclarePassive()

	// 交换机名字为空，则不需要创建交换机
	if config.ExchangeName != "" {
		// 声明一个持久化的交换机：交换机不存在是，则创建
		err = ch.ExchangeDeclare(
			config.ExchangeName,         // 交换机名称
			string(config.ExchangeType), // 交换机类型： topic(topics模式)主题模式/direct(routing模式)路由模式、直连模式/fanout(publish/subscribe发布订阅模式)广播模式/headers/match/rabbitmq.trace
			true,                        // 指定交换机是否持久化： RabbitMQ重启后交换机仍然存在，交换机元素持久化
			false,                       // 是否自动删除交换机：指定交换机在没有任何队列绑定时是否自动删除交换机
			false,                       // 是否内部使用
			false,                       // 是否等待服务器相应
			nil,                         // 交换机的结构化参数
		)
		if err != nil {
			return nil, fmt.Errorf("Failed to declare an exchange: %s", err.Error())
		}
	}

	// 路由模式/直连模式(direct)：RoutingKey为队列名称，消息会直接发送到队列
	// c(fanout)/发布订阅模式：RoutingKey为空，消息会发送到所有绑定到交换机的队列
	// 主题模式(topic)：RoutingKey为主题表达式，消息会发送到所有匹配主题表达式的队列，交换机根据路由键匹配到对应的队列，队列可以接收到消息
	// 匹配模式(match)：RoutingKey为正则表达式，消息会发送到所有匹配正则表达式的队列，交换机根据路由键匹配到对应的队列，队列可以接收到消息
	// 头模式(headers)：RoutingKey为键值对，消息会发送到所有匹配键值对的队列，交换机根据路由键匹配到对应的队列，队列可以接收到消息
	// 跟踪模式(rabbitmq.trace)： RoutingKey为空，消息会发送到所有跟踪模式的队列，交换机记录消息的详细信息，可以查看消息的详细信息，如消息的路由、交换机、队列等信息

	// 声明一个持久化的队列：队列不存在是，则报错
	//ch.QueueDeclarePassive()

	// 生成者不指定Queue：则不创建队列，则直接发送至交换机
	// 消费者不指定Queue：如果为空则表示由RabbitMQ自动生成
	var queue amqp.Queue
	if config.QueueName != "" {
		// 消费者自己声明队列，并将队列绑定到交换机上，消费队列中的消息
		queue, err = ch.QueueDeclare(
			config.QueueName, // 队列名称：如果为空则表示由RabbitMQ自动生成
			true,             // 队列是否持久化：队列元素/队列本身持久化，不是队列的消息持久化
			false,            // 是否自动删除：当没有任何消费者链接时删除
			false,            // 是否排他队列：只能当前程序链接
			false,            // 是否等待服务器相应
			nil,              // 队列的结构化参数：比如设置消息TTL、最大长度、死信队列、磁盘队列等
		)
		if err != nil {
			return nil, fmt.Errorf("Failed to declare a queue: %s", err.Error())
		}
	}

	// 都存在时，才绑定交换机
	if config.ExchangeName != "" && config.QueueName != "" {
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
			config.RoutingKey,   // 路由键: 匹配模式、主题模式、头模式、跟踪模式使用，直连模式、广播模式不需要
			config.ExchangeName, // 交换机名称
			false,
			nil,
		)
		if err != nil {
			return nil, fmt.Errorf("Failed to bind a queue: %s", err.Error())
		}
	}

	producer := &Producer{
		config:     config,
		connection: conn,
		channel:    ch,
		queue:      &queue,
		confirms:   ch.NotifyPublish(make(chan amqp.Confirmation, 1)), // 创建一个用于接收确认信息的通道
	}
	return producer, nil
}

func (p *Producer) Publish(ctx context.Context, data []byte) error {
	//confirms := p.channel.NotifyPublish(make(chan amqp.Confirmation, 1))
	var err error
	if ctx == nil {
		err = p.channel.Publish(
			p.config.ExchangeName, // 交换机名称
			p.config.RoutingKey,   // 路由键
			false,                 // 强制性标志
			false,                 // 立即标志
			amqp.Publishing{
				ContentType:  "text/plain",    // 内容类型
				DeliveryMode: amqp.Persistent, // 持久化：RabbitMQ重启后消息仍然存在
				Body:         data,            // 消息体
			},
		)
	} else {
		err = p.channel.PublishWithContext(
			ctx,
			p.config.ExchangeName, // 交换机名称
			p.config.RoutingKey,   // 路由键
			false,                 // 强制性标志
			false,                 // 立即标志
			amqp.Publishing{
				ContentType:  "text/plain",    // 内容类型
				DeliveryMode: amqp.Persistent, // 持久化：RabbitMQ重启后消息仍然存在
				Body:         data,            // 消息体
			},
		)
	}
	if err != nil {
		return err
	}

	// 等待消息确认
	if confirmed := <-p.confirms; confirmed.Ack {
		//fmt.Println("Message confirmed:", confirmed.DeliveryTag)
		return nil
	} else {
		//fmt.Println("Message failed to be confirmed:", confirmed.DeliveryTag)
		return fmt.Errorf("Message has been nacked (not confirmed)")
	}
	// returnd := <-p.channel.NotifyReturn() // 开启返回通知
}

func (p *Producer) AmqpPublishing(ctx context.Context, publishing *amqp.Publishing) error {
	var err error
	if ctx == nil {
		err = p.channel.Publish(
			p.config.ExchangeName, // 交换机名称
			p.config.RoutingKey,   // 路由键
			false,                 // 强制性标志
			false,                 // 立即标志
			*publishing,
		)
	} else {
		err = p.channel.PublishWithContext(
			ctx,
			p.config.ExchangeName, // 交换机名称
			p.config.RoutingKey,   // 路由键
			false,                 // 强制性标志
			false,                 // 立即标志
			*publishing,
		)
	}
	if err != nil {
		return err
	}

	// 等待消息确认
	if confirmed := <-p.confirms; confirmed.Ack {
		return nil
	} else {
		return fmt.Errorf("Message has been nacked (not confirmed)")
	}
}
