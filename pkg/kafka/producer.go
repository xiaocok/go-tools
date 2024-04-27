package pkg

import (
	"context"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

type Producer struct {
	writer *kafka.Writer
}

func NewProducer(brokers []string, topic string, opts ...Option) *Producer {
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  topic,
		Balancer:               &kafka.LeastBytes{},
		RequiredAcks:           kafka.RequireAll, // 收到所有的ACK才算成功
		AllowAutoTopicCreation: true,             // 创建Topic
	}

	for i := range opts {
		opt := opts[i]
		switch opt.(type) {
		case *authentication:
			writer.Transport = &kafka.Transport{
				Dial: kafka.DefaultDialer.DialFunc,
				SASL: plain.Mechanism{
					Username: opt.(*authentication).Username,
					Password: opt.(*authentication).Password,
				},
			}
		}
	}

	return &Producer{
		writer: writer,
	}
}

func (p *Producer) Produce(ctx context.Context, message kafka.Message) error {
	return p.writer.WriteMessages(ctx, message)
}
