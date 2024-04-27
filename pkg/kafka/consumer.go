package pkg

import (
	"context"
	"errors"
	"io"
	"log"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

type Consumer struct {
	reader *kafka.Reader
}

func NewConsumer(brokers []string, topic string, groupID string, opts ...Option) *Consumer {
	config := kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: groupID,
	}

	for i := range opts {
		opt := opts[i]
		switch opt.(type) {
		case *authentication:
			config.Dialer = &kafka.Dialer{
				DialFunc: kafka.DefaultDialer.DialFunc,
				SASLMechanism: plain.Mechanism{
					Username: opt.(*authentication).Username,
					Password: opt.(*authentication).Password,
				},
			}
		}
	}

	reader := kafka.NewReader(config)
	return &Consumer{
		reader: reader,
	}
}

func (c *Consumer) Consume(ctx context.Context, handle func(ctx context.Context, msg kafka.Message) error) {
	go func() {
		for {
			msg, err := c.reader.FetchMessage(ctx)
			if errors.Is(err, io.EOF) {
				// close
				log.Printf("failed to fetch kafka message by close reader: %s\n", err.Error())
				return
			} else if err != nil {
				log.Printf("failed to fetch kafka message: %s\n", err.Error())
				continue
			}

			if err = handle(ctx, msg); err != nil {
				log.Printf("failed to handle kafka message: %s\n", err.Error())
				continue
			}

			err = c.reader.CommitMessages(ctx, msg)
			if errors.Is(err, io.ErrClosedPipe) {
				// close
				log.Printf("failed to commit kafka message by close reader: %s\n", err.Error())
				return
			} else if err != nil {
				log.Printf("failed to commit kafka messages: %s\n", err.Error())
			}
		}
	}()
}

func (c *Consumer) Close() {
	log.Printf("close kafka reader\n")
	if err := c.reader.Close(); err != nil {
		log.Printf("failed to close kafka consumer: %s\n", err.Error())
	} else {
		log.Printf("kafka consumer closed\n")
	}
}
