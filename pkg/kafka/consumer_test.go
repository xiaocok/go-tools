package pkg

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

func TestConsumer(t *testing.T) {
	ctx := context.Background()
	consumer := NewConsumer([]string{"192.168.195.134:9081", "192.168.195.134:9082", "192.168.195.134:9083"}, "test", "group-test")
	consumer.Consume(ctx, func(ctx context.Context, msg kafka.Message) error {
		log.Printf("key: %s, value=%s\n", string(msg.Key), string(msg.Value))
		return nil
	})
	time.Sleep(time.Second * 10)
	consumer.Close()
}
