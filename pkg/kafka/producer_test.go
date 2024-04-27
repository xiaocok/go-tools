package pkg

import (
	"context"
	"strconv"
	"testing"

	"github.com/segmentio/kafka-go"
)

func TestProducer(t *testing.T) {
	ctx := context.Background()
	producer := NewProducer([]string{"192.168.195.134:9081", "192.168.195.134:9082", "192.168.195.134:9083"}, "test")
	for i := 0; i < 3; i++ {
		err := producer.Produce(ctx, kafka.Message{Key: []byte("key" + strconv.Itoa(i)), Value: []byte("value" + strconv.Itoa(i))})
		if err != nil {
			t.Errorf("Produce failed: %s\n", err.Error())
		}
	}
}
