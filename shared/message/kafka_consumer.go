package shared

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	st "shared/types"

	"github.com/segmentio/kafka-go"
)

type SpanMessageConsumer struct {
	reader *kafka.Reader
}

func NewSpanMessageConsumer(consumerGroup string) *SpanMessageConsumer {
	return &SpanMessageConsumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:   []string{"kafka:9092"},
			GroupID:   consumerGroup,
			Topic:     "incomingSpans",
			Partition: 0,
			MaxBytes:  10e6, // 10MB
		}),
	}
}

func (k *SpanMessageConsumer) Start(msgChan chan st.SpanMessage) {
	go func() {
		for {
			m, err := k.reader.ReadMessage(context.Background())
			if err != nil {
				fmt.Printf("Consumer error: %v (%v)\n", err, m)
				close(msgChan)
				log.Fatal("dying")
			}
			//fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
			var msg st.SpanMessage
			json.Unmarshal(m.Value, &msg)
			msgChan <- msg
		}
	}()
}
