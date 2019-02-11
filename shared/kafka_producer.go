package shared

import (
	"context"
	"encoding/json"

	"github.com/segmentio/kafka-go"
)

type ErrorMessageProducer struct {
	writer *kafka.Writer
}

func NewErrorMessageProducer() *ErrorMessageProducer {
	return &ErrorMessageProducer{
		writer: kafka.NewWriter(kafka.WriterConfig{
			Brokers:   []string{"kafka:9092"},
			Topic:     "errors",
		Balancer: &kafka.LeastBytes{},
		}),
	}
}

func (k *ErrorMessageProducer) Write(errorMsg ErrorMessage) error {
	msg, err := json.Marshal(errorMsg)
	if err != nil {
		return err
	}
	k.writer.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte("msg"),
			Value: []byte(msg),
		},
	)
	return nil
}
