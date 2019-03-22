package shared

import (
	"context"
	"encoding/json"
	"log"

	st "shared/types"

	"github.com/segmentio/kafka-go"
)

type ErrorMessageProducer struct {
	writer *kafka.Writer
}

func NewErrorMessageProducer() *ErrorMessageProducer {
	return &ErrorMessageProducer{
		writer: kafka.NewWriter(kafka.WriterConfig{
			Brokers:  []string{"kafka:9092"},
			Topic:    "errors",
			Balancer: &kafka.LeastBytes{},
		}),
	}
}

func (k *ErrorMessageProducer) Write(messageId string, msgErr *st.Error) error {
	errorMsg := st.ErrorMessage{
		MessageId: messageId,
		Error:     *msgErr,
	}
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

type ErrorHandler struct {
	errWriter   *ErrorMessageProducer
	errProducer *st.ErrorProducer
}

func (eh *ErrorHandler) HandleErr(messageId *string, err error, event string) {
	writerErr := eh.errWriter.Write(
		*messageId,
		eh.errProducer.Produce(
			err.Error(),
			"", //TODO: get a stack
			event,
		),
	)
	if writerErr != nil {
		log.Fatalln(writerErr)
	}
}

func NewErrorHandler(component string) *ErrorHandler {
	errWriter := NewErrorMessageProducer()
	errProducer := st.NewErrorProducer(component)
	return &ErrorHandler{
		errWriter:   errWriter,
		errProducer: errProducer,
	}
}
