package main

import (
	"encoding/json"
	"log"
	"time"

	"shared"

	"github.com/segmentio/kafka-go"
)

func startReader(msgChan chan shared.ErrorMessage) error {
    r := kafka.NewReader(kafka.ReaderConfig{
        Brokers:   []string{"kafka:9092"},
        GroupID:   "error-consumers",
        Topic:     "errors",
        Partition: 0,
        MaxBytes:  10e6, // 10MB
    })
    // TODO: dedupe this with the shared kafka consumer
    // probably a switch on the topic and return an interface that is
    // cast by the thing consuming the channel
    go func() {
		for {
			m, err := r.ReadMessage(context.Background())
			if err != nil {
				fmt.Printf("Consumer error: %v (%v)\n", err, m)
				close(msgChan)
				log.Fatal("dying")
			}
			//fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
			var msg ErrorMessage
			json.Unmarshal(m.Value, &msg)
			msgChan <- msg
		}
    }()
    return nil
}

func main() {
	msgChan := make(chan shared.ErrorMessage)
    startReader(msgChan)

    for msg := range msgChan {
        log.Print(msg)
    }
}
