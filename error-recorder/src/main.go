package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	sdb "shared/db"
	st "shared/types"

	"github.com/segmentio/kafka-go"
)

func startReader(msgChan chan st.ErrorMessage) error {
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
			var msg st.ErrorMessage
			json.Unmarshal(m.Value, &msg)
			msgChan <- msg
		}
	}()
	return nil
}

func main() {
	KEYSPACE := "span_collector"
	TABLE_NAME := KEYSPACE + ".system_errors"
	tableSchema := map[string]string{
		"message":   "text",
		"stack":     "text",
		"timestamp": "timestamp",
		"component": "text",
		"event":     "text",
	}
	session, err := sdb.SetupCassandraSchema(KEYSPACE, TABLE_NAME, tableSchema, "timestamp")
	for err != nil {
		log.Print("ran into an error while setting up cassandra, waiting 5 seconds: ", err)
		time.Sleep(5 * time.Second)
		session, err = sdb.SetupCassandraSchema(KEYSPACE, TABLE_NAME, tableSchema, "component, timestamp")
	}
	defer session.Close()

	msgChan := make(chan st.ErrorMessage)
	startReader(msgChan)

	placeholderValues := []string{"?"}
	for msg := range msgChan {
		query := "BEGIN BATCH "
		values := make([]interface{}, 0)
		for _, e := range msg.Errors {
			fields, errorValues := sdb.GetKeysAndValues(e)
			values = append(values, *errorValues...)
			query += "INSERT into " + TABLE_NAME + " (" + strings.Join(*fields, ",") + ") VALUES (" + sdb.MakePlaceholderString(&placeholderValues, len(*fields)) + ");"
		}
		query += "APPLY BATCH;"
		err := session.Query(query, values...).Exec()
		if err != nil {
			log.Fatalln(err)
		}
	}
}
