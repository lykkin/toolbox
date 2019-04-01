package main

import (
	"context"
	"fmt"
	"log"
	"time"

	sdb "shared/db"
	sm "shared/message"
	st "shared/types"

	"github.com/gocql/gocql"
	"github.com/segmentio/kafka-go"
)

var KEYSPACE string = "span_collector"
var TABLE_NAME string = KEYSPACE + ".interesting_traces"

// TODO: include reason for selection
type InterestingTrace struct {
	TraceId string `cassandra: "trace_id"`
}

func isInteresting(s *st.Span) bool {
	// just check if it has an error for now
	_, ok := s.Tags["error"]
	return ok
}

func startTraceMessageConsumer(session *gocql.Session) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"kafka:9092"},
		GroupID:   "traceConsumers",
		Topic:     "interestingTraces",
		Partition: 0,
		MaxBytes:  10e6, // 10MB
	})
	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			fmt.Printf("Consumer error: %v (%v)\n", err, m)
			log.Fatal("dying")
		}
		traceId := string(m.Value)
		log.Print("got an interesting trace ", traceId)
		err = session.Query("INSERT into "+TABLE_NAME+" (trace_id) VALUES (?);", traceId).Exec()
		if err != nil {
			fmt.Printf("Consumer error (on insert): %v\n", err)
			log.Fatal("dying")
		}
	}
}

func main() {
	//setup cassandra
	// TODO: make this less awful (e.g. do proper migrations)
	// TODO?: tie this to the cassandra tags on the struct we are using
	// to interface with this thing
	tableSchema := map[string]string{
		"trace_id": "text",
	}
	session, err := sdb.SetupCassandraSchema(KEYSPACE, TABLE_NAME, tableSchema, "trace_id")
	for err != nil {
		log.Print("ran into an error while setting up cassandra, waiting 5 seconds: ", err)
		time.Sleep(5 * time.Second)
		session, err = sdb.SetupCassandraSchema(KEYSPACE, TABLE_NAME, tableSchema, "trace_id")
	}
	defer session.Close()

	//read from kafka
	reader := sm.NewSpanMessageConsumer("trace-selectors")
	msgChan := make(chan st.SpanMessage)
	reader.Start(msgChan)

	go startTraceMessageConsumer(session)

	errHandler := sm.NewErrorHandler("trace-selector")

	for msg := range msgChan {
		interestingTraces := make(map[string]bool)
		for _, span := range msg.Spans {
			if isInteresting(&span) {
				if msg.LicenseKey == "" {
					// TODO: mark this as an error. there will be no way to send this without
					// the right credentials
					continue
				}
				interestingTraces[span.TraceId] = true
			}
		}
		if len(interestingTraces) > 0 {
			batch := gocql.NewBatch(gocql.LoggedBatch)
			for traceId, _ := range interestingTraces {
				batch.Query("INSERT into "+TABLE_NAME+"(trace_id) VALUES (?);", traceId)
				if batch.Size() >= 10 {
					err := session.ExecuteBatch(batch)
					if err != nil {
						log.Print(err)
						errHandler.HandleErr(
							&msg.MessageId,
							err,
							"insert",
						)
					}
					batch = gocql.NewBatch(gocql.LoggedBatch)
				}
			}

			err := session.ExecuteBatch(batch)
			if err != nil {
				log.Print(err)
				errHandler.HandleErr(
					&msg.MessageId,
					err,
					"insert",
				)
			}
		}
	}
}
