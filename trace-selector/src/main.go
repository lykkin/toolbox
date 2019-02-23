package main

import (
	"log"
	"time"

	sdb "shared/db"
	sm "shared/message"
	st "shared/types"
)

type InterestingTrace struct {
	TraceId string `cassandra: "trace_id"`
}

func isInteresting(s *st.Span) bool {
	// just check if it has an error for now
	_, ok := s.Tags["error"]
	return ok
}

func main() {
	//setup cassandra
	// TODO: make this less awful (e.g. do proper migrations)
	// TODO?: tie this to the cassandra tags on the struct we are using
	// to interface with this thing
	KEYSPACE := "span_collector"
	TABLE_NAME := KEYSPACE + ".interesting_traces"
	tableSchema := map[string]string{
		"inserted_at": "timestamp",
		"trace_id":    "text",
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

	errWriter := sm.NewErrorMessageProducer()
	errProducer := st.NewErrorProducer("trace-selector")

	go func() {
		for {
			result := make(map[string]interface{})
			session.Query("SELECT COUNT(*) FROM " + TABLE_NAME).MapScan(result)
			log.Println("counts yo:", result)
			time.Sleep(10 * time.Second)
		}
	}()

	for msg := range msgChan {
		interestingTraces := make(map[string]bool)
		for _, span := range msg.Spans {
			if isInteresting(&span) {
				interestingTraces[span.TraceId] = true
			}
		}
		if len(interestingTraces) > 0 {
			query := "BEGIN BATCH "
			values := make([]interface{}, len(interestingTraces))
			idx := 0
			for traceId, _ := range interestingTraces {
				query += "INSERT into " + TABLE_NAME + "(inserted_at, trace_id) VALUES (toUnixTimestamp(now()), ?);"
				values[idx] = traceId
				idx++
			}
			query += "APPLY BATCH;"
			err := session.Query(query, values...).Exec()
			if err != nil {
				writerErr := errWriter.Write(
					msg.MessageId,
					errProducer.Produce(err.Error(), "", "insert"), //TODO: get a stack
				)
				if writerErr != nil {
					log.Fatalln(err)
				}
			}
		}
	}
}
