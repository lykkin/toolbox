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

	errWriter := sm.NewErrorMessageProducer()
	errProducer := st.NewErrorProducer("trace-selector")

	errChan := make(chan error)
	qb := sdb.NewQueryBatcher(session, errChan)

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
			for traceId, _ := range interestingTraces {
				if !qb.CanFit(1) {
					qb.Execute()
					qb.Reset()
				}
				qb.AddQuery("INSERT into "+TABLE_NAME+"(trace_id) VALUES (?);", &[]interface{}{traceId})
			}

			qb.Execute()
			qb.Reset()

			for qb.ActiveQueries > 0 {
				err := <-errChan
				if err != nil {
					writerErr := errWriter.Write(
						msg.MessageId,
						errProducer.Produce(err.Error(), "", "insert"), //TODO: get a stack
					)
					if writerErr != nil {
						log.Fatalln(err)
					}
				}
				qb.ActiveQueries--
			}
		}
	}
}
