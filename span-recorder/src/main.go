package main

import (
	"log"
	"strings"
	"time"

    "context"
    "encoding/json"

	sdb "shared/db"
	sm "shared/message"
	st "shared/types"

    "github.com/segmentio/kafka-go"
)

func main() {
	//setup cassandra
	// TODO: make this less awful (e.g. do proper migrations)
	// TODO?: tie this to the cassandra tags on the struct we are using
	// to interface with this thing
	KEYSPACE := "span_collector"
	TABLE_NAME := KEYSPACE + ".spans"
	tableSchema := map[string]string{
		"trace_id":    "text",
		"span_id":     "text",
		"parent_id":   "text",
		"name":        "text",
		"sent":        "Boolean",
		"start_time":  "double",
		"finish_time": "double",
		"license_key": "text",
		"entity_id":   "text",
		"entity_name": "text",
		"tags":        "map<text,text>",
	}
	session, err := sdb.SetupCassandraSchema(KEYSPACE, TABLE_NAME, tableSchema, "trace_id, span_id, sent")
	for err != nil {
		log.Print("ran into an error while setting up cassandra, waiting 5 seconds: ", err)
		time.Sleep(5 * time.Second)
		session, err = sdb.SetupCassandraSchema(KEYSPACE, TABLE_NAME, tableSchema, "trace_id, sent, span_id")
	}
	defer session.Close()

	//read from kafka
	reader := sm.NewSpanMessageConsumer("span-recorders")
	msgChan := make(chan st.SpanMessage)
	reader.Start(msgChan)

	errHandler := sm.NewErrorHandler("span-recorder")

    w := kafka.NewWriter(kafka.WriterConfig{
        Brokers: []string{"kafka:9092"},
        Topic: "parentSpans",
        Balancer: &kafka.LeastBytes{},
    })

	go func() {
		for {
			result := make(map[string]interface{})
			session.Query("SELECT COUNT(*) FROM " + TABLE_NAME).MapScan(result)
			log.Println("counts yo:", result)
			time.Sleep(10 * time.Second)
		}
	}()

	queryErrChan := make(chan error)
	qb := sdb.NewQueryBatcher(session, queryErrChan)
	placeholderValues := []string{"?"}

	for msg := range msgChan {
        parentSpans := make([]st.Span, 0)
		// TODO: break this up into smaller chunks, cassandra will only
		// accept payloads less than 50kb
		for _, span := range msg.Spans {
            if span.ParentId == "" {
                parentSpans = append(parentSpans, span)
            }

			fields, spanValues := sdb.GetKeysAndValues(span)

			// Add on all the message level info
			*fields = append(*fields, "entity_name")
			*spanValues = append(*spanValues, msg.EntityName)

			if msg.LicenseKey != "" {
				*fields = append(*fields, "license_key")
				*spanValues = append(*spanValues, msg.LicenseKey)
			}

			if msg.EntityId != "" {
				*fields = append(*fields, "entity_id")
				*spanValues = append(*spanValues, msg.EntityId)
			}
			query := "INSERT INTO " + TABLE_NAME + " (sent, " + strings.Join(*fields, ",") + ") VALUES (false, " + sdb.MakePlaceholderString(&placeholderValues, len(*fields)) + ");"
			if !qb.CanFit(1) {
                err := qb.SyncExecute()
                if err != nil {
                    log.Print(err)
                    errHandler.HandleErr(
                        &msg.MessageId,
                        err,
                        "insert",
                    )
                }
				qb.Reset()
			}
			qb.AddQuery(query, spanValues)
		}

        err := qb.SyncExecute()
        if err != nil {
            log.Print(err)
            errHandler.HandleErr(
                &msg.MessageId,
                err,
                "insert",
            )
        }
        qb.Reset()

        if len(parentSpans) > 0 {
            spanMessage := st.SpanMessage{
                EntityName: msg.EntityName,
                MessageId:  msg.MessageId,
                Spans:      parentSpans,
            }

            msg, err := json.Marshal(spanMessage)
            if err != nil {
                log.Printf("Serialization error: %s\n", err)
            } else {
                w.WriteMessages(context.Background(),
                    kafka.Message{
                        Key: []byte("msg"),
                        Value: []byte(msg),
                    },
                )
            }
        }
	}
}
