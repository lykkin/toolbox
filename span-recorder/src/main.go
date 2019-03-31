package main

import (
	"log"
	"strings"
	"time"

	sdb "shared/db"
	sm "shared/message"
	st "shared/types"

	"github.com/gocql/gocql"
)

func main() {
	//setup cassandra
	// TODO: make this less awful (e.g. do proper migrations)
	// TODO?: tie this to the cassandra tags on the struct we are using
	// to interface with this thing
	KEYSPACE := "span_collector"
	TABLE_NAME := KEYSPACE + ".spans"
	tableSchema := map[string]string{
		"trace_id":     "text",
		"span_id":      "text",
		"parent_id":    "text",
		"name":         "text",
		"sent":         "Boolean",
		"start_time":   "double",
		"finish_time":  "double",
		"license_key":  "text",
		"entity_id":    "text",
		"entity_name":  "text",
		"string_tags":  "map<text,text>",
		"boolean_tags": "map<text,Boolean>",
		"number_tags":  "map<text,double>",
	}
	session, err := sdb.SetupCassandraSchema(KEYSPACE, TABLE_NAME, tableSchema, "trace_id, sent, span_id")
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

	go func() {
		for {
			result := make(map[string]interface{})
			e := session.Query("SELECT COUNT(*) FROM " + TABLE_NAME).MapScan(result)
			log.Println("counts yo:", result)
			log.Println("err yo:", e)
			time.Sleep(10 * time.Second)
		}
	}()

	placeholderValues := []string{"?"}

	for msg := range msgChan {
		batch := gocql.NewBatch(gocql.LoggedBatch)
		// TODO: break this up into smaller chunks, cassandra will only
		// accept payloads less than 50kb
		for _, span := range msg.Spans {
			record := st.SpanToRecord(span)
			fields, spanValues := sdb.GetKeysAndValues(*record)

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
			batch.Query(query, *spanValues...)
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
