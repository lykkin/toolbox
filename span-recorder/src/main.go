package main

import (
	"log"
	"strings"
	"time"

	sdb "shared/db"
	sm "shared/message"
	st "shared/types"
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
		"start_time":  "double",
		"finish_time": "double",
		"tags":        "map<text,text>",
	}
	session, err := sdb.SetupCassandraSchema(KEYSPACE, TABLE_NAME, tableSchema, "trace_id, span_id")
	for err != nil {
		log.Print("ran into an error while setting up cassandra, waiting 5 seconds: ", err)
		time.Sleep(5 * time.Second)
		session, err = sdb.SetupCassandraSchema(KEYSPACE, TABLE_NAME, tableSchema, "trace_id, span_id")
	}
	defer session.Close()

	//read from kafka
	reader := sm.NewSpanMessageConsumer("span-recorders")
	msgChan := make(chan st.SpanMessage)
	reader.Start(msgChan)

	go func() {
		for {
			result := make(map[string]interface{})
			session.Query("SELECT COUNT(*) FROM " + TABLE_NAME).MapScan(result)
			log.Println("counts yo:", result)
			time.Sleep(10 * time.Second)
		}
	}()

	placeholderValues := []string{"?"}
	for msg := range msgChan {
		query := "BEGIN BATCH "
		values := make([]interface{}, 0)
		for _, span := range msg.Spans {
			fields, spanValues := sdb.GetKeysAndValues(span)
			values = append(values, *spanValues...)
			query += "INSERT into " + TABLE_NAME + " (" + strings.Join(*fields, ",") + ") VALUES (" + sdb.MakePlaceholderString(&placeholderValues, len(*fields)) + ");"
		}
		query += "APPLY BATCH;"
		err := session.Query(query, values...).Exec()
		if err != nil {
			log.Fatalln(err)
		}
	}
}
