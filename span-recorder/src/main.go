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

func doQuery(values *[]interface{}, query *string, session *gocql.Session) error {
	batchQuery := "BEGIN BATCH " + *query + "APPLY BATCH;"
	return session.Query(batchQuery, *values...).Exec()
}

type errorHandler struct {
	errWriter   *sm.ErrorMessageProducer
	errProducer *st.ErrorProducer
}

func (eh *errorHandler) handleErr(messageId *string, err *error) {
	writerErr := eh.errWriter.Write(
		*messageId,
		eh.errProducer.Produce((*err).Error(), "", "insert"), //TODO: get a stack
	)
	if writerErr != nil {
		log.Fatalln(*err)
	}
}

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

	errWriter := sm.NewErrorMessageProducer()
	errProducer := st.NewErrorProducer("span-recorder")
	errHandler := &errorHandler{
		errWriter:   errWriter,
		errProducer: errProducer,
	}

	go func() {
		for {
			result := make(map[string]interface{})
			session.Query("SELECT COUNT(*) FROM " + TABLE_NAME).MapScan(result)
			log.Println("counts yo:", result)
			time.Sleep(10 * time.Second)
		}
	}()

	// used to control the max number of spans written per batch
	MAX_BATCH_SIZE := 15
	placeholderValues := []string{"?"}
	for msg := range msgChan {
		// TODO: break this up into smaller chunks, cassandra will only
		// accept payloads less than 50kb
		query := ""
		values := make([]interface{}, 0)
		batchSize := 0
		for _, span := range msg.Spans {
			fields, spanValues := sdb.GetKeysAndValues(span)
			values = append(values, *spanValues...)
			query += "INSERT into " + TABLE_NAME + " (" + strings.Join(*fields, ",") + ") VALUES (" + sdb.MakePlaceholderString(&placeholderValues, len(*fields)) + ");"
			batchSize++
			if batchSize == MAX_BATCH_SIZE {
				err := doQuery(&values, &query, session)
				if err != nil {
					errHandler.handleErr(&msg.MessageId, &err)
				}
				query = ""
				values = make([]interface{}, 0)
				batchSize = 0
			}
		}
		err := doQuery(&values, &query, session)
		if err != nil {
			errHandler.handleErr(&msg.MessageId, &err)
		}
	}
}
