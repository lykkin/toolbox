package main

import (
	"log"
	"shared"
	"strings"
	"time"

	"github.com/gocql/gocql"
)

func setupCassandraSchema() {
	cluster := gocql.NewCluster("cassandra")
	cluster.CreateSession()
	session, err := cluster.CreateSession()

	if err != nil {
		log.Fatalln(err)
	}

	defer session.Close()

	if err := createKeyspace(session); err != nil {
		log.Fatalln(err)
	}
	if err := createSpanTable(session); err != nil {
		log.Fatalln(err)
	}
}

func createKeyspace(session *gocql.Session) error {
	query := "CREATE KEYSPACE IF NOT EXISTS span_collector WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};"
	return session.Query(query).Exec()
}

func createSpanTable(session *gocql.Session) error {
	query := "CREATE TABLE IF NOT EXISTS span_collector.span (trace_id text, span_id text, parent_id text, name text, start_time double, finish_time double, tags map<text,text>, license_key text, entity_id text, entity_name text, PRIMARY KEY(trace_id, span_id));"
	return session.Query(query).Exec()
}

func main() {
	//setup cassandra
	setupCassandraSchema()
	cluster := gocql.NewCluster("cassandra")
	cluster.Consistency = gocql.One
	cluster.Keyspace = "span_collector"
	session, err := cluster.CreateSession()

	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	//read from kafka
	reader := shared.NewSpanMessageConsumer("span-recorders")
	msgChan := make(chan shared.SpanMessage)
	reader.Start(msgChan)

	go func() {
		result := make(map[string]interface{})
		session.Query("SELECT COUNT(*) FROM span_collector.span").MapScan(result)
		log.Println("counts yo:", result)
		time.Sleep(1 * time.Second)
	}()

	for msg := range msgChan {
		query := "BEGIN BATCH "
		values := make([]interface{}, 0)
		for _, span := range msg.Spans {
			fields, spanValues := shared.GetKeysAndValues(span)
			qms := make([]string, len(*fields))
			for i := range *fields {
				qms[i] = "?"
			}
			values = append(values, *spanValues...)
			query += "INSERT into span_collector.span (" + strings.Join(*fields, ",") + ") VALUES (" + strings.Join(qms, ",") + ");"
		}
		query += "APPLY BATCH;"
		log.Println("values yo:", values)
		log.Println("query yo:", query)
		if err := session.Query(query, values...).Exec(); err != nil {
			log.Fatalln(err)
		}
	}
}
