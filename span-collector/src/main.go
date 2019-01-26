package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"shared"

	"github.com/gocql/gocql"
	"github.com/gorilla/mux"
)

func NewSpanCollector(session *gocql.Session) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {

		queryParams := r.URL.Query()

		incomingSpans := []span.Span{}
		body, _ := ioutil.ReadAll(r.Body)
		json.Unmarshal(body, &incomingSpans)

		var values []interface{} = make([]interface{}, 0)

		query := "BEGIN BATCH "
		for _, span := range incomingSpans {
			spanQuery, spanValues := span.GetInsertQueryAndValues(queryParams)

			query += spanQuery
			values = append(values, spanValues...)
		}
		query += "APPLY BATCH;"
		log.Printf("The query is: %s", query)
		log.Printf("The values are: %s", values)
		log.Printf("The query params are %s", queryParams)
		log.Printf("From insert: %s", session.Query(query, values...).Exec())
	}
}

func NewSpanViewer(session *gocql.Session) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		spans := make([]span.Span, 0)

		iter := session.Query("SELECT * FROM span_collector.span;").Iter()
		for {
			row := make(map[string]interface{})
			if !iter.MapScan(row) {
				break
			}

			spans = append(spans, span.FromRow(row))
		}

		if err := iter.Close(); err != nil {
			log.Fatal(err)
		}

		body, _ := json.Marshal(spans)
		log.Printf("Sending %s", body)
		fmt.Fprintf(w, "%s\n", body)
	}
}

func main() {
	r := mux.NewRouter()
	cluster := gocql.NewCluster("cassandra")
	cluster.Keyspace = "span_collector"
	cluster.Consistency = gocql.One
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()
	r.HandleFunc("/", NewSpanCollector(session)).Methods("POST")
	r.HandleFunc("/", NewSpanViewer(session)).Methods("GET")
	http.Handle("/", r)
	log.Print("Listening on port 12345!")
	log.Fatal(http.ListenAndServe(":12345", nil))
}
