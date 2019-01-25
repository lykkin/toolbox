package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"reflect"
	"strings"

	"github.com/gocql/gocql"
	"github.com/gorilla/mux"
)

type Span struct {
	TraceId    string            `json:"trace_id"`
	SpanId     string            `json:"span_id"`
	ParentId   string            `json:"parent_id"`
	Name       string            `json:"name"`
	StartTime  float64           `json:"start_time"`
	FinishTime float64           `json:"finish_time"`
	Category   string            `json:"category"`
	Tags       map[string]string `json:"tags"`
	LicenseKey string
	EntityName string
	EntityId   string
}

func NewSpanCollector(session *gocql.Session) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {

		queryParams := r.URL.Query()
		licenseKey := queryParams["license_key"][0]
		entityName := queryParams["entity_name"][0]
		entityId := queryParams["entity_id"][0]

		incomingSpans := []Span{}
		body, _ := ioutil.ReadAll(r.Body)
		json.Unmarshal(body, &incomingSpans)

		var values []interface{} = make([]interface{}, 0)

		query := "BEGIN BATCH "
		for _, span := range incomingSpans {
			spanType := reflect.TypeOf(span)
			numFields := spanType.NumField()
			fields := make([]string, 0)
			valuePlaceholders := make([]string, 0)
			for i := 0; i < numFields; i++ {
				field := spanType.Field(i)
				jTag := field.Tag.Get("json")
				if jTag == "" {
					continue
				}
				fields = append(fields, jTag)
				valuePlaceholders = append(valuePlaceholders, "?")
				values = append(values, getField(&span, field.Name))
			}

			query += "INSERT INTO span_collector.span (license_key, entity_name, entity_id, " + strings.Join(fields, ", ") + ") VALUES (\"" + licenseKey + "\",\"" + entityName + "\",\"" + entityId + "\"," + strings.Join(valuePlaceholders, ", ") + ");"
		}
		query += "APPLY BATCH;"
		log.Printf("The query is: %s", query)
		log.Printf("From insert: %s", session.Query(query, values...).Exec())
	}
}

func getField(s *Span, field string) interface{} {
	r := reflect.ValueOf(s)
	f := reflect.Indirect(r).FieldByName(field)
	return f.Interface()
}

func NewSpanViewer(session *gocql.Session) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		spans := make([]Span, 0)

		iter := session.Query("SELECT * FROM span_collector.span;").Iter()
		for {
			row := make(map[string]interface{})
			if !iter.MapScan(row) {
				break
			}

			var span Span

			spanType := reflect.TypeOf(span)
			spanValue := reflect.ValueOf(&span).Elem()
			numFields := spanValue.NumField()
			for i := 0; i < numFields; i++ {
				field := spanType.Field(i)
				tag := field.Tag.Get("json")
				spanValue.FieldByName(field.Name).Set(reflect.ValueOf(row[tag]))
			}

			span.LicenseKey = row["license_key"].(string)
			span.EntityName = row["entity_name"].(string)
			span.EntityId = row["entity_id"].(string)

			spans = append(spans, span)
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
	log.Fatal(http.ListenAndServe(":12345", nil))
}
