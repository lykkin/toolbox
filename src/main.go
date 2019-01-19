package main

import (
    "encoding/json"
	"fmt"
    "net/http"
    "io/ioutil"
    "log"

    "github.com/gorilla/mux"
    "github.com/gocql/gocql"
)

type Span struct {
    TraceId string `json:"trace_id"`
    SpanId string `json:"span_id"`
    ParentId string `json:"parent_id"`
    Name string `json:"name"`
    StartTime float64 `json:"start_time"`
    FinishTime float64 `json:"finish_time"`
}

func NewSpanCollector(session *gocql.Session) func(http.ResponseWriter, *http.Request) {
    return func(w http.ResponseWriter, r *http.Request) {
        incomingSpans := []Span{}
        body, _ := ioutil.ReadAll(r.Body)
        json.Unmarshal(body, &incomingSpans)
        query := "BEGIN BATCH "
        var values []interface{} = make([]interface{}, 0)
        for _, span := range incomingSpans {
            query += "INSERT INTO span_collector.span (name, span_id, parent_id, trace_id, start_time, finish_time) VALUES (?, ?, ?, ?, ?, ?);"
            values = append(values, span.Name, span.SpanId, span.ParentId, span.TraceId, span.StartTime, span.FinishTime)
        }
        query += "APPLY BATCH;"
        log.Printf("From insert: %s", session.Query(query, values...).Exec())
    }
}

func NewSpanViewer(session *gocql.Session) func(http.ResponseWriter, *http.Request) {
    return func(w http.ResponseWriter, r *http.Request) {
        iter := session.Query("SELECT name, span_id, trace_id, parent_id, start_time, finish_time FROM span_collector.span;").Iter()
        log.Print(iter)
        spans := make([]Span, 0)

        var name string
        var traceId string
        var spanId string
        var parentId string
        var startTime float64
        var finishTime float64

        for iter.Scan(&name, &spanId, &traceId, &parentId, &startTime, &finishTime) {
            spans = append(spans, Span{traceId, spanId, parentId, name, startTime, finishTime})
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
