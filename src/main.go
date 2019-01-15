package main

import (
    "encoding/json"
	"fmt"
    "net/http"
    "io/ioutil"
    "log"

    "github.com/gorilla/mux"
)

type Span struct {
    TraceId string `json:"trace_id"`
    SpanId string `json:"span_id"`
    Name string `json:"name"`
    StartTime float64 `json:"start_time"`
    FinishTime float64 `json:"finish_time"`
}

func NewSpanCollector(spans *[]Span) func(http.ResponseWriter, *http.Request) {
    return func(w http.ResponseWriter, r *http.Request) {
        incomingSpans := []Span{}
        body, _ := ioutil.ReadAll(r.Body)
        json.Unmarshal(body, &incomingSpans)
        *spans = append(*spans, incomingSpans...)
    }
}

func NewSpanViewer(spans *[]Span) func(http.ResponseWriter, *http.Request) {
    return func(w http.ResponseWriter, r *http.Request) {
        body, _ := json.Marshal(*spans)
        fmt.Fprintf(w, "%s\n", body)
    }
}

func main() {
    r := mux.NewRouter()
    spans := make([]Span, 0)
    r.HandleFunc("/", NewSpanCollector(&spans)).Methods("POST")
    r.HandleFunc("/", NewSpanViewer(&spans)).Methods("GET")
    http.Handle("/", r)
    log.Fatal(http.ListenAndServe(":12345", nil))
}
