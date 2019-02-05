package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"shared"

	"github.com/gorilla/mux"
	"github.com/segmentio/kafka-go"
)

func NewSpanCollector(p *kafka.Writer) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
        // TODO: set some proper response codes

		queryParams := r.URL.Query()
		licenseKeyParams, ok := queryParams["license_key"]
		if !ok {
			fmt.Fprint(w, "license_key query param is required")
			return
		}

		licenseKey := licenseKeyParams[0]

		entityNameParams, ok := queryParams["entity_name"]
		if !ok {
			fmt.Fprint(w, "entity_name query param is required")
			return
		}

		entityName := entityNameParams[0]

		incomingSpans := []span.Span{}
		body, _ := ioutil.ReadAll(r.Body)
		json.Unmarshal(body, &incomingSpans)

		for _, s := range incomingSpans {
			if msg, ok := s.IsValid(); !ok {
				fmt.Fprintf(w, "Invalid span format: %s\n", msg)
				return
			}
		}
		spanMessage := span.SpanMessage{
			EntityName: entityName,
			LicenseKey: licenseKey,
			Spans:      incomingSpans,
		}

		if entityId, ok := queryParams["entity_id"]; ok {
			spanMessage.EntityId = entityId[0]
		}

		msg, err := json.Marshal(spanMessage)
		if err != nil {
            fmt.Fprintf(w, "Serialization error: %s\n", err)
			return
		}

		p.WriteMessages(context.Background(),
			kafka.Message{
				Key:   []byte("msg"),
				Value: []byte(msg),
			},
		)
        // TODO: respond to the request
	}
}

func main() {
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{"kafka:9092"},
		Topic:    "incomingSpans",
		Balancer: &kafka.LeastBytes{},
	})
	defer w.Close()

	r := mux.NewRouter()
	r.HandleFunc("/", NewSpanCollector(w)).Methods("POST")
	http.Handle("/", r)

	log.Print("Listening on port 12345!")
	log.Fatal(http.ListenAndServe(":12345", nil))
}
