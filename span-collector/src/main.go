package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	st "shared/types"

	"github.com/gorilla/mux"
	"github.com/satori/go.uuid"
	"github.com/segmentio/kafka-go"
)

func NewSpanCollector(p *kafka.Writer, rsw *kafka.Writer) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		// TODO: set some proper response codes

		queryParams := r.URL.Query()
		licenseKeyParams, licenseKeyFound := queryParams["license_key"]
		insightsKeyParams, insightsKeyFound := queryParams["insights_key"]

		if !licenseKeyFound && !insightsKeyFound {
			fmt.Fprint(w, "one (or both) of license_key or insights_key query params are required")
			return
		}

		entityNameParams, ok := queryParams["entity_name"]
		if !ok {
			fmt.Fprint(w, "entity_name query param is required")
			return
		}

		entityName := entityNameParams[0]

		incomingSpans := []st.Span{}
		rootSpans := []st.Span{}
		body, _ := ioutil.ReadAll(r.Body)
		json.Unmarshal(body, &incomingSpans)

		for _, s := range incomingSpans {
			if msg, ok := s.IsValid(); !ok {
				fmt.Fprintf(w, "Invalid span format: %s\n", msg)
				return
			}
			if s.ParentId == "" { // TODO: check for entry point tag for this
				rootSpans = append(rootSpans, s)
			}
		}
		messageId, err := uuid.NewV4()
		if err != nil {
			fmt.Fprintf(w, "Error occured while generating message ID: %s\n", err)
			return
			// send an error message
		}

		spanMessage := st.SpanMessage{
			EntityName: entityName,
			MessageId:  messageId.String(),
			Spans:      incomingSpans,
		}

		if licenseKeyFound {
			spanMessage.LicenseKey = licenseKeyParams[0]
		}

		if insightsKeyFound {
			spanMessage.InsightsKey = insightsKeyParams[0]
		}

		if entityId, ok := queryParams["entity_id"]; ok {
			spanMessage.EntityId = entityId[0]
		}

		if len(rootSpans) > 0 {
			rootMessage := st.SpanMessage{
				EntityName: entityName,
				MessageId:  messageId.String(),
				Spans:      rootSpans,
			}

			rootMsg, err := json.Marshal(rootMessage)
			if err != nil {
				log.Printf("Root span serialization error: %s\n", err)
			} else {
				rsw.WriteMessages(context.Background(),
					kafka.Message{
						Key:   []byte("msg"),
						Value: []byte(rootMsg),
					},
				)
			}
		}

		msg, err := json.Marshal(spanMessage)
		if err != nil {
			fmt.Fprintf(w, "Serialization error: %s\n", err)
			return
		}
		//log.Print("writing ", string(msg))
		p.WriteMessages(context.Background(),
			kafka.Message{
				Key:   []byte("msg"),
				Value: []byte(msg),
			},
		)
		fmt.Fprintf(w, "success! message id: %s", messageId.String())
	}
}

func main() {
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{"kafka:9092"},
		Topic:    "incomingSpans",
		Balancer: &kafka.LeastBytes{},
	})

	rootSpanWriter := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{"kafka:9092"},
		Topic:    "rootSpans",
		Balancer: &kafka.LeastBytes{},
	})

	defer rootSpanWriter.Close()
	defer w.Close()

	r := mux.NewRouter()
	r.HandleFunc("/", NewSpanCollector(w, rootSpanWriter)).Methods("POST")
	http.Handle("/", r)

	log.Print("Listening on port 12345!")
	log.Fatal(http.ListenAndServe(":12345", nil))
}
