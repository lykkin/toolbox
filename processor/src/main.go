package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"shared"

	"github.com/segmentio/kafka-go"
)

type SpanEvent struct {
	TraceId    string                 `json:"traceId"`
	SpanId     string                 `json:"guid"`
	ParentId   string                 `json:"parentId"`
	Name       string                 `json:"name"`
	Timestamp  uint64                 `json:"timestamp"`
	Duration   float64                `json:"durationMs"`
	Tags       map[string]interface{} `json:"tags,omitempty"`
	EntityName string                 `json:"entityName"`
	EntityId   string                 `json:"entityId,omitempty"`
}

func SpanToEvent(s span.Span, entityName string, entityId string) SpanEvent {
	return SpanEvent{
		TraceId:    s.TraceId,
		SpanId:     s.SpanId,
		ParentId:   s.ParentId,
		Name:       s.Name,
		Timestamp:  uint64(s.StartTime),
		Duration:   s.FinishTime - s.StartTime,
		Tags:       s.Tags,
		EntityName: entityName,
		EntityId:   entityId,
	}
}

type RequestResult struct {
	Err        error
	LicenseKey string
}

func SendEvents(licenseKey string, events []SpanEvent, errChan chan RequestResult) {
	payload := map[string][]SpanEvent{"spans": events}
	body, err := json.Marshal(payload)
	if err != nil {
		errChan <- RequestResult{err, ""}
		return
	}

	req, err := http.NewRequest("POST", "https://staging-collector.newrelic.com/agent_listener/invoke_raw_method", bytes.NewBuffer(body))
	if err != nil {
		errChan <- RequestResult{err, ""}
		return
	}
	// set query params and headers
	q := req.URL.Query()
	q.Add("protocol_version", "1")
	q.Add("license_key", licenseKey)
	q.Add("method", "external_span_data")
	req.Header.Set("Content-Type", "application/json")
	req.URL.RawQuery = q.Encode()
	client := &http.Client{}
	res, err := client.Do(req)
	defer res.Body.Close()

	// TODO: parse response and propagate it to the main goroutine
	errChan <- RequestResult{nil, licenseKey}
}

func main() {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"kafka:9092"},
		GroupID:   "span-consumers",
		Topic:     "incomingSpans",
		Partition: 0,
		MaxBytes:  10e6, // 10MB
	})
	defer r.Close()

	// used to break events out into payloads to send
	LicenseKeyToEvents := make(map[string][]SpanEvent)

	var HarvestPeriod time.Duration = 10 // In seconds
	go func() {
		for {
			m, err := r.ReadMessage(context.Background())
			if err != nil {
				fmt.Printf("Consumer error: %v (%v)\n", err, m)
				log.Fatal("dying")
				break
			}
			fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
			var msg span.SpanMessage
			json.Unmarshal(m.Value, &msg)
			licenseKey := msg.LicenseKey
			for _, s := range msg.Spans {
				// record the span in the payload to be set
				LicenseKeyToEvents[licenseKey] = append(LicenseKeyToEvents[licenseKey], SpanToEvent(s, msg.EntityName, msg.EntityId))
			}
			// keep this from blocking all the time
			timer := time.NewTimer(HarvestPeriod * time.Second)
			<-timer.C
		}
	}()

	for {
		// only process if there are events to send
		if len(LicenseKeyToEvents) > 0 {

			numRequestsAwaiting := 0
			comms := make(chan RequestResult)
			// loop through the events bucketed by license key and kick the request off in parallel
			for licenseKey, events := range LicenseKeyToEvents {
				go SendEvents(licenseKey, events, comms)
				// record the number of outstanding requests
				numRequestsAwaiting++
			}

			// read off the return channel till all requests have come back
			for ; numRequestsAwaiting != 0; numRequestsAwaiting-- {
				result := <-comms
				// if there was an error, don't remove the events from
				// the queue, and let next pass pick them up.

				// TODO: extra error handling here? are there cases where we want to throw the events away anyway?
				if result.Err != nil {
					continue
				}

				// delete the processed spans from the queue
				delete(LicenseKeyToEvents, result.LicenseKey)
			}
		}

		log.Print("waiting")
		// wait some time for more spans to roll in
		timer := time.NewTimer(HarvestPeriod * time.Second)
		<-timer.C
	}
}
