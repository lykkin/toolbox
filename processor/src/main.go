package main

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"shared"

	"github.com/gocql/gocql"
)

type SpanEvent struct {
	TraceId    string            `json:"traceId"`
	SpanId     string            `json:"guid"`
	ParentId   string            `json:"parentId"`
	Name       string            `json:"name"`
	Timestamp  uint64            `json:"timestamp"`
	Duration   float64           `json:"durationMs"`
	Tags       map[string]string `json:"tags,omitempty"`
	EntityName string            `json:"entityName"`
	EntityId   string            `json:"entityId,omitempty"`
}

func SpanToEvent(s span.Span) SpanEvent {
	return SpanEvent{
		TraceId:    s.TraceId,
		SpanId:     s.SpanId,
		ParentId:   s.ParentId,
		Name:       s.Name,
		Timestamp:  uint64(s.StartTime),
		Duration:   s.FinishTime - s.StartTime,
		Tags:       s.Tags,
		EntityName: s.EntityName,
		EntityId:   s.EntityId,
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
	cluster := gocql.NewCluster("cassandra")
	cluster.Keyspace = "span_collector"
	cluster.Consistency = gocql.One
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()
	var HarvestPeriod time.Duration = 10 // In seconds
	for {
		// used to break events out into payloads to send
		LicenseKeyToEvents := make(map[string][]SpanEvent)
		// used to track unique trace ids to delete
		LicenseKeyToTraceIds := make(map[string]map[string]bool)

		// grab all spans to process
		iter := session.Query("SELECT * FROM span_collector.to_process;").Iter()
		for {
			row := make(map[string]interface{})
			if !iter.MapScan(row) {
				break
			}

			s := span.FromRow(row)
			licenseKey := s.LicenseKey
			// seed the trace id set for the license key if it doesn't exist
			if _, ok := LicenseKeyToTraceIds[licenseKey]; !ok {
				LicenseKeyToTraceIds[licenseKey] = make(map[string]bool)
			}
			// set the trace id as seen
			LicenseKeyToTraceIds[licenseKey][s.TraceId] = true
			// record the span in the payload to be setn
			LicenseKeyToEvents[licenseKey] = append(LicenseKeyToEvents[licenseKey], SpanToEvent(s))
		}

		if err := iter.Close(); err != nil {
			log.Fatal(err)
		}

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

			query := "BEGIN BATCH "
			values := make([]interface{}, 0)
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
				licenseKey := result.LicenseKey
				for traceId, _ := range LicenseKeyToTraceIds[licenseKey] {
					query += "DELETE FROM span_collector.to_process WHERE trace_id=? AND license_key=?;"
					values = append(values, traceId, licenseKey)
				}
			}

			query += "APPLY BATCH;"
			log.Printf("EXECUTING QUERY: %s", query)
			log.Printf("WITH VALUES: %s", values)
			log.Printf("FROM DELETE: %s", session.Query(query, values...).Exec())
		}

		log.Print("waiting")
		// wait some time for more spans to roll in
		timer1 := time.NewTimer(HarvestPeriod * time.Second)
		<-timer1.C
	}
}
