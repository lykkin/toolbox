package main

import (
	"log"
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
	Duration   float64           `json:"duration"`
	Category   string            `json:"category"`
	Tags       map[string]string `json:"tags,omitempty"`
	EntityName string            `json:"appName"`
	EntityId   string            `json:"appId,omitempty"`
}

func SpanToEvent(s span.Span) SpanEvent {
	return SpanEvent{
		TraceId:    s.TraceId,
		SpanId:     s.SpanId,
		ParentId:   s.ParentId,
		Name:       s.Name,
		Timestamp:  uint64(s.StartTime),
		Duration:   s.FinishTime - s.StartTime,
		Category:   s.Category,
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
	//TODO: actually send the events
	log.Printf("sending %s: %s", licenseKey, events)
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
	log.Print("Started processing!")
	var HarvestPeriod time.Duration = 10 // In seconds
	for {
		LicenseKeyToEvents := make(map[string][]SpanEvent)
		LicenseKeyToTraceIds := make(map[string][]string)

		iter := session.Query("SELECT * FROM span_collector.to_process;").Iter()
		for {
			row := make(map[string]interface{})
			if !iter.MapScan(row) {
				break
			}

			// TODO: MARK SPAN AS SENT
			s := span.FromRow(row)
			licenseKey := s.LicenseKey
			LicenseKeyToTraceIds[licenseKey] = append(LicenseKeyToTraceIds[licenseKey], s.TraceId)
			LicenseKeyToEvents[licenseKey] = append(LicenseKeyToEvents[licenseKey], SpanToEvent(s))
		}

		if err := iter.Close(); err != nil {
			log.Fatal(err)
		}

		numRequestsAwaiting := 0
		comms := make(chan RequestResult)
		for licenseKey, events := range LicenseKeyToEvents {
			go SendEvents(licenseKey, events, comms)
			numRequestsAwaiting++
		}

		query := "BEGIN BATCH "
		values := make([]interface{}, 0)
		for ; numRequestsAwaiting != 0; numRequestsAwaiting-- {
			result := <-comms
			if result.Err != nil {
				log.Printf("ran into an error while sending events: %s", result.Err)
				continue
			}

			// Build query to mark events as seen
			licenseKey := result.LicenseKey
			for _, traceId := range LicenseKeyToTraceIds[licenseKey] {
				query += "DELETE FROM span_collector.to_process WHERE trace_id=?;"
				values = append(values, traceId)
			}
		}

		query += "APPLY BATCH;"
		log.Printf("FROM DELETE: %s", session.Query(query, values...).Exec())

		log.Print("waiting")
		timer1 := time.NewTimer(HarvestPeriod * time.Second)
		<-timer1.C
	}
}
