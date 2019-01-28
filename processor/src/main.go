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

func SendEvents(licenseKey string, events []SpanEvent, errChan chan error) {
	log.Printf("sending %s: %s", licenseKey, events)
	errChan <- nil
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

		iter := session.Query("SELECT * FROM span_collector.span;").Iter()
		for {
			row := make(map[string]interface{})
			if !iter.MapScan(row) {
				break
			}

			// TODO: MARK SPAN AS SENT
			s := span.FromRow(row)
			licenseKey := s.LicenseKey
			LicenseKeyToEvents[licenseKey] = append(LicenseKeyToEvents[licenseKey], SpanToEvent(s))
		}

		if err := iter.Close(); err != nil {
			log.Fatal(err)
		}

		numRequestsAwaiting := 0
		errChan := make(chan error)
		for licenseKey, events := range LicenseKeyToEvents {
			go SendEvents(licenseKey, events, errChan)
			numRequestsAwaiting++
			log.Print("queuing #%s, %s: %s", numRequestsAwaiting, licenseKey, events)
		}

		for ; numRequestsAwaiting != 0; numRequestsAwaiting-- {
			err := <-errChan
			log.Printf("hi %s", err)
			if err != nil {
				// HANDLE ERROR
			}
		}

		log.Print("waiting")
		timer1 := time.NewTimer(HarvestPeriod * time.Second)
		<-timer1.C
	}
}
