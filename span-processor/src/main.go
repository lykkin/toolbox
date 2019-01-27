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

        if len(spans) > 0 {
            log.Printf("got %s", spans)
        }

        log.Print("waiting")
        timer1 := time.NewTimer(HarvestPeriod * time.Second)
        <-timer1.C
        // PROCESS SPANS HERE
    }
}
