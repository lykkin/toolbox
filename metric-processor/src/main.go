package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"shared"

	"github.com/segmentio/kafka-go"
)

type Aggregator interface {
    Add(duration float64) bool
}

type Metric struct {
    Aggregators map[string]Aggregator
    Attributes map[string]interface{}
    Name string
}

type MetricsMap map[string]Metric

func main() {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"kafka:9092"},
		GroupID:   "metric-consumers",
		Topic:     "incomingSpans",
		Partition: 0,
		MaxBytes:  10e6, // 10MB
	})
	defer r.Close()

	Metrics := make(MetricsMap)
    // since the map is shared between consumer and producer goroutines,
    // we have to lock it.
	lock := sync.RWMutex{}

	var HarvestPeriod time.Duration = 10 // In seconds
    // kick off a gorouting responsible for reading messages in from kafka
	go func() {
		for {
			m, err := r.ReadMessage(context.Background())
			if err != nil {
				fmt.Printf("Consumer error: %v (%v)\n", err, m)
				log.Fatal("dying")
			}
			fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
			var msg span.SpanMessage
			json.Unmarshal(m.Value, &msg)
			lock.Lock()
			for _, s := range msg.Spans {
                // collect spans into metrics bucket
                log.Print(s)
			}
			lock.Unlock()
			// keep this from blocking all the time
			timer := time.NewTimer(HarvestPeriod * time.Second)
			<-timer.C
		}
	}()

	for {
		if len(Metrics) > 0 {
			lock.RLock()
			for _, metric := range Metrics {
                log.Print(metric)
			}
			lock.RUnlock()

			lock.Lock()
            // send data
            // remerge in the case of a failure
            Metrics = make(MetricsMap)
			lock.Unlock()
		}

		log.Print("waiting")
		// wait some time for more spans to roll in
		timer := time.NewTimer(HarvestPeriod * time.Second)
		<-timer.C
	}
}
