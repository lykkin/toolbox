package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"

	"shared"

	"github.com/segmentio/kafka-go"
)

type MetricValue struct {
	Count uint64  `json:"count"`
	Sum   float64 `json:"total"`
	Min   float64 `json:"min"`
	Max   float64 `json:"max"`
}

type Metric struct {
	Attributes map[string]interface{} `json:"attributes,omitempty"`
	Name       string                 `json:"name"`
	Type       string                 `json:"type"`
	Value      MetricValue            `json:"value"`
}

func (m *Metric) Recognizes(name string, attrs map[string]interface{}) bool {
	if name != m.Name || len(attrs) != len(m.Attributes) {
		return false
	}

	for k, v := range m.Attributes {
		attrVal, ok := attrs[k]
		if !ok || attrVal != v {
			return false
		}
	}
	return true
}

func (m *Metric) Add(duration float64) {
	v := &m.Value
	v.Count++
	v.Sum += duration
	if v.Min > duration {
		v.Min = duration
	}

	if v.Max < duration {
		v.Max = duration
	}
}

type MetricsList []*Metric

type RequestResult struct {
	Err error
}

func SendMetrics(insightsKey string, metrics *MetricsList, startTime uint64, interval uint64, resChan chan RequestResult) {
	payload := map[string]interface{}{
		"version": "0.3.0",
		"metric_buckets": []map[string]interface{}{map[string]interface{}{
			"start_time_ms": startTime,
			"interval_ms":   interval,
			"metrics":       *metrics,
		}},
	}
	body, err := json.Marshal(payload)
	log.Print(string(body), err)
	if err != nil {
		resChan <- RequestResult{err}
		return
	}

	req, err := http.NewRequest("POST", "https://staging-metric-api.newrelic.com/metric/v1", bytes.NewBuffer(body))
	if err != nil {
		resChan <- RequestResult{err}
		return
	}
	log.Print(insightsKey, len(body))
	//// set query params and headers
	req.Header.Set("X-Insert-Key", insightsKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Length", string(len(body)))
	client := &http.Client{}
	res, err := client.Do(req)
	defer res.Body.Close()
	bodyBytes, err2 := ioutil.ReadAll(res.Body)
	bodyString := string(bodyBytes)
	log.Print(bodyString, err2)

	// TODO: parse response and propagate it to the main goroutine
	resChan <- RequestResult{nil}
}

func main() {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"kafka:9092"},
		GroupID:   "metric-consumers",
		Topic:     "incomingSpans",
		Partition: 0,
		MaxBytes:  10e6, // 10MB
	})
	defer r.Close()

	whitelistJSON, err := ioutil.ReadFile("/conf/whitelist.json")
	if err != nil {
		log.Fatal(err)
	}

	tagWhitelist := make([]string, 0)
	json.Unmarshal(whitelistJSON, &tagWhitelist)

	InsightsKeyToMetrics := make(map[string]*MetricsList)
	// since the map is shared between consumer and producer goroutines,
	// we have to lock it.
	lock := sync.RWMutex{}
	startTime := uint64(time.Now().UnixNano() / int64(time.Millisecond))

	var HarvestPeriod time.Duration = 10                               // In seconds
	interval := uint64(HarvestPeriod * time.Second / time.Millisecond) // In milliseconds
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
			if msg.InsightsKey != "" {
				Metrics, ok := InsightsKeyToMetrics[msg.InsightsKey]
				if !ok {
					InsightsKeyToMetrics[msg.InsightsKey] = new(MetricsList)
					Metrics, ok = InsightsKeyToMetrics[msg.InsightsKey]
				}
			SPAN_LOOP:
				for _, s := range msg.Spans {
					// collect spans into metrics bucket
					attrs := make(map[string]interface{})
					for _, tagName := range tagWhitelist {
						if val, ok := s.Tags[tagName]; ok {
							attrs[tagName] = val
						}
					}
					duration := s.FinishTime - s.StartTime
					// break this out as a list by names for SPEED
					for _, m := range *Metrics {
						if m.Recognizes(s.Name, attrs) {
							lock.Lock()
							m.Add(duration)
							log.Print(m)
							lock.Unlock()
							continue SPAN_LOOP
						}
					}
					var metric Metric
					metric.Type = "summary"
					metric.Name = s.Name
					metric.Attributes = attrs
					metric.Add(duration)
					lock.Lock()
					*Metrics = append(*Metrics, &metric)
					lock.Unlock()
				}
			} else {
				log.Print("no insights key")
			}
		}
	}()

	for {
		if len(InsightsKeyToMetrics) > 0 {
			numRequestsAwaiting := 0
			comms := make(chan RequestResult)
			// loop through the events bucketed by license key and kick the request off in parallel
			lock.RLock()
			for insightsKey, metrics := range InsightsKeyToMetrics {
				go SendMetrics(insightsKey, metrics, startTime, interval, comms)
				delete(InsightsKeyToMetrics, insightsKey)
				// record the number of outstanding requests
				numRequestsAwaiting++
			}
			lock.RUnlock()

			// read off the return channel till all requests have come back
			lock.Lock()
			for ; numRequestsAwaiting != 0; numRequestsAwaiting-- {
				result := <-comms
				// if there was an error, don't remove the events from
				// the queue, and let next pass pick them up.

				// TODO: extra error handling here? are there cases where we want to throw the events away anyway?
				if result.Err != nil {
					// TODO: remerge
				}
			}
			lock.Unlock()
		}

		log.Print("waiting")
		// wait some time for more spans to roll in
		timer := time.NewTimer(HarvestPeriod * time.Second)
		<-timer.C
		startTime = uint64(time.Now().UnixNano() / int64(time.Millisecond))
	}
}
