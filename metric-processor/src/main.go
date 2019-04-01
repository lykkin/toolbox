package main

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"

	sm "shared/message"
	st "shared/types"
)

func SendMetrics(insightsKey string, metrics *MetricList, startTime uint64, interval uint64, resChan chan *RequestResult) {
	payload := []map[string]interface{}{
		map[string]interface{}{
			"timestamp.ms": startTime,
			"internal.ms":  interval,
			"metrics":      *metrics,
		},
	}

	body, err := json.Marshal(payload)
	response := new(RequestResult)
	if err != nil {
		response.Err = err
		resChan <- response
		return
	}

	req, err := http.NewRequest("POST", "https://staging-metric-api.newrelic.com/metric/v1", bytes.NewBuffer(body))
	if err != nil {
		response.Err = err
		resChan <- response
		return
	}
	//// set query params and headers
	req.Header.Set("X-Insert-Key", insightsKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Length", string(len(body)))
	client := &http.Client{}
	res, err := client.Do(req)
	defer res.Body.Close()
	//bodyBytes, err2 := ioutil.ReadAll(res.Body)
	//bodyString := string(bodyBytes)
	//log.Print(err2, res, bodyString)

	// TODO: parse response and propagate it to the main goroutine
	resChan <- response
}

func consume(msgChan chan st.SpanMessage, lock *sync.RWMutex, InsightsKeyToMetrics *map[string]MetricsMap, tagWhitelist *[]string) {
	for msg := range msgChan {
		if msg.InsightsKey != "" {
			// lock for the whole consume loop, since we will be making
			// new metric buckets, and we don't want them to get dropped
			// on accident. this is probably awful for performance, and
			// should be reworked.
			// TODO: lockless?
			lock.Lock()
			NameToMetrics, ok := (*InsightsKeyToMetrics)[msg.InsightsKey]
			if !ok {
				(*InsightsKeyToMetrics)[msg.InsightsKey] = make(MetricsMap)
				NameToMetrics = (*InsightsKeyToMetrics)[msg.InsightsKey]
			}
		SPAN_LOOP:
			for _, s := range msg.Spans {
				Metrics, ok := NameToMetrics[s.Name]
				if !ok {
					NameToMetrics[s.Name] = new(MetricList)
					Metrics = NameToMetrics[s.Name]
				}
				// collect spans into metrics bucket
				attrs := make(map[string]interface{})
				for _, tagName := range *tagWhitelist {
					if val, ok := s.Tags[tagName]; ok {
						attrs[tagName] = val
					}
				}
				duration := s.FinishTime - s.StartTime
				for _, m := range *Metrics {
					if m.Recognizes(attrs) {
						m.Add(duration)
						//log.Print(m)
						continue SPAN_LOOP
					}
				}
				metric := Metric{
					Type:       "summary",
					Name:       s.Name,
					Attributes: attrs,
				}
				metric.Add(duration)
				*Metrics = append(*Metrics, &metric)
			}
			lock.Unlock()
		} else {
			log.Print("no insights key")
		}
	}
	log.Fatal("kafka message channel closed unexpectedly!")
}

func getTimestampMs() uint64 {
	return uint64(time.Now().UnixNano() / int64(time.Millisecond))
}

func main() {
	whitelistJSON, err := ioutil.ReadFile("/conf/whitelist.json")
	if err != nil {
		log.Fatal(err)
	}

	tagWhitelist := make([]string, 0)
	json.Unmarshal(whitelistJSON, &tagWhitelist)

	reader := sm.NewSpanMessageConsumer("metric-consumers")
	msgChan := make(chan st.SpanMessage)
	reader.Start(msgChan)

	// since the map is shared between consumer and producer goroutines,
	// we have to lock it.
	// TODO: test out a concurrent map
	lock := sync.RWMutex{}
	InsightsKeyToMetrics := make(map[string]MetricsMap)

	startTime := uint64(time.Now().UnixNano() / int64(time.Millisecond))

	// kick off a gorouting responsible for reading messages in from kafka
	go consume(msgChan, &lock, &InsightsKeyToMetrics, &tagWhitelist)

	for {
		if len(InsightsKeyToMetrics) > 0 {
			resChan := make(chan *RequestResult)
			// loop through the events bucketed by license key and kick the request off in parallel
			lock.Lock()
			numRequestsAwaiting := len(InsightsKeyToMetrics)
			interval := getTimestampMs() - startTime // In milliseconds
			for insightsKey, metrics := range InsightsKeyToMetrics {
				metricsToSend := make(MetricList, 0)
				for _, ms := range metrics {
					metricsToSend = append(metricsToSend, *ms...)
				}
				log.Printf("sending %d metric buckets under key %s", len(metricsToSend), insightsKey)
				go SendMetrics(insightsKey, &metricsToSend, startTime, interval, resChan)
				delete(InsightsKeyToMetrics, insightsKey)
				// record the number of outstanding requests
			}
			lock.Unlock()

			// read off the return channel till all requests have come back
			for ; numRequestsAwaiting != 0; numRequestsAwaiting-- {
				result := <-resChan
				// if there was an error, don't remove the events from
				// the queue, and let next pass pick them up.

				// TODO: extra error handling here? are there cases where we want to throw the events away anyway?
				if result.Err != nil {
					lock.Lock()
					// TODO: remerge
					lock.Unlock()
				}
			}
			log.Print("waiting 10 seconds to send again")
			startTime = getTimestampMs()
			time.Sleep(10 * time.Second)
		} else {
			log.Print("no input found, waiting 3 seconds to check again")
			time.Sleep(3 * time.Second)
		}
	}
}
