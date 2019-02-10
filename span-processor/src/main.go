package main

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"shared"
)

func SendEvents(licenseKey string, events SpanList, resChan chan *RequestResult) {
	payload := map[string][]SpanEvent{"spans": *events}
	body, err := json.Marshal(payload)
	response := new(RequestResult)
	if err != nil {
		response.Err = err
		response.Events = events
		resChan <- response
		return
	}
	log.Print("sending", string(body))

	req, err := http.NewRequest("POST", "https://staging-collector.newrelic.com/agent_listener/invoke_raw_method", bytes.NewBuffer(body))
	if err != nil {
		response.Err = err
		response.Events = events
		resChan <- response
		return
	}
	// set query params and headers
	q := req.URL.Query()
	q.Add("protocol_version", "1")
	q.Add("license_key", licenseKey)
	q.Add("method", "external_span_data")
	req.URL.RawQuery = q.Encode()

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	res, err := client.Do(req)
	defer res.Body.Close()

	// TODO: parse response and propagate it to the main goroutine
	response.LicenseKey = licenseKey
	resChan <- response
}

func consume(msgChan chan shared.SpanMessage, lock *sync.RWMutex, LicenseKeyToEvents *map[string]SpanList) {
	for msg := range msgChan {
		if licenseKey := msg.LicenseKey; licenseKey != "" {
			lock.Lock()
			SpanBucket, ok := (*LicenseKeyToEvents)[licenseKey]
			if !ok {
				SpanBucket = new([]SpanEvent)
				(*LicenseKeyToEvents)[licenseKey] = SpanBucket
			}
			for _, s := range msg.Spans {
				// record the span in the payload to be set
				*SpanBucket = append(*SpanBucket, SpanToEvent(s, msg.EntityName, msg.EntityId))
			}
			lock.Unlock()
		}
	}
	log.Fatal("kafka message channel closed unexpectedly!")
}

func main() {
	reader := shared.NewSpanMessageConsumer("span-consumers")
	msgChan := make(chan shared.SpanMessage)
	reader.Start(msgChan)

	// since the map is shared between consumer and producer goroutines,
	// we have to lock it.
	lock := sync.RWMutex{}
	// used to break events out into payloads to send
	LicenseKeyToEvents := make(map[string]SpanList)

	var timer *time.Timer
	// kick off a gorouting responsible for reading messages in from kafka
	go consume(msgChan, &lock, &LicenseKeyToEvents)
	for {
		// only process if there are events to send
		if len(LicenseKeyToEvents) > 0 {
			numRequestsAwaiting := len(LicenseKeyToEvents)
			log.Printf("events found, sending %s requests", numRequestsAwaiting)
			resChan := make(chan *RequestResult)
			// loop through the events bucketed by license key and kick the request off in parallel
			lock.Lock()
			for licenseKey, events := range LicenseKeyToEvents {
				go SendEvents(licenseKey, events, resChan)
				delete(LicenseKeyToEvents, licenseKey)
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
					SpanBucket := LicenseKeyToEvents[result.LicenseKey]
					*SpanBucket = append(*SpanBucket, *result.Events...)
					lock.Unlock()
				}
			}
			log.Print("waiting 10 seconds to send again")
			timer = time.NewTimer(10 * time.Second)
		} else {
			log.Print("no input found, waiting 3 seconds to check again")
			timer = time.NewTimer(3 * time.Second)
		}

		// wait some time for more spans to roll in
		<-timer.C
	} // END FOR
}
