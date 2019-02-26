package main

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"

	sdb "shared/db"
	st "shared/types"

	"github.com/gocql/gocql"
)

func SendEvents(licenseKey string, events SpanList, resChan chan *RequestResult) {
	payload := map[string][]SpanEvent{"spans": *events}
	body, err := json.Marshal(payload)
	response := new(RequestResult)
	response.Events = events
	response.LicenseKey = licenseKey
	if err != nil {
		response.Err = err
		resChan <- response
		return
	}
	log.Printf("sending %d events for license key %s", len(*events), licenseKey)

	req, err := http.NewRequest("POST", "https://staging-collector.newrelic.com/agent_listener/invoke_raw_method", bytes.NewBuffer(body))
	if err != nil {
		response.Err = err
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
	resChan <- response
}

func main() {
	cluster := gocql.NewCluster("cassandra")
	cluster.Consistency = gocql.One
	cluster.Keyspace = "span_collector"
	session, err := cluster.CreateSession()

	for err != nil {
		log.Print("ran into an error while setting up cassandra, waiting 5 seconds: ", err)
		time.Sleep(5 * time.Second)
		session, err = cluster.CreateSession()
	}

	// used to break events out into payloads to send
	LicenseKeyToEvents := make(map[string]SpanList)

	placeholderValues := []string{"?"}
	for {
		iter := session.Query("SELECT trace_id FROM span_collector.interesting_traces").Iter()
		interestingTraces := make([]string, 0)
		for {
			result := make(map[string]interface{})
			if !iter.MapScan(result) {
				break
			}
			interestingTraces = append(interestingTraces, result["trace_id"].(string))
		}
		if len(interestingTraces) == 0 {
			log.Print("no interesting traces found, sleeping for 10 seconds")
			time.Sleep(10 * time.Second)
			continue
		}

		iter = session.Query("SELECT * FROM span_collector.spans WHERE trace_id IN ? AND sent = false", interestingTraces).Iter()
		for {
			result := make(map[string]interface{})
			if !iter.MapScan(result) {
				break
			}
			licenseKey := result["license_key"].(string)
			eventBucketPtr, ok := LicenseKeyToEvents[licenseKey]
			if !ok {
				eventBucketPtr = new([]SpanEvent)
				LicenseKeyToEvents[licenseKey] = eventBucketPtr
			}
			err, spanInterface := sdb.ParseRow(st.Span{}, result)
			if err != nil {
				log.Print("uh oh", err)
			}
			spanEvent := SpanToEvent(spanInterface.(st.Span), result["entity_name"].(string), result["entity_id"].(string))
			*eventBucketPtr = append(*eventBucketPtr, spanEvent)
		}
		// only process if there are events to send
		if len(LicenseKeyToEvents) > 0 {
			numRequestsAwaiting := len(LicenseKeyToEvents)
			log.Printf("events found, sending %s requests", numRequestsAwaiting)
			resChan := make(chan *RequestResult)
			// loop through the events bucketed by license key and kick the request off in parallel
			for licenseKey, events := range LicenseKeyToEvents {
				go SendEvents(licenseKey, events, resChan)
				delete(LicenseKeyToEvents, licenseKey)
				// record the number of outstanding requests
			}

			// read off the return channel till all requests have come back
			for ; numRequestsAwaiting != 0; numRequestsAwaiting-- {
				result := <-resChan
				// if there was an error, don't remove the events from
				// the queue, and let next pass pick them up.

				// TODO: extra error handling here? are there cases where we want to throw the events away anyway?
				if result.Err == nil {
					spanEvents := *result.Events
					deleteQuery := "BEGIN BATCH "
					deleteValues := make([]interface{}, 0)
					insertQuery := "BEGIN BATCH "
					insertValues := make([]interface{}, 0)
					for _, s := range spanEvents {
						deleteQuery += "DELETE FROM span_collector.spans WHERE trace_id = ? AND sent = false AND span_id = ?;"
						deleteValues = append(deleteValues, s.TraceId, s.SpanId)
						fields, spanValues := sdb.GetKeysAndValues(EventToSpan(s))
						// Add on all the message level info
						*fields = append(*fields, "entity_name", "license_key", "entity_id")
						*spanValues = append(*spanValues, s.EntityName, result.LicenseKey, s.EntityId)
						insertValues = append(insertValues, *spanValues...)
						insertQuery += "INSERT INTO span_collector.spans (sent, " + strings.Join(*fields, ",") + ") VALUES (true, " + sdb.MakePlaceholderString(&placeholderValues, len(*fields)) + ");"
					}
					deleteQuery += "APPLY BATCH;"
					insertQuery += "APPLY BATCH;"
					err := session.Query(insertQuery, insertValues...).Exec()
					log.Print(err)
					err = session.Query(deleteQuery, deleteValues...).Exec()
					log.Print(err)
				}
			}
			log.Print("waiting 10 seconds to send again")
			time.Sleep(10 * time.Second)
		} else {
			log.Print("no input found, waiting 3 seconds to check again")
			time.Sleep(3 * time.Second)
		}
	} // END FOR
}
