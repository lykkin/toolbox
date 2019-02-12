package shared

import (
	"reflect"
	"strings"
)

type Span struct {
	TraceId    string                 `json:"trace_id"`
	SpanId     string                 `json:"span_id"`
	ParentId   string                 `json:"parent_id,omitempty"`
	Name       string                 `json:"name"`
	StartTime  float64                `json:"start_time"`
	FinishTime float64                `json:"finish_time"`
	Tags       map[string]interface{} `json:"tags,omitempty"`
}

type SpanMessage struct {
	LicenseKey  string `json:"license_key,omitempty"`
	InsightsKey string `json:"insights_key,omitempty"`
	EntityName  string `json:"entity_name"`
	EntityId    string `json:"entity_id,omitempty"`
	Spans       []Span `json:"spans"`
}

func (span Span) IsValid() (string, bool) {
	if span.SpanId == "" {
		return "span_id is required", false
	}

	spanType := reflect.TypeOf(span)
	numFields := spanType.NumField()
	val := reflect.ValueOf(span)
	for i := 0; i < numFields; i++ {
		field := spanType.Field(i)
		// TODO: check if this is ok
		jTag, _ := field.Tag.Lookup("json")

		if !strings.Contains(jTag, "omitempty") {
			if !val.FieldByName(field.Name).IsValid() {
				return jTag + " is missing from span " + span.SpanId, false
			}
		}
	}

	return "", true
}
