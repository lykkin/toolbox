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

type SpanRecord struct {
	TraceId     string             `json:"trace_id" cassandra:"trace_id"`
	SpanId      string             `json:"span_id" cassandra:"span_id"`
	ParentId    string             `json:"parent_id,omitempty" cassandra:"parent_id"`
	Name        string             `json:"name" cassandra:"name"`
	StartTime   float64            `json:"start_time" cassandra:"start_time"`
	FinishTime  float64            `json:"finish_time" cassandra:"finish_time"`
	StringTags  map[string]string  `json:"string_tags,omitempty" cassandra:"string_tags"`
	BooleanTags map[string]bool    `json:"boolean_tags,omitempty" cassandra:"boolean_tags"`
	NumberTags  map[string]float64 `json:"number_tags,omitempty" cassandra:"number_tags"`
}

type SpanMessage struct {
	LicenseKey  string `json:"license_key,omitempty"`
	InsightsKey string `json:"insights_key,omitempty"`
	EntityName  string `json:"entity_name"`
	MessageId   string `json:"message_id"`
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

func SpanToRecord(s Span) *SpanRecord {
	stringTags := make(map[string]string)
	numberTags := make(map[string]float64)
	boolTags := make(map[string]bool)

	for k, v := range s.Tags {
		t := reflect.TypeOf(v).Name()
		switch t {
		case "string":
			stringTags[k] = v.(string)
			break
		case "bool":
			boolTags[k] = v.(bool)
			break
		case "float64":
			numberTags[k] = v.(float64)
			break
		default:
			// TODO: log here?
			break

		}
	}

	return &SpanRecord{
		TraceId:     s.TraceId,
		SpanId:      s.SpanId,
		ParentId:    s.ParentId,
		Name:        s.Name,
		StartTime:   s.StartTime,
		FinishTime:  s.FinishTime,
		StringTags:  stringTags,
		BooleanTags: boolTags,
		NumberTags:  numberTags,
	}
}

func RecordToSpan(sr SpanRecord) *Span {
	tags := make(map[string]interface{})
	for k, v := range sr.StringTags {
		tags[k] = v
	}
	for k, v := range sr.BooleanTags {
		tags[k] = v
	}
	for k, v := range sr.NumberTags {
		tags[k] = v
	}
	return &Span{
		TraceId:    sr.TraceId,
		SpanId:     sr.SpanId,
		ParentId:   sr.ParentId,
		Name:       sr.Name,
		StartTime:  sr.StartTime,
		FinishTime: sr.FinishTime,
		Tags:       tags,
	}
}
