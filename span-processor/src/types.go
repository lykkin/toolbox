package main

import (
	st "shared/types"
)

type SpanEvent struct {
	TraceId    string            `json:"traceId"`
	SpanId     string            `json:"guid"`
	ParentId   string            `json:"parentId"`
	Name       string            `json:"name"`
	Timestamp  uint64            `json:"timestamp"`
	Duration   float64           `json:"durationMs"`
	Tags       map[string]string `json:"tags,omitempty"`
	EntityName string            `json:"entityName"`
	EntityId   string            `json:"entityId,omitempty"`
}

type SpanList *[]SpanEvent

func EventToSpan(ev SpanEvent) st.Span {
	return st.Span{
		TraceId:    ev.TraceId,
		SpanId:     ev.SpanId,
		ParentId:   ev.ParentId,
		Name:       ev.Name,
		StartTime:  float64(ev.Timestamp),
		FinishTime: ev.Duration - float64(ev.Timestamp),
		Tags:       ev.Tags,
	}
}

func SpanToEvent(s st.Span, entityName string, entityId string) SpanEvent {
	return SpanEvent{
		TraceId:    s.TraceId,
		SpanId:     s.SpanId,
		ParentId:   s.ParentId,
		Name:       s.Name,
		Timestamp:  uint64(s.StartTime),
		Duration:   s.FinishTime - s.StartTime,
		Tags:       s.Tags,
		EntityName: entityName,
		EntityId:   entityId,
	}
}

type RequestResult struct {
	Err        error
	LicenseKey string
	Events     *[]SpanEvent
}
