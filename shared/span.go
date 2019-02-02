package span

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
	LicenseKey string `json:"license_key"`
	EntityName string `json:"entity_name"`
	EntityId   string `json:"entity_id,omitempty"`
	Spans      []Span `json:"spans"`
}

func (span Span) IsValid() (string, bool) {
	// TODO: do this with reflection
	if span.SpanId == "" {
		return "SpanId is required", false
	}

	if span.TraceId == "" {
		return "TraceId is missing", false
	}
	return "", true
}
