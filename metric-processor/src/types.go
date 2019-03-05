package main

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

func (m *Metric) Recognizes(attrs map[string]interface{}) bool {
	if len(attrs) != len(m.Attributes) {
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

	if v.Count == 0 {
		v.Min = duration
		v.Max = duration
	} else if v.Min > duration {
		v.Min = duration
	} else if v.Max < duration {
		v.Max = duration
	}

	v.Count++
	v.Sum += duration
}

type MetricBucket struct {
	StartTime uint64     `json:"start_time_ms"`
	Interval  uint64     `json:"interval_ms"`
	Metrics   MetricList `json:"metrics"`
}
type MetricList []*Metric

// Metric name -> Metric list
type MetricsMap map[string]*MetricList

type RequestResult struct {
	Err error
}
