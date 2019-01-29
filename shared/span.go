package span

import (
	"reflect"
	"strings"
)

type Span struct {
	TraceId    string            `cassandra:"trace_id" json:"trace_id"`
	SpanId     string            `cassandra:"span_id" json:"span_id"`
	ParentId   string            `cassandra:"parent_id" json:"parent_id"`
	Name       string            `cassandra:"name" json:"name"`
	StartTime  float64           `cassandra:"start_time" json:"start_time"`
	FinishTime float64           `cassandra:"finish_time" json:"finish_time"`
	Category   string            `cassandra:"category" json:"category"`
	Tags       map[string]string `cassandra:"tags" json:"tags,omitempty"`
	LicenseKey string            `cassandra:"license_key" json:"license_key" query:"license_key"`
	EntityName string            `cassandra:"entity_name" json:"entity_name" query:"entity_name"`
	EntityId   string            `cassandra:"entity_id" json:"entity_id,omitempty" query:"entity_id"`
}

func FromRow(row map[string]interface{}) Span {
	var span Span
	spanType := reflect.TypeOf(span)
	spanValue := reflect.ValueOf(&span).Elem()
	numFields := spanValue.NumField()
	for i := 0; i < numFields; i++ {
		field := spanType.Field(i)
		tag := field.Tag.Get("cassandra")
		val := reflect.ValueOf(row[tag])
		if val.IsValid() {
			spanValue.FieldByName(field.Name).Set(val)
		}
	}
	return span
}

func (span Span) GetInsertQueryAndValues(queryParams map[string][]string, destinations []string) (string, []interface{}) {
	spanType := reflect.TypeOf(span)
	numFields := spanType.NumField()
	fields := make([]string, numFields)
	values := make([]interface{}, numFields)
	valuePlaceholders := make([]string, numFields)
	for i := 0; i < numFields; i++ {
		valuePlaceholders[i] = "?"
	}
	for i := 0; i < numFields; i++ {
		field := spanType.Field(i)
		cTag, ok := field.Tag.Lookup("cassandra")
		if !ok {
			continue
		}
		tag, isQuery := field.Tag.Lookup("query")
		if !isQuery {
			values[i] = getField(&span, field.Name)
		} else {
			values[i] = queryParams[tag][0]
		}
		fields[i] = cTag
	}
	query := ""
	resVals := make([]interface{}, 0)
	for _, dest := range destinations {
		query += "INSERT INTO " + dest + "(" + strings.Join(fields, ", ") + ") VALUES (" + strings.Join(valuePlaceholders, ", ") + ");"
		resVals = append(resVals, values...)
	}

	return query, resVals
}

func getField(s *Span, field string) interface{} {
	r := reflect.ValueOf(s)
	f := reflect.Indirect(r).FieldByName(field)
	return f.Interface()
}
