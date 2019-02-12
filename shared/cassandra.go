package shared

import (
	"errors"
	"reflect"
)

func ParseRow(dest *interface{}, row map[string]interface{}) error {
	destType := reflect.TypeOf(dest)
	destVal := reflect.ValueOf(dest).Elem()
	numFields := destType.NumField()
	for i := 0; i < numFields; i++ {
		field := destType.Field(i)
		cTag, ok := field.Tag.Lookup("cassandra")
		if !ok {
			continue
		}
		cassVal := reflect.ValueOf(row[cTag])
		if cassVal.IsValid() {
			destVal.FieldByName(field.Name).Set(cassVal)
		} else {
			return errors.New("Expected a value to be present for " + cTag + " in cassandra row")
		}
	}
	return nil
}

func GetKeysAndValues(source interface{}) (*[]string, *[]interface{}) {
	fields := make([]string, 0)
	values := make([]interface{}, 0)
	srcType := reflect.TypeOf(source)
	srcVal := reflect.ValueOf(source)
	numFields := srcType.NumField()
	for i := 0; i < numFields; i++ {
		field := srcType.Field(i)
		cTag, ok := field.Tag.Lookup("cassandra")
		if !ok {
			continue
		}
		fields = append(fields, cTag)
		values = append(values, srcVal.FieldByName(field.Name).Interface())
	}
	return &fields, &values
}
