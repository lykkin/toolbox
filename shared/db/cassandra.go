package shared

import (
	"errors"
	"log"
	"reflect"
	"strings"

	"github.com/gocql/gocql"
)

func MakePlaceholderString(cachedStrings *[]string, wanted int) string {
	sliceVal := *cachedStrings
	for len(sliceVal) < wanted {
		sliceVal = append(sliceVal, sliceVal[len(sliceVal)-1]+", ?")
	}
	*cachedStrings = sliceVal
	return sliceVal[wanted-1]
}

func SetupCassandraSchema(keyspace string, tableName string, tableSchema map[string]string, primaryKeys string) (*gocql.Session, error) {
	cluster := gocql.NewCluster("cassandra")
	session, err := cluster.CreateSession()

	if err != nil {
		return nil, err
	}

	defer session.Close()

	if err := createKeyspace(session, keyspace); err != nil {
		log.Print("from create ks")
		return nil, err
	}
	if err := createSpanTable(session, tableName, tableSchema, primaryKeys); err != nil {
		log.Print("from create table")
		return nil, err
	}
	cluster.Consistency = gocql.One
	cluster.Keyspace = keyspace

	return cluster.CreateSession()
}

func createKeyspace(session *gocql.Session, keyspace string) error {
	query := "CREATE KEYSPACE IF NOT EXISTS " + keyspace + " WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};"
	return session.Query(query).Exec()
}

func createSpanTable(session *gocql.Session, tableName string, tableSchema map[string]string, primaryKeys string) error {
	fieldSchema := make([]string, len(tableSchema))
	idx := 0
	for fieldName, fieldType := range tableSchema {
		fieldSchema[idx] = fieldName + " " + fieldType
		idx++
	}
	query := "CREATE TABLE IF NOT EXISTS " + tableName + " (" + strings.Join(fieldSchema, ", ") + ", PRIMARY KEY(" + primaryKeys + "));"
	return session.Query(query).Exec()
}

func ParseRow(dest interface{}, row map[string]interface{}) (error, interface{}) {
	destType := reflect.TypeOf(dest)
	resVal := reflect.New(destType).Elem()
	numFields := destType.NumField()
	for i := 0; i < numFields; i++ {
		field := destType.Field(i)
		cTag, ok := field.Tag.Lookup("cassandra")
		if !ok {
			continue
		}
		cassVal := reflect.ValueOf(row[cTag])
		if cassVal.IsValid() {
			resVal.FieldByName(field.Name).Set(cassVal.Convert(field.Type))
		} else {
			return errors.New("Expected a value to be present for " + cTag + " in cassandra row"), reflect.New(destType).Elem().Interface()
		}
	}
	return nil, resVal.Interface()
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
		bos := srcVal.FieldByName(field.Name)
		if !ok || !bos.IsValid() {
			continue
		}

		fields = append(fields, cTag)
		values = append(values, bos.Interface())
	}
	return &fields, &values
}
