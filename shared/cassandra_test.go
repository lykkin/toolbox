package shared

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type TestStruct struct {
	Foo string `cassandra:"foo"`
	Bar uint32 `cassandra:"bar"`
	Baz int
}

func TestParseRow(t *testing.T) {
	err, testValInterface := ParseRow(TestStruct{}, map[string]interface{}{
		"foo": "test",
		"bar": 1,
	})
	testVal := testValInterface.(TestStruct)
	assert.Nil(t, err)
	assert.Equal(t, testVal.Foo, "test", "should pick up foo")
	assert.Equal(t, testVal.Bar, uint32(1), "should pick up bar")
	assert.Equal(t, testVal.Baz, 0, "should not pick up baz")
}

func TestParseRowFail(t *testing.T) {
	err, testValInterface := ParseRow(TestStruct{}, map[string]interface{}{
		"foo": "test",
	})
	testVal := testValInterface.(TestStruct)
	assert.NotNil(t, err)
	assert.Equal(t, testVal.Foo, "", "should pick up foo")
	assert.Equal(t, testVal.Bar, uint32(0), "should pick up bar")
	assert.Equal(t, testVal.Baz, 0, "should not pick up baz")
}

func TestGetKeysAndValues(t *testing.T) {
	fields, values := GetKeysAndValues(TestStruct{
		Foo: "test",
		Bar: 1,
		Baz: 0,
	})
	assert.Equal(t, *fields, []string{
		"foo",
		"bar",
	}, "fields should be pulled out properly")
	assert.Equal(t, *values, []interface{}{
		"test",
		uint32(1),
	}, "values should be pulled out properly")
}
