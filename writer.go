package protocol

import (
	"time"
)

type metric struct {
	name   string
	tags   []*Tag
	fields []*Field
	t      time.Time
}

func (m *metric) Name() string {
	return m.name
}
func (m *metric) TagList() []*Tag {
	return m.tags
}

func (m *metric) FieldList() []*Field {
	return m.fields
}

func (m *metric) Time() time.Time {
	return m.t
}

// Write writes out data to a line protocol encoder.  Note: it does no sorting.  It assumes you have done your own sorting for tagValues
func (e *Encoder) Write(name []byte, ts time.Time, tagKeys, tagVals, fieldKeys [][]byte, fieldVals []interface{}) (int, error) {
	metric := &metric{
		name:   string(name),
		t:      ts,
		tags:   make([]*Tag, len(tagKeys)),
		fields: make([]*Field, len(fieldKeys)),
	}

	for i := range tagKeys {
		metric.tags[i] = &Tag{
			Key:   string(tagKeys[i]),
			Value: string(tagVals[i]),
		}
	}

	for i := range fieldKeys {
		metric.fields[i] = &Field{
			Key:   string(fieldKeys[i]),
			Value: fieldVals[i],
		}
	}

	return e.Encode(metric)
}
