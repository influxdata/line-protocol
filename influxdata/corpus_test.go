package influxdata

import (
	"bytes"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/line-protocol-corpus/lpcorpus"
)

func TestCorpusDecode(t *testing.T) {
	c := qt.New(t)
	corpus, err := readCorpusDecodeResults()
	c.Assert(err, qt.IsNil)
	for _, test := range corpus {
		c.Run(test.Input.Key, func(c *qt.C) {
			precision := fromCorpusPrecision(test.Input.Precision)
			ps, err := decodeToCorpusPoints(test.Input.Text, precision, test.Input.DefaultTime)
			// We'll treat it as success if we match any of the result
			if test.Output.Error != "" {
				c.Assert(err, qt.Not(qt.IsNil))
				// Check exact error?
				return
			}
			c.Assert(err, qt.IsNil)
			c.Assert(ps, qt.CmpEquals(cmpopts.EquateEmpty()), test.Output.Result)
		})
	}
}

func TestCorpusEncode(t *testing.T) {
	c := qt.New(t)
	corpus, err := readCorpusEncodeResults()
	c.Assert(err, qt.IsNil)
	for _, test := range corpus {
		c.Run(test.Input.Key, func(c *qt.C) {
			precision := fromCorpusPrecision(test.Input.Precision)
			data, err := encodeWithCorpusInput(test.Input.Point, precision)
			if err == nil {
				// The encoding succeeded. Check that we can round-trip back to the
				// original values.
				ms, err := decodeToCorpusPoints(data, precision, 0)
				if c.Check(err, qt.IsNil) {
					c.Check(ms, qt.HasLen, 1)
					c.Check(ms[0], qt.DeepEquals, test.Input.Point)
				}
			}
			if test.Output.Error != "" {
				c.Assert(err, qt.IsNotNil)
				// assert exact string?
				return
			}
			c.Assert(err, qt.IsNil)
			c.Assert(string(data), qt.Equals, string(test.Output.Result))
		})
	}
}

func encodeWithCorpusInput(m *lpcorpus.Point, precision Precision) ([]byte, error) {
	var e Encoder
	e.SetPrecision(precision)
	e.StartLineRaw(m.Name)
	for _, tag := range m.Tags {
		e.AddTagRaw(tag.Key, tag.Value)
	}
	for _, field := range m.Fields {
		v, ok := NewValue(field.Value.Interface())
		if !ok {
			return nil, fmt.Errorf("invalid value for encoding %v", field.Value)
		}
		e.AddFieldRaw(field.Key, v)
	}
	e.EndLine(time.Unix(0, m.Time))
	return bytes.TrimSuffix(e.Bytes(), []byte("\n")), e.Err()
}

func decodeToCorpusPoints(text []byte, precision Precision, defaultTime int64) ([]*lpcorpus.Point, error) {
	dec := NewDecoderWithBytes(text)
	ms := []*lpcorpus.Point{}
	for dec.Next() {
		m, err := decodeToCorpusPoint(dec, precision, defaultTime)
		if err != nil {
			return nil, fmt.Errorf("cannot get metric for point %d: %v", len(ms), err)
		}
		ms = append(ms, m)
	}
	return ms, nil
}

func decodeToCorpusPoint(dec *Decoder, precision Precision, defaultTime int64) (*lpcorpus.Point, error) {
	m := lpcorpus.Point{
		Tags:   []lpcorpus.Tag{},
		Fields: []lpcorpus.Field{},
	}
	var err error
	m.Name, err = dec.Measurement()
	if err != nil {
		return nil, fmt.Errorf("cannot get measurement: %v", err)
	}
	for {
		key, val, err := dec.NextTag()
		if err != nil {
			return nil, fmt.Errorf("cannot get tag %v: %v", len(m.Tags), err)
		}
		if key == nil {
			break
		}
		m.Tags = append(m.Tags, lpcorpus.Tag{
			Key:   dupBytes(key),
			Value: dupBytes(val),
		})
	}
	sort.Slice(m.Tags, func(i, j int) bool {
		return bytes.Compare(m.Tags[i].Key, m.Tags[j].Key) < 0
	})
	for i := range m.Tags {
		if i > 0 && bytes.Equal(m.Tags[i-1].Key, m.Tags[i].Key) {
			return nil, fmt.Errorf("duplicate key %q", m.Tags[i].Key)
		}
	}
	for {
		key, val, err := dec.NextField()
		if err != nil {
			return nil, fmt.Errorf("cannot get field %d: %v", len(m.Fields), err)
		}
		if key == nil {
			break
		}
		m.Fields = append(m.Fields, lpcorpus.Field{
			Key:   dupBytes(key),
			Value: lpcorpus.MustNewValue(val.Interface()),
		})
	}

	timestamp, err := dec.Time(precision, time.Unix(0, defaultTime))
	if err != nil {
		return nil, fmt.Errorf("cannot get time: %v", err)
	}
	m.Time = timestamp.UnixNano()
	return &m, nil
}

func fromCorpusPrecision(precision lpcorpus.Precision) Precision {
	switch precision.Duration {
	case time.Nanosecond:
		return Nanosecond
	case time.Microsecond:
		return Microsecond
	case time.Millisecond:
		return Millisecond
	case time.Second:
		return Second
	default:
		panic(fmt.Errorf("unknown precision in test corpus %q", precision))
	}
}

func dupBytes(b []byte) []byte {
	return append([]byte(nil), b...)
}

var corpus struct {
	once   sync.Once
	decode []*lpcorpus.DecodeCorpusEntry
	encode []*lpcorpus.EncodeCorpusEntry
	err    error
}

func readCorpusOnce() {
	corpus.once.Do(func() {
		corp, err := lpcorpus.ReadCorpusJSON("testdata/corpus.json")
		if err != nil {
			corpus.err = err
			return
		}
		// Create slices rather than using the maps directly so that
		// it's easy to execute the tests in deterministic order.
		drs := make([]*lpcorpus.DecodeCorpusEntry, 0, len(corp.Decode))
		for _, d := range corp.Decode {
			drs = append(drs, d)
		}
		sort.Slice(drs, func(i, j int) bool {
			return drs[i].Input.Key < drs[j].Input.Key
		})
		corpus.decode = drs

		ers := make([]*lpcorpus.EncodeCorpusEntry, 0, len(corp.Encode))
		for _, e := range corp.Encode {
			ers = append(ers, e)
		}
		sort.Slice(ers, func(i, j int) bool {
			return ers[i].Input.Key < ers[j].Input.Key
		})
		corpus.encode = ers
	})
}

func readCorpusDecodeResults() ([]*lpcorpus.DecodeCorpusEntry, error) {
	readCorpusOnce()
	return corpus.decode, corpus.err
}

func readCorpusEncodeResults() ([]*lpcorpus.EncodeCorpusEntry, error) {
	readCorpusOnce()
	return corpus.encode, corpus.err
}
