package influxdata

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"sort"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	qt "github.com/frankban/quicktest"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

// corpusAllowList lists the test cases that have been explicitly checked and
// all the current implementations are wrong.
var corpusAllowList = map[string]string{
	"4e8bcb126274d3290789538902dab6ee": "no trailing backslash",
}

func TestCorpus(t *testing.T) {
	c := qt.New(t)
	corpus, err := readCorpus()
	c.Assert(err, qt.IsNil)
	for _, test := range corpus {
		c.Run(test.Input.Key, func(c *qt.C) {
			m, err := tokenizeToCorpusMetrics(test.Input.Text, test.Input.Precision, test.Input.DefaultTime)
			// We'll treat it as success if we match any of the result
			ok := false
			validResults := 0
			for impl, out := range test.Output {
				if impl == "delorean" {
					// Ignore delorean results because they're screwy.
					continue
				}
				if strings.HasPrefix(out.Error, "crash: ") {
					// A crash is not a valid result.
					continue
				}
				validResults++
				if out.Error != "" {
					ok = ok || err != nil
					continue
				}
				ok = ok || cmp.Equal(m, out.Result, cmpopts.EquateEmpty())
			}
			allowListReason := corpusAllowList[test.Input.Key]
			if ok {
				if allowListReason != "" {
					c.Errorf("unexpected success on allowListed corpus test")
				}
				return
			}
			if hasBackslashVIssue(test.Input.Text) {
				allowListReason = "leading char-\\v ignored"
			}
			if allowListReason != "" {
				c.Skipf("test has been explicitly marked for success (reason: %s)", allowListReason)
			}
			if validResults == 0 {
				c.Errorf("no implementation produced a valid result")
				return
			}
			if err != nil {
				c.Errorf("all implementations succeeded but Tokenize failed with error: %v", err)
				return
			}
			c.Errorf("result doesn't match any implementation")
			data, _ := json.MarshalIndent(m, "\t", "\t")
			c.Logf("%s", data)
		})
	}
}

func hasBackslashVIssue(s []byte) bool {
	if len(s) > 2 && s[1] == '\v' {
		return true
	}
	for ; len(s) > 3; s = s[1:] {
		if s[0] == '\n' && s[2] == '\v' {
			return true
		}
	}
	return false
}

func tokenizeToCorpusMetrics(text []byte, precision string, defaultTime int64) ([]*corpusMetric, error) {
	tok := NewTokenizerWithBytes(text)
	ms := []*corpusMetric{}
	for tok.Next() {
		m, err := tokenizeToCorpusMetric(tok, precision, defaultTime)
		if err != nil {
			return nil, fmt.Errorf("cannot get metric %v: %v", len(ms), err)
		}
		ms = append(ms, m)
	}
	return ms, nil
}

func tokenizeToCorpusMetric(tok *Tokenizer, precision string, defaultTime int64) (*corpusMetric, error) {
	var m corpusMetric
	var err error
	m.Name, err = tok.Measurement()
	if err != nil {
		return nil, fmt.Errorf("cannot get measurement: %v", err)
	}
	for {
		key, val, err := tok.NextTag()
		if err != nil {
			return nil, fmt.Errorf("cannot get tag %v: %v", len(m.Tags), err)
		}
		if key == nil {
			break
		}
		m.Tags = append(m.Tags, corpusTag{
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
		key, val, err := tok.NextField()
		if err != nil {
			return nil, fmt.Errorf("cannot get field %d: %v", len(m.Fields), err)
		}
		if key == nil {
			break
		}
		field := corpusField{
			Key: dupBytes(key),
		}
		switch val.Kind() {
		case Int:
			x := val.IntV()
			field.Value.Int = &x
		case Uint:
			x := val.UintV()
			field.Value.Uint = &x
		case Bool:
			x := val.BoolV()
			field.Value.Bool = &x
		case Float:
			x := val.FloatV()
			field.Value.Float = &x
		case String:
			x := val.StringV()
			field.Value.String = &x
		default:
			panic(fmt.Errorf("unknown value kind %v", val))
		}
		m.Fields = append(m.Fields, field)
	}
	var p Precision
	switch precision {
	case "1ns":
		p = Nanosecond
	case "1µs":
		p = Microsecond
	case "1ms":
		p = Millisecond
	case "1s":
		p = Second
	default:
		panic(fmt.Errorf("unknown precision in test corpus %q", precision))
	}
	timestamp, err := tok.Time(p, time.Unix(0, defaultTime))
	if err != nil {
		return nil, fmt.Errorf("cannot get time: %v", err)
	}
	m.Time = timestamp.UnixNano()
	return &m, nil
}

func dupBytes(b []byte) []byte {
	return append([]byte(nil), b...)
}

// unused but we'd like to keep around, so pacify staticcheck.
var _ = fromCorpusValue

func fromCorpusValue(v corpusValue) (Value, error) {
	switch {
	case v.Int != nil:
		return MustNewValue(*v.Int), nil
	case v.Uint != nil:
		return MustNewValue(*v.Uint), nil
	case v.Float != nil:
		return MustNewValue(*v.Float), nil
	case v.Bool != nil:
		return MustNewValue(*v.Bool), nil
	case v.String != nil:
		return MustNewValue(*v.String), nil
	case v.Binary != nil:
		return Value{}, fmt.Errorf("non-utf8 values not supported")
	case v.FloatNonNumber != nil:
		return Value{}, fmt.Errorf("non-numeric float values not supported")
	default:
		return Value{}, fmt.Errorf("unknown corpus value kind %#v", v)
	}
}

func readCorpus() ([]*corpusDecodeResults, error) {
	const corpusFile = "testdata/corpus-results.json"
	data, err := ioutil.ReadFile(corpusFile)
	if err != nil {
		return nil, err
	}
	var r corpusResults
	if err := json.Unmarshal(data, &r); err != nil {
		if terr, ok := err.(*json.UnmarshalTypeError); ok {
			return nil, fmt.Errorf("unmarshal error at %s:#%d: %v", corpusFile, terr.Offset, err)
		}
		return nil, fmt.Errorf("unmarshal error at %s: %v", corpusFile, err)
	}
	// Return a slice rather than the map so that
	// it's easy to execute the tests in deterministic order.
	rs := make([]*corpusDecodeResults, 0, len(r.Decode))
	for _, d := range r.Decode {
		rs = append(rs, d)
	}
	sort.Slice(rs, func(i, j int) bool {
		return rs[i].Input.Key < rs[j].Input.Key
	})
	return rs, nil
}

type corpusResults struct {
	Decode map[string]*corpusDecodeResults `json:"decode,omitempty"`
	// TODO include encode results too when we've got an encoder.
}

type corpusDecodeResults struct {
	Input  *corpusDecodeInput             `json:"input"`
	Output map[string]*corpusDecodeOutput `json:"output"`
}

type corpusDecodeInput struct {
	Key         string      `json:"key"`
	Text        corpusBytes `json:"text"`
	DefaultTime int64       `json:"defaultTime"`
	Precision   string      `json:"precision"`
}

type corpusDecodeOutput struct {
	Result []*corpusMetric `json:"result,omitempty"`
	Error  string          `json:"error,omitempty"`
}

type corpusMetric struct {
	Time   int64         `json:"time"`
	Name   corpusBytes   `json:"name"`
	Tags   []corpusTag   `json:"tags,omitempty"`
	Fields []corpusField `json:"fields,omitempty"`
}

type corpusField struct {
	Key   corpusBytes `json:"key"`
	Value corpusValue `json:"value"`
}

type corpusTag struct {
	Key   corpusBytes `json:"key"`
	Value corpusBytes `json:"value"`
}

type corpusValue struct {
	Int            *int64   `json:"int,omitempty"`
	Uint           *uint64  `json:"uint,omitempty"`
	Float          *float64 `json:"float,omitempty"`
	FloatNonNumber *string  `json:"floatNonNumber,omitempty"`
	Bool           *bool    `json:"bool,omitempty"`
	String         *string  `json:"string,omitempty"`
	Binary         *string  `json:"binary,omitempty"`
}

// corpusBytes  implements a byte slice that knows how to marshal itself into
// and out of JSON while preserving raw-byte integrity.
type corpusBytes []byte

type base64Str struct {
	Binary string `yaml:"binary" json:"binary"`
}

func (b corpusBytes) MarshalJSON() ([]byte, error) {
	if utf8.Valid(b) {
		return json.Marshal(string(b))
	}
	return json.Marshal(base64Str{
		Binary: base64.StdEncoding.EncodeToString(b),
	})
}

func (b *corpusBytes) UnmarshalJSON(data []byte) error {
	if data[0] == '{' {
		var bstr base64Str
		if err := json.Unmarshal(data, &bstr); err != nil {
			return err
		}
		dec, err := base64.StdEncoding.DecodeString(bstr.Binary)
		if err != nil {
			return fmt.Errorf("cannot decode binary base64 value: %v", err)
		}
		*b = dec
		return nil
	}
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	*b = []byte(s)
	return nil
}
