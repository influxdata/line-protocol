package influxdata

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"sort"
	"strconv"
	"strings"
	"sync"
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
	"13a9b07a16a3adfe3480654a86d02e38": "non-printable character in measurement or key",
	"25957c0a9f5e3a8c28124d338e73bad3": "non-printable character in measurement or key",
	"2cad3ab7d1f6c69175256bd38427e41b": "non-printable character in measurement or key",
	"2df1f638db625653021bdd81d082aa7b": "non-printable character in measurement or key",
	"34e61d66cbcbb9396d649d96abb9cfc8": "non-printable character in measurement or key",
	"43020303d156ae3e021b05606aac1e4e": "non-printable character in measurement or key",
	"51909957b5a97015a9fde47953331e91": "non-printable character in measurement or key",
	"7e0ed32d7d67962c4aca9cdb4a33810b": "non-printable character in measurement or key",
	"9017c9c3ef7ab576ecca88c4e7c4f93d": "non-printable character in measurement or key",
	"97d1c1087409d74398d4c1a2ba0db15c": "non-printable character in measurement or key",
	"9f2ffee457c600e2bb7dc02863e206ab": "non-printable character in measurement or key",
	"a2104143356bbe4f000c8d513d8f73e8": "non-printable character in measurement or key",
	"a5ad697c142e6043f0891a33b762ec73": "non-printable character in measurement or key",
	"a667a7c1a0587907bf12d6c3cc130fa8": "non-printable character in measurement or key",
	"b08459b50de67f9c766806fac2039f5e": "non-printable character in measurement or key",
	"b5c0ed5b3beaaff0210c645b3e8f42b1": "non-printable character in measurement or key",
	"ca0fab37d66bfe0a70b01297141f179c": "non-printable character in measurement or key",
	"f8ccd78f52d6485b24303a99b4c314b2": "non-printable character in measurement or key",
	"f9d6e94df94cb7ae71cc5468ede46fcd": "non-printable character in measurement or key",
	"fd0df398c06cca4efbb3fcb7061acbb7": "non-printable character in measurement or key",
}

func TestCorpusDecode(t *testing.T) {
	c := qt.New(t)
	corpus, err := readCorpusDecodeResults()
	c.Assert(err, qt.IsNil)
	for _, test := range corpus {
		c.Run(test.Input.Key, func(c *qt.C) {
			precision := fromCorpusPrecision(test.Input.Precision)
			m, err := decodeToCorpusMetrics(test.Input.Text, precision, test.Input.DefaultTime)
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
				c.Errorf("all implementations succeeded but Decode failed with error: %v", err)
				return
			}
			c.Errorf("result doesn't match any implementation")
			data, _ := json.MarshalIndent(m, "\t", "\t")
			c.Logf("%s", data)
		})
	}
}

func TestCorpusEncode(t *testing.T) {
	c := qt.New(t)
	corpus, err := readCorpusEncodeResults()
	c.Assert(err, qt.IsNil)
	for _, test := range corpus {
		c.Run(test.Input.Key, func(c *qt.C) {
			if !test.Input.UintSupport {
				c.Skip("uint support cannot be disabled")
			}
			if test.Input.OmitInvalidFields {
				c.Skip("cannot omit invalid fields")
			}
			precision := fromCorpusPrecision(test.Input.Precision)
			data, err := encodeWithCorpusInput(test.Input.Metric, precision)
			if err == nil {
				// The encoding succeeded. Check that we can round-trip back to the
				// original values.
				ms, err := decodeToCorpusMetrics(data, precision, 0)
				if c.Check(err, qt.IsNil) {
					c.Check(ms, qt.HasLen, 1)
					c.Check(ms[0], qt.DeepEquals, test.Input.Metric)
				}
			}
			if metricContainsDifferentlyRepresentedNumber(test.Input.Metric) {
				// TODO there are multiple valid encodings. what should the corpus do in this case?
				c.Skip("large numbers encode using exponential notation, which is a change from other implementations")
			}
			ok := false
			validResults := 0
			for _, out := range test.Output {
				validResults++
				if out.Error != "" {
					ok = ok || err != nil
					continue
				}
				ok = ok || bytes.Equal(data, out.Result)
			}
			if ok {
				return
			}
			if validResults == 0 {
				c.Errorf("no implementation produced a valid result")
				return
			}
			if err != nil {
				c.Errorf("all implementations succeeded but Encode failed with error: %v", err)
				return
			}
			c.Errorf("result doesn't match any implementation")
			c.Logf("%s", data)
		})
	}
}

// metricContainsDifferentlyRepresentedNumber reports whether the metric
// contains a field that holds a float value that's encoded differently
// by the old line-protocol implementations which never use exponential
// notation for encoded numbers.
func metricContainsDifferentlyRepresentedNumber(m *corpusMetric) bool {
	for _, field := range m.Fields {
		v, err := fromCorpusValue(field.Value)
		if err != nil {
			// This error will be picked up later.
			continue
		}
		if v.Kind() != Float {
			continue
		}
		f1 := strconv.FormatFloat(v.FloatV(), 'f', -1, 64)
		f2 := strconv.FormatFloat(v.FloatV(), 'g', -1, 64)
		if f1 != f2 {
			return true
		}
	}
	return false
}

func encodeWithCorpusInput(m *corpusMetric, precision Precision) ([]byte, error) {
	var e Encoder
	e.SetPrecision(precision)
	e.StartLineRaw(m.Name)
	for _, tag := range m.Tags {
		e.AddTagRaw(tag.Key, tag.Value)
	}
	for _, field := range m.Fields {
		v, err := fromCorpusValue(field.Value)
		if err != nil {
			return nil, fmt.Errorf("invalid value for encoding: %v", err)
		}
		e.AddFieldRaw(field.Key, v)
	}
	e.EndLine(time.Unix(0, m.Time))
	return bytes.TrimSuffix(e.Bytes(), []byte("\n")), e.Err()
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

func decodeToCorpusMetrics(text []byte, precision Precision, defaultTime int64) ([]*corpusMetric, error) {
	dec := NewDecoderWithBytes(text)
	ms := []*corpusMetric{}
	for dec.Next() {
		m, err := decodeToCorpusMetric(dec, precision, defaultTime)
		if err != nil {
			return nil, fmt.Errorf("cannot get metric for point %d: %v", len(ms), err)
		}
		ms = append(ms, m)
	}
	return ms, nil
}

func decodeToCorpusMetric(dec *Decoder, precision Precision, defaultTime int64) (*corpusMetric, error) {
	m := corpusMetric{
		Tags:   []corpusTag{},
		Fields: []corpusField{},
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
		key, val, err := dec.NextField()
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

	timestamp, err := dec.Time(precision, time.Unix(0, defaultTime))
	if err != nil {
		return nil, fmt.Errorf("cannot get time: %v", err)
	}
	m.Time = timestamp.UnixNano()
	return &m, nil
}

func fromCorpusPrecision(precision string) Precision {
	switch precision {
	case "1ns":
		return Nanosecond
	case "1µs":
		return Microsecond
	case "1ms":
		return Millisecond
	case "1s":
		return Second
	default:
		panic(fmt.Errorf("unknown precision in test corpus %q", precision))
	}
}

func dupBytes(b []byte) []byte {
	return append([]byte(nil), b...)
}

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

var corpus struct {
	once          sync.Once
	decodeResults []*corpusDecodeResults
	encodeResults []*corpusEncodeResults
	err           error
}

func readCorpusOnce() {
	corpus.once.Do(func() {
		const corpusFile = "testdata/corpus-results.json"
		data, err := ioutil.ReadFile(corpusFile)
		if err != nil {
			corpus.err = err
			return
		}
		var r corpusResults
		if err := json.Unmarshal(data, &r); err != nil {
			if terr, ok := err.(*json.UnmarshalTypeError); ok {
				corpus.err = fmt.Errorf("unmarshal error at %s:#%d: %v", corpusFile, terr.Offset, err)
				return
			}
			corpus.err = fmt.Errorf("unmarshal error at %s: %v", corpusFile, err)
			return
		}
		// Create slices rather than using the maps directly so that
		// it's easy to execute the tests in deterministic order.
		drs := make([]*corpusDecodeResults, 0, len(r.Decode))
		for _, d := range r.Decode {
			drs = append(drs, d)
		}
		sort.Slice(drs, func(i, j int) bool {
			return drs[i].Input.Key < drs[j].Input.Key
		})
		corpus.decodeResults = drs

		ers := make([]*corpusEncodeResults, 0, len(r.Encode))
		for _, e := range r.Encode {
			ers = append(ers, e)
		}
		sort.Slice(ers, func(i, j int) bool {
			return ers[i].Input.Key < ers[j].Input.Key
		})
		corpus.encodeResults = ers
	})
}

func readCorpusDecodeResults() ([]*corpusDecodeResults, error) {
	readCorpusOnce()
	return corpus.decodeResults, corpus.err
}

func readCorpusEncodeResults() ([]*corpusEncodeResults, error) {
	readCorpusOnce()
	return corpus.encodeResults, corpus.err
}

type corpusResults struct {
	Decode map[string]*corpusDecodeResults `json:"decode,omitempty"`
	Encode map[string]*corpusEncodeResults `json:"encode,omitempty"`
}

type corpusEncodeResults struct {
	Input  *corpusEncodeInput             `json:"input"`
	Output map[string]*corpusEncodeOutput `json:"output"`
}

type corpusEncodeInput struct {
	Key               string        `json:"key"`
	Metric            *corpusMetric `json:"metric"`
	OmitInvalidFields bool          `json:"omitInvalidFields"`
	UintSupport       bool          `json:"uintSupport"`
	Precision         string        `json:"precision"`
}

type corpusEncodeOutput struct {
	Result corpusBytes `json:"result,omitempty"`
	Error  string      `json:"error,omitempty"`
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
