package protocol

import (
	"bytes"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

func MustMetric(v Metric, err error) Metric {
	if err != nil {
		panic(err)
	}
	return v
}

var DefaultTime = func() time.Time {
	return time.Unix(42, 0)
}

var ptests = []struct {
	name     string
	input    []byte
	timeFunc func() time.Time
	metrics  []Metric
	err      error
}{
	{
		name:  "minimal",
		input: []byte("cpu value=42 0"),
		metrics: []Metric{
			MustMetric(
				New(
					"cpu",
					map[string]string{},
					map[string]interface{}{
						"value": 42.0,
					},
					time.Unix(0, 0),
				),
			),
		},
		err: nil,
	},
	{
		name:  "minimal with newline",
		input: []byte("cpu value=42 0\n"),
		metrics: []Metric{
			MustMetric(
				New(
					"cpu",
					map[string]string{},
					map[string]interface{}{
						"value": 42.0,
					},
					time.Unix(0, 0),
				),
			),
		},
		err: nil,
	},
	{
		name:  "measurement escape space",
		input: []byte(`c\ pu value=42`),
		metrics: []Metric{
			MustMetric(
				New(
					"c pu",
					map[string]string{},
					map[string]interface{}{
						"value": 42.0,
					},
					time.Unix(42, 0),
				),
			),
		},
		err: nil,
	},
	{
		name:  "measurement escape comma",
		input: []byte(`c\,pu value=42`),
		metrics: []Metric{
			MustMetric(
				New(
					"c,pu",
					map[string]string{},
					map[string]interface{}{
						"value": 42.0,
					},
					time.Unix(42, 0),
				),
			),
		},
		err: nil,
	},
	{
		name:  "tags",
		input: []byte(`cpu,cpu=cpu0,host=localhost value=42`),
		metrics: []Metric{
			MustMetric(
				New(
					"cpu",
					map[string]string{
						"cpu":  "cpu0",
						"host": "localhost",
					},
					map[string]interface{}{
						"value": 42.0,
					},
					time.Unix(42, 0),
				),
			),
		},
		err: nil,
	},
	{
		name:  "tags escape unescapable",
		input: []byte(`cpu,ho\st=localhost value=42`),
		metrics: []Metric{
			MustMetric(
				New(
					"cpu",
					map[string]string{
						`ho\st`: "localhost",
					},
					map[string]interface{}{
						"value": 42.0,
					},
					time.Unix(42, 0),
				),
			),
		},
		err: nil,
	},
	{
		name:  "tags escape equals",
		input: []byte(`cpu,ho\=st=localhost value=42`),
		metrics: []Metric{
			MustMetric(
				New(
					"cpu",
					map[string]string{
						"ho=st": "localhost",
					},
					map[string]interface{}{
						"value": 42.0,
					},
					time.Unix(42, 0),
				),
			),
		},
		err: nil,
	},
	{
		name:  "tags escape comma",
		input: []byte(`cpu,ho\,st=localhost value=42`),
		metrics: []Metric{
			MustMetric(
				New(
					"cpu",
					map[string]string{
						"ho,st": "localhost",
					},
					map[string]interface{}{
						"value": 42.0,
					},
					time.Unix(42, 0),
				),
			),
		},
		err: nil,
	},
	{
		name:  "tag value escape space",
		input: []byte(`cpu,host=two\ words value=42`),
		metrics: []Metric{
			MustMetric(
				New(
					"cpu",
					map[string]string{
						"host": "two words",
					},
					map[string]interface{}{
						"value": 42.0,
					},
					time.Unix(42, 0),
				),
			),
		},
		err: nil,
	},
	{
		name:  "tag value double escape space",
		input: []byte(`cpu,host=two\\ words value=42`),
		metrics: []Metric{
			MustMetric(
				New(
					"cpu",
					map[string]string{
						"host": `two\ words`,
					},
					map[string]interface{}{
						"value": 42.0,
					},
					time.Unix(42, 0),
				),
			),
		},
		err: nil,
	},
	{
		name:  "tag value triple escape space",
		input: []byte(`cpu,host=two\\\ words value=42`),
		metrics: []Metric{
			MustMetric(
				New(
					"cpu",
					map[string]string{
						"host": `two\\ words`,
					},
					map[string]interface{}{
						"value": 42.0,
					},
					time.Unix(42, 0),
				),
			),
		},
		err: nil,
	},
	{
		name:  "field key escape not escapable",
		input: []byte(`cpu va\lue=42`),
		metrics: []Metric{
			MustMetric(
				New(
					"cpu",
					map[string]string{},
					map[string]interface{}{
						`va\lue`: 42.0,
					},
					time.Unix(42, 0),
				),
			),
		},
		err: nil,
	},
	{
		name:  "field key escape equals",
		input: []byte(`cpu va\=lue=42`),
		metrics: []Metric{
			MustMetric(
				New(
					"cpu",
					map[string]string{},
					map[string]interface{}{
						`va=lue`: 42.0,
					},
					time.Unix(42, 0),
				),
			),
		},
		err: nil,
	},
	{
		name:  "field key escape comma",
		input: []byte(`cpu va\,lue=42`),
		metrics: []Metric{
			MustMetric(
				New(
					"cpu",
					map[string]string{},
					map[string]interface{}{
						`va,lue`: 42.0,
					},
					time.Unix(42, 0),
				),
			),
		},
		err: nil,
	},
	{
		name:  "field key escape space",
		input: []byte(`cpu va\ lue=42`),
		metrics: []Metric{
			MustMetric(
				New(
					"cpu",
					map[string]string{},
					map[string]interface{}{
						`va lue`: 42.0,
					},
					time.Unix(42, 0),
				),
			),
		},
		err: nil,
	},
	{
		name:  "field int",
		input: []byte("cpu value=42i"),
		metrics: []Metric{
			MustMetric(
				New(
					"cpu",
					map[string]string{},
					map[string]interface{}{
						"value": 42,
					},
					time.Unix(42, 0),
				),
			),
		},
		err: nil,
	},
	{
		name:    "field int overflow",
		input:   []byte("cpu value=9223372036854775808i"),
		metrics: nil,
		err: &ParseError{
			Offset:     30,
			LineNumber: 1,
			Column:     31,
			msg:        strconv.ErrRange.Error(),
			buf:        "cpu value=9223372036854775808i",
		},
	},
	{
		name:  "field int max value",
		input: []byte("cpu value=9223372036854775807i"),
		metrics: []Metric{
			MustMetric(
				New(
					"cpu",
					map[string]string{},
					map[string]interface{}{
						"value": int64(9223372036854775807),
					},
					time.Unix(42, 0),
				),
			),
		},
		err: nil,
	},
	{
		name:  "field uint",
		input: []byte("cpu value=42u"),
		metrics: []Metric{
			MustMetric(
				New(
					"cpu",
					map[string]string{},
					map[string]interface{}{
						"value": uint64(42),
					},
					time.Unix(42, 0),
				),
			),
		},
		err: nil,
	},
	{
		name:    "field uint overflow",
		input:   []byte("cpu value=18446744073709551616u"),
		metrics: nil,
		err: &ParseError{
			Offset:     31,
			LineNumber: 1,
			Column:     32,
			msg:        strconv.ErrRange.Error(),
			buf:        "cpu value=18446744073709551616u",
		},
	},
	{
		name:  "field uint max value",
		input: []byte("cpu value=18446744073709551615u"),
		metrics: []Metric{
			MustMetric(
				New(
					"cpu",
					map[string]string{},
					map[string]interface{}{
						"value": uint64(18446744073709551615),
					},
					time.Unix(42, 0),
				),
			),
		},
		err: nil,
	},
	{
		name:  "field boolean",
		input: []byte("cpu value=true"),
		metrics: []Metric{
			MustMetric(
				New(
					"cpu",
					map[string]string{},
					map[string]interface{}{
						"value": true,
					},
					time.Unix(42, 0),
				),
			),
		},
		err: nil,
	},
	{
		name:  "field string",
		input: []byte(`cpu value="42"`),
		metrics: []Metric{
			MustMetric(
				New(
					"cpu",
					map[string]string{},
					map[string]interface{}{
						"value": "42",
					},
					time.Unix(42, 0),
				),
			),
		},
		err: nil,
	},
	{
		name:  "field string escape quote",
		input: []byte(`cpu value="how\"dy"`),
		metrics: []Metric{
			MustMetric(
				New(
					"cpu",
					map[string]string{},
					map[string]interface{}{
						`value`: `how"dy`,
					},
					time.Unix(42, 0),
				),
			),
		},
		err: nil,
	},
	{
		name:  "field string escape backslash",
		input: []byte(`cpu value="how\\dy"`),
		metrics: []Metric{
			MustMetric(
				New(
					"cpu",
					map[string]string{},
					map[string]interface{}{
						`value`: `how\dy`,
					},
					time.Unix(42, 0),
				),
			),
		},
		err: nil,
	},
	{
		name:  "field string newline",
		input: []byte("cpu value=\"4\n2\""),
		metrics: []Metric{
			MustMetric(
				New(
					"cpu",
					map[string]string{},
					map[string]interface{}{
						"value": "4\n2",
					},
					time.Unix(42, 0),
				),
			),
		},
		err: nil,
	},
	{
		name:  "no timestamp",
		input: []byte("cpu value=42"),
		metrics: []Metric{
			MustMetric(
				New(
					"cpu",
					map[string]string{},
					map[string]interface{}{
						"value": 42.0,
					},
					time.Unix(42, 0),
				),
			),
		},
		err: nil,
	},
	{
		name:  "no timestamp",
		input: []byte("cpu value=42"),
		timeFunc: func() time.Time {
			return time.Unix(42, 123456789)
		},
		metrics: []Metric{
			MustMetric(
				New(
					"cpu",
					map[string]string{},
					map[string]interface{}{
						"value": 42.0,
					},
					time.Unix(42, 123456789),
				),
			),
		},
		err: nil,
	},
	{
		name:  "multiple lines",
		input: []byte("cpu value=42\ncpu value=42"),
		metrics: []Metric{
			MustMetric(
				New(
					"cpu",
					map[string]string{},
					map[string]interface{}{
						"value": 42.0,
					},
					time.Unix(42, 0),
				),
			),
			MustMetric(
				New(
					"cpu",
					map[string]string{},
					map[string]interface{}{
						"value": 42.0,
					},
					time.Unix(42, 0),
				),
			),
		},
		err: nil,
	},
	{
		name:    "invalid measurement only",
		input:   []byte("cpu"),
		metrics: nil,
		err: &ParseError{
			Offset:     3,
			LineNumber: 1,
			Column:     4,
			msg:        ErrTagParse.Error(),
			buf:        "cpu",
		},
	},
	{
		name:  "procstat",
		input: []byte("procstat,exe=bash,process_name=bash voluntary_context_switches=42i,memory_rss=5103616i,rlimit_memory_data_hard=2147483647i,cpu_time_user=0.02,rlimit_file_locks_soft=2147483647i,pid=29417i,cpu_time_nice=0,rlimit_memory_locked_soft=65536i,read_count=259i,rlimit_memory_vms_hard=2147483647i,memory_swap=0i,rlimit_num_fds_soft=1024i,rlimit_nice_priority_hard=0i,cpu_time_soft_irq=0,cpu_time=0i,rlimit_memory_locked_hard=65536i,realtime_priority=0i,signals_pending=0i,nice_priority=20i,cpu_time_idle=0,memory_stack=139264i,memory_locked=0i,rlimit_memory_stack_soft=8388608i,cpu_time_iowait=0,cpu_time_guest=0,cpu_time_guest_nice=0,rlimit_memory_data_soft=2147483647i,read_bytes=0i,rlimit_cpu_time_soft=2147483647i,involuntary_context_switches=2i,write_bytes=106496i,cpu_time_system=0,cpu_time_irq=0,cpu_usage=0,memory_vms=21659648i,memory_data=1576960i,rlimit_memory_stack_hard=2147483647i,num_threads=1i,rlimit_memory_rss_soft=2147483647i,rlimit_realtime_priority_soft=0i,num_fds=4i,write_count=35i,rlimit_signals_pending_soft=78994i,cpu_time_steal=0,rlimit_num_fds_hard=4096i,rlimit_file_locks_hard=2147483647i,rlimit_cpu_time_hard=2147483647i,rlimit_signals_pending_hard=78994i,rlimit_nice_priority_soft=0i,rlimit_memory_rss_hard=2147483647i,rlimit_memory_vms_soft=2147483647i,rlimit_realtime_priority_hard=0i 1517620624000000000"),
		metrics: []Metric{
			MustMetric(
				New(
					"procstat",
					map[string]string{
						"exe":          "bash",
						"process_name": "bash",
					},
					map[string]interface{}{
						"cpu_time":                      0,
						"cpu_time_guest":                float64(0),
						"cpu_time_guest_nice":           float64(0),
						"cpu_time_idle":                 float64(0),
						"cpu_time_iowait":               float64(0),
						"cpu_time_irq":                  float64(0),
						"cpu_time_nice":                 float64(0),
						"cpu_time_soft_irq":             float64(0),
						"cpu_time_steal":                float64(0),
						"cpu_time_system":               float64(0),
						"cpu_time_user":                 float64(0.02),
						"cpu_usage":                     float64(0),
						"involuntary_context_switches":  2,
						"memory_data":                   1576960,
						"memory_locked":                 0,
						"memory_rss":                    5103616,
						"memory_stack":                  139264,
						"memory_swap":                   0,
						"memory_vms":                    21659648,
						"nice_priority":                 20,
						"num_fds":                       4,
						"num_threads":                   1,
						"pid":                           29417,
						"read_bytes":                    0,
						"read_count":                    259,
						"realtime_priority":             0,
						"rlimit_cpu_time_hard":          2147483647,
						"rlimit_cpu_time_soft":          2147483647,
						"rlimit_file_locks_hard":        2147483647,
						"rlimit_file_locks_soft":        2147483647,
						"rlimit_memory_data_hard":       2147483647,
						"rlimit_memory_data_soft":       2147483647,
						"rlimit_memory_locked_hard":     65536,
						"rlimit_memory_locked_soft":     65536,
						"rlimit_memory_rss_hard":        2147483647,
						"rlimit_memory_rss_soft":        2147483647,
						"rlimit_memory_stack_hard":      2147483647,
						"rlimit_memory_stack_soft":      8388608,
						"rlimit_memory_vms_hard":        2147483647,
						"rlimit_memory_vms_soft":        2147483647,
						"rlimit_nice_priority_hard":     0,
						"rlimit_nice_priority_soft":     0,
						"rlimit_num_fds_hard":           4096,
						"rlimit_num_fds_soft":           1024,
						"rlimit_realtime_priority_hard": 0,
						"rlimit_realtime_priority_soft": 0,
						"rlimit_signals_pending_hard":   78994,
						"rlimit_signals_pending_soft":   78994,
						"signals_pending":               0,
						"voluntary_context_switches":    42,
						"write_bytes":                   106496,
						"write_count":                   35,
					},
					time.Unix(0, 1517620624000000000),
				),
			),
		},
		err: nil,
	},
}

func TestParser(t *testing.T) {
	for _, tt := range ptests {
		t.Run(tt.name, func(t *testing.T) {
			handler := NewMetricHandler()
			parser := NewParser(handler)
			parser.SetTimeFunc(DefaultTime)
			if tt.timeFunc != nil {
				parser.SetTimeFunc(tt.timeFunc)
			}

			metrics, err := parser.Parse(tt.input)
			if (err != nil) != (tt.err != nil) {
				t.Errorf("unexpected error difference: %v, want = %v", err, tt.err)
				return
			} else if tt.err != nil && err.Error() != tt.err.Error() {
				t.Errorf("unexpected error difference: %v, want = %v", err, tt.err)
			}

			if got, want := len(metrics), len(tt.metrics); got != want {
				t.Errorf("unexpected metric length difference: %d, want = %d", got, want)
			}

			for i, expected := range tt.metrics {
				RequireMetricEqual(t, expected, metrics[i])
			}
		})
	}
}

func BenchmarkParser(b *testing.B) {
	for _, tt := range ptests {
		b.Run(tt.name, func(b *testing.B) {
			handler := NewMetricHandler()
			parser := NewParser(handler)
			for n := 0; n < b.N; n++ {
				metrics, err := parser.Parse(tt.input)
				_ = err
				_ = metrics
			}
		})
	}
}

func TestStreamParser(t *testing.T) {
	for _, tt := range ptests {
		t.Run(tt.name, func(t *testing.T) {
			r := bytes.NewBuffer(tt.input)
			parser := NewStreamParser(r)
			parser.SetTimeFunc(DefaultTime)
			if tt.timeFunc != nil {
				parser.SetTimeFunc(tt.timeFunc)
			}

			var i int
			for {
				m, err := parser.Next()
				if err != nil {
					if err == EOF {
						break
					}
					if (err != nil) == (tt.err != nil) && err.Error() != tt.err.Error() {
						t.Errorf("unexpected error difference: %v, want = %v", err, tt.err)
					}
					break
				}

				RequireMetricEqual(t, tt.metrics[i], m)
				i++
			}
		})
	}
}

func TestSeriesParser(t *testing.T) {
	var tests = []struct {
		name     string
		input    []byte
		timeFunc func() time.Time
		metrics  []Metric
		err      error
	}{
		{
			name:    "empty",
			input:   []byte(""),
			metrics: []Metric{},
		},
		{
			name:  "minimal",
			input: []byte("cpu"),
			metrics: []Metric{
				MustMetric(
					New(
						"cpu",
						map[string]string{},
						map[string]interface{}{},
						time.Unix(0, 0),
					),
				),
			},
		},
		{
			name:  "tags",
			input: []byte("cpu,a=x,b=y"),
			metrics: []Metric{
				MustMetric(
					New(
						"cpu",
						map[string]string{
							"a": "x",
							"b": "y",
						},
						map[string]interface{}{},
						time.Unix(0, 0),
					),
				),
			},
		},
		{
			name:    "missing tag value",
			input:   []byte("cpu,a="),
			metrics: []Metric{},
			err: &ParseError{
				Offset:     6,
				LineNumber: 1,
				Column:     7,
				msg:        ErrTagParse.Error(),
				buf:        "cpu,a=",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := NewMetricHandler()
			parser := NewSeriesParser(handler)
			if tt.timeFunc != nil {
				parser.SetTimeFunc(tt.timeFunc)
			}

			metrics, err := parser.Parse(tt.input)

			if (err != nil) != (tt.err != nil) {
				t.Errorf("unexpected error difference: %v, want = %v", err, tt.err)
				return
			} else if tt.err != nil && err.Error() != tt.err.Error() {
				t.Errorf("unexpected error difference: %v, want = %v", err, tt.err)
			}

			if got, want := len(metrics), len(tt.metrics); got != want {
				t.Errorf("unexpected metric length difference: %d, want = %d", got, want)
			}

			for i, expected := range tt.metrics {
				if got, want := metrics[i].Name(), expected.Name(); got != want {
					t.Errorf("unexpected metric name difference: %v, want = %v", got, want)
				}
				if got, want := len(metrics[i].TagList()), len(expected.TagList()); got != want {
					t.Errorf("unexpected tag length difference: %d, want = %d", got, want)
					break
				}

				got := metrics[i].TagList()
				want := expected.TagList()
				for i := range got {
					if got[i].Key != want[i].Key {
						t.Errorf("unexpected tag key difference: %v, want = %v", got[i].Key, want[i].Key)
					}
					if got[i].Value != want[i].Value {
						t.Errorf("unexpected tag key difference: %v, want = %v", got[i].Value, want[i].Value)
					}
				}
			}
		})
	}
}

func TestParserErrorString(t *testing.T) {
	var ptests = []struct {
		name      string
		input     []byte
		errString string
	}{
		{
			name:      "multiple line error",
			input:     []byte("cpu value=42\ncpu value=invalid\ncpu value=42"),
			errString: `metric parse error: expected field at 2:11: "cpu value=invalid"`,
		},
		{
			name:      "handler error",
			input:     []byte("cpu value=9223372036854775808i\ncpu value=42"),
			errString: `metric parse error: value out of range at 1:31: "cpu value=9223372036854775808i"`,
		},
		{
			name:      "buffer too long",
			input:     []byte("cpu " + strings.Repeat("ab", maxErrorBufferSize) + "=invalid\ncpu value=42"),
			errString: "metric parse error: expected field at 1:2054: \"cpu " + strings.Repeat("ab", maxErrorBufferSize)[:maxErrorBufferSize-4] + "...\"",
		},
		{
			name:      "multiple line error",
			input:     []byte("cpu value=42\ncpu value=invalid\ncpu value=42\ncpu value=invalid"),
			errString: `metric parse error: expected field at 2:11: "cpu value=invalid"`,
		},
	}

	for _, tt := range ptests {
		t.Run(tt.name, func(t *testing.T) {
			handler := NewMetricHandler()
			parser := NewParser(handler)

			_, err := parser.Parse(tt.input)
			if err.Error() != tt.errString {
				t.Errorf("unexpected error difference: %v, want = %v", err.Error(), tt.errString)
			}
		})
	}
}

func TestStreamParserErrorString(t *testing.T) {
	var ptests = []struct {
		name  string
		input []byte
		errs  []string
	}{
		{
			name:  "multiple line error",
			input: []byte("cpu value=42\ncpu value=invalid\ncpu value=42"),
			errs: []string{
				`metric parse error: expected field at 2:11: "cpu value="`,
			},
		},
		{
			name:  "handler error",
			input: []byte("cpu value=9223372036854775808i\ncpu value=42"),
			errs: []string{
				`metric parse error: value out of range at 1:31: "cpu value=9223372036854775808i"`,
			},
		},
		{
			name:  "buffer too long",
			input: []byte("cpu " + strings.Repeat("ab", maxErrorBufferSize) + "=invalid\ncpu value=42"),
			errs: []string{
				"metric parse error: expected field at 1:2054: \"cpu " + strings.Repeat("ab", maxErrorBufferSize)[:maxErrorBufferSize-4] + "...\"",
			},
		},
		{
			name:  "multiple errors",
			input: []byte("foo value=1asdf2.0\nfoo value=2.0\nfoo value=3asdf2.0\nfoo value=4.0"),
			errs: []string{
				`metric parse error: expected field at 1:12: "foo value=1"`,
				`metric parse error: expected field at 3:12: "foo value=3"`,
			},
		},
	}

	for _, tt := range ptests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewStreamParser(bytes.NewBuffer(tt.input))

			var errs []error
			for i := 0; i < 20; i++ {
				_, err := parser.Next()
				if err == EOF {
					break
				}

				if err != nil {
					errs = append(errs, err)
				}
			}

			if got, want := len(errs), len(tt.errs); got != want {
				t.Errorf("unexpected error length difference: %d, want = %d", got, want)
			}

			for i, err := range errs {
				if err.Error() != tt.errs[i] {
					t.Errorf("unexpected error difference: %v, want = %v", err.Error(), tt.errs[i])
				}
			}
		})
	}
}

// RequireMetricEqual halts the test with an error if the metrics are not
// equal.
func RequireMetricEqual(t *testing.T, expected, actual Metric) {
	t.Helper()

	var lhs, rhs *metricDiff
	if expected != nil {
		lhs = newMetricDiff(expected)
	}
	if actual != nil {
		rhs = newMetricDiff(actual)
	}

	if !reflect.DeepEqual(lhs, rhs) {
		t.Fatalf("Metric %v, want=%v", rhs, lhs)
	}
}

type metricDiff struct {
	Measurement string
	Tags        []*Tag
	Fields      []*Field
	Time        time.Time
}

func newMetricDiff(metric Metric) *metricDiff {
	if metric == nil {
		return nil
	}

	m := &metricDiff{}
	m.Measurement = metric.Name()
	m.Tags = append(m.Tags, metric.TagList()...)
	m.Fields = append(m.Fields, metric.FieldList()...)

	sort.Slice(m.Tags, func(i, j int) bool {
		return m.Tags[i].Key < m.Tags[j].Key
	})

	sort.Slice(m.Fields, func(i, j int) bool {
		return m.Fields[i].Key < m.Fields[j].Key
	})

	m.Time = metric.Time()
	return m
}
