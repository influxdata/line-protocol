package influxdata

import (
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

func TestEncoderWithDecoderTests(t *testing.T) {
	c := qt.New(t)
	runTests := func(c *qt.C, lax bool) {
		for _, test := range decoderTests {
			if pointsHaveError(test.expect) {
				// Can't encode a test that results in an error.
				continue
			}
			c.Run(test.testName, func(c *qt.C) {
				// Always use sorted tags even though they might not
				// be sorted in the test case.
				points := append([]Point(nil), test.expect...)
				for i := range points {
					points[i] = pointWithSortedTags(points[i])
				}
				var e Encoder
				e.SetLax(lax)
				for _, p := range points {
					encodePoint(&e, p)
				}
				c.Assert(e.Err(), qt.IsNil)
				data := e.Bytes()
				c.Logf("encoded: %q", data)
				// Check that the data round-trips OK
				dec := NewDecoderWithBytes(data)
				assertDecodeResult(c, dec, points, false)
			})
		}
	}
	c.Run("strict", func(c *qt.C) {
		runTests(c, false)
	})
	c.Run("lax", func(c *qt.C) {
		runTests(c, true)
	})
}

func TestEncoderErrorOmitsLine(t *testing.T) {
	c := qt.New(t)
	var e Encoder
	e.StartLine("m1")
	e.AddField("\xff", MustNewValue(int64(1)))
	c.Assert(e.Err(), qt.ErrorMatches, `invalid field key "\\xff"`)
	c.Assert(e.Bytes(), qt.HasLen, 0)
	e.ClearErr()

	// Check that an error after the first line doesn't erase
	// everything.
	e.StartLine("m1")
	e.AddField("f", MustNewValue(int64(1)))
	e.StartLine("m2")
	c.Assert(e.Err(), qt.IsNil)
	c.Assert(string(e.Bytes()), qt.Equals, "m1 f=1i\nm2")
	e.AddField("g", MustNewValue(int64(3)))
	e.AddField("\\", MustNewValue(int64(4)))
	c.Assert(e.Err(), qt.ErrorMatches, `encoding point 2: invalid field key "\\\\"`)
	c.Assert(string(e.Bytes()), qt.Equals, "m1 f=1i")

	// Check that we can add a new line while retaining the first error.
	e.StartLine("m3")
	e.AddField("f", MustNewValue(int64(3)))
	c.Assert(string(e.Bytes()), qt.Equals, "m1 f=1i\nm3 f=3i")
	c.Assert(e.Err(), qt.ErrorMatches, `encoding point 2: invalid field key "\\\\"`)
}

func TestEncoderErrorWithOutOfOrderTags(t *testing.T) {
	c := qt.New(t)
	var e Encoder
	e.StartLine("m1")
	e.AddTag("b", "1")
	c.Assert(e.Err(), qt.IsNil)
	e.AddTag("a", "1")
	c.Assert(e.Err(), qt.ErrorMatches, `tag key "a" out of order \(previous key "b"\)`)
}

func TestEncoderAddFieldBeforeMeasurement(t *testing.T) {
	c := qt.New(t)
	var e Encoder
	e.AddField("hello", MustNewValue(int64(1)))
	c.Assert(e.Err(), qt.ErrorMatches, `field must be added after tag or measurement section`)
}

func TestEncoderEndLineWithNoField(t *testing.T) {
	c := qt.New(t)
	var e Encoder
	e.StartLine("hello")
	e.EndLine(time.Time{})
	c.Assert(e.Err(), qt.ErrorMatches, `timestamp must be added after adding at least one field`)
}

func TestEncoderAddTagBeforeStartLine(t *testing.T) {
	c := qt.New(t)
	var e Encoder
	e.AddTag("a", "b")
	c.Assert(e.Err(), qt.ErrorMatches, `tag must be added after adding a measurement and before adding fields`)
}

func TestEncoderAddTagAfterAddField(t *testing.T) {
	c := qt.New(t)
	var e Encoder
	e.StartLine("m")
	e.AddField("f", MustNewValue(int64(12)))
	e.AddTag("a", "b")
	c.Assert(e.Err(), qt.ErrorMatches, `tag must be added after adding a measurement and before adding fields`)
}

func TestEncoderStartLineWithNoFieldsOnPreviousLine(t *testing.T) {
	c := qt.New(t)
	var e Encoder
	e.StartLine("m")
	e.StartLine("n")
	c.Assert(e.Err(), qt.ErrorMatches, `encoding point 1: cannot start line without adding at least one field to previous line`)
}

func TestEncoderStartLineWithInvalidMeasurementAndNoFieldsOnPreviousLine(t *testing.T) {
	c := qt.New(t)
	var e Encoder
	e.StartLine("m")
	e.StartLine("")
	c.Assert(e.Err(), qt.ErrorMatches, `encoding point 1: cannot start line without adding at least one field to previous line`)

	// The current line is now in error state, so fields won't be added.
	e.AddField("f", MustNewValue(int64(1)))
	c.Assert(e.Bytes(), qt.HasLen, 0)

	// The next line gets added OK though.
	e.StartLine("m")
	e.AddField("f", MustNewValue(int64(1)))
	c.Assert(string(e.Bytes()), qt.Equals, "m f=1i")
}

func TestEncoderWithPrecision(t *testing.T) {
	c := qt.New(t)
	var e Encoder
	e.StartLine("x")
	e.SetPrecision(Second)
	e.AddField("f", MustNewValue(int64(1)))
	e.EndLine(time.Unix(0, 1615196563_299_053_942))
	c.Assert(string(e.Bytes()), qt.Equals, `x f=1i 1615196563`)

	e.Reset()
	e.SetPrecision(Microsecond)
	e.StartLine("x")
	e.AddField("f", MustNewValue(int64(1)))
	e.EndLine(time.Unix(0, 1615196563_299_053_942))
	c.Assert(string(e.Bytes()), qt.Equals, `x f=1i 1615196563299053`)
}

var encoderDataErrorTests = []struct {
	testName    string
	point       Point
	expectError string
}{{
	testName: "EmptyMeasurement",
	point: Point{
		Measurement: "",
		Fields: []FieldKeyValue{{
			Key:   "f",
			Value: int64(1),
		}},
	},
	expectError: `invalid measurement ""`,
}, {
	testName: "NonPrintableMeasurement",
	point: Point{
		Measurement: "\x01",
		Fields: []FieldKeyValue{{
			Key:   "f",
			Value: int64(1),
		}},
	},
	expectError: `invalid measurement "\\x01"`,
}, {
	testName: "NonUTF8Measurement",
	point: Point{
		Measurement: "\xff",
		Fields: []FieldKeyValue{{
			Key:   "f",
			Value: int64(1),
		}},
	},
	expectError: `invalid measurement "\\xff"`,
}, {
	testName: "MeasurementWithTrailingBackslash",
	point: Point{
		Measurement: "x\\",
		Fields: []FieldKeyValue{{
			Key:   "f",
			Value: int64(1),
		}},
	},
	expectError: `invalid measurement "x\\\\"`,
}, {
	testName: "InvalidTagKey",
	point: Point{
		Measurement: "m",
		Tags: []TagKeyValue{{
			Key:   "",
			Value: "x",
		}, {
			Key:   "b",
			Value: "x",
		}},
		Fields: []FieldKeyValue{{
			Key:   "f",
			Value: int64(1),
		}},
	},
	expectError: `invalid tag key ""`,
}, {
	testName: "InvalidTagValue",
	point: Point{
		Measurement: "m",
		Tags: []TagKeyValue{{
			Key:   "x",
			Value: "",
		}},
		Fields: []FieldKeyValue{{
			Key:   "f",
			Value: int64(1),
		}},
	},
	expectError: `invalid tag value x=""`,
}, {
	testName: "OutOfOrderTag",
	point: Point{
		Measurement: "m",
		Tags: []TagKeyValue{{
			Key:   "x",
			Value: "1",
		}, {
			Key:   "a",
			Value: "1",
		}},
		Fields: []FieldKeyValue{{
			Key:   "f",
			Value: int64(1),
		}},
	},
	expectError: `tag key "a" out of order \(previous key "x"\)`,
}, {
	testName: "InvalidFieldKey",
	point: Point{
		Measurement: "m",
		Fields: []FieldKeyValue{{
			Key:   "",
			Value: int64(1),
		}},
		// Include an explicit timestamp so that we test the path
		// in Endline that checks lineHasError.
		Time: "123456",
	},
	expectError: `invalid field key ""`,
}, {
	testName: "TimeStampTooEarly",
	point: Point{
		Measurement: "m",
		Fields: []FieldKeyValue{{
			Key:   "x",
			Value: int64(1),
		}},
		Time: "1000-01-01T12:00:00Z",
	},
	expectError: `timestamp 1000-01-01T12:00:00Z: line-protocol value out of range`,
}, {
	testName: "TimeStampTooLate",
	point: Point{
		Measurement: "m",
		Fields: []FieldKeyValue{{
			Key:   "x",
			Value: int64(1),
		}},
		Time: "8888-01-01T12:00:00Z",
	},
	expectError: `timestamp 8888-01-01T12:00:00Z: line-protocol value out of range`,
}}

func TestEncoderDataError(t *testing.T) {
	c := qt.New(t)
	for _, test := range encoderDataErrorTests {
		c.Run(test.testName, func(c *qt.C) {
			var e Encoder
			e.StartLine("m")
			e.AddField("f", MustNewValue(int64(1)))
			c.Assert(e.Err(), qt.IsNil)
			initialBytes := string(e.Bytes())
			encodePoint(&e, test.point)
			c.Assert(e.Err(), qt.ErrorMatches, "encoding point 1: "+test.expectError)

			// Check that the original line is still intact without any of
			// the new one.
			c.Assert(string(e.Bytes()), qt.Equals, initialBytes)

			// Check that we can add another line OK.
			e.ClearErr()
			e.StartLine("n")
			e.AddField("g", MustNewValue(int64(1)))
			c.Assert(e.Err(), qt.IsNil)
			c.Assert(string(e.Bytes()), qt.Equals, "m f=1i\nn g=1i")
		})
	}
}

func BenchmarkEncode(b *testing.B) {
	ts := time.Now()
	field1Val := []byte("ds;livjdsflvkfesdvljkdsnbvlkdfsjbldfsjhbdfklsjbvkdsjhbv")
	field2Val := []byte("12343456")
	benchmarks := []struct {
		name   string
		encode func(b *testing.B, encoder *Encoder)
	}{{
		name: "100-points",
		encode: func(b *testing.B, e *Encoder) {
			for j := 0; j < 100; j++ {
				e.StartLine("measurement")
				e.AddTag("tagfffffffdsffsdfvgdsfvdsfvdsfvd1", "blahblahblah")
				e.AddTag("uag2", "dfsvdfsvbsdfvs")
				e.AddTag("zzzzzzzzzz", "fdbgfdbgf")
				v, err := NewValueFromBytes(String, field1Val)
				if err != nil {
					b.Fatal(err)
				}
				e.AddField("f", v)
				v, err = NewValueFromBytes(Int, field2Val)
				if err != nil {
					b.Fatal(err)
				}
				e.AddField("f2", v)
				e.EndLine(ts)
			}
		},
	}, {
		name: "1-point",
		encode: func(b *testing.B, e *Encoder) {
			e.StartLine("measurement")
			e.AddTag("tagfffffffdsffsdfvgdsfvdsfvdsfvd1", "blahblahblah")
			e.AddTag("uag2", "dfsvdfsvbsdfvs")
			e.AddTag("zzzzzzzzzz", "fdbgfdbgf")
			v, err := NewValueFromBytes(String, field1Val)
			if err != nil {
				b.Fatal(err)
			}
			e.AddField("f", v)
			v, err = NewValueFromBytes(Int, field2Val)
			if err != nil {
				b.Fatal(err)
			}
			e.AddField("f2", v)
			e.EndLine(ts)
		},
	}}
	runBench := func(b *testing.B, lax bool) {
		name := "strict"
		if lax {
			name = "lax"
		}
		b.Run(name, func(b *testing.B) {
			for _, benchmark := range benchmarks {
				b.Run(benchmark.name, func(b *testing.B) {
					b.ReportAllocs()
					var e Encoder
					e.SetLax(lax)
					benchmark.encode(b, &e)
					b.SetBytes(int64(len(e.Bytes())))
					e.Reset()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						benchmark.encode(b, &e)
					}
				})
			}
		})
	}
	runBench(b, false)
	runBench(b, true)
}

func encodePoint(e *Encoder, p Point) {
	e.StartLine(p.Measurement)
	for _, tag := range p.Tags {
		e.AddTag(tag.Key, tag.Value)
	}
	for _, field := range p.Fields {
		e.AddField(field.Key, MustNewValue(field.Value))
	}
	var t time.Time
	if p.Time != "" {
		if strings.Contains(p.Time, "T") {
			// Allow RFC3339 time values so we can test out-of-bands
			// timestamps.
			t1, err := time.Parse(time.RFC3339, p.Time)
			if err != nil {
				panic(err)
			}
			t = t1
		} else {
			ns, err := strconv.ParseInt(p.Time, 10, 64)
			if err != nil {
				panic(err)
			}
			t = time.Unix(0, ns)
		}
	}
	e.EndLine(t)
}

func pointsHaveError(ps []Point) bool {
	for _, p := range ps {
		if p.MeasurementError != "" || p.TimeError != "" {
			return true
		}
		for _, tag := range p.Tags {
			if tag.Error != "" {
				return true
			}
		}
		for _, field := range p.Fields {
			if field.Error != "" {
				return true
			}
		}
	}
	return false
}

func pointWithSortedTags(p Point) Point {
	p.Tags = append([]TagKeyValue(nil), p.Tags...)
	sort.Slice(p.Tags, func(i, j int) bool {
		return p.Tags[i].Key < p.Tags[j].Key
	})
	return p
}
