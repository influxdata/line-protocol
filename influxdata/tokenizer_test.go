package influxdata

import (
	"bytes"
	"fmt"
	"io"
	"regexp"
	"strings"
	"testing"
	"testing/iotest"

	qt "github.com/frankban/quicktest"
)

type TagKeyValue struct {
	Key, Value string
	Error      string
}

type FieldKeyValue struct {
	Key, Value string
	Kind       ValueKind
	Error      string
}

type Point struct {
	Measurement      string
	MeasurementError string
	Tags             []TagKeyValue
	Fields           []FieldKeyValue
	Time             string
	TimeError        string
}

// sectionCheckers holds a function for each section that checks that the result of tokenization
// for that section is as expected.
var sectionCheckers = []func(c *qt.C, tok *Tokenizer, expect Point){
	MeasurementSection: func(c *qt.C, tok *Tokenizer, expect Point) {
		m, err := tok.Measurement()
		if expect.MeasurementError != "" {
			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(expect.MeasurementError), qt.Commentf("measurement %q", m))
			return
		}

		c.Assert(err, qt.IsNil)
		c.Assert(string(m), qt.Equals, expect.Measurement, qt.Commentf("runes: %x", []rune(string(m))))
	},
	TagSection: func(c *qt.C, tok *Tokenizer, expect Point) {
		var tags []TagKeyValue
		for {
			key, value, err := tok.NextTag()
			if err != nil {
				c.Assert(key, qt.IsNil)
				c.Assert(value, qt.IsNil)
				tags = append(tags, TagKeyValue{
					Error: err.Error(),
				})
				continue
			}
			if key == nil {
				break
			}
			tags = append(tags, TagKeyValue{
				Key:   string(key),
				Value: string(value),
			})
		}
		c.Assert(tags, qt.DeepEquals, expect.Tags)
	},
	FieldSection: func(c *qt.C, tok *Tokenizer, expect Point) {
		var fields []FieldKeyValue
		for {
			key, kind, value, err := tok.NextFieldBytes()
			if err != nil {
				c.Assert(key, qt.IsNil)
				c.Assert(value, qt.IsNil)
				fields = append(fields, FieldKeyValue{
					Error: err.Error(),
				})
				continue
			}
			if key == nil {
				break
			}
			fields = append(fields, FieldKeyValue{
				Key:   string(key),
				Value: string(value),
				Kind:  kind,
			})
		}
		c.Assert(fields, qt.DeepEquals, expect.Fields)
	},
	TimeSection: func(c *qt.C, tok *Tokenizer, expect Point) {
		timeBytes, err := tok.TimeBytes()
		if expect.TimeError != "" {
			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(expect.TimeError))
			c.Assert(timeBytes, qt.IsNil)
			return
		}
		c.Assert(err, qt.IsNil)
		if expect.Time == "" {
			c.Assert(timeBytes, qt.IsNil)
			return
		}
		c.Assert(string(timeBytes), qt.Equals, expect.Time)
	},
}

var tokenizerTests = []struct {
	testName string
	// text holds the text to be tokenized.
	// sections are separated by § characters.
	// entries are separated by ¶ characters.
	text        string
	expect      []Point
	expectStats Stats
}{{
	testName: "all-fields-present-no-escapes",
	text: `
   # comment
 somename,§tag1=val1,tag2=val2  §floatfield=1,strfield="hello",intfield=-1i,uintfield=1u,boolfield=true  §1602841605822791506
`,
	expect: []Point{{
		Measurement: "somename",
		Tags: []TagKeyValue{{
			Key:   "tag1",
			Value: "val1",
		}, {
			Key:   "tag2",
			Value: "val2",
		}},
		Fields: []FieldKeyValue{{
			Key:   "floatfield",
			Kind:  Float,
			Value: "1",
		}, {
			Key:   "strfield",
			Kind:  String,
			Value: "hello",
		}, {
			Key:   "intfield",
			Kind:  Int,
			Value: "-1",
		}, {
			Key:   "uintfield",
			Kind:  Uint,
			Value: "1",
		}, {
			Key:   "boolfield",
			Kind:  Bool,
			Value: "true",
		}},
		Time: "1602841605822791506",
	}},
	expectStats: mkStats(StatComment),
}, {
	testName: "multiple-entries",
	text: `
   # comment
 m1,§tag1=val1  §x="first"  §1602841605822791506
¶  m2,§foo=bar  §x="second"  §1602841605822792000

 # last comment
`,
	expect: []Point{{
		Measurement: "m1",
		Tags: []TagKeyValue{{
			Key:   "tag1",
			Value: "val1",
		}},
		Fields: []FieldKeyValue{{
			Key:   "x",
			Kind:  String,
			Value: "first",
		}},
		Time: "1602841605822791506",
	}, {
		Measurement: "m2",
		Tags: []TagKeyValue{{
			Key:   "foo",
			Value: "bar",
		}},
		Fields: []FieldKeyValue{{
			Key:   "x",
			Kind:  String,
			Value: "second",
		}},
		Time: "1602841605822792000",
	}},
	expectStats: mkStats(StatComment),
}, {
	testName: "escaped-values",
	text: `
 comma\,1,§equals\==e\,x,two=val2 §field\=x="fir\"
,st\\"  §1602841605822791506

 # last comment
`,
	expect: []Point{{
		Measurement: "comma,1",
		Tags: []TagKeyValue{{
			Key:   "equals=",
			Value: "e,x",
		}, {
			Key:   "two",
			Value: "val2",
		}},
		Fields: []FieldKeyValue{{
			Key:   "field=x",
			Kind:  String,
			Value: "fir\"\n,st\\",
		}},
		Time: "1602841605822791506",
	}},
	expectStats: mkStats(StatStrLiteralNewline, StatComment),
}, {
	testName: "missing-quotes",
	text:     `TestBucket§ §FieldOne=Happy,FieldTwo=sad§`,
	expect: []Point{{
		Measurement: "TestBucket",
		Fields: []FieldKeyValue{{
			Key:   "FieldOne",
			Kind:  Unknown,
			Value: "Happy",
		}, {
			Key:   "FieldTwo",
			Kind:  Unknown,
			Value: "sad",
		}},
	}},
	expectStats: mkStats(StatUnknownFieldType),
}, {
	testName: "trailing-comma-after-measurement",
	text: `TestBucket,§ §FieldOne=Happy§¶
next§ §x=1§`,
	expect: []Point{{
		MeasurementError: "expected tag key after comma; got white space instead",
	}, {
		Measurement: "next",
		Fields: []FieldKeyValue{{
			Key:   "x",
			Kind:  Float,
			Value: "1",
		}},
	}},
}, {
	testName: "missing-comma-after-field",
	text:     `TestBucket§ §TagOne="Happy" §FieldOne=123.45`,
	expect: []Point{{
		Measurement: "TestBucket",
		Fields: []FieldKeyValue{{
			Key:   "TagOne",
			Kind:  String,
			Value: "Happy",
		}},
		TimeError: `invalid timestamp ("FieldOne=123.45")`,
	}},
}, {
	testName: "missing timestamp",
	text:     "b§ §f=1§",
	expect: []Point{{
		Measurement: "b",
		Fields: []FieldKeyValue{{
			Key:   "f",
			Kind:  Float,
			Value: "1",
		}},
		Time: "",
	}},
}, {
	testName: "missing timestamp with newline",
	text:     "b§ §f=1§\n",
	expect: []Point{{
		Measurement: "b",
		Fields: []FieldKeyValue{{
			Key:   "f",
			Kind:  Float,
			Value: "1",
		}},
		Time: "",
	}},
}, {
	testName: "field-with-space-and-no-timestamp",
	text:     "9§ §f=-7 §",
	expect: []Point{{
		Measurement: "9",
		Fields: []FieldKeyValue{{
			Key:   "f",
			Kind:  Float,
			Value: "-7",
		}},
		Time: "",
	}},
}, {
	testName: "measurement-with-leading-quote",
	text:     `"x x=1`,
	expect: []Point{{
		Measurement: `"x`,
		Fields: []FieldKeyValue{{
			Key:   "x",
			Kind:  Float,
			Value: "1",
		}},
	}},
	expectStats: mkStats(StatQuoteAtStart),
}, {
	testName: "tag-key-with-leading-quote",
	text:     `x,"t=x x=1`,
	expect: []Point{{
		Measurement: `x`,
		Tags: []TagKeyValue{{
			Key:   `"t`,
			Value: "x",
		}},
		Fields: []FieldKeyValue{{
			Key:   "x",
			Kind:  Float,
			Value: "1",
		}},
	}},
	expectStats: mkStats(StatQuoteAtStart),
}, {
	testName: "tag-value-with-leading-quote",
	text:     `x,t="x x=1`,
	expect: []Point{{
		Measurement: `x`,
		Tags: []TagKeyValue{{
			Key:   `t`,
			Value: `"x`,
		}},
		Fields: []FieldKeyValue{{
			Key:   "x",
			Kind:  Float,
			Value: "1",
		}},
	}},
	expectStats: mkStats(StatQuoteAtStart),
}, {
	testName: "field-key-with-leading-quote",
	text:     `x "x=1`,
	expect: []Point{{
		Measurement: `x`,
		Fields: []FieldKeyValue{{
			Key:   `"x`,
			Kind:  Float,
			Value: "1",
		}},
	}},
	expectStats: mkStats(StatQuoteAtStart),
}, {
	testName: "measurement-with-backslash-n",
	text:     `x\n x=1`,
	expect: []Point{{
		Measurement: `x\n`,
		Fields: []FieldKeyValue{{
			Key:   `x`,
			Kind:  Float,
			Value: "1",
		}},
	}},
	expectStats: mkStats(StatBackslashN),
}, {
	testName: "tag-key-with-backslash-n",
	text:     `x,t\n=x x=1`,
	expect: []Point{{
		Measurement: `x`,
		Tags: []TagKeyValue{{
			Key:   `t\n`,
			Value: "x",
		}},
		Fields: []FieldKeyValue{{
			Key:   "x",
			Kind:  Float,
			Value: "1",
		}},
	}},
	expectStats: mkStats(StatBackslashN),
}, {
	testName: "tag-value-with-backslash-n",
	text:     `x,t=x\n x=1`,
	expect: []Point{{
		Measurement: `x`,
		Tags: []TagKeyValue{{
			Key:   `t`,
			Value: `x\n`,
		}},
		Fields: []FieldKeyValue{{
			Key:   "x",
			Kind:  Float,
			Value: "1",
		}},
	}},
	expectStats: mkStats(StatBackslashN),
}, {
	testName: "field-key-with-backslash-n",
	text:     `x x\n=1`,
	expect: []Point{{
		Measurement: `x`,
		Fields: []FieldKeyValue{{
			Key:   `x\n`,
			Kind:  Float,
			Value: "1",
		}},
	}},
	expectStats: mkStats(StatBackslashN),
}, {
	testName: "measurement-with-backslash-r",
	text:     `x\r x=1`,
	expect: []Point{{
		Measurement: `x\r`,
		Fields: []FieldKeyValue{{
			Key:   `x`,
			Kind:  Float,
			Value: "1",
		}},
	}},
	expectStats: mkStats(StatBackslashR),
}, {
	testName: "tag-key-with-backslash-r",
	text:     `x,t\r=x x=1`,
	expect: []Point{{
		Measurement: `x`,
		Tags: []TagKeyValue{{
			Key:   `t\r`,
			Value: "x",
		}},
		Fields: []FieldKeyValue{{
			Key:   "x",
			Kind:  Float,
			Value: "1",
		}},
	}},
	expectStats: mkStats(StatBackslashR),
}, {
	testName: "tag-value-with-backslash-r",
	text:     `x,t=x\r x=1`,
	expect: []Point{{
		Measurement: `x`,
		Tags: []TagKeyValue{{
			Key:   `t`,
			Value: `x\r`,
		}},
		Fields: []FieldKeyValue{{
			Key:   "x",
			Kind:  Float,
			Value: "1",
		}},
	}},
	expectStats: mkStats(StatBackslashR),
}, {
	testName: "field-key-with-backslash-r",
	text:     `x x\r=1`,
	expect: []Point{{
		Measurement: `x`,
		Fields: []FieldKeyValue{{
			Key:   `x\r`,
			Kind:  Float,
			Value: "1",
		}},
	}},
	expectStats: mkStats(StatBackslashR),
}, {
	testName: "measurement-with-backslash-t",
	text:     `x\t x=1`,
	expect: []Point{{
		Measurement: `x\t`,
		Fields: []FieldKeyValue{{
			Key:   `x`,
			Kind:  Float,
			Value: "1",
		}},
	}},
	expectStats: mkStats(StatBackslashT),
}, {
	testName: "tag-key-with-backslash-t",
	text:     `x,t\t=x x=1`,
	expect: []Point{{
		Measurement: `x`,
		Tags: []TagKeyValue{{
			Key:   `t\t`,
			Value: "x",
		}},
		Fields: []FieldKeyValue{{
			Key:   "x",
			Kind:  Float,
			Value: "1",
		}},
	}},
	expectStats: mkStats(StatBackslashT),
}, {
	testName: "tag-value-with-backslash-t",
	text:     `x,t=x\t x=1`,
	expect: []Point{{
		Measurement: `x`,
		Tags: []TagKeyValue{{
			Key:   `t`,
			Value: `x\t`,
		}},
		Fields: []FieldKeyValue{{
			Key:   "x",
			Kind:  Float,
			Value: "1",
		}},
	}},
	expectStats: mkStats(StatBackslashT),
}, {
	testName: "field-key-with-backslash-t",
	text:     `x x\t=1`,
	expect: []Point{{
		Measurement: `x`,
		Fields: []FieldKeyValue{{
			Key:   `x\t`,
			Kind:  Float,
			Value: "1",
		}},
	}},
	expectStats: mkStats(StatBackslashT),
}, {
	testName: "measurement-with-backslash-f",
	text:     `x\f x=1`,
	expect: []Point{{
		Measurement: `x\f`,
		Fields: []FieldKeyValue{{
			Key:   `x`,
			Kind:  Float,
			Value: "1",
		}},
	}},
	expectStats: mkStats(StatBackslashF),
}, {
	testName: "tag-key-with-backslash-f",
	text:     `x,t\f=x x=1`,
	expect: []Point{{
		Measurement: `x`,
		Tags: []TagKeyValue{{
			Key:   `t\f`,
			Value: "x",
		}},
		Fields: []FieldKeyValue{{
			Key:   "x",
			Kind:  Float,
			Value: "1",
		}},
	}},
	expectStats: mkStats(StatBackslashF),
}, {
	testName: "tag-value-with-backslash-f",
	text:     `x,t=x\f x=1`,
	expect: []Point{{
		Measurement: `x`,
		Tags: []TagKeyValue{{
			Key:   `t`,
			Value: `x\f`,
		}},
		Fields: []FieldKeyValue{{
			Key:   "x",
			Kind:  Float,
			Value: "1",
		}},
	}},
	expectStats: mkStats(StatBackslashF),
}, {
	testName: "field-key-with-backslash-f",
	text:     `x x\f=1`,
	expect: []Point{{
		Measurement: `x`,
		Fields: []FieldKeyValue{{
			Key:   `x\f`,
			Kind:  Float,
			Value: "1",
		}},
	}},
	expectStats: mkStats(StatBackslashF),
}, {
	testName: "measurement-with-non-printable-char",
	text:     "x\x01 x=1",
	expect: []Point{{
		Measurement: "x\x01",
		Fields: []FieldKeyValue{{
			Key:   `x`,
			Kind:  Float,
			Value: "1",
		}},
	}},
	expectStats: mkStats(StatNonPrintable),
}, {
	testName: "measurement-with-equals",
	text:     "x= x=1",
	expect: []Point{{
		Measurement: "x=",
		Fields: []FieldKeyValue{{
			Key:   `x`,
			Kind:  Float,
			Value: "1",
		}},
	}},
	expectStats: mkStats(StatMeasurementEquals),
}, {
	testName: "field-key-with-equals",
	text:     `x x\==1`,
	expect: []Point{{
		Measurement: "x",
		Fields: []FieldKeyValue{{
			Key:   `x=`,
			Kind:  Float,
			Value: "1",
		}},
	}},
	expectStats: 0,
}, {
	testName: "field-key-with-double-backslash-separator",
	text:     `x x\\==1`,
	expect: []Point{{
		Measurement: `x`,
		Fields: []FieldKeyValue{{
			Key:   `x\=`,
			Kind:  Float,
			Value: "1",
		}},
	}},
	expectStats: mkStats(StatBackslashBackslash, StatDoubleBackslashSeparator),
}, {
	testName: "field-key-with-more-backslash-separator",
	text:     `x x\\\\==1`,
	expect: []Point{{
		Measurement: `x`,
		Fields: []FieldKeyValue{{
			Key:   `x\\\=`,
			Kind:  Float,
			Value: "1",
		}},
	}},
	expectStats: mkStats(StatBackslashBackslash, StatDoubleBackslashSeparator),
}, {
	testName: "field-str-value-with-backslash-n",
	text:     `x x="x\n"`,
	expect: []Point{{
		Measurement: `x`,
		Fields: []FieldKeyValue{{
			Key:   "x",
			Kind:  String,
			Value: `x\n`,
		}},
	}},
	expectStats: mkStats(StatStrBackslashN),
}, {
	testName: "field-str-value-with-backslash-r",
	text:     `x x="x\r"`,
	expect: []Point{{
		Measurement: `x`,
		Fields: []FieldKeyValue{{
			Key:   "x",
			Kind:  String,
			Value: `x\r`,
		}},
	}},
	expectStats: mkStats(StatStrBackslashR),
}, {
	testName: "field-str-value-with-backslash-t",
	text:     `x x="x\t"`,
	expect: []Point{{
		Measurement: `x`,
		Fields: []FieldKeyValue{{
			Key:   "x",
			Kind:  String,
			Value: `x\t`,
		}},
	}},
	expectStats: mkStats(StatStrBackslashT),
}, {
	testName: "field-str-value-with-backslash-f",
	text:     `x x="x\f"`,
	expect: []Point{{
		Measurement: `x`,
		Fields: []FieldKeyValue{{
			Key:   "x",
			Kind:  String,
			Value: `x\f`,
		}},
	}},
	expectStats: mkStats(StatStrBackslashF),
}, {
	testName: "whitespace-with-tab",
	text:     "x\ty=1",
	expect: []Point{{
		Measurement: `x`,
		Fields: []FieldKeyValue{{
			Key:   "y",
			Kind:  Float,
			Value: `1`,
		}},
	}},
	expectStats: mkStats(StatWhitespaceTab),
}, {
	testName: "whitespace-with-cr",
	text:     "x\ry=1",
	expect: []Point{{
		Measurement: `x`,
		Fields: []FieldKeyValue{{
			Key:   "y",
			Kind:  Float,
			Value: `1`,
		}},
	}},
	expectStats: mkStats(StatWhitespaceCR),
}, {
	testName: "whitespace-with-ff",
	text:     "x\fy=1",
	expect: []Point{{
		Measurement: `x`,
		Fields: []FieldKeyValue{{
			Key:   "y",
			Kind:  Float,
			Value: `1`,
		}},
	}},
	expectStats: mkStats(StatWhitespaceFF),
}, {
	testName: "all-the-stats",
	text:     "\t\"=\x02X\fF=unknown,y\\n\\r\\t\\f\\\\==\"\n\\n\\r\\t\\f\"\t1602841605822791506\n\r#comment",
	expect: []Point{{
		Measurement: "\"=\x02X",
		Fields: []FieldKeyValue{{
			Key:   "F",
			Kind:  Unknown,
			Value: `unknown`,
		}, {
			Key:   "y\\n\\r\\t\\f\\=",
			Kind:  String,
			Value: "\n\\n\\r\\t\\f",
		}},
		Time: "1602841605822791506",
	}},
	expectStats: mkStats(
		StatStrLiteralNewline,
		StatBackslashN,
		StatBackslashR,
		StatBackslashT,
		StatBackslashF,
		StatBackslashBackslash,
		StatNonPrintable,
		StatMeasurementEquals,
		StatDoubleBackslashSeparator,
		StatQuoteAtStart,
		StatStrBackslashN,
		StatStrBackslashR,
		StatStrBackslashT,
		StatStrBackslashF,
		StatWhitespaceTab,
		StatWhitespaceCR,
		StatWhitespaceFF,
		StatUnknownFieldType,
		StatComment,
	),
}}

func TestTokenizer(t *testing.T) {
	c := qt.New(t)
	for _, test := range tokenizerTests {
		c.Run(test.testName, func(c *qt.C) {
			// Remove section and entry separators, as we're testing all sections.
			tok := NewTokenizerWithBytes([]byte(removeTestSeparators(test.text)))
			assertTokenizeResult(c, tok, test.expect, false)
			got, want := tok.Stats(), test.expectStats
			c.Assert(got, qt.Equals, want, qt.Commentf("only in got: %v; only in want: %v", got&^want, want&^got))
		})
	}
}

// assertTokenizeResult asserts that the tokens from tok match
// the expected points and returns the number of points
// consumed. If allowMore is true, it's OK for there
// to be more points than expected.
func assertTokenizeResult(c *qt.C, tok *Tokenizer, expect []Point, allowMore bool) int {
	i := 0
	for {
		if i >= len(expect) && allowMore {
			return i
		}
		if !tok.Next() {
			break
		}
		if i >= len(expect) {
			c.Errorf("too many points found")
		}
		for _, checkSection := range sectionCheckers {
			if i < len(expect) {
				checkSection(c, tok, expect[i])
			}
		}
		i++
	}
	c.Assert(i, qt.Equals, len(expect))
	return i
}

func TestTokenizerAtSection(t *testing.T) {
	c := qt.New(t)
	for _, test := range tokenizerTests {
		c.Run(test.testName, func(c *qt.C) {
			if !strings.Contains(test.text, "§") {
				c.Skip("no separators found")
			}
			for secti := range sectionCheckers {
				sect := Section(secti)
				c.Run(sect.String(), func(c *qt.C) {
					entries := strings.Split(test.text, "¶")
					c.Assert(entries, qt.HasLen, len(test.expect))
					for i, entry := range entries {
						sections := strings.Split(entry, "§")
						c.Assert(sections, qt.HasLen, int(TimeSection)+1)
						if expectedSectionError(test.expect[i], sect-1) != "" {
							continue
						}
						// Tokenize all sections at sect and beyond unless there was
						// a previous error, in which case the parser
						sectText := strings.Join(sections[sect:], "")
						c.Logf("trying entry %d: %q", i, sectText)
						tok := NewTokenizerAtSection([]byte(sectText), Section(sect))
						for _, checkSection := range sectionCheckers[sect:] {
							checkSection(c, tok, test.expect[i])
						}
					}
				})
			}
		})
	}
}

func doSection(tok *Tokenizer, section Section) error {
	switch section {
	case MeasurementSection:
		_, err := tok.Measurement()
		return err
	case TagSection:
		_, _, err := tok.NextTag()
		return err
	case FieldSection:
		_, _, _, err := tok.NextFieldBytes()
		return err
	case TimeSection:
		_, err := tok.TimeBytes()
		return err
	}
	panic("unreachable")
}

// expectedSectionError returns the error that's expected when
// reading any complete section up to and including
// the given section.
func expectedSectionError(p Point, section Section) string {
	switch section {
	case MeasurementSection:
		if p.MeasurementError != "" {
			return p.MeasurementError
		}
	case TagSection:
		for _, tag := range p.Tags {
			if tag.Error != "" {
				return tag.Error
			}
		}
	case FieldSection:
		for _, field := range p.Fields {
			if field.Error != "" {
				return field.Error
			}
		}
	case TimeSection:
		if p.TimeError != "" {
			return p.TimeError
		}
	default:
		return ""
	}
	return expectedSectionError(p, section-1)
}

func TestTokenizerSkipSection(t *testing.T) {
	// This test tests that we can call an individual tokenization method
	// without calling any of the others. The tokenization logic
	// should skip the other tokens.
	c := qt.New(t)
	for _, test := range tokenizerTests {
		c.Run(test.testName, func(c *qt.C) {
			for secti := range sectionCheckers {
				sect := Section(secti)
				c.Run(sect.String(), func(c *qt.C) {
					// Remove section and entry separators, as we're scanning all sections.
					tok := NewTokenizerWithBytes([]byte(removeTestSeparators(test.text)))
					i := 0
					for tok.Next() {
						if i >= len(test.expect) {
							c.Fatalf("too many points found")
						}
						if e := expectedSectionError(test.expect[i], sect-1); e != "" {
							// If there's an error earlier in the line, it gets returned on the
							// later section.
							c.Assert(doSection(tok, sect), qt.ErrorMatches, regexp.QuoteMeta(e))
						} else {
							sectionCheckers[sect](c, tok, test.expect[i])
						}
						i++
					}
					c.Assert(i, qt.Equals, len(test.expect))
				})
			}
		})
	}
}

func removeTestSeparators(s string) string {
	// Note: we can't use strings.Map here because we
	// need to preserve invalid utf-8 sequences.
	s = strings.Replace(s, "¶", "", -1)
	s = strings.Replace(s, "§", "", -1)
	return s
}

func TestTokenizerTokenizeTagsOnly(t *testing.T) {
	// One specific use case we'd like to support is that of tokenizing just
	// the tags on their, own.
	c := qt.New(t)
	tok := NewTokenizerAtSection([]byte(`a=b,c=hello\,,d=e`), TagSection)
	var tags []TagKeyValue
	for {
		key, val, err := tok.NextTag()
		c.Assert(err, qt.IsNil)
		if key == nil {
			break
		}
		tags = append(tags, TagKeyValue{
			Key:   string(key),
			Value: string(val),
		})
	}
	c.Assert(tags, qt.DeepEquals, []TagKeyValue{{
		Key:   "a",
		Value: "b",
	}, {
		Key:   "c",
		Value: "hello,",
	}, {
		Key:   "d",
		Value: "e",
	}})
	_, _, err := tok.NextField()
	c.Assert(err, qt.ErrorMatches, `expected field key but none found`)
}

var tokenizerTakeTests = []struct {
	testName     string
	newTokenizer func(s string) *Tokenizer
	expectError  string
}{{
	testName: "bytes",
	newTokenizer: func(s string) *Tokenizer {
		return NewTokenizerWithBytes([]byte(s))
	},
}, {
	testName: "reader",
	newTokenizer: func(s string) *Tokenizer {
		return NewTokenizer(strings.NewReader(s))
	},
}, {
	testName: "one-byte-reader",
	newTokenizer: func(s string) *Tokenizer {
		return NewTokenizer(iotest.OneByteReader(strings.NewReader(s)))
	},
}, {
	testName: "data-err-reader",
	newTokenizer: func(s string) *Tokenizer {
		return NewTokenizer(iotest.DataErrReader(strings.NewReader(s)))
	},
}, {
	testName: "error-reader",
	newTokenizer: func(s string) *Tokenizer {
		return NewTokenizer(&errorReader{
			r:   strings.NewReader(s),
			err: fmt.Errorf("some error"),
		})
	},
	expectError: "some error",
}}

// TestTokenizerTake tests the internal Tokenizer.take method.
func TestTokenizerTake(t *testing.T) {
	c := qt.New(t)
	for _, test := range tokenizerTakeTests {
		c.Run(test.testName, func(c *qt.C) {
			s := "aabbcccddefga"
			tok := test.newTokenizer(s)
			data1 := tok.take(newByteSet("abc"))
			c.Assert(string(data1), qt.Equals, "aabbccc")

			data2 := tok.take(newByteSet("d"))
			c.Assert(string(data2), qt.Equals, "dd")

			data3 := tok.take(newByteSet(" ").invert())
			c.Assert(string(data3), qt.Equals, "efga")
			c.Assert(tok.complete, qt.Equals, true)

			data4 := tok.take(newByteSet(" ").invert())
			c.Assert(string(data4), qt.Equals, "")

			// Check that none of them have been overwritten.
			c.Assert(string(data1), qt.Equals, "aabbccc")
			c.Assert(string(data2), qt.Equals, "dd")
			c.Assert(string(data3), qt.Equals, "efga")
			if test.expectError != "" {
				c.Assert(tok.err, qt.ErrorMatches, test.expectError)
			} else {
				c.Assert(tok.err, qt.IsNil)
			}
		})
	}
}

func TestLongTake(t *testing.T) {
	c := qt.New(t)
	// Test that we can take segments that are longer than the
	// read buffer size.
	src := strings.Repeat("abcdefgh", (minRead*3)/8)
	tok := NewTokenizer(strings.NewReader(src))
	data := tok.take(newByteSet("abcdefgh"))
	c.Assert(string(data), qt.Equals, src)
}

func TestTakeWithReset(t *testing.T) {
	c := qt.New(t)
	// Test that we can take segments that are longer than the
	// read buffer size.
	lineCount := (minRead * 3) / 9
	src := strings.Repeat("abcdefgh\n", lineCount)
	tok := NewTokenizer(strings.NewReader(src))
	n := 0
	for {
		data := tok.take(newByteSet("abcdefgh"))
		if len(data) == 0 {
			break
		}
		n++
		c.Assert(string(data), qt.Equals, "abcdefgh")
		b := tok.at(0)
		c.Assert(b, qt.Equals, byte('\n'))
		tok.advance(1)
		tok.reset()
	}
	c.Assert(n, qt.Equals, lineCount)
}

func TestTokenizerTakeWithReset(t *testing.T) {
	c := qt.New(t)
	// With a byte-at-a-time reader, we won't read any more
	// than we absolutely need.
	tok := NewTokenizer(iotest.OneByteReader(strings.NewReader("aabbcccddefg")))
	data1 := tok.take(newByteSet("abc"))
	c.Assert(string(data1), qt.Equals, "aabbccc")
	c.Assert(tok.at(0), qt.Equals, byte('d'))
	tok.advance(1)
	tok.reset()
	c.Assert(tok.r0, qt.Equals, 0)
	c.Assert(tok.r1, qt.Equals, 0)
}

func TestTokenizerTakeEsc(t *testing.T) {
	c := qt.New(t)
	for _, test := range tokenizerTakeTests {
		c.Run(test.testName, func(c *qt.C) {
			tok := test.newTokenizer(`hello\ \t\\z\XY`)
			data := tok.takeEsc(newByteSet("X").invert(), &newEscaper(" \t").revTable)
			c.Assert(string(data), qt.Equals, "hello \t\\\\z\\")

			// Check that an escaped character will be included when
			// it's not part of the take set.
			tok = test.newTokenizer(`hello\ \t\\z\XYX`)
			data1 := tok.takeEsc(newByteSet("X").invert(), &newEscaper("X \t").revTable)
			c.Assert(string(data1), qt.Equals, "hello \t\\\\zXY")

			// Check that the next call to takeEsc continues where it left off.
			data2 := tok.takeEsc(newByteSet(" ").invert(), &newEscaper(" ").revTable)
			c.Assert(string(data2), qt.Equals, "X")
			// Check that data1 hasn't been overwritten.
			c.Assert(string(data1), qt.Equals, "hello \t\\\\zXY")

			// Check that a backslash followed by EOF is taken as literal.
			tok = test.newTokenizer(`x\`)
			data = tok.takeEsc(newByteSet("").invert(), &newEscaper(" ").revTable)
			c.Assert(string(data), qt.Equals, "x\\")
		})
	}
}

func TestTokenizerTakeEscSkipping(t *testing.T) {
	c := qt.New(t)
	tok := NewTokenizer(strings.NewReader(`hello\ \t\\z\XY`))
	tok.skipping = true
	data := tok.takeEsc(newByteSet("X").invert(), &newEscaper(" \t").revTable)
	// When skipping is true, the data isn't unquoted (that's just unnecessary extra work).
	c.Assert(string(data), qt.Equals, `hello\ \t\\z\`)
}

func TestTokenizerTakeEscGrowBuffer(t *testing.T) {
	// This test tests the code path in Tokenizer.readMore
	// when the buffer needs to grow while we're reading a token.
	c := qt.New(t)
	tok := NewTokenizer(&nbyteReader{
		buf: []byte(`hello\ \ \ \  foo`),
		next: []int{
			len(`hello\`),
			len(` \ \ \`),
			len(`  foo`),
		},
	})
	data := tok.takeEsc(newByteSet(" ").invert(), &newEscaper(" ").revTable)
	c.Assert(string(data), qt.Equals, `hello    `)
	data = tok.take(newByteSet("").invert())
	c.Assert(string(data), qt.Equals, ` foo`)
}

func TestTokenizerTakeSlideBuffer(t *testing.T) {
	// This test tests the code path in Tokenizer.readMore
	// when the read buffer is large enough but the current
	// data is inconveniently in the wrong place, so
	// it gets slid to the front of the buffer.
	c := qt.New(t)
	// The first string that we'll read takes up almost all of the
	// initially added buffer, leaving just a little left at the end,
	// that will be moved to the front when we come to read that part.
	firstToken := strings.Repeat("a", minGrow-4)
	tok := NewTokenizer(strings.NewReader(firstToken + ` helloworld there`))
	data := tok.take(newByteSet(" ").invert())
	c.Assert(string(data), qt.Equals, firstToken)
	data = tok.take(newByteSet(" "))
	c.Assert(string(data), qt.Equals, " ")

	// Reset the buffer. There's still the data from `helloworld` onwards that will remain in the buffer.
	tok.reset()

	data = tok.take(newByteSet(" ").invert())
	c.Assert(string(data), qt.Equals, "helloworld")
	data = tok.take(newByteSet(" "))
	c.Assert(string(data), qt.Equals, " ")
	data = tok.take(newByteSet(" ").invert())
	c.Assert(string(data), qt.Equals, "there")
}

type nbyteReader struct {
	// next holds the read counts for subsequent calls to Read.
	// If next is empty and buf is not empty, Read will panic.
	next []int
	// buf holds the data remaining to be read.
	buf []byte
}

func (r *nbyteReader) Read(buf []byte) (int, error) {
	if len(r.buf) == 0 && len(r.next) == 0 {
		return 0, io.EOF
	}
	n := r.next[0]
	r.next = r.next[1:]
	nb := copy(buf, r.buf[:n])
	if nb != n {
		panic(fmt.Errorf("read count for return (%d) is too large for provided buffer (%d)", n, len(r.buf)))
	}
	r.buf = r.buf[n:]
	return n, nil
}

type errorReader struct {
	r   io.Reader
	err error
}

func (r *errorReader) Read(buf []byte) (int, error) {
	n, err := r.r.Read(buf)
	if err != nil {
		err = r.err
	}
	return n, err
}

var scanEntriesBenchmarks = []struct {
	name     string
	makeData func() (data []byte, totalEntries int)
	expect   Point
}{{
	name: "long-lines",
	makeData: func() (data []byte, totalEntries int) {
		entry := `name,tag1=baz,tag2=asdfvdfsvdsvdfs,tagvdsvd=hello field="` + strings.Repeat("a", 4500) + `" 1602841605822791506`
		var buf bytes.Buffer
		for buf.Len() < 25*1024*1024 {
			buf.WriteString(entry)
			buf.WriteByte('\n')
			totalEntries++
		}
		return buf.Bytes(), totalEntries
	},
	expect: Point{
		Measurement: "name",
		Tags: []TagKeyValue{{
			Key:   "tag1",
			Value: "baz",
		}, {
			Key:   "tag2",
			Value: "asdfvdfsvdsvdfs",
		}, {
			Key:   "tagvdsvd",
			Value: "hello",
		}},
		Fields: []FieldKeyValue{{
			Key:   "field",
			Kind:  String,
			Value: strings.Repeat("a", 4500),
		}},
		Time: "1602841605822791506",
	},
}, {
	name: "long-lines-with-escapes",
	makeData: func() (data []byte, totalEntries int) {
		entry := `name,ta\=g1=foo\ bar\,baz,tag2=asdfvdfsvdsvdfs,tag\=vdsvd=hello field="` + strings.Repeat(`\"`, 4500) + `" 1602841605822791506`
		var buf bytes.Buffer
		for buf.Len() < 25*1024*1024 {
			buf.WriteString(entry)
			buf.WriteByte('\n')
			totalEntries++
		}
		return buf.Bytes(), totalEntries
	},
	expect: Point{
		Measurement: "name",
		Tags: []TagKeyValue{{
			Key:   "ta=g1",
			Value: "foo bar,baz",
		}, {
			Key:   "tag2",
			Value: "asdfvdfsvdsvdfs",
		}, {
			Key:   "tag=vdsvd",
			Value: "hello",
		}},
		Fields: []FieldKeyValue{{
			Key:   "field",
			Kind:  String,
			Value: strings.Repeat(`"`, 4500),
		}},
		Time: "1602841605822791506",
	},
}, {
	name: "single-short-line",
	makeData: func() ([]byte, int) {
		return []byte(`x,t=y y=1 1602841605822791506`), 1
	},
	expect: Point{
		Measurement: "x",
		Tags: []TagKeyValue{{
			Key:   "t",
			Value: "y",
		}},
		Fields: []FieldKeyValue{{
			Key:   "y",
			Kind:  Float,
			Value: "1",
		}},
		Time: "1602841605822791506",
	},
}, {
	name:     "single-short-line-with-escapes",
	makeData: singleEntry(`x,t=y\,y y=1 1602841605822791506`),
	expect: Point{
		Measurement: "x",
		Tags: []TagKeyValue{{
			Key:   "t",
			Value: "y,y",
		}},
		Fields: []FieldKeyValue{{
			Key:   "y",
			Kind:  Float,
			Value: "1",
		}},
		Time: "1602841605822791506",
	},
}, {
	name: "many-short-lines",
	makeData: func() (data []byte, totalEntries int) {
		entry := `x,t=y y=1 1602841605822791506`
		var buf bytes.Buffer
		for buf.Len() < 25*1024*1024 {
			buf.WriteString(entry)
			buf.WriteByte('\n')
			totalEntries++
		}
		return buf.Bytes(), totalEntries
	},
	expect: Point{
		Measurement: "x",
		Tags: []TagKeyValue{{
			Key:   "t",
			Value: "y",
		}},
		Fields: []FieldKeyValue{{
			Key:   "y",
			Kind:  Float,
			Value: "1",
		}},
		Time: "1602841605822791506",
	},
}, {
	name:     "field-key-escape-not-escapable",
	makeData: singleEntry(`cpu va\lue=42 1602841605822791506`),
	expect: Point{
		Measurement: "cpu",
		Fields: []FieldKeyValue{{
			Key:   `va\lue`,
			Kind:  Float,
			Value: "42",
		}},
		Time: "1602841605822791506",
	},
}, {
	name:     "tag-value-triple-escape-space",
	makeData: singleEntry(`cpu,host=two\\\ words value=42 1602841605822791506`),
	expect: Point{
		Measurement: "cpu",
		Tags: []TagKeyValue{{
			Key:   "host",
			Value: `two\\ words`,
		}},
		Fields: []FieldKeyValue{{
			Key:   `value`,
			Kind:  Float,
			Value: "42",
		}},
		Time: "1602841605822791506",
	},
}, {
	name:     "procstat",
	makeData: singleEntry(`procstat,exe=bash,process_name=bash voluntary_context_switches=42i,memory_rss=5103616i,rlimit_memory_data_hard=2147483647i,cpu_time_user=0.02,rlimit_file_locks_soft=2147483647i,pid=29417i,cpu_time_nice=0,rlimit_memory_locked_soft=65536i,read_count=259i,rlimit_memory_vms_hard=2147483647i,memory_swap=0i,rlimit_num_fds_soft=1024i,rlimit_nice_priority_hard=0i,cpu_time_soft_irq=0,cpu_time=0i,rlimit_memory_locked_hard=65536i,realtime_priority=0i,signals_pending=0i,nice_priority=20i,cpu_time_idle=0,memory_stack=139264i,memory_locked=0i,rlimit_memory_stack_soft=8388608i,cpu_time_iowait=0,cpu_time_guest=0,cpu_time_guest_nice=0,rlimit_memory_data_soft=2147483647i,read_bytes=0i,rlimit_cpu_time_soft=2147483647i,involuntary_context_switches=2i,write_bytes=106496i,cpu_time_system=0,cpu_time_irq=0,cpu_usage=0,memory_vms=21659648i,memory_data=1576960i,rlimit_memory_stack_hard=2147483647i,num_threads=1i,rlimit_memory_rss_soft=2147483647i,rlimit_realtime_priority_soft=0i,num_fds=4i,write_count=35i,rlimit_signals_pending_soft=78994i,cpu_time_steal=0,rlimit_num_fds_hard=4096i,rlimit_file_locks_hard=2147483647i,rlimit_cpu_time_hard=2147483647i,rlimit_signals_pending_hard=78994i,rlimit_nice_priority_soft=0i,rlimit_memory_rss_hard=2147483647i,rlimit_memory_vms_soft=2147483647i,rlimit_realtime_priority_hard=0i 1517620624000000000`),
	expect: Point{
		Measurement: "procstat",
		Tags: []TagKeyValue{{
			Key:   "exe",
			Value: "bash",
		}, {
			Key:   "process_name",
			Value: "bash",
		}},
		Fields: []FieldKeyValue{{
			Key:   "voluntary_context_switches",
			Kind:  Int,
			Value: "42",
		}, {
			Key:   "memory_rss",
			Kind:  Int,
			Value: "5103616",
		}, {
			Key:   "rlimit_memory_data_hard",
			Kind:  Int,
			Value: "2147483647",
		}, {
			Key:   "cpu_time_user",
			Kind:  Float,
			Value: "0.02",
		}, {
			Key:   "rlimit_file_locks_soft",
			Kind:  Int,
			Value: "2147483647",
		}, {
			Key:   "pid",
			Kind:  Int,
			Value: "29417",
		}, {
			Key:   "cpu_time_nice",
			Kind:  Float,
			Value: "0",
		}, {
			Key:   "rlimit_memory_locked_soft",
			Kind:  Int,
			Value: "65536",
		}, {
			Key:   "read_count",
			Kind:  Int,
			Value: "259",
		}, {
			Key:   "rlimit_memory_vms_hard",
			Kind:  Int,
			Value: "2147483647",
		}, {
			Key:   "memory_swap",
			Kind:  Int,
			Value: "0",
		}, {
			Key:   "rlimit_num_fds_soft",
			Kind:  Int,
			Value: "1024",
		}, {
			Key:   "rlimit_nice_priority_hard",
			Kind:  Int,
			Value: "0",
		}, {
			Key:   "cpu_time_soft_irq",
			Kind:  Float,
			Value: "0",
		}, {
			Key:   "cpu_time",
			Kind:  Int,
			Value: "0",
		}, {
			Key:   "rlimit_memory_locked_hard",
			Kind:  Int,
			Value: "65536",
		}, {
			Key:   "realtime_priority",
			Kind:  Int,
			Value: "0",
		}, {
			Key:   "signals_pending",
			Kind:  Int,
			Value: "0",
		}, {
			Key:   "nice_priority",
			Kind:  Int,
			Value: "20",
		}, {
			Key:   "cpu_time_idle",
			Kind:  Float,
			Value: "0",
		}, {
			Key:   "memory_stack",
			Kind:  Int,
			Value: "139264",
		}, {
			Key:   "memory_locked",
			Kind:  Int,
			Value: "0",
		}, {
			Key:   "rlimit_memory_stack_soft",
			Kind:  Int,
			Value: "8388608",
		}, {
			Key:   "cpu_time_iowait",
			Kind:  Float,
			Value: "0",
		}, {
			Key:   "cpu_time_guest",
			Kind:  Float,
			Value: "0",
		}, {
			Key:   "cpu_time_guest_nice",
			Kind:  Float,
			Value: "0",
		}, {
			Key:   "rlimit_memory_data_soft",
			Kind:  Int,
			Value: "2147483647",
		}, {
			Key:   "read_bytes",
			Kind:  Int,
			Value: "0",
		}, {
			Key:   "rlimit_cpu_time_soft",
			Kind:  Int,
			Value: "2147483647",
		}, {
			Key:   "involuntary_context_switches",
			Kind:  Int,
			Value: "2",
		}, {
			Key:   "write_bytes",
			Kind:  Int,
			Value: "106496",
		}, {
			Key:   "cpu_time_system",
			Kind:  Float,
			Value: "0",
		}, {
			Key:   "cpu_time_irq",
			Kind:  Float,
			Value: "0",
		}, {
			Key:   "cpu_usage",
			Kind:  Float,
			Value: "0",
		}, {
			Key:   "memory_vms",
			Kind:  Int,
			Value: "21659648",
		}, {
			Key:   "memory_data",
			Kind:  Int,
			Value: "1576960",
		}, {
			Key:   "rlimit_memory_stack_hard",
			Kind:  Int,
			Value: "2147483647",
		}, {
			Key:   "num_threads",
			Kind:  Int,
			Value: "1",
		}, {
			Key:   "rlimit_memory_rss_soft",
			Kind:  Int,
			Value: "2147483647",
		}, {
			Key:   "rlimit_realtime_priority_soft",
			Kind:  Int,
			Value: "0",
		}, {
			Key:   "num_fds",
			Kind:  Int,
			Value: "4",
		}, {
			Key:   "write_count",
			Kind:  Int,
			Value: "35",
		}, {
			Key:   "rlimit_signals_pending_soft",
			Kind:  Int,
			Value: "78994",
		}, {
			Key:   "cpu_time_steal",
			Kind:  Float,
			Value: "0",
		}, {
			Key:   "rlimit_num_fds_hard",
			Kind:  Int,
			Value: "4096",
		}, {
			Key:   "rlimit_file_locks_hard",
			Kind:  Int,
			Value: "2147483647",
		}, {
			Key:   "rlimit_cpu_time_hard",
			Kind:  Int,
			Value: "2147483647",
		}, {
			Key:   "rlimit_signals_pending_hard",
			Kind:  Int,
			Value: "78994",
		}, {
			Key:   "rlimit_nice_priority_soft",
			Kind:  Int,
			Value: "0",
		}, {
			Key:   "rlimit_memory_rss_hard",
			Kind:  Int,
			Value: "2147483647",
		}, {
			Key:   "rlimit_memory_vms_soft",
			Kind:  Int,
			Value: "2147483647",
		}, {
			Key:   "rlimit_realtime_priority_hard",
			Kind:  Int,
			Value: "0",
		}},
		Time: "1517620624000000000",
	},
}}

func singleEntry(s string) func() ([]byte, int) {
	return func() ([]byte, int) {
		return []byte(s), 1
	}
}

func BenchmarkScanEntriesSkipping(b *testing.B) {
	for _, bench := range scanEntriesBenchmarks {
		b.Run(bench.name, func(b *testing.B) {
			data, total := bench.makeData()
			c := qt.New(b)
			// Sanity check that the tokenizer is doing what we're expecting.
			// Only check the first entry because checking them all is slow.
			tok := NewTokenizerWithBytes(data)
			assertTokenizeResult(c, tok, []Point{bench.expect}, true)
			b.ReportAllocs()
			b.ResetTimer()
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				n := 0
				tok := NewTokenizerWithBytes(data)
				for tok.Next() {
					n++
				}
				if n != total {
					b.Fatalf("unexpected read count; got %v want %v", n, total)
				}
			}
		})
	}
}

func BenchmarkScanEntriesWithoutSkipping(b *testing.B) {
	for _, bench := range scanEntriesBenchmarks {
		b.Run(bench.name, func(b *testing.B) {
			data, total := bench.makeData()
			c := qt.New(b)
			// Sanity check that the tokenizer is doing what we're expecting.
			// Only check the first entry because checking them all is slow.
			tok := NewTokenizerWithBytes(data)
			assertTokenizeResult(c, tok, []Point{bench.expect}, true)
			b.ReportAllocs()
			b.ResetTimer()
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				n := 0
				tok := NewTokenizerWithBytes(data)
				for tok.Next() {
					tok.Measurement()
					for {
						key, _, _ := tok.NextTag()
						if key == nil {
							break
						}
					}
					for {
						key, _, _ := tok.NextField()
						if key == nil {
							break
						}
					}
					tok.TimeBytes()
					n++
				}
				if n != total {
					b.Fatalf("unexpected read count; got %v want %v", n, total)
				}
			}
		})
	}
}

func BenchmarkTokenize(b *testing.B) {
	var buf bytes.Buffer
	b.ReportAllocs()
	total := 0
	for buf.Len() < 25*1024*1024 {
		buf.WriteString("foo ba\\ rfle ")
		for i := 0; i < 5000; i += 5 {
			buf.WriteString("abcde")
		}
		buf.WriteByte('\n')
		total++
	}
	b.SetBytes(int64(buf.Len()))
	esc := newEscaper(" \t")
	whitespace := newByteSet(" \t")
	word := newByteSet(" \t\n").invert()
	tokBytes := buf.Bytes()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n := 0
		tok := NewTokenizerWithBytes(tokBytes)
		for {
			tok.reset()
			if !tok.ensure(1) {
				break
			}
			tok.takeEsc(word, &esc.revTable)
			tok.take(whitespace)
			if !tok.ensure(1) {
				break
			}
			tok.takeEsc(word, &esc.revTable)
			tok.take(whitespace)
			if !tok.ensure(1) {
				break
			}
			tok.takeEsc(word, &esc.revTable)
			tok.take(whitespace)
			if !tok.ensure(1) {
				break
			}
			if tok.at(0) != '\n' {
				b.Fatalf("unexpected character %q at eol", string(rune(tok.at(0))))
			}
			tok.advance(1)
			n++
		}
		if n != total {
			b.Fatalf("unexpected read count; got %v want %v", n, total)
		}
	}
}

func mkStats(stats ...Stat) Stats {
	var s Stats
	for _, st := range stats {
		s |= 1 << st
	}
	return s
}
