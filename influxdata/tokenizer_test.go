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
		c.Assert(string(m), qt.Equals, expect.Measurement)
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
			if key == nil && value == nil {
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
			if key == nil && value == nil && kind == Unknown {
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
	text   string
	expect []Point
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
}}

func TestTokenizer(t *testing.T) {
	c := qt.New(t)
	for _, test := range tokenizerTests {
		c.Run(test.testName, func(c *qt.C) {
			// Remove section and entry separators, as we're testing all sections.
			tok := NewTokenizerWithBytes([]byte(removeTestSeparators(test.text)))
			i := 0
			for tok.Next() {
				if i >= len(test.expect) {
					c.Fatalf("too many points found")
				}
				for sect, checkSection := range sectionCheckers {
					c.Logf("checking %v", Section(sect))
					checkSection(c, tok, test.expect[i])
				}
				i++
			}
			c.Assert(i, qt.Equals, len(test.expect))
		})
	}
}

func TestTokenizerAtSection(t *testing.T) {
	c := qt.New(t)
	for _, test := range tokenizerTests {
		c.Run(test.testName, func(c *qt.C) {
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
	return strings.Map(func(r rune) rune {
		if r == '¶' || r == '§' {
			return -1
		}
		return r
	}, s)
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
