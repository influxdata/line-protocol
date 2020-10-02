package influxdata

import (
	"fmt"
	"io"
	"strings"
	"testing"
	"testing/iotest"

	qt "github.com/frankban/quicktest"
)

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
			data1 := tok.take(newByteSet('a', 'b', 'c'), 0)
			c.Assert(string(data1), qt.Equals, "aabbccc")

			data2 := tok.take(newByteSet('d'), len(data1))
			c.Assert(string(data2), qt.Equals, "dd")
			c.Assert(tok.r, qt.Equals, 0)

			data3 := tok.take(newByteSet(' ').invert(), len(data1)+len(data2))
			c.Assert(string(data3), qt.Equals, "efga")
			c.Assert(tok.complete, qt.Equals, true)

			data4 := tok.take(newByteSet(' ').invert(), len(data1)+len(data2)+len(data3))
			c.Assert(string(data4), qt.Equals, "")

			data5 := tok.take(newByteSet('a', 'b', 'c'), len(s))
			c.Assert(string(data5), qt.Equals, "")

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

func TestTokenizerTakeWithDiscard(t *testing.T) {
	c := qt.New(t)
	// With a byte-at-a-time reader, we won't read any more
	// than we absolutely need, so discard should cause the
	// buffer to be overwritten each time.
	tok := NewTokenizer(iotest.OneByteReader(strings.NewReader("aabbcccddefg")))
	data1 := tok.take(newByteSet('a', 'b', 'c'), 0)
	c.Assert(string(data1), qt.Equals, "aabbccc")
	tok.discard(len(data1))
	c.Assert(tok.at(0), qt.Equals, byte('d'))
	tok.discard(1)
	c.Assert(tok.r, qt.Equals, 0)
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
