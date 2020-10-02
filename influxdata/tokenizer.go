package influxdata

import (
	"io"
)

// minRead holds the minimum buffer size passed to io.Reader.Read.
const minRead = 8192

// NewTokenizer returns a tokenizer that splits the line-protocol text
// inside buf. The text in buf represent a complete line (or multiple lines) - that
// is, the tokenization will not work correctly if presented with a line
// split at an arbitrary place.
func NewTokenizerWithBytes(buf []byte) *Tokenizer {
	return &Tokenizer{
		buf:      buf,
		complete: true,
	}
}

func NewTokenizer(r io.Reader) *Tokenizer {
	return &Tokenizer{
		rd: r,
	}
}

// Tokenizer implements low level parsing of a line-protocol message.
type Tokenizer struct {
	// rd holds the reader, if any. If there is no reader,
	// complete will be true.
	rd io.Reader

	// buf holds read data.
	buf []byte

	// complete holds whether the data in buffer
	// is known to be all the data that's available.
	complete bool

	// r holds the read position in buf.
	r int

	// err holds any non-EOF error that was returned from rd.
	err error
}

// take returns the next slice of bytes within t.buf that are in the given set
// starting at t.r + from, reading more data as needed.
//
// It does not update t.r.
func (t *Tokenizer) take(set *byteSet, from int) []byte {
	n := from
outer:
	for {
		if !t.ensure(n + 1) {
			break
		}
		buf := t.buf[t.r+n:]
		for i, c := range buf {
			if !set.get(c) {
				n += i
				break outer
			}
		}
		n += len(buf)
	}
	return t.buf[t.r+from : t.r+n]
}

// at returns the byte at i bytes after the current read position.
// It assumes that the byte has already been read (use t.ensure to check
// whether it's there).
func (t *Tokenizer) at(i int) byte {
	return t.buf[t.r+i]
}

// discard advances the read offset by n bytes.
// There must be at least n bytes available in the buffer.
func (t *Tokenizer) discard(n int) {
	t.r += n
	if t.r == len(t.buf) {
		// No bytes in the buffer, so we can start from the beginning without
		// needing to copy anything (and get better cache behaviour too).
		t.buf = t.buf[:0]
		t.r = 0
	}
}

// ensure ensures that there are at least n bytes available in
// t.buf[t.r:], reading more bytes if necessary.
// It reports whether enough bytes are available.
func (t *Tokenizer) ensure(n int) bool {
	for {
		if t.r+n <= len(t.buf) {
			// There are enough bytes available.
			return true
		}
		if t.complete {
			// No possibility of more data.
			return false
		}
		t.readMore()
	}
}

// readMore reads more data into t.buf.
func (t *Tokenizer) readMore() {
	if t.complete {
		return
	}
	n := cap(t.buf) - len(t.buf)
	if n < minRead {
		// There's not enough available space at the end of the buffer to read into.
		if n+t.r >= minRead {
			// There's enough space when we take into account already-used
			// part of buf, so slide the data to the front.
			copy(t.buf, t.buf[t.r:])
			t.buf = t.buf[:len(t.buf)-t.r]
		} else {
			// We need to grow the buffer. Note that we don't have to copy
			// the unused part of the buffer (t.buf[:t.r]).
			// TODO provide a way to limit the maximum size that
			// the buffer can grow to.
			used := len(t.buf) - t.r
			n1 := cap(t.buf) * 2
			if n1-used < minRead {
				n1 = used + minRead
			}
			buf1 := make([]byte, used, n1)
			copy(buf1, t.buf[t.r:])
			t.buf = buf1
		}
	}
	n, err := t.rd.Read(t.buf[len(t.buf):cap(t.buf)])
	t.buf = t.buf[:len(t.buf)+n]
	if err == nil {
		return
	}
	t.complete = true
	if err != io.EOF {
		t.err = err
	}
}
