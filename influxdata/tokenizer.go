package influxdata

import (
	"io"
)

// minRead holds the minimum buffer size passed to io.Reader.Read.
const (
	// When the buffer is grown, it will be grown be a minimum of 8K.
	minGrow = 8192
	// The buffer will be grown if there's less than minRead space available
	// to read into.
	minRead = minGrow / 2
)

// Tokenizer implements low level parsing of a line-protocol message.
type Tokenizer struct {
	// rd holds the reader, if any. If there is no reader,
	// complete will be true.
	rd io.Reader

	// buf holds data that's been read.
	buf []byte

	// complete holds whether the data in buffer
	// is known to be all the data that's available.
	complete bool

	// skipping holds whether we will need
	// to return the values that we're tokenizing.
	skipping bool

	// r0 holds the earliest read position in buf.
	// Data in buf[0:r0] is considered to be discarded.
	r0 int

	// r1 holds the read position in buf. Data in buf[r1:] is
	// next to be read. Data in buf[len(buf):cap(buf)] is
	// available for reading into.
	r1 int

	// escBuf holds a buffer for unescaped characters.
	escBuf []byte

	// err holds any non-EOF error that was returned from rd.
	err error
}

// NewTokenizer returns a tokenizer that splits the line-protocol text
// inside buf. The text in buf represent a complete line (or multiple lines) - that
// is, the tokenization will not work correctly if presented with a line
// split at an arbitrary place.
func NewTokenizerWithBytes(buf []byte) *Tokenizer {
	return &Tokenizer{
		buf:      buf,
		complete: true,
		escBuf:   make([]byte, 0, 512),
	}
}

func NewTokenizer(r io.Reader) *Tokenizer {
	return &Tokenizer{
		rd: r,
	}
}

// take returns the next slice of bytes that are in the given set
// reading more data as needed. It updates t.r1.
func (t *Tokenizer) take(set *byteSet) []byte {
	// Note: use a relative index for start because absolute
	// indexes aren't stable (the contents of the buffer can be
	// moved when reading more data).
	start := t.r1 - t.r0
outer:
	for {
		if !t.ensure(1) {
			break
		}
		buf := t.buf[t.r1:]
		for i, c := range buf {
			if !set.get(c) {
				t.r1 += i
				break outer
			}
		}
		t.r1 += len(buf)
	}
	return t.buf[t.r0+start : t.r1]
}

// takeEsc is like take except that escaped characters also count as
// part of the set. The escapeTable determines which characters
// can be escaped.
// It returns the unescaped string (unless t.skipping is true, in which
// case it doesn't need to go to the trouble of unescaping it).
func (t *Tokenizer) takeEsc(set *byteSet, escapeTable *[256]byte) []byte {
	// start holds the offset from r0 of the start of the taken slice.
	// Note that we can't use t.r1 directly, because the offsets can change
	// when the buffer is grown.
	start := t.r1 - t.r0

	// startUnesc holds the offset from t0 of the start of the most recent
	// unescaped segment.
	startUnesc := start

	// startEsc holds the index into r.escBuf of the start of the escape buffer.
	startEsc := len(t.escBuf)
outer:
	for {
		if !t.ensure(1) {
			break
		}
		buf := t.buf[t.r1:]
		for i := 0; i < len(buf); i++ {
			c := buf[i]
			if c != '\\' {
				if !set.get(c) {
					// We've found the end, so we're done here.
					t.r1 += i
					break outer
				}
				continue
			}
			if i+1 >= len(buf) {
				// Not enough room in the buffer. Try reading more so that
				// we can see the next byte (note: ensure(i+2) is asking
				// for exactly one more character, because we know we already
				// have i+1 bytes in the buffer).
				if !t.ensure(i + 2) {
					// No character to escape, so leave the \ intact.
					t.r1 = len(t.buf)
					break outer
				}
				// Note that t.ensure can change t.buf, so we need to
				// update buf to point to the correct buffer.
				buf = t.buf[t.r1:]
			}
			replc := escapeTable[buf[i+1]]
			if replc == 0 {
				// The backslash doesn't precede a value escaped
				// character, so it stays intact.
				continue
			}
			if !t.skipping {
				t.escBuf = append(t.escBuf, t.buf[t.r0+startUnesc:t.r1+i]...)
				t.escBuf = append(t.escBuf, replc)
				startUnesc = t.r1 - t.r0 + i + 2
			}
			i++
		}
		// We've consumed all the bytes in the buffer. Now continue
		// the loop, trying to acquire more.
		t.r1 += len(buf)
	}
	if len(t.escBuf) > startEsc {
		// We've got an unescaped result: append any remaining unescaped bytes
		// and return the relevant portion of the escape buffer.
		t.escBuf = append(t.escBuf, t.buf[startUnesc+t.r0:t.r1]...)
		return t.escBuf[startEsc:]
	}
	return t.buf[t.r0+start : t.r1]
}

// at returns the byte at i bytes after the current read position.
// It assumes that the index has already been ensured.
// If there's no byte there, it returns zero.
func (t *Tokenizer) at(i int) byte {
	if t.r1+i < len(t.buf) {
		return t.buf[t.r1+i]
	}
	return 0
}

// reset discards all the data up to t.r1 and data in t.escBuf
func (t *Tokenizer) reset() {
	if t.r1 == len(t.buf) {
		// No bytes in the buffer, so we can start from the beginning without
		// needing to copy anything (and get better cache behaviour too).
		t.buf = t.buf[:0]
		t.r1 = 0
	}
	t.r0 = t.r1
	t.escBuf = t.escBuf[:0]
}

// advance advances the read point by n.
// This should only be used when it's known that
// there are already n bytes available in the buffer.
func (t *Tokenizer) advance(n int) {
	t.r1 += n
}

// ensure ensures that there are at least n bytes available in
// t.buf[t.r1:], reading more bytes if necessary.
// It reports whether enough bytes are available.
func (t *Tokenizer) ensure(n int) bool {
	for {
		if t.r1+n <= len(t.buf) {
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
		if t.r0+n >= minRead {
			// There's enough space when we take into account already-used
			// part of buf, so slide the data to the front.
			copy(t.buf, t.buf[t.r0:])
			t.buf = t.buf[:len(t.buf)-t.r0]
			t.r1 -= t.r0
			t.r0 = 0
		} else {
			// We need to grow the buffer. Note that we don't have to copy
			// the unused part of the buffer (t.buf[:t.r0]).
			// TODO provide a way to limit the maximum size that
			// the buffer can grow to.
			used := len(t.buf) - t.r0
			n1 := cap(t.buf) * 2
			if n1-used < minGrow {
				n1 = used + minGrow
			}
			buf1 := make([]byte, used, n1)
			copy(buf1, t.buf[t.r0:])
			t.buf = buf1
			t.r1 -= t.r0
			t.r0 = 0
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
