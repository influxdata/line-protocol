package influxdata

import (
	"fmt"
	"io"
	"time"
)

const (
	// When the buffer is grown, it will be grown by a minimum of 8K.
	minGrow = 8192
	// The buffer will be grown if there's less than minRead space available
	// to read into.
	minRead = minGrow / 2
)

var (
	measurementChars      = newByteSet(",").union(whitespace).invert()
	measurementEscapes    = newEscaper(" ,")
	tagKeyChars           = newByteSet(",=").union(whitespace).invert()
	tagKeyEscapes         = newEscaper(",= ")
	tagValChars           = newByteSet(",=").union(whitespace).invert()
	tagValEscapes         = newEscaper(", =")
	fieldSeparatorSpace   = newByteSet(" \t\r\f")
	fieldKeyChars         = tagKeyChars
	fieldKeyEscapes       = tagKeyEscapes
	fieldStringValChars   = newByteSet(`"`).invert()
	fieldStringValEscapes = newEscaper(`\"`)
	fieldValChars         = newByteSet(",").union(whitespace).invert()
	timeChars             = newByteSet("-0123456789")
	whitespace            = fieldSeparatorSpace.union(newByteSet("\n"))
	notNewline            = newByteSet("\n").invert()
)

// Tokenizer implements low level parsing of a set of line-protocol entries.
type Tokenizer struct {
	// rd holds the reader, if any. If there is no reader,
	// complete will be true.
	rd io.Reader

	// buf holds data that's been read.
	buf []byte

	// r0 holds the earliest read position in buf.
	// Data in buf[0:r0] is considered to be discarded.
	r0 int

	// r1 holds the read position in buf. Data in buf[r1:] is
	// next to be read. Data in buf[len(buf):cap(buf)] is
	// available for reading into.
	r1 int

	// complete holds whether the data in buffer
	// is known to be all the data that's available.
	complete bool

	// stats records features about the data that's being read.
	stats Stats

	// section holds the current section of the entry that's being
	// read.
	section Section

	// skipping holds whether we will need
	// to return the values that we're tokenizing.
	skipping bool

	// escBuf holds a buffer for unescaped characters.
	escBuf []byte

	// err holds any non-EOF error that was returned from rd.
	err error
}

// NewTokenizer returns a tokenizer that splits the line-protocol text
// inside buf.
func NewTokenizerWithBytes(buf []byte) *Tokenizer {
	return &Tokenizer{
		buf:      buf,
		complete: true,
		escBuf:   make([]byte, 0, 512),
		section:  endSection,
	}
}

// NewTokenizer returns a tokenizer that reads from the given reader.
func NewTokenizer(r io.Reader) *Tokenizer {
	return &Tokenizer{
		rd:      r,
		escBuf:  make([]byte, 0, 512),
		section: endSection,
	}
}

// NewTokenizerAtSection returns a tokenizer that parses the given
// bytes but starting at the given section. This enables (for example)
// parsing of the tags section of an entry without the preceding measurement name.
//
// This does not scan forward to the given section; it assumes the byte array
// starts at the given section.
func NewTokenizerAtSection(buf []byte, section Section) *Tokenizer {
	tok := &Tokenizer{
		buf:      buf,
		complete: true,
		escBuf:   make([]byte, 0, 512),
		section:  section,
	}
	if section != TagSection || !whitespace.get(tok.at(0)) {
		return tok
	}
	// As a special case, if we're asking to parse the tag section,
	// move straight to the field section if the start isn't whitespace,
	// because NextTag assumes that it's positioned at the start
	// of a valid non-empty tag section. This saves further special cases
	// below to avoid accepting an entry with a comma followed by whitespace.
	tok.takeFieldSpace()
	tok.section = FieldSection
	return tok
}

func (t *Tokenizer) takeFieldSpace() {
	t.takeStats(fieldSeparatorSpace, fieldSpaceStats)
}

// Next advances to the next entry, and reports whether there is an
// entry available. Syntax errors on individual lines do not cause this
// to return false (the tokenizer attempts to recover from badly
// formatted lines), but I/O errors do. Call t.Err to discover if there
// was any I/O error.
//
// After calling Next, the various components of a line can be retrieved
// by calling Measurement, NextTag, NextField and Time in that order
// (the same order that the components are held in the entry).
//
// IMPORTANT NOTE: the byte slices returned by the Tokenizer methods are
// only valid until the next call to any other Tokenize method.
//
// Tokenizer will skip earlier components if a later method is called,
// but it doesn't retain the entire entry, so it cannot go backwards.
//
// For example, to retrieve only the timestamp of all lines, this suffices:
//
//	for t.Next() {
//		timestamp, err := t.TimeBytes()
//	}
//
func (t *Tokenizer) Next() bool {
	t.advanceToSection(endSection)
	t.skipEmptyLines()
	t.section = MeasurementSection
	t.reset()
	return t.ensure(1)
}

// Stats returns all the flags that have been set since the
// last time Stats was called.
func (t *Tokenizer) Stats() Stats {
	s := t.stats
	t.stats = 0
	return s
}

// Err returns any I/O error encountered when reading
// tokens. If t was created with NewTokenizerWithBytes,
// Err will always return nil.
func (t *Tokenizer) Err() error {
	return t.err
}

// Measurement returns the measurement name. It returns nil
// unless called before NextTag, NextField or Time.
func (t *Tokenizer) Measurement() ([]byte, error) {
	if ok, err := t.advanceToSection(MeasurementSection); err != nil {
		return nil, err
	} else if !ok {
		return nil, nil
	}
	// skipEmptyLines seems like it might be redundant here because t.Next
	// also skips empty lines, but call it here too, so that
	// NewTokenizerAtSection(MeasurementSection) skips initial
	// empty/comment lines too. Maybe that's a bad idea.
	t.skipEmptyLines()
	t.reset()
	measure := t.takeEscUnquotedStats(measurementChars, &measurementEscapes.revTable)
	if len(measure) == 0 {
		return nil, t.syntaxErrorf("no measurement name found")
	}
	if err := t.advanceTagComma(); err != nil {
		return nil, err
	}
	t.section = TagSection
	return measure, nil
}

// NextTag returns the next tag in the entry.
// If there are no more tags, it returns nil, nil, nil.
// Note that this must be called before NextField because
// tags precede fields in the line-protocol entry.
func (t *Tokenizer) NextTag() (key, value []byte, err error) {
	if ok, err := t.advanceToSection(TagSection); err != nil {
		return nil, nil, err
	} else if !ok {
		return nil, nil, nil
	}
	if t.ensure(1) && fieldSeparatorSpace.get(t.at(0)) {
		t.takeFieldSpace()
		t.section = FieldSection
		return nil, nil, nil
	}
	tagKey := t.takeEscUnquotedStats(tagKeyChars, &tagKeyEscapes.revTable)
	if len(tagKey) == 0 {
		return nil, nil, t.syntaxErrorf("empty tag name")
	}
	if !t.ensure(1) || t.at(0) != '=' {
		return nil, nil, t.syntaxErrorf("expected '=' after tag key %q, but got %q instead", tagKey, t.at(0))
	}
	t.advance(1)
	tagVal := t.takeEscUnquotedStats(tagValChars, &tagValEscapes.revTable)
	if len(tagVal) == 0 {
		return nil, nil, t.syntaxErrorf("expected tag value after tag key %q, but none found", tagKey)
	}
	if !t.ensure(1) {
		// There's no more data after the tag value. Instead of returning an error
		// immediately, advance to the field section and return the tag and value.
		// This means that we'll see all the tags even when there's no value,
		// and it also allows a client to parse the tags in isolation even when there
		// are no keys. We'll return an error if the client tries to read values from here.
		t.section = FieldSection
		return tagKey, tagVal, nil
	}
	if err := t.advanceTagComma(); err != nil {
		return nil, nil, err
	}
	return tagKey, tagVal, nil
}

// advanceTagComma consumes a comma after a measurement
// or a tag value, making sure it's not followed by whitespace.
func (t *Tokenizer) advanceTagComma() error {
	if !t.ensure(1) {
		return nil
	}
	nextc := t.at(0)
	if nextc != ',' {
		return nil
	}
	// If there's a comma, there's a tag, so check that there's the start
	// of a tag name there.
	t.advance(1)
	if !t.ensure(1) {
		return t.syntaxErrorf("expected tag key after comma; got end of input")
	}
	if whitespace.get(t.at(0)) {
		return t.syntaxErrorf("expected tag key after comma; got white space instead")
	}
	return nil
}

// NextFieldBytes returns the next field in the entry.
// If there are no more fields, it returns all zero values.
// Note that this must be called before Time because
// fields precede the timestamp in the line-protocol entry.
//
// The returned value slice may not be valid: to
// check its validity, use NewValueFromBytes, or use NextField.
func (t *Tokenizer) NextFieldBytes() (key []byte, kind ValueKind, value []byte, err error) {
	if ok, err := t.advanceToSection(FieldSection); err != nil {
		return nil, Unknown, nil, err
	} else if !ok {
		return nil, Unknown, nil, nil
	}
	fieldKey := t.takeEscUnquotedStats(fieldKeyChars, &fieldKeyEscapes.revTable)
	if len(fieldKey) == 0 {
		return nil, Unknown, nil, t.syntaxErrorf("expected field key but none found")
	}
	if !t.ensure(1) {
		return nil, Unknown, nil, t.syntaxErrorf("want '=' after field key %q, found end of input", fieldKey)
	}
	if nextc := t.at(0); nextc != '=' {
		return nil, Unknown, nil, t.syntaxErrorf("want '=' after field key %q, found %q", fieldKey, nextc)
	}
	t.advance(1)
	if !t.ensure(1) {
		return nil, Unknown, nil, t.syntaxErrorf("expected field value, found end of input")
	}
	var fieldVal []byte
	var fieldKind ValueKind
	switch t.at(0) {
	case '"':
		// Skip leading quote.
		t.advance(1)
		fieldKind = String
		var stats Stats
		fieldVal, stats = t.takeEscStats(fieldStringValChars, &fieldStringValEscapes.revTable, quotedStats, quotedEscStats)
		t.stats |= stats
		if !t.ensure(1) {
			return nil, Unknown, nil, t.syntaxErrorf("expected closing quote for string field value, found end of input")
		}
		if t.at(0) != '"' {
			// This can't happen, as all characters are allowed in a string.
			return nil, Unknown, nil, t.syntaxErrorf("unexpected string termination")
		}
		// Skip trailing quote
		t.advance(1)
	case 't', 'T', 'f', 'F':
		fieldVal = t.take(fieldValChars)
		fieldKind = Bool
	case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '.':
		fieldVal = t.take(fieldValChars)
		switch fieldVal[len(fieldVal)-1] {
		case 'i':
			fieldVal = fieldVal[:len(fieldVal)-1]
			fieldKind = Int
		case 'u':
			fieldVal = fieldVal[:len(fieldVal)-1]
			fieldKind = Uint
		default:
			fieldKind = Float
		}
	default:
		t.stats |= 1 << StatUnknownFieldType
		fieldVal = t.take(fieldValChars)
		fieldKind = Unknown
	}
	if !t.ensure(1) {
		t.section = endSection
		return fieldKey, fieldKind, fieldVal, nil
	}
	nextc := t.at(0)
	if nextc == ',' {
		t.advance(1)
		return fieldKey, fieldKind, fieldVal, nil
	}
	if !whitespace.get(nextc) {
		return nil, Unknown, nil, t.syntaxErrorf("unexpected character %q after field", nextc)
	}
	t.takeFieldSpace()
	if !t.ensure(1) || t.at(0) == '\n' {
		t.section = endSection
		return fieldKey, fieldKind, fieldVal, nil
	}
	t.section = TimeSection
	return fieldKey, fieldKind, fieldVal, nil
}

// NextField is a wrapper around NextFieldBytes that parses
// the field value. Note: the returned value is only valid
// until the next call method call on Tokenizer because when
// it's a string, it refers to an internal buffer.
func (t *Tokenizer) NextField() (key []byte, val Value, err error) {
	key, kind, data, err := t.NextFieldBytes()
	if err != nil || key == nil {
		return nil, Value{}, err
	}
	v, err := ParseValue(kind, data)
	if err != nil {
		return nil, Value{}, fmt.Errorf("cannot parse value for field key %q: %v", key, err)
	}
	return key, v, nil
}

// TimeBytes returns the timestamp of the entry as a byte slice.
// If there is no timestamp, it returns nil, nil.
func (t *Tokenizer) TimeBytes() ([]byte, error) {
	if ok, err := t.advanceToSection(TimeSection); err != nil {
		return nil, err
	} else if !ok {
		return nil, nil
	}
	start := t.r1 - t.r0
	timeBytes := t.take(timeChars)
	if len(timeBytes) == 0 {
		t.section = endSection
		timeBytes = nil
	}
	if !t.ensure(1) {
		t.section = endSection
		return timeBytes, nil
	}
	if !whitespace.get(t.at(0)) {
		// Absorb the rest of the line so that we get a better error.
		t.take(notNewline)
		return nil, t.syntaxErrorf("invalid timestamp (%q)", t.buf[t.r0+start:t.r1])
	}
	t.takeFieldSpace()
	if !t.ensure(1) {
		t.section = endSection
		return timeBytes, nil
	}
	if t.at(0) != '\n' {
		extra := t.take(notNewline)
		return nil, t.syntaxErrorf("unexpected text after timestamp (%q)", extra)
	}
	t.advance(1)
	t.section = endSection
	return timeBytes, nil
}

// Time is a wrapper around TimeBytes that returns the timestamp
// assuming the given precision.
func (t *Tokenizer) Time(prec Precision, defaultTime time.Time) (time.Time, error) {
	data, err := t.TimeBytes()
	if err != nil {
		return time.Time{}, err
	}
	if data == nil {
		return defaultTime.Truncate(prec.Duration()), nil
	}
	ts, err := parseIntBytes(data, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid timestamp %q", data)
	}
	ns, ok := prec.asNanoseconds(ts)
	if !ok {
		return time.Time{}, fmt.Errorf("timestamp %s%s out of range", data, prec)
	}
	return time.Unix(0, ns), nil
}

// consumeLine consumes an entire line. This is used to recover
// from syntax errors.
func (t *Tokenizer) consumeLine() {
	t.take(notNewline)
	t.reset()
	if t.ensure(1) {
		// Consume the newline byte.
		t.advance(1)
	}
	t.section = endSection
}

func (t *Tokenizer) skipEmptyLines() {
	for {
		t.takeFieldSpace()
		switch t.at(0) {
		case '#':
			// Found a comment.
			t.stats |= 1 << StatComment
			t.take(notNewline)
			if t.at(0) != '\n' {
				return
			}
			t.advance(1)
		case '\n':
			t.advance(1)
		default:
			return
		}
	}
}

func (t *Tokenizer) advanceToSection(section Section) (_ok bool, _err error) {
	if t.section == section {
		return true, nil
	}
	if t.section > section {
		return false, nil
	}
	// Enable skipping to avoid unnecessary unescaping work.
	t.skipping = true
	for t.section < section {
		if err := t.consumeSection(); err != nil {
			t.skipping = false
			return false, err
		}
	}
	t.skipping = false
	return true, nil
}

//go:generate stringer -type Section

// Section represents one tokenization section of a line-protocol entry.
type Section byte

const (
	MeasurementSection Section = iota
	TagSection
	FieldSection
	TimeSection

	// newlineSection represents the newline at the end of the line.
	// This section also absorbs any invalid characters at the end
	// of the line - it's used as a recovery state if we find an error
	// when parsing an earlier part of an entry.
	newlineSection

	// endSection represents the end of an entry. When we're at this
	// stage, calling More will cycle back to MeasurementSection again.
	endSection
)

func (t *Tokenizer) consumeSection() error {
	switch t.section {
	case MeasurementSection:
		_, err := t.Measurement()
		return err
	case TagSection:
		for {
			key, _, err := t.NextTag()
			if err != nil || key == nil {
				return err
			}
		}
	case FieldSection:
		for {
			key, _, _, err := t.NextFieldBytes()
			if err != nil || key == nil {
				return err
			}
		}
	case TimeSection:
		_, err := t.TimeBytes()
		return err
	case newlineSection:
		t.consumeLine()
		return nil
	default:
		return nil
	}
}

// takeEscUnquotedStats is like takeEsc but updates stats for measurements, tag keys and tag values.
func (t *Tokenizer) takeEscUnquotedStats(set *byteSet, escapeTable *[256]byte) []byte {
	data, stats := t.takeEscStats(set, escapeTable, unquotedStats, unquotedEscStats)
	if len(data) > 0 && data[0] == '"' {
		stats |= 1 << StatQuoteAtStart
	}
	if (stats & (1 << StatBackslashBackslash)) != 0 {
		// There's a double backslash somewhere: see whether there's
		// one followed by a separator (an awkard edge case).
		// Note: the backslash before the separator has already
		// been removed by unescaping, so we only need to
		// find a single backslash.
		for i := 0; i < len(data)-1; i++ {
			if data[i] == '\\' && !set[data[i+1]] {
				stats |= 1 << StatDoubleBackslashSeparator
				break
			}
		}
	}
	t.stats |= stats
	return data
}

// take returns the next slice of bytes that are in the given set
// reading more data as needed. It updates t.r1.
func (t *Tokenizer) take(set *byteSet) []byte {
	return t.takeStats(set, nil)
}

// takeStats is like take but updates t.stats according to stats.
func (t *Tokenizer) takeStats(set *byteSet, stats *statBytes) []byte {
	// Note: use a relative index for start because absolute
	// indexes aren't stable (the contents of the buffer can be
	// moved when reading more data).
	start := t.r1 - t.r0
	var bstats byteStatFlags
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
			bstats = stats.update(bstats, c)

		}
		t.r1 += len(buf)
	}
	t.stats |= stats.stats(bstats)
	return t.buf[t.r0+start : t.r1]
}

// takeEsc is like take except that escaped characters also count as
// part of the set. The escapeTable determines which characters
// can be escaped.
// It returns the unescaped string (unless t.skipping is true, in which
// case it doesn't need to go to the trouble of unescaping it).
func (t *Tokenizer) takeEsc(set *byteSet, escapeTable *[256]byte) []byte {
	data, _ := t.takeEscStats(set, escapeTable, nil, nil)
	return data
}

// takeEscStats is like takeEsc except that it updates t.stats according to stats (for normal characters)
// and escStats (for any characters preceded by a \ character).
func (t *Tokenizer) takeEscStats(set *byteSet, escapeTable *[256]byte, stats, escStats *statBytes) ([]byte, Stats) {
	// start holds the offset from r0 of the start of the taken slice.
	// Note that we can't use t.r1 directly, because the offsets can change
	// when the buffer is grown.
	start := t.r1 - t.r0

	// startUnesc holds the offset from t0 of the start of the most recent
	// unescaped segment.
	startUnesc := start

	var bstats, escbstats byteStatFlags

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
				bstats = stats.update(bstats, c)
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
			escbstats = escStats.update(escbstats, buf[i+1])
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
	statsFlags := stats.stats(bstats) | escStats.stats(escbstats)
	if len(t.escBuf) > startEsc {
		// We've got an unescaped result: append any remaining unescaped bytes
		// and return the relevant portion of the escape buffer.
		t.escBuf = append(t.escBuf, t.buf[startUnesc+t.r0:t.r1]...)
		return t.escBuf[startEsc:], statsFlags
	}
	return t.buf[t.r0+start : t.r1], statsFlags
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
	if t.r1+n <= len(t.buf) {
		// There are enough bytes available.
		return true
	}
	return t.ensure1(n)
}

// ensure1 is factored out of ensure so that ensure
// itself can be inlined.
func (t *Tokenizer) ensure1(n int) bool {
	for {
		if t.complete {
			// No possibility of more data.
			return false
		}
		t.readMore()
		if t.r1+n <= len(t.buf) {
			// There are enough bytes available.
			return true
		}
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

func (t *Tokenizer) syntaxErrorf(f string, a ...interface{}) error {
	// TODO make this return an error that points to the point of failure.

	// We'll recover from a syntax error by reading all bytes until
	// the next newline.
	t.section = newlineSection
	return fmt.Errorf(f, a...)
}
