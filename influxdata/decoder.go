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
	fieldSeparatorSpace   = newByteSet(" ")
	whitespace            = fieldSeparatorSpace.union(newByteSet("\r\n"))
	tagKeyChars           = newByteSet(",= ").union(nonPrintable).invert()
	tagKeyEscapes         = newEscaper(",= ")
	nonPrintable          = newByteSetRange(0, 31).union(newByteSet("\x7f"))
	eolChars              = newByteSet("\r\n")
	measurementChars      = newByteSet(", ").union(nonPrintable).invert()
	measurementEscapes    = newEscaper(" ,")
	tagValChars           = newByteSet(",=").union(whitespace).invert()
	tagValEscapes         = newEscaper(", =")
	fieldKeyChars         = tagKeyChars
	fieldKeyEscapes       = tagKeyEscapes
	fieldStringValChars   = newByteSet(`"`).invert()
	fieldStringValEscapes = newEscaper(`\"`)
	fieldValChars         = newByteSet(",").union(whitespace).invert()
	timeChars             = newByteSet("-0123456789")
	commentChars          = nonPrintable.invert().without(eolChars)
	notEOL                = eolChars.invert()
	notNewline            = newByteSet("\n").invert()
)

// Decoder implements low level parsing of a set of line-protocol entries.
type Decoder struct {
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

	// section holds the current section of the entry that's being
	// read.
	section Section

	// skipping holds whether we will need
	// to return the values that we're decoding.
	skipping bool

	// escBuf holds a buffer for unescaped characters.
	escBuf []byte

	// err holds any non-EOF error that was returned from rd.
	err error
}

// NewDecoder returns a decoder that splits the line-protocol text
// inside buf.
func NewDecoderWithBytes(buf []byte) *Decoder {
	return &Decoder{
		buf:      buf,
		complete: true,
		escBuf:   make([]byte, 0, 512),
		section:  endSection,
	}
}

// NewDecoder returns a decoder that reads from the given reader.
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{
		rd:      r,
		escBuf:  make([]byte, 0, 512),
		section: endSection,
	}
}

// NewDecoderAtSection returns a decoder that parses the given
// bytes but starting at the given section. This enables (for example)
// parsing of the tags section of an entry without the preceding measurement name.
//
// This does not scan forward to the given section; it assumes the byte array
// starts at the given section.
func NewDecoderAtSection(buf []byte, section Section) *Decoder {
	dec := &Decoder{
		buf:      buf,
		complete: true,
		escBuf:   make([]byte, 0, 512),
		section:  section,
	}
	if section != TagSection || !whitespace.get(dec.at(0)) {
		return dec
	}
	// As a special case, if we're asking to parse the tag section,
	// move straight to the field section if the start isn't whitespace,
	// because NextTag assumes that it's positioned at the start
	// of a valid non-empty tag section. This saves further special cases
	// below to avoid accepting an entry with a comma followed by whitespace.
	dec.take(fieldSeparatorSpace)
	dec.section = FieldSection
	return dec
}

// Next advances to the next entry, and reports whether there is an
// entry available. Syntax errors on individual lines do not cause this
// to return false (the decoder attempts to recover from badly
// formatted lines), but I/O errors do. Call d.Err to discover if there
// was any I/O error.
//
// After calling Next, the various components of a line can be retrieved
// by calling Measurement, NextTag, NextField and Time in that order
// (the same order that the components are held in the entry).
//
// IMPORTANT NOTE: the byte slices returned by the Decoder methods are
// only valid until the next call to any other Decode method.
//
// Decoder will skip earlier components if a later method is called,
// but it doesn't retain the entire entry, so it cannot go backwards.
//
// For example, to retrieve only the timestamp of all lines, this suffices:
//
//	for d.Next() {
//		timestamp, err := d.TimeBytes()
//	}
//
func (d *Decoder) Next() bool {
	if _, err := d.advanceToSection(endSection); err != nil {
		// There was a syntax error and the line might not be
		// fully consumed, so make sure that we do actually
		// consume the rest of the line. This relies on the fact
		// that when we return a syntax error, we abandon the
		// rest of the line by going to newlineSection. If we
		// changed that behaviour (for example to allow obtaining
		// multiple errors per line), then we might need to loop here.
		d.advanceToSection(endSection)
	}
	d.skipEmptyLines()
	d.section = MeasurementSection
	return d.ensure(1)
}

// Err returns any I/O error encountered when reading
// entries. If d was created with NewDecoderWithBytes,
// Err will always return nil.
func (d *Decoder) Err() error {
	return d.err
}

// Measurement returns the measurement name. It returns nil
// unless called before NextTag, NextField or Time.
func (d *Decoder) Measurement() ([]byte, error) {
	if ok, err := d.advanceToSection(MeasurementSection); err != nil {
		return nil, err
	} else if !ok {
		return nil, nil
	}
	// skipEmptyLines seems like it might be redundant here because d.Next
	// also skips empty lines, but call it here too, so that
	// NewDecoderAtSection(MeasurementSection) skips initial
	// empty/comment lines too. Maybe that's a bad idea.
	d.skipEmptyLines()
	d.reset()
	measure := d.takeEsc(measurementChars, &measurementEscapes.revTable)
	if len(measure) == 0 {
		if !d.ensure(1) {
			return nil, d.syntaxErrorf("no measurement name found")
		}
		return nil, d.syntaxErrorf("invalid character %q found at start of measurement name", d.at(0))
	}
	if measure[0] == '#' {
		// Comments are usually skipped earlier but if a comment contains invalid white space,
		// there's no way for the comment-parsing code to return an error, so instead
		// the read point is set to the start of the comment and we hit this case.
		return nil, d.syntaxErrorf("invalid character found in comment line")
	}
	if err := d.advanceTagComma(); err != nil {
		return nil, err
	}
	d.section = TagSection
	return measure, nil
}

// NextTag returns the next tag in the entry.
// If there are no more tags, it returns nil, nil, nil.
// Note that this must be called before NextField because
// tags precede fields in the line-protocol entry.
func (d *Decoder) NextTag() (key, value []byte, err error) {
	if ok, err := d.advanceToSection(TagSection); err != nil {
		return nil, nil, err
	} else if !ok {
		return nil, nil, nil
	}
	if d.ensure(1) && fieldSeparatorSpace.get(d.at(0)) {
		d.take(fieldSeparatorSpace)
		d.section = FieldSection
		return nil, nil, nil
	}
	tagKey := d.takeEsc(tagKeyChars, &tagKeyEscapes.revTable)
	if len(tagKey) == 0 || !d.ensure(1) || d.at(0) != '=' {
		if !d.ensure(1) {
			return nil, nil, d.syntaxErrorf("empty tag name")
		}
		return nil, nil, d.syntaxErrorf("expected '=' after tag key %q, but got %q instead", tagKey, d.at(0))
	}
	d.advance(1)
	tagVal := d.takeEsc(tagValChars, &tagValEscapes.revTable)
	if len(tagVal) == 0 {
		return nil, nil, d.syntaxErrorf("expected tag value after tag key %q, but none found", tagKey)
	}
	if !d.ensure(1) {
		// There's no more data after the tag value. Instead of returning an error
		// immediately, advance to the field section and return the tag and value.
		// This means that we'll see all the tags even when there's no value,
		// and it also allows a client to parse the tags in isolation even when there
		// are no keys. We'll return an error if the client tries to read values from here.
		d.section = FieldSection
		return tagKey, tagVal, nil
	}
	if err := d.advanceTagComma(); err != nil {
		return nil, nil, err
	}
	return tagKey, tagVal, nil
}

// advanceTagComma consumes a comma after a measurement
// or a tag value, making sure it's not followed by whitespace.
func (d *Decoder) advanceTagComma() error {
	if !d.ensure(1) {
		return nil
	}
	nextc := d.at(0)
	if nextc != ',' {
		return nil
	}
	// If there's a comma, there's a tag, so check that there's the start
	// of a tag name there.
	d.advance(1)
	if !d.ensure(1) {
		return d.syntaxErrorf("expected tag key after comma; got end of input")
	}
	if whitespace.get(d.at(0)) {
		return d.syntaxErrorf("expected tag key after comma; got white space instead")
	}
	return nil
}

// NextFieldBytes returns the next field in the entry.
// If there are no more fields, it returns all zero values.
// Note that this must be called before Time because
// fields precede the timestamp in the line-protocol entry.
//
// The returned value slice may not be valid: to
// check its validity, use NewValueFromBytes(kind, value), or use NextField.
func (d *Decoder) NextFieldBytes() (key []byte, kind ValueKind, value []byte, err error) {
	if ok, err := d.advanceToSection(FieldSection); err != nil {
		return nil, Unknown, nil, err
	} else if !ok {
		return nil, Unknown, nil, nil
	}
	fieldKey := d.takeEsc(fieldKeyChars, &fieldKeyEscapes.revTable)
	if len(fieldKey) == 0 {
		if !d.ensure(1) {
			return nil, Unknown, nil, d.syntaxErrorf("expected field key but none found")
		}
		return nil, Unknown, nil, d.syntaxErrorf("invalid character %q found at start of field key", d.at(0))
	}
	if !d.ensure(1) {
		return nil, Unknown, nil, d.syntaxErrorf("want '=' after field key %q, found end of input", fieldKey)
	}
	if nextc := d.at(0); nextc != '=' {
		return nil, Unknown, nil, d.syntaxErrorf("want '=' after field key %q, found %q", fieldKey, nextc)
	}
	d.advance(1)
	if !d.ensure(1) {
		return nil, Unknown, nil, d.syntaxErrorf("expected field value, found end of input")
	}
	var fieldVal []byte
	var fieldKind ValueKind
	switch d.at(0) {
	case '"':
		// Skip leading quote.
		d.advance(1)
		fieldVal = d.takeEsc(fieldStringValChars, &fieldStringValEscapes.revTable)
		fieldKind = String
		if !d.ensure(1) {
			return nil, Unknown, nil, d.syntaxErrorf("expected closing quote for string field value, found end of input")
		}
		if d.at(0) != '"' {
			// This can't happen, as all characters are allowed in a string.
			return nil, Unknown, nil, d.syntaxErrorf("unexpected string termination")
		}
		// Skip trailing quote
		d.advance(1)
	case 't', 'T', 'f', 'F':
		fieldVal = d.take(fieldValChars)
		fieldKind = Bool
	case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '.':
		fieldVal = d.take(fieldValChars)
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
		return nil, Unknown, nil, d.syntaxErrorf("field value has unrecognized type")
	}
	if !d.ensure(1) {
		d.section = endSection
		return fieldKey, fieldKind, fieldVal, nil
	}
	nextc := d.at(0)
	if nextc == ',' {
		d.advance(1)
		return fieldKey, fieldKind, fieldVal, nil
	}
	if !whitespace.get(nextc) {
		return nil, Unknown, nil, d.syntaxErrorf("unexpected character %q after field", nextc)
	}
	d.take(fieldSeparatorSpace)
	if d.takeEOL() {
		d.section = endSection
		return fieldKey, fieldKind, fieldVal, nil
	}
	d.section = TimeSection
	return fieldKey, fieldKind, fieldVal, nil
}

// takeEOL consumes input up until the next end of line.
func (d *Decoder) takeEOL() bool {
	if !d.ensure(1) {
		// End of input.
		return true
	}
	switch d.at(0) {
	case '\n':
		// Regular NL.
		d.advance(1)
		return true
	case '\r':
		if !d.ensure(2) {
			// CR at end of input.
			d.advance(1)
			return true
		}
		if d.at(1) == '\n' {
			// CR-NL
			d.advance(2)
			return true
		}
	}
	return false
}

// NextField is a wrapper around NextFieldBytes that parses
// the field value. Note: the returned value is only valid
// until the next call method call on Decoder because when
// it's a string, it refers to an internal buffer.
//
// If the value cannot be parsed because it's out of range
// (as opposed to being syntactically invalid),
// the errors.Is(err, ErrValueOutOfRange) will return true.
func (d *Decoder) NextField() (key []byte, val Value, err error) {
	key, kind, data, err := d.NextFieldBytes()
	if err != nil || key == nil {
		return nil, Value{}, err
	}
	v, err := NewValueFromBytes(kind, data)
	if err != nil {
		return nil, Value{}, fmt.Errorf("cannot parse value for field key %q: %w", key, err)
	}
	return key, v, nil
}

// TimeBytes returns the timestamp of the entry as a byte slice.
// If there is no timestamp, it returns nil, nil.
func (d *Decoder) TimeBytes() ([]byte, error) {
	if ok, err := d.advanceToSection(TimeSection); err != nil {
		return nil, err
	} else if !ok {
		return nil, nil
	}
	start := d.r1 - d.r0
	timeBytes := d.take(timeChars)
	if len(timeBytes) == 0 {
		d.section = endSection
		timeBytes = nil
	}
	if !d.ensure(1) {
		d.section = endSection
		return timeBytes, nil
	}
	if !whitespace.get(d.at(0)) {
		// Absorb the rest of the line so that we get a better error.
		d.take(notEOL)
		return nil, d.syntaxErrorf("invalid timestamp (%q)", d.buf[d.r0+start:d.r1])
	}
	d.take(fieldSeparatorSpace)
	if !d.ensure(1) {
		d.section = endSection
		return timeBytes, nil
	}
	if !d.takeEOL() {
		extra := d.take(notEOL)
		return nil, d.syntaxErrorf("unexpected text after timestamp (%q)", extra)
	}
	d.section = endSection
	return timeBytes, nil
}

// Time is a wrapper around TimeBytes that returns the timestamp
// assuming the given precision.
func (d *Decoder) Time(prec Precision, defaultTime time.Time) (time.Time, error) {
	data, err := d.TimeBytes()
	if err != nil {
		return time.Time{}, err
	}
	if data == nil {
		return defaultTime.Truncate(prec.Duration()), nil
	}
	ts, err := parseIntBytes(data, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid timestamp: %w", maybeOutOfRange(err, "invalid syntax"))
	}
	ns, ok := prec.asNanoseconds(ts)
	if !ok {
		return time.Time{}, fmt.Errorf("invalid timestamp: %w", ErrValueOutOfRange)
	}
	return time.Unix(0, ns), nil
}

// consumeLine is used to recover from errors by reading an entire
// line even if it contains invalid characters.
func (d *Decoder) consumeLine() {
	d.take(notNewline)
	if d.at(0) == '\n' {
		d.advance(1)
	}
	d.reset()
	d.section = endSection
}

func (d *Decoder) skipEmptyLines() {
	for {
		startLine := d.r1 - d.r0
		d.take(fieldSeparatorSpace)
		switch d.at(0) {
		case '#':
			// Found a comment.
			d.take(commentChars)
			if !d.takeEOL() {
				// Comment has invalid characters.
				// Rewind input to start of comment so
				// that next section will return the error.
				d.r1 = d.r0 + startLine
				return
			}
		case '\n':
			d.advance(1)
		case '\r':
			if !d.takeEOL() {
				// Solitary carriage return.
				// Leave it there and next section will return an error.
				return
			}
		default:
			return
		}
	}
}

func (d *Decoder) advanceToSection(section Section) (bool, error) {
	if d.section == section {
		return true, nil
	}
	if d.section > section {
		return false, nil
	}
	// Enable skipping to avoid unnecessary unescaping work.
	d.skipping = true
	for d.section < section {
		if err := d.consumeSection(); err != nil {
			d.skipping = false
			return false, err
		}
	}
	d.skipping = false
	return true, nil
}

//go:generate stringer -type Section

// Section represents one decoder section of a line-protocol entry.
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

func (d *Decoder) consumeSection() error {
	switch d.section {
	case MeasurementSection:
		_, err := d.Measurement()
		return err
	case TagSection:
		for {
			key, _, err := d.NextTag()
			if err != nil || key == nil {
				return err
			}
		}
	case FieldSection:
		for {
			key, _, _, err := d.NextFieldBytes()
			if err != nil || key == nil {
				return err
			}
		}
	case TimeSection:
		_, err := d.TimeBytes()
		return err
	case newlineSection:
		d.consumeLine()
		return nil
	default:
		return nil
	}
}

// take returns the next slice of bytes that are in the given set
// reading more data as needed. It updates d.r1.
func (d *Decoder) take(set *byteSet) []byte {
	// Note: use a relative index for start because absolute
	// indexes aren't stable (the contents of the buffer can be
	// moved when reading more data).
	start := d.r1 - d.r0
outer:
	for {
		if !d.ensure(1) {
			break
		}
		buf := d.buf[d.r1:]
		for i, c := range buf {
			if !set.get(c) {
				d.r1 += i
				break outer
			}
		}
		d.r1 += len(buf)
	}
	return d.buf[d.r0+start : d.r1]
}

// takeEsc is like take except that escaped characters also count as
// part of the set. The escapeTable determines which characters
// can be escaped.
// It returns the unescaped string (unless d.skipping is true, in which
// case it doesn't need to go to the trouble of unescaping it).
func (d *Decoder) takeEsc(set *byteSet, escapeTable *[256]byte) []byte {
	// start holds the offset from r0 of the start of the taken slice.
	// Note that we can't use d.r1 directly, because the offsets can change
	// when the buffer is grown.
	start := d.r1 - d.r0

	// startUnesc holds the offset from r0 of the start of the most recent
	// unescaped segment.
	startUnesc := start

	// startEsc holds the index into r.escBuf of the start of the escape buffer.
	startEsc := len(d.escBuf)
outer:
	for {
		if !d.ensure(1) {
			break
		}
		buf := d.buf[d.r1:]
		for i := 0; i < len(buf); i++ {
			c := buf[i]
			if c != '\\' {
				if !set.get(c) {
					// We've found the end, so we're done here.
					d.r1 += i
					break outer
				}
				continue
			}
			if i+1 >= len(buf) {
				// Not enough room in the buffer. Try reading more so that
				// we can see the next byte (note: ensure(i+2) is asking
				// for exactly one more character, because we know we already
				// have i+1 bytes in the buffer).
				if !d.ensure(i + 2) {
					// No character to escape, so leave the \ intact.
					d.r1 = len(d.buf)
					break outer
				}
				// Note that d.ensure can change d.buf, so we need to
				// update buf to point to the correct buffer.
				buf = d.buf[d.r1:]
			}
			replc := escapeTable[buf[i+1]]
			if replc == 0 {
				// The backslash doesn't precede a value escaped
				// character, so it stays intact.
				continue
			}
			if !d.skipping {
				d.escBuf = append(d.escBuf, d.buf[d.r0+startUnesc:d.r1+i]...)
				d.escBuf = append(d.escBuf, replc)
				startUnesc = d.r1 - d.r0 + i + 2
			}
			i++
		}
		// We've consumed all the bytes in the buffer. Now continue
		// the loop, trying to acquire more.
		d.r1 += len(buf)
	}
	if len(d.escBuf) > startEsc {
		// We've got an unescaped result: append any remaining unescaped bytes
		// and return the relevant portion of the escape buffer.
		d.escBuf = append(d.escBuf, d.buf[startUnesc+d.r0:d.r1]...)
		return d.escBuf[startEsc:]
	}
	return d.buf[d.r0+start : d.r1]
}

// at returns the byte at i bytes after the current read position.
// It assumes that the index has already been ensured.
// If there's no byte there, it returns zero.
func (d *Decoder) at(i int) byte {
	if d.r1+i < len(d.buf) {
		return d.buf[d.r1+i]
	}
	return 0
}

// reset discards all the data up to d.r1 and data in d.escBuf
func (d *Decoder) reset() {
	if d.r1 == len(d.buf) {
		// No bytes in the buffer, so we can start from the beginning without
		// needing to copy anything (and get better cache behaviour too).
		d.buf = d.buf[:0]
		d.r1 = 0
	}
	d.r0 = d.r1
	d.escBuf = d.escBuf[:0]
}

// advance advances the read point by n.
// This should only be used when it's known that
// there are already n bytes available in the buffer.
func (d *Decoder) advance(n int) {
	d.r1 += n
}

// ensure ensures that there are at least n bytes available in
// d.buf[d.r1:], reading more bytes if necessary.
// It reports whether enough bytes are available.
func (d *Decoder) ensure(n int) bool {
	if d.r1+n <= len(d.buf) {
		// There are enough bytes available.
		return true
	}
	return d.ensure1(n)
}

// ensure1 is factored out of ensure so that ensure
// itself can be inlined.
func (d *Decoder) ensure1(n int) bool {
	for {
		if d.complete {
			// No possibility of more data.
			return false
		}
		d.readMore()
		if d.r1+n <= len(d.buf) {
			// There are enough bytes available.
			return true
		}
	}
}

// readMore reads more data into d.buf.
func (d *Decoder) readMore() {
	if d.complete {
		return
	}
	n := cap(d.buf) - len(d.buf)
	if n < minRead {
		// There's not enough available space at the end of the buffer to read into.
		if d.r0+n >= minRead {
			// There's enough space when we take into account already-used
			// part of buf, so slide the data to the front.
			copy(d.buf, d.buf[d.r0:])
			d.buf = d.buf[:len(d.buf)-d.r0]
			d.r1 -= d.r0
			d.r0 = 0
		} else {
			// We need to grow the buffer. Note that we don't have to copy
			// the unused part of the buffer (d.buf[:d.r0]).
			// TODO provide a way to limit the maximum size that
			// the buffer can grow to.
			used := len(d.buf) - d.r0
			n1 := cap(d.buf) * 2
			if n1-used < minGrow {
				n1 = used + minGrow
			}
			buf1 := make([]byte, used, n1)
			copy(buf1, d.buf[d.r0:])
			d.buf = buf1
			d.r1 -= d.r0
			d.r0 = 0
		}
	}
	n, err := d.rd.Read(d.buf[len(d.buf):cap(d.buf)])
	d.buf = d.buf[:len(d.buf)+n]
	if err == nil {
		return
	}
	d.complete = true
	if err != io.EOF {
		d.err = err
	}
}

func (d *Decoder) syntaxErrorf(f string, a ...interface{}) error {
	// TODO make this return an error that points to the point of failure.

	// We'll recover from a syntax error by reading all bytes until
	// the next newline.
	d.section = newlineSection
	return fmt.Errorf(f, a...)
}
