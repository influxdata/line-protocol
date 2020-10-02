package influxdata

// save these methods from staticcheck. we're going to use them later.
var (
	_ = whitespace
	_ = (*byteSet).union
)

// newByteset returns a set representation
// of the given bytes.
func newByteSet(bytes ...byte) *byteSet {
	var set byteSet
	for _, b := range bytes {
		set.set(b)
	}
	return &set
}

// TODO benchmark it. This is compact (good cache behaviour)
// but maybe [256]bool might be faster (less operations).
type byteSet [4]uint64

// holds reports whether b holds the byte x.
func (b *byteSet) get(x uint8) bool {
	return b[x>>6]&(1<<(x&63)) != 0
}

// set ensures that x is in the set.
func (b *byteSet) set(x uint8) {
	b[x>>6] |= 1 << (x & 63)
}

// union returns the union of b and b1.
func (b *byteSet) union(b1 *byteSet) *byteSet {
	r := *b
	for i := range r {
		r[i] |= b1[i]
	}
	return &r
}

// invert returns everything not in b.
func (b *byteSet) invert() *byteSet {
	r := *b
	for i := range r {
		r[i] = ^r[i]
	}
	return &r
}

// whitespace returns a set containing all characters
// recognized as white space.
func whitespace() *byteSet {
	return newByteSet(' ', '\t', '\n', '\r', '\f')
}
