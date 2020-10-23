package influxdata

// newByteset returns a set representation
// of the bytes in the given string.
func newByteSet(s string) *byteSet {
	var set byteSet
	for i := 0; i < len(s); i++ {
		set.set(s[i])
	}
	return &set
}

// TODO benchmark it. This is compact (good cache behaviour)
// but maybe [256]bool might be faster (less operations).
type byteSet [4]uint64

// get reports whether b holds the byte x.
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
