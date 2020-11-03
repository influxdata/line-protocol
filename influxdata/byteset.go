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

type byteSet [256]bool

// holds reports whether b holds the byte x.
func (b *byteSet) get(x uint8) bool {
	return b[x]
}

// set ensures that x is in the set.
func (b *byteSet) set(x uint8) {
	b[x] = true
}

// union returns the union of b and b1.
func (b *byteSet) union(b1 *byteSet) *byteSet {
	r := *b
	for i := range r {
		r[i] = r[i] || b1[i]
	}
	return &r
}

// invert returns everything not in b.
func (b *byteSet) invert() *byteSet {
	r := *b
	for i := range r {
		r[i] = !r[i]
	}
	return &r
}
