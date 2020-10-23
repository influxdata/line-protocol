package influxdata

import (
	"fmt"
	"strings"
)

// save these methods from staticcheck. we're going to use them later.
var (
	_ = (*byteSet).intersect
)

// newByteset returns a set representation
// of the bytes in the given string.
func newByteSet(s string) *byteSet {
	var set byteSet
	for i := 0; i < len(s); i++ {
		set.set(s[i])
	}
	return &set
}

func (b *byteSet) String() string {
	var buf strings.Builder
	for i := 0; i < 256; i++ {
		if b.get(byte(i)) {
			buf.WriteByte(byte(i))
		}
	}
	s := buf.String()
	if len(s) > 128 {
		return fmt.Sprintf("not(%v)", b.invert())
	}
	return fmt.Sprintf("%q", s)
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

// intersect returns the intersection of b and b1.
func (b *byteSet) intersect(b1 *byteSet) *byteSet {
	r := *b
	for i := range r {
		r[i] &= b1[i]
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
