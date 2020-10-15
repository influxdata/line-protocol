package influxdata

import (
	"strconv"
	"strings"
)

// escaper represents a set of characters that can be escaped.
type escaper struct {
	// table maps from byte value to the byte used to escape that value.
	// If an entry is zero, it doesn't need to be escaped.
	table [256]byte

	// revTable holds the inverse of table - it maps
	// from escaped value to the unescaped value.
	revTable [256]byte
}

// newEscaper returns an escaper that escapes the
// given characters.
func newEscaper(escapes string) *escaper {
	var e escaper
	for _, b := range escapes {
		// Note that this works because the Go escaping rules
		// for white space are the same as line-protocol's.
		q := strconv.QuoteRune(b)
		q = q[1 : len(q)-1]             // strip single quotes.
		q = strings.TrimPrefix(q, "\\") // remove backslash if present.
		e.table[byte(b)] = q[0]         // use single remaining character.
		e.revTable[q[0]] = byte(b)
	}
	return &e
}
