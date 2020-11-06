package influxdata

import (
	"strings"
	"unicode"
)

// Stats holds a bitmask of Stat values. A Stat constant c is represented
// by the bit 1<<c.
type Stats uint32

func (s Stats) String() string {
	var buf strings.Builder
	if s == 0 {
		return "0"
	}
	for i := Stat(0); i < NumStat; i++ {
		if s&(1<<i) == 0 {
			continue
		}
		if buf.Len() > 0 {
			buf.WriteByte('|')
		}
		buf.WriteString(i.String())
	}
	return buf.String()
}

// Ensure that all the Stat constants fit into a Stats value.
var _ Stats = 1 << NumStat

//go:generate stringer -type Stat -trimprefix Stat

type Stat byte

const (
	// A field string value contains a literal newline character.
	StatStrLiteralNewline Stat = iota

	// A tag key, tag value, field key or measurement contains a \n, \r, \t or \f sequence.
	StatBackslashN
	StatBackslashR
	StatBackslashT
	StatBackslashF
	StatBackslashBackslash

	// A tag key, tag value, field key or measurement contains non-printable ascii.
	StatNonPrintable

	// A measurement contains an '=' character.
	StatMeasurementEquals

	// A tag key, tag value, field key or measurement contains two backslashes followed
	// by a separator character.
	StatDoubleBackslashSeparator

	// A tag key, tag value, field key or a measurement name begins with a '"' character.
	StatQuoteAtStart

	// A string value contains a \n, \r, \t or \f sequence.
	StatStrBackslashN
	StatStrBackslashR
	StatStrBackslashT
	StatStrBackslashF

	// Whitespace contains '\t', '\r', or '\f' characters respectively
	StatWhitespaceTab
	StatWhitespaceCR
	StatWhitespaceFF

	// There's a field type that we can't determine the type of.
	StatUnknownFieldType

	// There's a comment (# ...)
	StatComment

	// The number of Stat values.
	NumStat
)

var unquotedStats = func() *statBytes {
	chars := make([]byte, 0, 35)
	var stats []Stat
	for i := 0; i < 128; i++ {
		if !unicode.IsPrint(rune(i)) {
			chars = append(chars, byte(i))
			stats = append(stats, StatNonPrintable)
		}
	}
	// Add =  to the list. Although unquotedStats is used for measurements, tag keys,
	// tag values and field keys, only measurements permit = as an unescaped
	// character, hence the specific StatMeasurementEquals stat.
	chars = append(chars, '=')
	stats = append(stats, StatMeasurementEquals)
	return newStatBytes(string(chars), stats...)
}()

var unquotedEscStats = newStatBytes(
	`nrtf\`,
	StatBackslashN,
	StatBackslashR,
	StatBackslashT,
	StatBackslashF,
	StatBackslashBackslash,
)

var quotedStats = newStatBytes(
	"\n",
	StatStrLiteralNewline,
)

var quotedEscStats = newStatBytes(
	"nrtf",
	StatStrBackslashN,
	StatStrBackslashR,
	StatStrBackslashT,
	StatStrBackslashF,
)

var fieldSpaceStats = newStatBytes(
	"\t\r\f",
	StatWhitespaceTab,
	StatWhitespaceCR,
	StatWhitespaceFF,
)

// statBytes helps to record occurrences of particular byte values.
type statBytes struct {
	flags [256]byteStatFlags
	base  Stat
}

// byteStatFlags represents a set of flags as maintained by a given statBytes instance.
type byteStatFlags byte

// update returns flags updated to include stats for the given byte.
func (s *statBytes) update(flags byteStatFlags, b byte) byteStatFlags {
	if s == nil {
		return flags
	}
	return flags | s.flags[b]
}

// stats returns the stats for the flags.
func (s *statBytes) stats(flags byteStatFlags) Stats {
	if s == nil {
		return 0
	}
	return Stats(flags) << s.base
}

func newStatBytes(bytes string, stats ...Stat) *statBytes {
	var s statBytes
	if len(stats) != len(bytes) {
		panic("number of flags must match number of bytes")
	}
	if len(bytes) == 0 {
		return &s
	}
	min, max := stats[0], stats[0]
	for _, stat := range stats {
		if stat < min {
			min = stat
		}
		if stat > max {
			max = stat
		}
	}
	if max-min > 8 {
		panic("flag range too great")
	}
	s.base = min
	for i, b := range bytes {
		s.flags[b] = byteStatFlags(1 << (stats[i] - min))
	}
	return &s
}
