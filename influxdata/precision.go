package influxdata

import (
	"fmt"
	"time"
)

// Precision specifies the scale at which a line-protocol timestamp
// is encoded.
type Precision byte

const (
	Nanosecond Precision = iota
	Microsecond
	Millisecond
	Second
)

// asNanoseconds returns x multiplied by p.Duration.
// It reports whether the multiplication succeeded without
// overflow.
func (p Precision) asNanoseconds(x int64) (int64, bool) {
	if p == Nanosecond {
		return x, true
	}
	d := int64(p.Duration())
	// Note: because p has a limited number of values, we don't have
	// to worry about edge cases like x being the most negative number.
	if c := x * d; c/d == x {
		return c, true
	}
	return 0, false
}

// Duration returns the time duration for the given precision.
func (p Precision) Duration() time.Duration {
	switch p {
	case Nanosecond:
		return time.Nanosecond
	case Microsecond:
		return time.Microsecond
	case Millisecond:
		return time.Millisecond
	case Second:
		return time.Second
	}
	panic(fmt.Errorf("unknown precision %d", p))
}

func (p Precision) String() string {
	switch p {
	case Nanosecond:
		return "ns"
	case Microsecond:
		return "µs"
	case Millisecond:
		return "ms"
	case Second:
		return "s"
	}
	panic(fmt.Errorf("unknown precision %d", p))
}
