package influxdata

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"strconv"
)

// ErrValueOutOfRange signals that a value is out of the acceptable numeric range.
var ErrValueOutOfRange = errors.New("line-protocol value out of range")

// Value holds one of the possible line-protocol field values.
type Value struct {
	// number covers:
	//	- signed integer
	//	- unsigned integer
	//	- bool
	//	- float
	number uint64
	// bytes holds the string bytes or a sentinel (see below)
	// when the value's not holding a string.
	bytes []byte
}

var (
	intSentinel   = [1]byte{'i'}
	uintSentinel  = [1]byte{'u'}
	floatSentinel = [1]byte{'f'}
	boolSentinel  = [1]byte{'b'}
)

func MustNewValue(x interface{}) Value {
	v, ok := NewValue(x)
	if !ok {
		panic(fmt.Errorf("invalid value for NewValue: %T (%#v)", x, x))
	}
	return v
}

func (v1 Value) Equal(v2 Value) bool {
	return v1.Kind() == v2.Kind() && v1.number == v2.number && bytes.Equal(v1.bytes, v2.bytes)
}

// ParseValue parses the data as a value of the given kind.
//
// If the value is out of range, errors.Is(err, ErrValueOutOfRange) will return true.
//
// The data for Int and Uint kinds should not include
// the type suffixes present in the line-protocol field values.
// For example, the data for the zero Int should be "0" not "0i".
//
// If kind is String, the returned value still refers to the data
// slice: it does not make a copy.
func ParseValue(kind ValueKind, data []byte) (Value, error) {
	switch kind {
	case Int:
		x, err := parseIntBytes(data, 10, 64)
		if err != nil {
			return Value{}, maybeOutOfRange(err, "invalid integer value syntax")
		}
		return Value{
			number: uint64(x),
			bytes:  intSentinel[:],
		}, nil
	case Uint:
		x, err := parseUintBytes(data, 10, 64)
		if err != nil {
			return Value{}, maybeOutOfRange(err, "invalid unsigned integer value syntax")
		}
		return Value{
			number: x,
			bytes:  uintSentinel[:],
		}, nil
	case Float:
		x, err := parseFloatBytes(data, 64)
		if err != nil {
			return Value{}, maybeOutOfRange(err, "invalid float value syntax")
		}
		if math.IsInf(x, 0) || math.IsNaN(x) {
			return Value{}, fmt.Errorf("non-number %q cannot be represented as a line-protocol field value", data)
		}
		return Value{
			number: math.Float64bits(x),
			bytes:  floatSentinel[:],
		}, nil
	case Bool:
		x, err := parseBoolBytes(data)
		if err != nil {
			return Value{}, fmt.Errorf("invalid bool value %q", data)
		}
		return Value{
			number: uint64(x),
			bytes:  boolSentinel[:],
		}, nil
	case String:
		// TODO check that it's valid utf-8 data?
		return Value{
			bytes: data,
		}, nil
	case Unknown:
		return Value{}, fmt.Errorf("cannot parse value %q with unknown kind", data)
	default:
		return Value{}, fmt.Errorf("unexpected value kind %d (value %q)", kind, data)
	}
}

// NewValue returns a Value containing the value of x, which must
// be of type int64 (Int), uint64 (Uint), float64 (Float), bool (Bool),
// string (String) or []byte (String).
//
// Unlike ParseValue, NewValue will make a copy of the byte
// slice if x is []byte - use ParseValue if you require zero-copy
// semantics.
func NewValue(x interface{}) (Value, bool) {
	switch x := x.(type) {
	case int64:
		return Value{
			number: uint64(x),
			bytes:  intSentinel[:],
		}, true
	case uint64:
		return Value{
			number: uint64(x),
			bytes:  uintSentinel[:],
		}, true
	case float64:
		if math.IsInf(x, 0) || math.IsNaN(x) {
			return Value{}, false
		}
		return Value{
			number: math.Float64bits(x),
			bytes:  floatSentinel[:],
		}, true
	case bool:
		n := uint64(0)
		if x {
			n = 1
		}
		return Value{
			number: uint64(n),
			bytes:  boolSentinel[:],
		}, true
	case string:
		return Value{
			bytes: []byte(x),
		}, true
	case []byte:
		return Value{
			bytes: append([]byte(nil), x...),
		}, true
	}
	return Value{}, false
}

// IntV returns the value as an int64. It panics if v.Kind is not Int.
func (v Value) IntV() int64 {
	v.mustBe(Int)
	return int64(v.number)
}

// UintV returns the value as a uint64. It panics if v.Kind is not Uint.
func (v Value) UintV() uint64 {
	v.mustBe(Uint)
	return v.number
}

// FloatV returns the value as a float64. It panics if v.Kind is not Float.
func (v Value) FloatV() float64 {
	v.mustBe(Float)
	return math.Float64frombits(v.number)
}

// StringV returns the value as a string. It panics if v.Kind is not String.
func (v Value) StringV() string {
	v.mustBe(String)
	return string(v.bytes)
}

// BytesV returns the value as a []byte. It panics if v.Kind is not String.
// Note that this may return a direct reference to the byte slice within the
// value - modifying the returned byte slice may mutate the contents
// of the Value.
func (v Value) BytesV() []byte {
	v.mustBe(String)
	return v.bytes
}

// BoolV returns the value as a bool. It panics if v.Kind is not Bool.
func (v Value) BoolV() bool {
	v.mustBe(Bool)
	return v.number != 0
}

// Interface returns the value as an interface. The returned value
// will have a different dynamic type depending on the value kind;
// one of int64 (Int), uint64 (Uint), float64 (Float), string (String), bool (Bool).
func (v Value) Interface() interface{} {
	switch v.Kind() {
	case Int:
		return v.IntV()
	case Uint:
		return v.UintV()
	case String:
		return v.StringV()
	case Bool:
		return v.BoolV()
	case Float:
		return v.FloatV()
	default:
		// Shouldn't be able to happen.
		panic("unknown value kind")
	}
}

func (v Value) mustBe(k ValueKind) {
	if v.Kind() != k {
		panic(fmt.Errorf("value has unexpected kind; got %v want %v", v.Kind(), k))
	}
}

func (v Value) Kind() ValueKind {
	if len(v.bytes) != 1 {
		return String
	}
	switch &v.bytes[0] {
	case &intSentinel[0]:
		return Int
	case &uintSentinel[0]:
		return Uint
	case &floatSentinel[0]:
		return Float
	case &boolSentinel[0]:
		return Bool
	}
	return String
}

// String returns the value similarly to how it would appear
// in a line-protocol entry, except that strings are quoted
// according to Go rules not line-protocol rules.
func (v Value) String() string {
	switch v.Kind() {
	case Float:
		return fmt.Sprint(v.FloatV())
	case Int:
		return fmt.Sprintf("%di", v.IntV())
	case Uint:
		return fmt.Sprintf("%du", v.UintV())
	case Bool:
		if v.BoolV() {
			return "true"
		}
		return "false"
	case String:
		return fmt.Sprintf("%q", v.StringV())
	default:
		panic("unknown kind")
	}
}

func maybeOutOfRange(err error, s string) error {
	if err, ok := err.(*strconv.NumError); ok && err.Err == strconv.ErrRange {
		return ErrValueOutOfRange
	}
	return errors.New(s)
}
