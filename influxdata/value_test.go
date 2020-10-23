package influxdata

import (
	"math"
	"testing"

	qt "github.com/frankban/quicktest"
)

var parseValueTests = []struct {
	testName        string
	kind            ValueKind
	data            string
	expectError     string
	expectInterface interface{}
	expectString    string
}{{
	testName:        "int",
	kind:            Int,
	data:            "1234",
	expectInterface: int64(1234),
	expectString:    "1234i",
}, {
	testName:        "uint",
	kind:            Uint,
	data:            "1234",
	expectInterface: uint64(1234),
	expectString:    "1234u",
}, {
	testName:        "float",
	kind:            Float,
	data:            "1e3",
	expectInterface: float64(1000),
	expectString:    "1000",
}, {
	testName:        "bool-true",
	kind:            Bool,
	data:            "true",
	expectInterface: true,
	expectString:    "true",
}, {
	testName:        "bool-false",
	kind:            Bool,
	data:            "false",
	expectInterface: false,
	expectString:    "false",
}, {
	testName:        "string",
	kind:            String,
	data:            "hello world",
	expectInterface: "hello world",
	expectString:    `"hello world"`,
}, {
	testName:    "invalid-int",
	kind:        Int,
	data:        "1e3",
	expectError: `invalid integer value "1e3"`,
}, {
	testName:    "invalid-uint",
	kind:        Uint,
	data:        "1e3",
	expectError: `invalid unsigned integer value "1e3"`,
}, {
	testName:    "invalid-float",
	kind:        Float,
	data:        "1e3a",
	expectError: `invalid float value "1e3a"`,
}, {
	testName:    "NaN",
	kind:        Float,
	data:        "NaN",
	expectError: `non-number "NaN" cannot be represented as a line-protocol field value`,
}, {
	testName:    "-Inf",
	kind:        Float,
	data:        "-Inf",
	expectError: `non-number "-Inf" cannot be represented as a line-protocol field value`,
}, {
	testName:    "invalid-bool",
	kind:        Bool,
	data:        "truE",
	expectError: `invalid bool value "truE"`,
}, {
	testName:    "unknown-kind",
	kind:        Unknown,
	data:        "nope",
	expectError: `cannot parse value "nope" with unknown kind`,
}, {
	testName:    "invalid-kind",
	kind:        125,
	data:        "nope",
	expectError: `unexpected value kind 125 \(value "nope"\)`,
}}

func TestValueCreation(t *testing.T) {
	c := qt.New(t)
	for _, test := range parseValueTests {
		c.Run(test.testName, func(c *qt.C) {
			v, err := ParseValue(test.kind, []byte(test.data))
			if test.expectError != "" {
				c.Assert(err, qt.ErrorMatches, test.expectError)
			} else {
				c.Assert(v.Kind(), qt.Equals, test.kind)
				c.Assert(v.Interface(), qt.Equals, test.expectInterface)
				c.Assert(v.String(), qt.Equals, test.expectString)

				// Check that we can create the same value with NewValue
				v1, ok := NewValue(v.Interface())
				c.Assert(ok, qt.IsTrue)
				c.Assert(v1.Kind(), qt.Equals, v.Kind())
				c.Assert(v1, qt.DeepEquals, v)
				c.Assert(v1.Interface(), qt.Equals, v.Interface())
				v2 := MustNewValue(v.Interface())
				c.Assert(v2, qt.DeepEquals, v1)
				if test.kind == String {
					// Check we can use bytes values too.
					v3, ok := NewValue(v.BytesV())
					c.Assert(ok, qt.IsTrue)
					c.Assert(v3.Kind(), qt.Equals, v.Kind())
					c.Assert(v3, qt.DeepEquals, v)
					c.Assert(v3.Interface(), qt.Equals, v.Interface())
				}
			}
		})
	}
}

// Note: many NewValue inputs are tested in TestValueCreation above.
// This test just tests values that can be represented as Go values
// but not as valid Values.
var newValueInvalidTests = []struct {
	testName string
	value    interface{}
}{{
	testName: "NaN",
	value:    math.NaN(),
}, {
	testName: "Inf",
	value:    math.Inf(1),
}, {
	testName: "unknown-type",
	value:    new(int),
}}

func TestNewValueInvalid(t *testing.T) {
	c := qt.New(t)
	for _, test := range newValueInvalidTests {
		c.Run(test.testName, func(c *qt.C) {
			_, ok := NewValue(test.value)
			c.Assert(ok, qt.IsFalse)
		})
	}
}
