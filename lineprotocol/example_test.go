package lineprotocol_test

import (
	"fmt"
	"time"

	"github.com/influxdata/line-protocol/v2/lineprotocol"
)

func ExampleDecoder() {
	data := []byte(`
foo,tag1=val1,tag2=val2 x=1,y="hello" 1625823259000000
bar enabled=true
`)
	dec := lineprotocol.NewDecoderWithBytes(data)
	for dec.Next() {
		fmt.Printf("\nstart entry\n")
		m, err := dec.Measurement()
		if err != nil {
			panic(err)
		}
		fmt.Printf("measurement %s\n", m)
		for {
			key, val, err := dec.NextTag()
			if err != nil {
				panic(err)
			}
			if key == nil {
				break
			}
			fmt.Printf("tag %s=%s\n", key, val)
		}
		for {
			key, val, err := dec.NextField()
			if err != nil {
				panic(err)
			}
			if key == nil {
				break
			}
			fmt.Printf("field %s=%v\n", key, val)
		}
		t, err := dec.Time(lineprotocol.Microsecond, time.Time{})
		if err != nil {
			panic(err)
		}
		if t.IsZero() {
			fmt.Printf("no timestamp\n")
		} else {
			fmt.Printf("timestamp %s\n", t.UTC().Format(time.RFC3339Nano))
		}
	}
	// Note: because we're decoding from a slice of bytes, dec.Error can't return
	// an error. If we were decoding from an io.Reader, we'd need to check dec.Error
	// here.

	// Output:
	//
	// start entry
	// measurement foo
	// tag tag1=val1
	// tag tag2=val2
	// field x=1
	// field y="hello"
	// timestamp 2021-07-09T09:34:19Z
	//
	// start entry
	// measurement bar
	// field enabled=true
	// no timestamp
}

func ExampleEncoder() {
	var enc lineprotocol.Encoder
	enc.SetPrecision(lineprotocol.Microsecond)
	enc.StartLine("foo")
	enc.AddTag("tag1", "val1")
	enc.AddTag("tag2", "val2")
	enc.AddField("x", lineprotocol.MustNewValue(1.0))
	enc.AddField("y", lineprotocol.MustNewValue("hello"))
	enc.EndLine(time.Unix(0, 1625823259000000000))
	enc.StartLine("bar")
	enc.AddField("enabled", lineprotocol.BoolValue(true))
	enc.EndLine(time.Time{})
	if err := enc.Err(); err != nil {
		panic(fmt.Errorf("encoding error: %v", err))
	}
	fmt.Printf("%s", enc.Bytes())
	// Output:
	// foo,tag1=val1,tag2=val2 x=1,y="hello" 1625823259000000
	// bar enabled=true
}
