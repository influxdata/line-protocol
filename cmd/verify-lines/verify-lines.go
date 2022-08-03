package main

import "C"
import (
	"fmt"
	"os"

	"github.com/influxdata/line-protocol/v2/lineprotocol"
)

func main() {}

// verifyLines provides a cgo hook to verify line protocol strings.
// The method will print encountered decode errors to stderr and returns
// a boolean to indicate if any errors where encountered following the
// unix standard of 0 for success and 1 for error conditions.
// It will continue to decode and verify all lines even after encountering
// an error. The line immediately after an error may verify but not be
// what was intended.
//export verifyLines
func verifyLines(lines *C.char) C.int {
	dec := lineprotocol.NewDecoderWithBytes([]byte(C.GoString(lines)))
	var failure bool
	logErr := func(err error) {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		failure = true
	}
nextLine:
	for dec.Next() {
		_, err := dec.Measurement()
		if err != nil {
			logErr(err)
			continue nextLine
		}
		for {
			key, _, err := dec.NextTag()
			if err != nil {
				logErr(err)
				continue nextLine
			}
			if key == nil {
				break
			}
		}
		for {
			key, _, err := dec.NextField()
			if err != nil {
				logErr(err)
				continue nextLine
			}
			if key == nil {
				break
			}
		}
		if _, err := dec.TimeBytes(); err != nil {
			logErr(err)
			continue nextLine
		}
	}
	if failure {
		return 1
	} else {
		return 0
	}
}
