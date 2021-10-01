//go:build go1.18
// +build go1.18

package lineprotocol_test

import (
	"testing"
	"time"

	"github.com/influxdata/line-protocol/v2/lineprotocol"
)

func FuzzDecoder(f *testing.F) {
	f.Fuzz(func(t *testing.T, data []byte) {
		dec := lineprotocol.NewDecoderWithBytes(data)
	line:
		for dec.Next() {
			if _, err := dec.Measurement(); err != nil {
				continue line
			}
			for {
				key, _, err := dec.NextTag()
				if err != nil {
					continue line
				}
				if key == nil {
					break
				}
			}
			for {
				key, _, err := dec.NextField()
				if err != nil {
					continue line
				}
				if key == nil {
					break
				}
			}
			dec.Time(lineprotocol.Nanosecond, time.Time{})
		}
	})
}
