package protocol

import (
	"bytes"
	"strings"
	"sync"
	"unicode/utf8"
)

const (
	escapes            = "\t\n\f\r ,="
	nameEscapes        = "\t\n\f\r ,"
	stringFieldEscapes = "\t\n\f\r\\\""
)

var (
	stringEscaper = strings.NewReplacer(
		"\t", `\t`,
		"\n", `\n`,
		"\f", `\f`,
		"\r", `\r`,
		`,`, `\,`,
		` `, `\ `,
		`=`, `\=`,
	)

	nameEscaper = strings.NewReplacer(
		"\t", `\t`,
		"\n", `\n`,
		"\f", `\f`,
		"\r", `\r`,
		`,`, `\,`,
		` `, `\ `,
	)

	stringFieldEscaper = strings.NewReplacer(
		"\t", `\t`,
		"\n", `\n`,
		"\f", `\f`,
		"\r", `\r`,
		`"`, `\"`,
		`\`, `\\`,
	)

	// var (
	// 	bytesEscaper = strings.NewReplacer(
	// 		"\t", `\t`,
	// 		"\n", `\n`,
	// 		"\f", `\f`,
	// 		"\r", `\r`,
	// 		`,`, `\,`,
	// 		` `, `\ `,
	// 		`=`, `\=`,
	// 	)

	// 	nameBscaper = strings.NewReplacer(
	// 		"\t", `\t`,
	// 		"\n", `\n`,
	// 		"\f", `\f`,
	// 		"\r", `\r`,
	// 		`,`, `\,`,
	// 		` `, `\ `,
	// 	)

	// 	stringFieldEscaper = strings.NewReplacer(
	// 		"\t", `\t`,
	// 		"\n", `\n`,
	// 		"\f", `\f`,
	// 		"\r", `\r`,
	// 		`"`, `\"`,
	// 		`\`, `\\`,
	// 	)
	// )
)

// The various escape functions allocate, I'd like to fix that.
// TODO: make escape not allocate

// Escape a tagkey, tagvalue, or fieldkey
func escape(s string) string {
	if strings.ContainsAny(s, escapes) {
		return stringEscaper.Replace(s)
	}
	return s
}

// Escape a measurement name
func nameEscape(s string) string {
	if strings.ContainsAny(s, nameEscapes) {
		return nameEscaper.Replace(s)
	}
	return s
}

// Escape a string field
func stringFieldEscape(s string) string {
	if strings.ContainsAny(s, stringFieldEscapes) {
		return stringFieldEscaper.Replace(s)
	}
	return s
}

var escaperPool = sync.Pool{New: func() interface{} { return make([]byte, 0, 10) }}

func resturnToEscaperPool(b []byte) {
	if cap(b) > 64 {
		// early exit
		return
	}
	escaperPool.Put(b)
}

func runesToUTF8Manual2(rs []rune) []byte {
	size := 0
	for _, r := range rs {
		size += utf8.RuneLen(r)
	}

	bs := make([]byte, size)

	count := 0
	for _, r := range rs {
		count += utf8.EncodeRune(bs[count:], r)
	}

	return bs
}

const (
	utf8mask  = byte(0x3F)
	utf8bytex = byte(0x80) // 1000 0000
	utf8len2  = byte(0xC0) // 1100 0000
	utf8len3  = byte(0xE0) // 1110 0000
	utf8len4  = byte(0xF0) // 1111 0000
)

func escapeBytes(b []byte) []byte {
	if bytes.ContainsAny(b, escapes) {
		var r rune
		var i int
		res := escaperPool.Get().([]byte)
		for len(b) > 0 {
			r, i = utf8.DecodeRune(b)
			switch {
			case r == '\t':
				res = append(res, `\t`...)
			case r == '\n':
				res = append(res, `\n`...)
			case r == '\f':
				res = append(res, `\f`...)
			case r == '\r':
				res = append(res, `\r`...)
			case r == ',':
				res = append(res, `\,`...)
			case r == ' ':
				res = append(res, `\ `...)
			case r == '=':
				res = append(res, `\=`...)
			case r <= 1<<7-1:
				res = append(res, byte(r))
			case r <= 1<<11-1:
				res = append(res, utf8len2|byte(r>>6), utf8bytex|byte(r)&utf8mask)
			case r <= 1<<16-1:
				res = append(res, utf8len3|byte(r>>12), utf8bytex|byte(r>>6)&utf8mask, utf8bytex|byte(r)&utf8mask)
			default:
				res = append(res, utf8len4|byte(r>>18), utf8bytex|byte(r>>12)&utf8mask, utf8bytex|byte(r>>6)&utf8mask, utf8bytex|byte(r)&utf8mask)
			}
			b = b[i:len(b):cap(b)]
		}
		return res
	}
	return b
}

// Escape a measurement name
func nameEscapeBytes(b []byte) []byte {
	if bytes.ContainsAny(b, nameEscapes) {
		var r rune
		var i int
		res := escaperPool.Get().([]byte)
		for len(b) > 0 {
			r, i = utf8.DecodeRune(b)
			switch {
			case r == '\t':
				res = append(res, `\t`...)
			case r == '\n':
				res = append(res, `\n`...)
			case r == '\f':
				res = append(res, `\f`...)
			case r == '\r':
				res = append(res, `\r`...)
			case r == ',':
				res = append(res, `\,`...)
			case r == ' ':
				res = append(res, `\ `...)
			case r == '\\':
				res = append(res, `\\`...)
			case r <= 1<<7-1:
				res = append(res, byte(r))
			case r <= 1<<11-1:
				res = append(res, utf8len2|byte(r>>6), utf8bytex|byte(r)&utf8mask)
			case r <= 1<<16-1:
				res = append(res, utf8len3|byte(r>>12), utf8bytex|byte(r>>6)&utf8mask, utf8bytex|byte(r)&utf8mask)
			default:
				res = append(res, utf8len4|byte(r>>18), utf8bytex|byte(r>>12)&utf8mask, utf8bytex|byte(r>>6)&utf8mask, utf8bytex|byte(r)&utf8mask)
			}
			b = b[i:len(b):cap(b)]
		}
		return res
	}
	return b
}

func stringFieldEscapeBytes(b []byte) []byte {
	if bytes.ContainsAny(b, stringFieldEscapes) {
		var r rune
		var i int
		res := escaperPool.Get().([]byte)
		for len(b) > 0 {
			r, i = utf8.DecodeRune(b)
			switch {
			case r == '\t':
				res = append(res, `\t`...)
			case r == '\n':
				res = append(res, `\n`...)
			case r == '\f':
				res = append(res, `\f`...)
			case r == '\r':
				res = append(res, `\r`...)
			case r == ',':
				res = append(res, `\,`...)
			case r == ' ':
				res = append(res, `\ `...)
			case r == '\\':
				res = append(res, `\\`...)
			case r <= 1<<7-1:
				res = append(res, byte(r))
			case r <= 1<<11-1:
				res = append(res, utf8len2|byte(r>>6), utf8bytex|byte(r)&utf8mask)
			case r <= 1<<16-1:
				res = append(res, utf8len3|byte(r>>12), utf8bytex|byte(r>>6)&utf8mask, utf8bytex|byte(r)&utf8mask)
			default:
				res = append(res, utf8len4|byte(r>>18), utf8bytex|byte(r>>12)&utf8mask, utf8bytex|byte(r>>6)&utf8mask, utf8bytex|byte(r)&utf8mask)
			}
			b = b[i:len(b):cap(b)]
		}
		return res
	}
	return b
}
