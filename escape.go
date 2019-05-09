package protocol

import (
	"bytes"
	"strings"
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

const (
	utf8mask  = byte(0x3F)
	utf8bytex = byte(0x80) // 1000 0000
	utf8len2  = byte(0xC0) // 1100 0000
	utf8len3  = byte(0xE0) // 1110 0000
	utf8len4  = byte(0xF0) // 1111 0000
)

func escapeBytes(dest *[]byte, b []byte) {
	if bytes.ContainsAny(b, escapes) {
		var r rune
		for i, j := 0, 0; i < len(b); i += j {
			r, j = utf8.DecodeRune(b[i:])
			switch {
			case r == '\t':
				*dest = append(*dest, `\t`...)
			case r == '\n':
				*dest = append(*dest, `\n`...)
			case r == '\f':
				*dest = append(*dest, `\f`...)
			case r == '\r':
				*dest = append(*dest, `\r`...)
			case r == ',':
				*dest = append(*dest, `\,`...)
			case r == ' ':
				*dest = append(*dest, `\ `...)
			case r == '=':
				*dest = append(*dest, `\=`...)
			case r <= 1<<7-1:
				*dest = append(*dest, byte(r))
			case r <= 1<<11-1:
				*dest = append(*dest, utf8len2|byte(r>>6), utf8bytex|byte(r)&utf8mask)
			case r <= 1<<16-1:
				*dest = append(*dest, utf8len3|byte(r>>12), utf8bytex|byte(r>>6)&utf8mask, utf8bytex|byte(r)&utf8mask)
			default:
				*dest = append(*dest, utf8len4|byte(r>>18), utf8bytex|byte(r>>12)&utf8mask, utf8bytex|byte(r>>6)&utf8mask, utf8bytex|byte(r)&utf8mask)
			}
		}
		return
	}
	*dest = append(*dest, b...)
}

// Escape a measurement name
func nameEscapeBytes(dest *[]byte, b []byte) {
	if bytes.ContainsAny(b, nameEscapes) {
		var r rune
		for i, j := 0, 0; i < len(b); i += j {
			r, j = utf8.DecodeRune(b[i:])
			switch {
			case r == '\t':
				*dest = append(*dest, `\t`...)
			case r == '\n':
				*dest = append(*dest, `\n`...)
			case r == '\f':
				*dest = append(*dest, `\f`...)
			case r == '\r':
				*dest = append(*dest, `\r`...)
			case r == ',':
				*dest = append(*dest, `\,`...)
			case r == ' ':
				*dest = append(*dest, `\ `...)
			case r == '\\':
				*dest = append(*dest, `\\`...)
			case r <= 1<<7-1:
				*dest = append(*dest, byte(r))
			case r <= 1<<11-1:
				*dest = append(*dest, utf8len2|byte(r>>6), utf8bytex|byte(r)&utf8mask)
			case r <= 1<<16-1:
				*dest = append(*dest, utf8len3|byte(r>>12), utf8bytex|byte(r>>6)&utf8mask, utf8bytex|byte(r)&utf8mask)
			default:
				*dest = append(*dest, utf8len4|byte(r>>18), utf8bytex|byte(r>>12)&utf8mask, utf8bytex|byte(r>>6)&utf8mask, utf8bytex|byte(r)&utf8mask)
			}
		}
		return
	}
	*dest = append(*dest, b...)
}

func stringFieldEscapeBytes(dest *[]byte, b []byte) {
	if bytes.ContainsAny(b, stringFieldEscapes) {
		var r rune
		for i, j := 0, 0; i < len(b); i += j {
			r, j = utf8.DecodeRune(b[i:])
			switch {
			case r == '\t':
				*dest = append(*dest, `\t`...)
			case r == '\n':
				*dest = append(*dest, `\n`...)
			case r == '\f':
				*dest = append(*dest, `\f`...)
			case r == '\r':
				*dest = append(*dest, `\r`...)
			case r == ',':
				*dest = append(*dest, `\,`...)
			case r == ' ':
				*dest = append(*dest, `\ `...)
			case r == '\\':
				*dest = append(*dest, `\\`...)
			case r <= 1<<7-1:
				*dest = append(*dest, byte(r))
			case r <= 1<<11-1:
				*dest = append(*dest, utf8len2|byte(r>>6), utf8bytex|byte(r)&utf8mask)
			case r <= 1<<16-1:
				*dest = append(*dest, utf8len3|byte(r>>12), utf8bytex|byte(r>>6)&utf8mask, utf8bytex|byte(r)&utf8mask)
			default:
				*dest = append(*dest, utf8len4|byte(r>>18), utf8bytex|byte(r>>12)&utf8mask, utf8bytex|byte(r>>6)&utf8mask, utf8bytex|byte(r)&utf8mask)
			}
		}
		return
	}
	*dest = append(*dest, b...)
}
