package protocol

import (
	"testing"
)

func BenchmarkStringEscFunc4(b *testing.B) {
	s := "h\tello ⛵ \t \t"
	for i := 0; i < b.N; i++ {
		b := escape(s)
		_ = b
	}
}

func BenchmarkEscFunc4(b *testing.B) {
	s := []byte("h\tello ⛵ \t \t")
	for i := 0; i < b.N; i++ {
		b := escapeBytes(s)
		resturnToEscaperPool(b)
	}
}

func TestBytesEscape(t *testing.T) {
	cases := []struct {
		// we use strings in test because its easier to read and write them for humans
		name string
		arg  string
		want string
	}{
		{
			name: "sailboat",
			arg:  `⛵`,
			want: `⛵`,
		},
		{
			name: "sentence",
			arg:  `hello I like to ⛵but do not like ☠`,
			want: `hello\ I\ like\ to\ ⛵but\ do\ not\ like\ ☠`,
		},
		{
			name: "escapes",
			arg:  "\t\n\f\r ,=",
			want: `\t\n\f\r\ \,\=`,
		},
		{
			name: "nameEscapes",
			arg:  "\t\n\f\r ,",
			want: `\t\n\f\r\ \,`,
		},
		{
			name: "stringFieldEscapes",
			arg:  "\t\n\f\r\\\"",
			want: `\t\n\f\r\"`,
		},
	}
	for _, x := range cases {
		got := string(escapeBytes([]byte(x.arg)))
		if got != string(x.want) {
			t.Fatalf("did not escape %s properly, expected %s got %s", x.name, x.want, got)
		}
	}
}
