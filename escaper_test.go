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
	dest := make([]byte, 32)
	for i := 0; i < b.N; i++ {
		escapeBytes(&dest, s)
		dest = dest[:0]
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
	got := []byte{}
	for _, x := range cases {
		escapeBytes(&got, []byte(x.arg))
		if string(got) != string(x.want) {
			t.Fatalf("did not escape %s properly, expected %s got %s", x.name, x.want, got)
		}
		got = got[:0]
	}
}
