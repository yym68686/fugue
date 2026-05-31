package proxyproto

import (
	"net"
	"testing"
)

func TestHeaderV1IPv4(t *testing.T) {
	got := HeaderV1(
		&net.TCPAddr{IP: net.ParseIP("203.0.113.10"), Port: 54321},
		&net.TCPAddr{IP: net.ParseIP("10.42.2.1"), Port: 443},
	)
	want := "PROXY TCP4 203.0.113.10 10.42.2.1 54321 443\r\n"
	if got != want {
		t.Fatalf("HeaderV1() = %q, want %q", got, want)
	}
}

func TestHeaderV1IPv6(t *testing.T) {
	got := HeaderV1(
		&net.TCPAddr{IP: net.ParseIP("2001:db8::10"), Port: 54321},
		&net.TCPAddr{IP: net.ParseIP("fd00::1"), Port: 443},
	)
	want := "PROXY TCP6 2001:db8::10 fd00::1 54321 443\r\n"
	if got != want {
		t.Fatalf("HeaderV1() = %q, want %q", got, want)
	}
}

func TestHeaderV1Unknown(t *testing.T) {
	for _, tt := range []struct {
		name        string
		source      net.Addr
		destination net.Addr
	}{
		{name: "nil source", destination: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 443}},
		{name: "nil destination", source: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 54321}},
		{
			name:        "mismatched families",
			source:      &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 54321},
			destination: &net.TCPAddr{IP: net.ParseIP("fd00::1"), Port: 443},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := HeaderV1(tt.source, tt.destination); got != unknownV1Header {
				t.Fatalf("HeaderV1() = %q, want %q", got, unknownV1Header)
			}
		})
	}
}
