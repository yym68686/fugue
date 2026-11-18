package platformconfig

import (
	"net/netip"
	"testing"
)

func TestLookupPrefixASNRequiresWholeUnambiguousPrefix(t *testing.T) {
	matcher, err := NewDNSClientMatcher([]DNSClientRule{
		{CIDR: "198.51.100.128/25", ASN: "AS64497"},
		{CIDR: "198.51.100.0/24", ASN: "64496"},
		{CIDR: "2001:db8:1::/48", ASN: "64498"},
		{CIDR: "0.0.0.0/0", ASN: "64499"},
	})
	if err != nil {
		t.Fatal(err)
	}
	for prefix, want := range map[string]uint32{
		"198.51.100.0/24": 0, "198.51.100.0/25": 64496, "198.51.100.128/25": 64497,
		"203.0.113.0/24": 0, "0.0.0.0/0": 0, "2001:db8::/32": 0, "2001:db8:1::/48": 64498,
	} {
		if got := matcher.LookupPrefixASN(netip.MustParsePrefix(prefix)); got != want {
			t.Errorf("%s: got %d, want %d", prefix, got, want)
		}
	}
}
