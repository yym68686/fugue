package dnsserver

import (
	"net"
	"testing"

	"fugue/internal/platformconfig"
	"github.com/miekg/dns"
)

func TestDecisionECSCoverageDoesNotTreatResolverScopeAsClient(t *testing.T) {
	state, now := decisionTestState(t)
	var err error
	state.matcher, err = platformconfig.NewDNSClientMatcher([]platformconfig.DNSClientRule{
		{CIDR: "198.51.100.0/24", ASN: "64496"},
		{CIDR: "203.0.113.0/24", ASN: "64497"},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name       string
		ecs        *dns.EDNS0_SUBNET
		seen, hint bool
	}{
		{name: "resolver only"},
		{name: "mapped ECS", ecs: &dns.EDNS0_SUBNET{Code: dns.EDNS0SUBNET, Family: 1, SourceNetmask: 24, Address: net.ParseIP("203.0.113.1")}, seen: true, hint: true},
		{name: "unmapped ECS", ecs: &dns.EDNS0_SUBNET{Code: dns.EDNS0SUBNET, Family: 1, SourceNetmask: 24, Address: net.ParseIP("192.0.2.1")}, seen: true},
		{name: "zero ECS", ecs: &dns.EDNS0_SUBNET{Code: dns.EDNS0SUBNET, Family: 1, SourceNetmask: 0, Address: net.IPv4zero}},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := new(dns.Msg).SetQuestion("target.example.test.", dns.TypeA)
			if test.ecs != nil {
				request.SetEdns0(1232, false).IsEdns0().Option = append(request.IsEdns0().Option, test.ecs)
			}
			receipt := captureDecision(t, state, request, "198.51.100.1:53", now)
			ecs, hint, valid := DecisionECSCoverage(receipt)
			if !valid || ecs != test.seen || hint != test.hint {
				t.Fatalf("ecs=%t hint=%t valid=%t", ecs, hint, valid)
			}
			receipt.EvidenceDigest = "invalid"
			if _, _, valid := DecisionECSCoverage(receipt); valid {
				t.Fatal("unverified receipt affected coverage")
			}
		})
	}
}
