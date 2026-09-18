package dnsserver

import (
	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"github.com/miekg/dns"
	"net"
	"testing"
)

func TestDNSClientPacketHintsUseSignedPolicy(t *testing.T) {
	rules := []platformconfig.DNSClientRule{{CIDR: "192.0.2.0/24", Country: "bb", Region: "region-b", ASN: "64500"}, {CIDR: "2001:db8::/32", Country: "bb"}, {CIDR: "198.51.100.0/24", Country: "aa"}}
	matcher, err := platformconfig.NewDNSClientMatcher(rules)
	if err != nil {
		t.Fatal(err)
	}
	view, plan, policy, facts, now := queryExecutionFixture()
	records, err := materializeDNSQueries(view, &plan, &policy, facts, now)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name, ip, remote, country string
		family                    uint16
		bits, scope               uint8
		duplicate                 bool
	}{
		{name: "ecs IPv4", ip: "192.0.2.222", family: 1, bits: 24, country: "bb"},
		{name: "ecs IPv6", ip: "2001:db8::99", family: 2, bits: 48, country: "bb"},
		{name: "resolver", remote: "198.51.100.2:53000", country: "aa"},
		{name: "mask invalid", ip: "192.0.2.2", family: 1, bits: 33, remote: "198.51.100.2:53000", country: "aa"},
		{name: "family invalid", ip: "192.0.2.2", family: 2, bits: 24, remote: "198.51.100.2:53000", country: "aa"},
		{name: "response scope", ip: "192.0.2.2", family: 1, bits: 24, scope: 1, country: ""},
		{name: "duplicate", ip: "192.0.2.2", family: 1, bits: 24, duplicate: true, country: ""},
		{name: "masked bits", ip: "192.0.2.2", family: 1, bits: 0, country: ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			packet := new(dns.Msg)
			packet.SetQuestion("app.example.test.", dns.TypeA)
			if test.ip != "" {
				packet.SetEdns0(1232, false)
				ecs := &dns.EDNS0_SUBNET{Code: dns.EDNS0SUBNET, Family: test.family, SourceNetmask: test.bits, SourceScope: test.scope, Address: net.ParseIP(test.ip)}
				packet.IsEdns0().Option = []dns.EDNS0{ecs}
				if test.duplicate {
					packet.IsEdns0().Option = append(packet.IsEdns0().Option, ecs)
				}
			}
			hint := platformDNSHintForQuery(matcher, packet, test.remote)
			if hint.Country != test.country {
				t.Fatalf("wrong client hint: %+v", hint)
			}
			if test.country == "bb" {
				answers, err := executeDNSQueryRecord(records[0], hint, now)
				if err != nil || len(answers) != 1 || answers[0].(*dns.A).A.String() != "9.9.9.9" {
					t.Fatal("signed client mapping not used", answers, err)
				}
			}
		})
	}
	// The shadow entry point reads the artifact policy, regardless of conflicting
	// ambient configuration, and includes the mapping digest in its receipt.
	service := &Service{Config: config.DNSConfig{DNSNodeID: view.NodeID, EdgeGroupID: view.EdgeGroupID, GeoIPOverrides: []config.DNSGeoIPOverride{{CIDR: "0.0.0.0/0", Country: "zz"}}}}
	a := model.PlatformConsumerAssignment{ReleaseSetID: "release", ExpectedConsumerSetID: "expected", FencingToken: 7}
	client := platformconfig.DNSClientPolicy{NodeID: view.NodeID, Rules: rules}
	c := dnsPlatformCandidate{Artifact: model.PlatformArtifact{ID: "artifact", ContentHash: "digest", Content: map[string]any{"query_views": []platformconfig.DNSQueryView{view}, "readiness_plan": plan, "policy": platformconfig.PolicySnapshot{DNSReadiness: &policy, DNSClientPolicies: []platformconfig.DNSClientPolicy{client}}}}}
	readiness := dnsReadinessReceipt{ArtifactID: "artifact", ArtifactDigest: "digest", ReleaseSetID: a.ReleaseSetID, ExpectedConsumerSetID: a.ExpectedConsumerSetID, FencingToken: a.FencingToken, NodeID: view.NodeID, Facts: facts}
	receipt, err := service.evaluatePlatformDNSQueries(c, a, &readiness)
	if err != nil {
		t.Fatal(err)
	}
	digest, _ := platformconfig.Digest(client)
	if receipt.Status.ClientPolicyDigest != digest || receipt.Status.ClientPolicyRules != len(rules) || receipt.Status.Serving {
		t.Fatal("client policy receipt incorrect")
	}
}
