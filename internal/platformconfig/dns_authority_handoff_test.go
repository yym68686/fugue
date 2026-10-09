package platformconfig_test

import (
	"testing"

	"fugue/internal/platformconfig"
	"fugue/internal/testfixture/celldns"
)

func TestDNSAuthorityHandoffPreservesWholeNodeAnswerBehavior(t *testing.T) {
	req := celldns.TransitionRequest(t)
	compiled := celldns.Compile(t, req)
	if err := platformconfig.ValidateDNSAuthorityHandoff(req.PreviousTrafficPublication.DNS, compiled.DNSArtifact, "dns-a"); err != nil {
		t.Fatal(err)
	}
	if err := platformconfig.ValidateDNSAuthorityHandoff(req.PreviousTrafficPublication.DNS, compiled.DNSArtifact, "dns-other"); err == nil {
		t.Fatal("undeclared physical DNS node accepted")
	}
	for name, change := range map[string]func(*platformconfig.CompileRequest){
		"ttl": func(r *platformconfig.CompileRequest) { r.Policy.DNSAnswerRules[0].TTLSeconds++ },
		"nameserver": func(r *platformconfig.CompileRequest) {
			r.Policy.DNSAuthorities[0].Nameservers = []string{"other.example.test"}
		},
		"ecs": func(r *platformconfig.CompileRequest) {
			r.Policy.DNSAnswerRules[0].PhysicalOrder = nil
			r.Policy.DNSAnswerRules[0].SelectionMode = "global"
			r.Policy.DNSAnswerRules[0].ECSEnabled = true
		},
		"extra record": func(r *platformconfig.CompileRequest) {
			r.Intent.DNS = append(r.Intent.DNS, platformconfig.DNSIntent{Hostname: "extra.example.test", Type: "TXT", Values: []string{"new"}, TTL: 60})
		},
		"selection": func(r *platformconfig.CompileRequest) {
			r.Policy.DNSAnswerRules[0].PhysicalOrder = nil
			r.Policy.DNSAnswerRules[0].SelectionMode = "weighted"
		},
	} {
		t.Run(name, func(t *testing.T) {
			r := celldns.TransitionRequest(t)
			change(&r)
			c := celldns.Compile(t, r)
			if err := platformconfig.ValidateDNSAuthorityHandoff(r.PreviousTrafficPublication.DNS, c.DNSArtifact, "dns-a"); err == nil {
				t.Fatal("changed node DNS behavior authorized handoff")
			}
		})
	}
}
