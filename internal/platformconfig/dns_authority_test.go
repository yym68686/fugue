package platformconfig

import (
	"reflect"
	"testing"
)

func TestDNSAuthorityPolicyIsVersionedBoundAndDeterministic(t *testing.T) {
	r := dnsQueryFixture()
	r.Policy.DNSAuthorities = []DNSAuthorityPolicy{{NodeID: "dns-a", Zone: "example.test", Nameservers: []string{"ns.example.test"}, TTLSeconds: 60, RefreshSeconds: 300, RetrySeconds: 60, ExpireSeconds: 3600}}
	rebindPlacement(&r)
	first, err := Compile(r)
	if err != nil {
		t.Fatal(err)
	}
	r.Policy.DNSAuthorities[0].TTLSeconds = 30
	rebindPlacement(&r)
	changed, err := Compile(r)
	if err != nil {
		t.Fatal(err)
	}
	if first.Lineage.PolicyDigest == changed.Lineage.PolicyDigest || first.Lineage.IntentDigest != changed.Lineage.IntentDigest {
		t.Fatal("authority policy not separately versioned")
	}
	replay, err := Compile(r)
	if err != nil || !reflect.DeepEqual(changed.DNSArtifact.Content, replay.DNSArtifact.Content) {
		t.Fatal("authority replay differs", err)
	}
	for _, mode := range []string{"missing-zone", "foreign-node", "duplicate", "bad-nameserver", "ttl", "expire"} {
		t.Run(mode, func(t *testing.T) {
			copy := r
			copy.Policy = NormalizePolicySnapshot(r.Policy)
			switch mode {
			case "missing-zone":
				copy.Policy.DNSAuthorities[0].Zone = "other.test"
			case "foreign-node":
				copy.Policy.DNSAuthorities[0].NodeID = "foreign"
			case "duplicate":
				copy.Policy.DNSAuthorities = append(copy.Policy.DNSAuthorities, copy.Policy.DNSAuthorities[0])
			case "bad-nameserver":
				copy.Policy.DNSAuthorities[0].Nameservers = []string{"*.example.test"}
			case "ttl":
				copy.Policy.DNSAuthorities[0].TTLSeconds = 0
			case "expire":
				copy.Policy.DNSAuthorities[0].ExpireSeconds = 1
			}
			if _, err := Compile(copy); err == nil {
				t.Fatal("invalid authority policy compiled")
			}
		})
	}
}
