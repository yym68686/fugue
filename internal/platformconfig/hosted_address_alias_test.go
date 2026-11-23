package platformconfig

import (
	"slices"
	"testing"
)

// For the external cell compiler test, supply the same declaration comparison
// used by the trusted global producer; this is not a production input.
func CompileHostedAliasCellSourceForTest(req CompileRequest, source HostedAddressSource) (CompileResult, error) {
	source.stableAliases = stableHostedAliasDeclarations(source.PriorIntent, req.Intent)
	return CompileWithHostedAddressSource(req, source)
}

func TestHostedAliasAddressRequiresExactSameOwnerAndProbeGraph(t *testing.T) {
	const parent = "owner.example.test"
	const name = "alias.example.test"
	key := hostedRecordKey{name, "A"}
	for _, tc := range []string{"exact", "foreign-app", "foreign-tenant", "foreign-address", "extra-dependency", "missing-dependency", "missing-edge", "absent-alias", "different-family", "duplicate-id", "other-kind", "retarget", "different-ttl", "mixed-hosts"} {
		t.Run(tc, func(t *testing.T) {
			r := DNSIntent{Hostname: parent, Type: "A", RecordKind: "hosted", AppID: "owner", TenantID: "tenant", Values: []string{"1.1.1.1"}, TTL: 60}
			a := r
			a.Hostname = name
			a.RecordKind = "custom-domain-target"
			p := DNSReadinessProbe{ID: "p", EdgeID: "a", EdgeGroupID: "edge-group-a", Address: r.Values[0], Hostname: parent, Path: "/"}
			q := p
			q.ID = "q"
			q.Path = "/child"
			x := p
			x.ID = "x"
			x.Hostname = "else.example.test"
			target := DNSReadinessTarget{EdgeID: p.EdgeID, EdgeGroupID: p.EdgeGroupID, Address: p.Address, Family: "A", ProbeIDs: []string{"p", "q"}}
			other := target
			other.ProbeIDs = []string{"q", "p"}
			symbolic := a
			symbolic.Type = "FUGUE_ROUTE"
			symbolic.Values = nil
			symbolic.Route = &DNSRouteIntent{Hostnames: []string{parent}}
			next := symbolic
			cfg := *symbolic.Route
			next.Route = &cfg
			switch tc {
			case "foreign-app":
				a.AppID = "other"
			case "foreign-tenant":
				a.TenantID = "other"
			case "foreign-address":
				a.Values = []string{"8.8.8.8"}
			case "extra-dependency":
				other.ProbeIDs = append(other.ProbeIDs, "x")
			case "missing-dependency":
				other.ProbeIDs = other.ProbeIDs[:1]
			case "missing-edge":
				other.EdgeID = "different"
			case "absent-alias":
				a.Hostname = "removed.example.test"
			case "different-family":
				a.Type = "AAAA"
			case "duplicate-id":
				other.ProbeIDs = []string{"p", "p", "q"}
			case "other-kind":
				a.RecordKind = "platform"
			case "retarget":
				cfg.Hostnames = []string{x.Hostname}
			case "different-ttl":
				next.TTL = 61
			case "mixed-hosts":
				cfg.Hostnames = []string{parent, x.Hostname}
			}
			plan := &DNSReadinessPlan{Probes: []DNSReadinessProbe{p, q, x}, Records: []DNSReadinessRecord{{Hostname: parent, Targets: []DNSReadinessTarget{target}}, {Hostname: name, Targets: []DNSReadinessTarget{other}}}}
			got := hostedAliasParents([]DNSIntent{r, a}, plan)[key]
			decl := stableHostedAliasDeclarations(PlatformIntent{DNS: []DNSIntent{symbolic}}, PlatformIntent{DNS: []DNSIntent{next}})
			if (got.host == parent && decl[name] == parent) != (tc == "exact") {
				t.Fatal("incorrect reverse dependency", got, decl)
			}
			if tc == "exact" && !slices.Equal(plan.Records[1].Targets[0].ProbeIDs, []string{"q", "p"}) {
				t.Fatal("changed original requirements")
			}
		})
	}
}
