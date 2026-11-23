package platformconfig

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestHostedAddressCompileKeepsDefaultAndScopedAdmissionIndependent(t *testing.T) {
	req := consumerPlacementFixture()
	req.Intent.DNS[0].RecordKind = "hosted"
	req.Policy.MinimumHealthyEdges = 1
	req.Intent.Scope, req.Policy.Scope = GlobalScopeKey, GlobalScopeKey
	full, err := Compile(req)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := func(c CompileResult) DNSArtifactPayload {
		t.Helper()
		var p DNSArtifactPayload
		b, e := json.Marshal(c.DNSArtifact.Content)
		if e != nil {
			t.Fatal(e)
		}
		if e = json.Unmarshal(b, &p); e != nil {
			t.Fatal(e)
		}
		return p
	}
	p := snapshot(full)
	req.RuntimeSnapshot.DNSEdgeEndpoints = req.RuntimeSnapshot.DNSEdgeEndpoints[:1]
	s := &req.RuntimeSnapshot.DNSSelections[0]
	s.Candidates = s.Candidates[:1]
	s.SelectedEdgeGroupID = s.Candidates[0].EdgeGroupID
	s.ScopedCandidates[0].Candidates = s.ScopedCandidates[0].Candidates[:1]
	eligible, err := Compile(req)
	if err != nil {
		t.Fatal(err)
	}
	d := "sha256:" + strings.Repeat("b", 64)
	pin := PreviousTrafficPublicationReference{ReleaseSetID: "parent-one", ReleaseSetDigest: d, ReleaseID: "full-one", FencingToken: 1, RouteArtifactID: "routes-one", RouteArtifactDigest: d, TLSArtifactID: "tls-one", TLSArtifactDigest: d, DNSArtifactID: "dns-one", DNSArtifactDigest: d}
	source := HostedAddressSource{Publication: pin, Prior: p, EffectiveHolds: []HostedAddressHold{{"edge-b", "edge-group-b", "93.184.216.35"}}}
	got, err := CompileWithHostedAddressSource(req, source)
	if err != nil {
		t.Fatal(err)
	}
	out, eff := snapshot(got), snapshot(eligible)
	keys := map[hostedRecordKey]bool{{p.Records[0].Hostname, p.Records[0].Type}: true}
	for _, test := range []string{"held static fallback", "no dynamic policy", "omitted selector", "static selector"} {
		t.Run(test, func(t *testing.T) {
			v := snapshot(eligible)
			for i := range v.QueryViews {
				for j := range v.QueryViews[i].Records {
					r := &v.QueryViews[i].Records[j]
					if !keys[hostedRecordKey{r.Name, r.Type}] {
						continue
					}
					switch test {
					case "held static fallback":
						r.Values = append(r.Values, source.EffectiveHolds[0].Address)
					case "static selector":
						r.Candidates = nil
					}
				}
			}
			if test == "no dynamic policy" {
				v.Policy.DNSQueryPolicy = nil
			}
			if test == "omitted selector" {
				v.QueryViews = nil
			}
			if e := checkHostedDynamicSelectors(keys, v.QueryViews, v.ReadinessPlan, v.Policy); e == nil {
				t.Fatal("unsafe fallback admitted")
			}
		})
	}
	if len(got.InputSnapshot.HostedAddressReservations) != 1 || !reflect.DeepEqual(out.Records, p.Records) || !reflect.DeepEqual(out.QueryViews, eff.QueryViews) || !reflect.DeepEqual(out.ReadinessPlan, eff.ReadinessPlan) {
		t.Fatal("did not separate physical declaration from the effective selector")
	}
	req.RuntimeSnapshot = got.InputSnapshot
	if _, err = Compile(req); err == nil {
		t.Fatal("unverified public compiler input was accepted")
	}
	replay, err := CompileWithHostedAddressSource(req, source)
	if err != nil || replay.Lineage != got.Lineage {
		t.Fatal("exact replay changed lineage", err)
	}
	req.RuntimeSnapshot.HostedAddressReservations[0].Source.FencingToken++
	if _, err = CompileWithHostedAddressSource(req, source); err == nil {
		t.Fatal("foreign predecessor fence accepted")
	}
}
