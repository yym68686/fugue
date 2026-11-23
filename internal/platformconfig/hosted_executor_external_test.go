package platformconfig_test

import (
	"encoding/json"
	"reflect"
	"slices"
	"testing"

	pc "fugue/internal/platformconfig"
	"fugue/internal/routeartifact"
)

func TestCompilerToHostProgramIncludesEveryPhysicalIngressWithoutDNSReadmission(t *testing.T) {
	pc.HostedTransitionFixtureForTest(t, true, func(s pc.HostedAddressSource, p pc.HostedScopeProposal) {
		a, b, err := routeartifact.HostedProgramDigests(s, p)
		if err != nil || a == "" || b == "" || a == b {
			t.Fatal("host phases indistinguishable", err)
		}
		wire, err := json.Marshal(p)
		if err != nil {
			t.Fatal(err)
		}
		var stored pc.HostedScopeProposal
		if err = json.Unmarshal(wire, &stored); err != nil {
			t.Fatal(err)
		}
		x, y, err := routeartifact.HostedProgramDigests(s, stored)
		if err != nil || x != a || y != b {
			t.Fatal("stored draft no longer binds the same reversible phases", err)
		}
		prepared, forward, e := routeartifact.BuildHostedProgram(s, p)
		if e != nil {
			t.Fatal(e)
		}
		for _, n := range p.Physical {
			v, ok := forward.Groups[n.EdgeGroupID]
			if !ok || !reflect.DeepEqual(v.TLSAllowlist, prepared.Groups[n.EdgeGroupID].TLSAllowlist) {
				t.Fatal("held ingress or its original TLS identity was lost")
			}
			paths := map[string]string{}
			for _, r := range v.Routes {
				if r.Hostname == p.Host {
					paths[r.PathPrefix] = r.AppID
				}
			}
			if len(paths) != len(p.After.Routes) {
				t.Fatal("incomplete routing for a physical ingress")
			}
		}
		if !reflect.DeepEqual(prepared.DNS.Views, forward.DNS.Views) {
			t.Fatal("path edit changed eligible DNS views")
		}
		q := p
		q.Authority.LKGID += "different-source"
		if _, _, e := routeartifact.BuildHostedProgram(s, q); e == nil {
			t.Fatal("borrowed authority")
		}
		q = p
		q.Eligible = slices.Clone(p.Physical)
		if _, _, e := routeartifact.BuildHostedProgram(s, q); e == nil {
			t.Fatal("readmitted held ingress")
		}
		q = p
		q.After.Routes = slices.Clone(p.After.Routes)
		q.After.Routes[0].RuntimeID += "different-origin"
		if _, _, e := routeartifact.BuildHostedProgram(s, q); e == nil {
			t.Fatal("DNS proof was not bound to its actual route")
		}
	})
}
