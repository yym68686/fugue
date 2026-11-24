package platformconfig_test

import (
	"encoding/json"
	"reflect"
	"slices"
	"testing"

	"fugue/internal/cellpublication"
	pc "fugue/internal/platformconfig"
	"fugue/internal/testfixture/celldns"
)

func TestCellInheritsReservedAddressOnlyFromExactGlobalFull(t *testing.T) {
	t.Run("managed root and alias", func(t *testing.T) { testCellInheritsReservedAddress(t, false) })
	t.Run("independent external target", func(t *testing.T) { testCellInheritsReservedAddress(t, true) })
}

func testCellInheritsReservedAddress(t *testing.T, shared bool) {
	t.Helper()
	cell, global := celldns.TransitionRequestWithSource(t)
	const alias = "shared.example.test"
	for _, req := range []*pc.CompileRequest{&cell, &global} {
		req.Intent.DNS = slices.Clone(req.Intent.DNS)
		req.Intent.DNS[0].RecordKind = "hosted"
		d := req.Intent.DNS[0]
		d.Route = &pc.DNSRouteIntent{Hostnames: []string{d.Hostname}, DNSApplicationIntent: *d.Application}
		d.Hostname, d.Type, d.RecordKind, d.Values, d.Application = alias, "FUGUE_ROUTE", "custom-domain-target", []string{}, nil
		req.Intent.DNS = append(req.Intent.DNS, d)
		rule := req.Policy.DNSAnswerRules[0]
		rule.Hostname = alias
		req.Policy.DNSAnswerRules = append(slices.Clone(req.Policy.DNSAnswerRules), rule)
		obs := req.RuntimeSnapshot.DNSSelections[0]
		obs.Hostname = alias
		req.RuntimeSnapshot.DNSSelections = append(slices.Clone(req.RuntimeSnapshot.DNSSelections), obs)
		if shared {
			req.Intent.DNS = req.Intent.DNS[1:]
			req.Policy.DNSAnswerRules = req.Policy.DNSAnswerRules[1:]
			req.RuntimeSnapshot.DNSSelections = req.RuntimeSnapshot.DNSSelections[1:]
		}
	}
	base, e := pc.Compile(global)
	if e != nil {
		t.Fatal(e)
	}
	read := func(c pc.CompileResult) pc.DNSArtifactPayload {
		var p pc.DNSArtifactPayload
		b, e := json.Marshal(c.DNSArtifact.Content)
		if e != nil {
			t.Fatal(e)
		}
		if e = json.Unmarshal(b, &p); e != nil {
			t.Fatal(e)
		}
		return p
	}
	old := read(base)
	const addr = "9.9.9.9"
	for i := range old.Records {
		old.Records[i].Values = append(slices.Clone(old.Records[i].Values), addr)
	}
	byID := map[string]string{}
	for _, p := range slices.Clone(old.ReadinessPlan.Probes) {
		if p.EdgeID != old.ReadinessPlan.Records[0].Targets[0].EdgeID {
			continue
		}
		id := p.ID
		p.EdgeID, p.Address = "held-member", addr
		p.ID, e = pc.DNSReadinessProbeID(p)
		if e != nil {
			t.Fatal(e)
		}
		byID[id] = p.ID
		old.ReadinessPlan.Probes = append(old.ReadinessPlan.Probes, p)
	}
	var held pc.DNSReadinessTarget
	for i := range old.ReadinessPlan.Records {
		rec := &old.ReadinessPlan.Records[i]
		held = rec.Targets[0]
		held.EdgeID, held.Address = "held-member", addr
		held.ProbeIDs = slices.Clone(held.ProbeIDs)
		for j, id := range held.ProbeIDs {
			held.ProbeIDs[j] = byID[id]
		}
		rec.Targets = append(slices.Clone(rec.Targets), held)
	}
	hold := pc.HostedAddressHold{EdgeID: held.EdgeID, EdgeGroupID: held.EdgeGroupID, Address: addr}
	source := pc.HostedAddressSource{Publication: cell.PreviousTrafficPublication.Reference, PriorIntent: global.Intent, Prior: old, EffectiveHolds: []pc.HostedAddressHold{hold}}
	var globalResult pc.CompileResult
	if shared {
		history := source.Publication
		history.ReleaseID, history.ReleaseSetID = "historic-release", "historic-parent"
		source.Publication.FencingToken++
		source.Prior, source.PriorHolds = read(base), source.EffectiveHolds
		source.PriorRuntime = base.InputSnapshot
		source.PriorDNSSelections = base.InputSnapshot.DNSSelections
		source.PriorRoute, source.PriorTLS = &base.RouteArtifact, &base.TLSArtifact
		globalResult, e = pc.CompileHostedDeclarationRestore(global, source, pc.HostedDeclarationRestorePlan{History: history, Target: alias, Family: "A", Held: hold}, old)
	} else {
		globalResult, e = pc.CompileHostedAliasCellSourceForTest(global, source)
	}
	if e != nil {
		t.Fatal(e)
	}
	p := cell.PreviousTrafficPublication
	p.Route = celldns.Sign(t, globalResult.RouteArtifact, "source-route", 2)
	p.TLS = celldns.Sign(t, globalResult.TLSArtifact, "source-tls", 2)
	p.DNS = celldns.Sign(t, globalResult.DNSArtifact, "source-dns", 2)
	parent := pc.BuildReleaseSetArtifact(globalResult.ReleaseSet, []string{p.Route.ID, p.DNS.ID, p.TLS.ID}, global.CreatedAt)
	for _, k := range []string{pc.HostedAddressReservationsDigestMetadata, pc.HostedAddressSourceMetadata} {
		parent.Metadata[k] = globalResult.ReleaseArtifact.Metadata[k]
	}
	p.Parent = celldns.Sign(t, parent, "source-parent", 2)
	p.Reference = pc.PreviousTrafficPublicationReference{ReleaseSetID: p.Parent.ID, ReleaseSetDigest: p.Parent.ContentHash, ReleaseID: "source-full", FencingToken: source.Publication.FencingToken + 1, RouteArtifactID: p.Route.ID, RouteArtifactDigest: p.Route.ContentHash, TLSArtifactID: p.TLS.ID, TLSArtifactDigest: p.TLS.ContentHash, DNSArtifactID: p.DNS.ID, DNSArtifactDigest: p.DNS.ContentHash}
	cell.Intent.RouteAuthorityTransition.PreviousPublication = p.Reference
	cell.Intent.RouteAuthorityTransition.ReferenceMode = pc.DNSRetainedGlobalReferenceV1
	cell.CellRoutePublications = nil
	cell.Intent.CellRoutePublications = nil
	result := celldns.Compile(t, cell)
	out := read(result)
	expected := read(globalResult)
	if len(out.Records) != len(expected.Records) || len(out.Records) != len(cell.Intent.DNS) {
		t.Fatal("missing hosted root or generated alias")
	}
	for i, r := range out.Records {
		if r.Hostname != expected.Records[i].Hostname || !reflect.DeepEqual(r.Values, expected.Records[i].Values) || !slices.Contains(r.Values, addr) {
			t.Fatal("cell rebuilt the physical record from the eligible subset", r.Hostname)
		}
	}
	for _, r := range out.ReadinessPlan.Records {
		for _, v := range r.Targets {
			if v.Address == addr {
				t.Fatal("cell re-enabled held member")
			}
		}
	}
	for _, v := range out.QueryViews {
		for _, r := range v.Records {
			if slices.Contains(r.Values, addr) {
				t.Fatal("cell offered held address")
			}
		}
	}
	if _, err := cellpublication.VerifyDNSArtifact(result.DNSArtifact, celldns.Keys()); err != nil {
		t.Fatal(err)
	}
	if e = pc.ValidateDNSAuthorityHandoff(p.DNS, result.DNSArtifact, "dns-a"); e != nil {
		t.Fatal(e)
	}
	for _, k := range []string{pc.HostedAddressSourceMetadata, pc.HostedAddressReservationsDigestMetadata} {
		original := p.Parent.Metadata[k]
		delete(p.Parent.Metadata, k)
		p.Parent = celldns.Sign(t, p.Parent, p.Parent.ID, 2)
		if _, e = pc.Compile(cell); e == nil {
			t.Fatal("one-sided reservation marker accepted", k)
		}
		p.Parent.Metadata[k] = original
		p.Parent = celldns.Sign(t, p.Parent, p.Parent.ID, 2)
	}
	cell.RuntimeSnapshot.DNSEdgeEndpoints[0].A = []string{addr}
	if _, e = pc.Compile(cell); e == nil {
		t.Fatal("cell gave held IP to a different member")
	}
}
