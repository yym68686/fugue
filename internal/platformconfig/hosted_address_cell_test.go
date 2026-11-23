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
	cell, global := celldns.TransitionRequestWithSource(t)
	global.Intent.DNS[0].RecordKind = "hosted"
	cell.Intent.DNS[0].RecordKind = "hosted"
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
	old.Records[0].Values = append(slices.Clone(old.Records[0].Values), addr)
	rec := &old.ReadinessPlan.Records[0]
	held := rec.Targets[0]
	held.EdgeID = "held-member"
	held.Address = addr
	rec.Targets = append(slices.Clone(rec.Targets), held)
	globalResult, e := pc.CompileWithHostedAddressSource(global, pc.HostedAddressSource{Publication: cell.PreviousTrafficPublication.Reference, Prior: old, EffectiveHolds: []pc.HostedAddressHold{{EdgeID: held.EdgeID, EdgeGroupID: held.EdgeGroupID, Address: addr}}})
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
	p.Reference = pc.PreviousTrafficPublicationReference{ReleaseSetID: p.Parent.ID, ReleaseSetDigest: p.Parent.ContentHash, ReleaseID: "source-full", FencingToken: 2, RouteArtifactID: p.Route.ID, RouteArtifactDigest: p.Route.ContentHash, TLSArtifactID: p.TLS.ID, TLSArtifactDigest: p.TLS.ContentHash, DNSArtifactID: p.DNS.ID, DNSArtifactDigest: p.DNS.ContentHash}
	cell.Intent.RouteAuthorityTransition.PreviousPublication = p.Reference
	cell.Intent.RouteAuthorityTransition.ReferenceMode = pc.DNSRetainedGlobalReferenceV1
	cell.CellRoutePublications = nil
	cell.Intent.CellRoutePublications = nil
	result := celldns.Compile(t, cell)
	out := read(result)
	expected := read(globalResult)
	if !reflect.DeepEqual(out.Records[0].Values, expected.Records[0].Values) || !slices.Contains(out.Records[0].Values, addr) {
		t.Fatal("cell rebuilt the physical record from the eligible subset")
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
