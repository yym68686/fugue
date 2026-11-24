package platformconfig

import (
	"maps"
	"reflect"
	"slices"
	"testing"

	"fugue/internal/model"
)

func TestCompileHostedDeclarationRestoreBindsSeparateHistoryAndExcludesRoutes(t *testing.T) {
	historical, _ := externalFourRouteEditWithThirdEndpoint(t, true)
	source, edit := externalFourRouteEdit(t)
	host := edit.Intent.Routes[0].Hostname
	plan := HostedDeclarationRestorePlan{
		History: historicalRestoreReference(historical.Publication),
		Target:  edit.Intent.DNS[0].Hostname,
		Family:  "A",
		Held:    HostedAddressHold{"edge-c", "edge-group-b", "93.184.216.36"},
	}
	source.Publication.FencingToken++
	source.Publication.ReleaseID, source.Publication.ReleaseSetID = "selected-full", "selected-parent"
	source.PriorHolds, source.EffectiveHolds = []HostedAddressHold{plan.Held}, []HostedAddressHold{plan.Held}
	baseline, err := stageBaselineExternalPaths(source.PriorIntent, edit, []string{host})
	if err != nil {
		t.Fatal(err)
	}
	c, err := CompileHostedDeclarationRestore(baseline, source, plan, historical.Prior)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range []model.PlatformArtifact{c.DNSArtifact, c.ReleaseArtifact} {
		got, err := HostedDeclarationRestoreAuthority(a)
		if err != nil || got == nil || *got != plan {
			t.Fatalf("missing pinned restore plan on %s: %+v %v", a.ArtifactKind, got, err)
		}
		a.Metadata = maps.Clone(a.Metadata)
		a.Metadata[HostedDeclarationRestoreMetadata] += " "
		if _, err := HostedDeclarationRestoreAuthority(a); err == nil {
			t.Fatal("noncanonical signed restore claim")
		}
	}
	if len(c.InputSnapshot.HostedAddressReservations) != 1 || c.InputSnapshot.HostedAddressReservations[0].Source != source.Publication {
		t.Fatal("restored custody mistakenly depends on historical serving authority")
	}
	if _, err = CompileHostedDeclarationRestore(edit, source, plan, historical.Prior); err == nil {
		t.Fatal("restore smuggled the host path edit")
	}
}

func historicalRestoreReference(ref PreviousTrafficPublicationReference) PreviousTrafficPublicationReference {
	ref.ReleaseSetID, ref.ReleaseID = "historical-parent", "historical-full"
	return ref
}

func TestRestoreExternalHeldDeclarationChangesOnlyOneRR(t *testing.T) {
	old, edit := externalFourRouteEditWithThirdEndpoint(t, true)
	current, _ := externalFourRouteEdit(t)
	held := HostedAddressHold{"edge-c", "edge-group-b", "93.184.216.36"}
	name := edit.Intent.DNS[0].Hostname
	holds := []HostedAddressHold{held}
	got, err := RestoreExternalHeldDeclaration(current.Prior, old.Prior, name, "A", held, holds, holds)
	if err != nil {
		t.Fatal(err)
	}
	unchanged := got
	unchanged.Records = slices.Clone(got.Records)
	changed := 0
	for i, r := range unchanged.Records {
		if r.Hostname == name && r.Type == "A" && len(r.Values) == 3 && slices.Contains(r.Values, held.Address) {
			changed++
			unchanged.Records[i].Values = slices.DeleteFunc(slices.Clone(r.Values), func(ip string) bool { return ip == held.Address })
		}
	}
	if changed != 1 || !reflect.DeepEqual(unchanged, current.Prior) {
		t.Fatal("restoration changed query views, probes, path bindings or another DNS declaration")
	}
	current.Prior, current.PriorHolds, current.EffectiveHolds = got, holds, holds
	_, pendingEdit := externalFourRouteEdit(t)
	stage, capture, err := CompileHostedPathWithCapture(pendingEdit, current)
	if err != nil {
		t.Fatal(err)
	}
	offers, err := BuildHostedScopeProposals(current, stage, capture)
	if err != nil || len(offers) != 1 || len(offers[0].Physical) != 3 || len(offers[0].Eligible) != 2 {
		t.Fatalf("restored declaration did not survive independent compilation: %v", err)
	}
}

func TestRestoreExternalHeldDeclarationRejectsUnboundChanges(t *testing.T) {
	old, edit := externalFourRouteEditWithThirdEndpoint(t, true)
	current, _ := externalFourRouteEdit(t)
	held := HostedAddressHold{"edge-c", "edge-group-b", "93.184.216.36"}
	name := edit.Intent.DNS[0].Hostname
	for _, tc := range []struct {
		name string
		edit func(*DNSArtifactPayload, *DNSArtifactPayload, *[]HostedAddressHold, *[]HostedAddressHold)
	}{
		{"not currently held", func(_, _ *DNSArtifactPayload, _, live *[]HostedAddressHold) { *live = nil }},
		{"not previously held", func(_, _ *DNSArtifactPayload, prior, _ *[]HostedAddressHold) { *prior = nil }},
		{"different owner", func(_, historical *DNSArtifactPayload, _, _ *[]HostedAddressHold) {
			for i := range historical.Records {
				if r := &historical.Records[i]; r.Hostname == name && r.Type == "A" {
					r.TenantID = "different-tenant"
				}
			}
		}},
		{"changed TTL", func(_, historical *DNSArtifactPayload, _, _ *[]HostedAddressHold) {
			for i := range historical.Records {
				if r := &historical.Records[i]; r.Hostname == name && r.Type == "A" {
					r.TTL++
				}
			}
		}},
		{"history did not register the held address", func(_, historical *DNSArtifactPayload, _, _ *[]HostedAddressHold) {
			for i := range historical.Records {
				if r := &historical.Records[i]; r.Hostname == name && r.Type == "A" {
					r.Values = slices.Clone(r.Values)
					r.Values[slices.Index(r.Values, held.Address)] = "93.184.216.37"
				}
			}
		}},
		{"fallback can return the held address", func(base, _ *DNSArtifactPayload, _, _ *[]HostedAddressHold) {
			base.QueryViews = slices.Clone(base.QueryViews)
			for i := range base.QueryViews {
				v := &base.QueryViews[i]
				v.Records = slices.Clone(v.Records)
				for j := range v.Records {
					if r := &v.Records[j]; r.Name == name && r.Type == "A" {
						r.Values = append(slices.Clone(r.Values), held.Address)
					}
				}
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base, historical := current.Prior, old.Prior
			historical.Records = slices.Clone(historical.Records)
			prior, live := []HostedAddressHold{held}, []HostedAddressHold{held}
			tc.edit(&base, &historical, &prior, &live)
			if _, err := RestoreExternalHeldDeclaration(base, historical, name, "A", held, prior, live); err == nil {
				t.Fatal("unbound declaration restore was accepted")
			}
		})
	}
}

func TestRestoredExternalDeclarationSurvivesRefreshAndIndependentPathEdit(t *testing.T) {
	history, _ := externalFourRouteEditWithThirdEndpoint(t, true)
	source, pending := externalFourRouteEdit(t)
	host := pending.Intent.Routes[0].Hostname
	target := pending.Intent.DNS[0].Hostname
	held := HostedAddressHold{"edge-c", "edge-group-b", "93.184.216.36"}
	source.PriorHolds, source.EffectiveHolds = []HostedAddressHold{held}, []HostedAddressHold{held}
	restored, err := RestoreExternalHeldDeclaration(source.Prior, history.Prior, target, "A", held, source.PriorHolds, source.EffectiveHolds)
	if err != nil {
		t.Fatal(err)
	}
	custody, err := BindRestoredExternalDeclaration(source, history.Prior, target, "A", held)
	if err != nil || custody.Shared == nil || len(custody.Physical.Targets) != 3 {
		t.Fatalf("missing shared declaration history: %+v %v", custody, err)
	}
	source.Prior = restored
	source.Inherited = []HostedAddressReservation{custody}
	source.Publication.ReleaseID = "restored-full"
	source.Publication.ReleaseSetID = "restored-parent"
	source.Publication.FencingToken++
	baselineReq, err := stageBaselineExternalPaths(source.PriorIntent, pending, []string{host})
	if err != nil {
		t.Fatal(err)
	}
	refresh, err := CompileIndependentManagedBaseline(source, baselineReq)
	if err != nil {
		t.Fatal(err)
	}
	got := refresh.InputSnapshot.HostedAddressReservations
	if len(got) != 1 || got[0].Source != source.Publication || !reflect.DeepEqual(got[0].Record, custody.Record) ||
		!reflect.DeepEqual(got[0].Physical, custody.Physical) || !reflect.DeepEqual(got[0].Shared, custody.Shared) {
		t.Fatalf("plain refresh lost shared custody: %+v", got)
	}
	compiled, capture, err := CompileHostedPathWithCapture(pending, source)
	if err != nil {
		t.Fatal(err)
	}
	p, err := BuildHostedScopeProposals(source, compiled, capture)
	if err != nil || len(p) != 1 || len(p[0].Physical) != 3 || len(p[0].Eligible) != 2 {
		t.Fatalf("independent path publication could not consume the retained RR: %v", err)
	}
	priorDNS, nextDNS := refresh.DNSArtifact, compiled.DNSArtifact
	priorDNS.ScopeKey, nextDNS.ScopeKey = GlobalScopeKey, GlobalScopeKey
	if err = ValidateScopedHostedPublicationTransition(priorDNS, nextDNS); err != nil {
		t.Fatalf("serving admission rejected unaffected declaration: %v", err)
	}
	fresh, err := DecodeDNSArtifact(nextDNS)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range fresh.QueryViews {
		for _, r := range v.Records {
			if r.Name == custody.Record.Hostname && slices.Contains(r.Values, held.Address) {
				t.Fatal("refresh made the held address eligible")
			}
		}
	}
	source.EffectiveHolds = nil
	if _, err = CompileIndependentManagedBaseline(source, baselineReq); err == nil {
		t.Fatal("a missing hold silently changed the retained declaration")
	}
}
