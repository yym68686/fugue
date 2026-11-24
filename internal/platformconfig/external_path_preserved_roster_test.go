package platformconfig

import (
	"reflect"
	"slices"
	"testing"
)

// A refreshed baseline can already carry the physical record plus a dynamic
// selector with the held endpoint absent. A subsequent host-only publication
// must prove and preserve both inputs, without expecting the old endpoint to
// become eligible before it can be withheld again.
func externalWithheldRosterCase(t *testing.T) (HostedAddressSource, CompileRequest, HostedAddressHold) {
	t.Helper()
	source, _ := externalFourRouteEditWithThirdEndpoint(t, true)
	steady, edit := externalFourRouteEdit(t)
	source.Prior.ReadinessPlan = steady.Prior.ReadinessPlan
	source.Prior.QueryViews = steady.Prior.QueryViews
	source.Prior.Policy.DNSAnswerRules = steady.Prior.Policy.DNSAnswerRules
	held := HostedAddressHold{"edge-c", "edge-group-b", "93.184.216.36"}
	source.PriorHolds = []HostedAddressHold{held}
	source.EffectiveHolds = []HostedAddressHold{held}
	return source, edit, held
}

func ExternalWithheldRosterFixtureForTest(t *testing.T) (HostedAddressSource, HostedScopeProposal) {
	t.Helper()
	source, edit, _ := externalWithheldRosterCase(t)
	stage, capture, err := CompileHostedPathWithCapture(edit, source)
	if err != nil {
		t.Fatal(err)
	}
	offers, err := BuildHostedScopeProposals(source, stage, capture)
	if err != nil || len(offers) != 1 {
		t.Fatalf("cannot offer a path on a selected three-address record with two eligible endpoints: %v", err)
	}
	return source, offers[0]
}

func TestExternalPathReplaysOnAlreadyWithheldSignedRoster(t *testing.T) {
	source, edit, held := externalWithheldRosterCase(t)
	declared := slices.Clone(source.Prior.Records)
	stage, capture, err := CompileHostedPathWithCapture(edit, source)
	if err != nil {
		t.Fatal(err)
	}
	offers, err := BuildHostedScopeProposals(source, stage, capture)
	if err != nil || len(offers) != 1 {
		t.Fatalf("cannot offer a path on a selected three-address record with two eligible endpoints: %v", err)
	}
	p := offers[0]
	if len(p.Physical) != 3 || len(p.Eligible) != 2 || !p.ExternalPath.PredecessorWithheld() || !slices.Equal(p.ExternalPath.Held, []HostedAddressHold{held}) || !reflect.DeepEqual(declared, p.Records) {
		t.Fatalf("offer lost its selected declaration or veto: %+v", p)
	}
	view := source.Prior
	for _, phase := range []string{"prepare", "forward", "withdraw"} {
		out, err := ComposeHostedDNSPhase(view, p, phase)
		if err != nil {
			t.Fatalf("%s: %v", phase, err)
		}
		if !slices.Equal(out.Withheld, []HostedAddressHold{held}) || !reflect.DeepEqual(out.Views, source.Prior.QueryViews) || !reflect.DeepEqual(view.Records, declared) {
			t.Fatalf("%s changed the physical declaration or selector", phase)
		}
		for _, row := range out.Plan.Records {
			for _, target := range row.Targets {
				if target.Address == held.Address {
					t.Fatalf("%s offered a held address", phase)
				}
			}
		}
		view.ReadinessPlan, view.QueryViews, view.Policy.DNSAnswerRules = &out.Plan, out.Views, out.Rules
	}
	for _, invalid := range []struct {
		prior, current []HostedAddressHold
	}{
		{nil, []HostedAddressHold{held}},
		{[]HostedAddressHold{held}, nil},
		{[]HostedAddressHold{held}, []HostedAddressHold{{"different-edge", held.EdgeGroupID, held.Address}}},
	} {
		bad := source
		bad.PriorHolds, bad.EffectiveHolds = invalid.prior, invalid.current
		if got, err := BuildHostedScopeProposals(bad, stage, capture); err == nil || len(got) != 0 {
			t.Fatalf("unbound selected absence was accepted: %+v", invalid)
		}
	}
	unsafe := source.Prior
	unsafe.QueryViews = slices.Clone(unsafe.QueryViews)
	for i := range unsafe.QueryViews {
		v := &unsafe.QueryViews[i]
		v.Records = slices.Clone(v.Records)
		for j := range v.Records {
			if r := &v.Records[j]; r.Name == p.ExternalPath.Target.Target && r.Type == "A" {
				r.Values = append(slices.Clone(r.Values), held.Address)
			}
		}
	}
	if _, err := ComposeHostedDNSPhase(unsafe, p, "prepare"); err == nil {
		t.Fatal("predecessor still offered the withheld address in its query selector")
	}
}
