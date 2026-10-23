package dnsserver

import (
	"encoding/json"
	"reflect"
	"testing"

	"fugue/internal/platformconfig"
	"fugue/internal/testfixture/celldns"
)

func TestCompactDNSPublicationInputsRetainsBindingAndComparison(t *testing.T) {
	a := celldns.Compile(t, celldns.TransitionRequest(t)).DNSArtifact
	raw, err := json.Marshal(a.Content)
	if err != nil {
		t.Fatal(err)
	}
	var original, equivalent, changed dnsServingPayload
	for _, p := range []*dnsServingPayload{&original, &equivalent, &changed} {
		if err = json.Unmarshal(raw, p); err != nil {
			t.Fatal(err)
		}
		if err = populateDNSCellBindings(p); err != nil {
			t.Fatal(err)
		}
	}
	oldBindings := original.previousBindings
	oldCells := original.cellBindings
	// An ancillary envelope change must still block bridge equivalence, even
	// when the derived release bindings themselves happen to be identical.
	changed.PreviousTrafficPublication.Route.Metadata["publisher_note"] = "different-publisher"
	for _, p := range []*dnsServingPayload{&original, &equivalent, &changed} {
		if err = compactDNSPublicationInputs(p); err != nil {
			t.Fatal(err)
		}
		if p.PreviousTrafficPublication != nil || p.CellRoutePublications != nil {
			t.Fatal("duplicate publication graph retained")
		}
	}
	if !reflect.DeepEqual(original.previousBindings, oldBindings) || !reflect.DeepEqual(original.cellBindings, oldCells) {
		t.Fatal("compaction changed proof bindings")
	}
	if !reflect.DeepEqual(original, equivalent) || reflect.DeepEqual(original, changed) {
		t.Fatal("compaction weakened exact source comparison")
	}
	if err = platformconfig.ValidateDNSCellPlan(a); err != nil {
		t.Fatal("compaction mutated authenticated artifact", err)
	}
}
