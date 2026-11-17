package store

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/platformconfig"
	"fugue/internal/testfixture/celldns"
)

func TestDNSReferenceObservationFixedStagesAndBounds(t *testing.T) {
	ctx, mutation := startPlatformMutationTrace(context.Background(), "platform-release")
	at := mutation.started
	mutation.now = func() time.Time { return at }
	admission := beginPlatformAdmissionTrace(ctx, nil)
	for i := 0; i < 100; i++ {
		dns := beginPlatformDNSReferenceTrace(admission)
		at = at.Add(5 * time.Millisecond)
		dns.advance(dnsReferenceSourceObservation)
		at = at.Add(7 * time.Millisecond)
		dns.advance(dnsReferenceDependencyLedger)
		at = at.Add(11 * time.Millisecond)
		dns.finish()
	}
	admission.finish(nil)
	want := []platformMutationStage{{"artifact_authentication", 500}, {"source_observation", 700}, {"dependency_ledger", 1100}}
	if got := mutation.admissions[0].DNSReferences; !reflect.DeepEqual(got, want) {
		t.Fatal("nested stages must aggregate into fixed slots", got)
	}
	var disabled *platformDNSReferenceTrace
	disabled.advance(dnsReferenceSourceObservation)
	disabled.finish()
	if beginPlatformDNSReferenceTrace(nil) != nil {
		t.Fatal("disabled admission unexpectedly retained observations")
	}
}

func TestDNSReferenceObservationPreservesDecisionsAndReadOnlyState(t *testing.T) {
	for _, scenario := range []string{"valid", "artifact authentication", "producer bindings", "source observation", "dependency ledger"} {
		t.Run(scenario, func(t *testing.T) {
			state, child := dnsSourceObservationFixture(t, true)
			previous, err := platformconfig.DecodePreviousTrafficPublication(child)
			if err != nil || previous == nil {
				t.Fatal("fixture previous publication unavailable", err)
			}
			state.PlatformArtifacts = append(state.PlatformArtifacts, previous.DNS)
			last := "dependency_ledger"
			switch scenario {
			case "artifact authentication":
				child.Content["cell_route_publications"] = "invalid-private-content"
				last = "artifact_authentication"
			case "producer bindings":
				state.PlatformArtifacts[0].Provenance.Signature = "invalid-private-signature"
				last = "producer_bindings"
			case "source observation":
				state.PlatformReleaseLanes[0].Frozen = true
				last = "source_observation"
			case "dependency ledger":
				i := platformArtifactIndex(state.PlatformArtifacts, previous.DNS.ID)
				state.PlatformArtifacts[i].Provenance.Signature = "invalid-private-signature"
			}
			before, err := json.Marshal(state)
			if err != nil {
				t.Fatal(err)
			}
			want := validateCellDNSReferencesInState(&state, child, celldns.Keys())
			ctx, mutation := startPlatformMutationTrace(context.Background(), "platform-release")
			admission := beginPlatformAdmissionTrace(ctx, &state)
			got := validateCellDNSReferencesInStateWithTrace(&state, child, celldns.Keys(), admission)
			admission.finish(got)
			if (want == nil) != (got == nil) || want != nil && (got.Error() != want.Error() || errors.Is(got, ErrConflict) != errors.Is(want, ErrConflict)) {
				t.Fatal("nested observation changed the original decision/error", want, got)
			}
			if (scenario == "valid") != (got == nil) {
				t.Fatal("scenario did not exercise its expected decision", got)
			}
			after, err := json.Marshal(state)
			if err != nil || string(before) != string(after) {
				t.Fatal("observation mutated coherent authority state", err)
			}
			receipt := mutation.admissions[0]
			stages := receipt.DNSReferences
			if len(stages) == 0 || len(stages) > int(dnsReferencePhaseCount) || stages[len(stages)-1].Stage != last || receipt.Failed != (got != nil) {
				t.Fatal("wrong reached-stage or failure evidence", receipt)
			}
			raw, err := json.Marshal(receipt)
			if err != nil || strings.Contains(string(raw), "private-") || strings.Contains(string(raw), child.ID) {
				t.Fatal("diagnostic retained authority content or identities", err)
			}
		})
	}
}
