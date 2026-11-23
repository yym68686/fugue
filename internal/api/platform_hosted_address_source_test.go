package api

import (
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"testing"

	pc "fugue/internal/platformconfig"
	"fugue/internal/store"
	"fugue/internal/testfixture/celldns"
)

func TestHostedAddressProducerPersistsAuthenticatedOperationalInput(t *testing.T) {
	s, _, _, _ := retainedTrafficAPIFixture(t)
	cell, req := celldns.TransitionRequestWithSource(t)
	req.Intent.DNS[0].RecordKind = "hosted"
	req.Policy.RuntimeInputMode = pc.RuntimeInputConfigurationV1
	req.OmitOriginalInput = true
	pin := cell.PreviousTrafficPublication.Reference
	src := platformCompilationSource{PolicyReleaseID: "policy-source", SourceDigest: pin.RouteArtifactDigest, StaticIntentID: "static-source", StaticIntentDigest: pin.TLSArtifactDigest, DNSPolicyID: "dns-policy-source", DNSPolicyDigest: pin.DNSArtifactDigest, hostedSource: pin}
	req.RuntimeSnapshot.Facts = map[string]any{"configuration_producer": map[string]any{"policy_release_id": src.PolicyReleaseID, "source_digest": src.SourceDigest, "static_intent_artifact_id": src.StaticIntentID, "static_intent_digest": src.StaticIntentDigest, "dns_policy_artifact_id": src.DNSPolicyID, "dns_policy_digest": src.DNSPolicyDigest}}
	ordinary, e := pc.Compile(req)
	if e != nil {
		t.Fatal(e)
	}
	var old pc.DNSArtifactPayload
	b, e := json.Marshal(ordinary.DNSArtifact.Content)
	if e != nil {
		t.Fatal(e)
	}
	if e = json.Unmarshal(b, &old); e != nil {
		t.Fatal(e)
	}
	rec := &old.ReadinessPlan.Records[0]
	target := rec.Targets[0]
	target.Address, target.EdgeID = "9.9.9.9", "reserved-edge"
	rec.Targets = append(slices.Clone(rec.Targets), target)
	old.Records[0].Values = append(slices.Clone(old.Records[0].Values), target.Address)
	result, e := pc.CompileWithHostedAddressSource(req, pc.HostedAddressSource{Publication: pin, Prior: old, EffectiveHolds: []pc.HostedAddressHold{{EdgeID: target.EdgeID, EdgeGroupID: target.EdgeGroupID, Address: target.Address}}})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.materializePlatformCompilation(t.Context(), result, platformProducerPrincipal(), nil); !errors.Is(e, store.ErrConflict) {
		t.Fatal("ordinary facts substituted for independent verified full", e)
	}
	got, e := s.materializePlatformCompilation(t.Context(), result, platformProducerPrincipal(), nil, src)
	if e != nil {
		t.Fatal(e)
	}
	for _, a := range []struct{ parent bool }{{true}, {false}} {
		artifact := got.DNSArtifact
		if a.parent {
			artifact = got.ReleaseArtifact
		}
		if e = s.store.VerifyPlatformArtifactIntegrity(artifact); e != nil {
			t.Fatal(e)
		}
		ref, d, e := pc.HostedAddressAuthority(artifact)
		if e != nil || ref == nil || *ref != pin || d == "" {
			t.Fatal("lost materialized reference", ref, e)
		}
	}
	stored, e := s.store.GetPlatformArtifactRuntimeInputContext(t.Context(), got.ReleaseArtifact)
	if e != nil || !reflect.DeepEqual(stored.HostedAddressReservations, result.InputSnapshot.HostedAddressReservations) {
		t.Fatal("persisted compact runtime changed reservation", e)
	}
}
