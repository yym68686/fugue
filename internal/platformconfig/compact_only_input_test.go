package platformconfig

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestCompactOnlyInputIsExplicitPreservesCommitmentAndOldContracts(t *testing.T) {
	at := time.Now().UTC()
	req := CompileRequest{CreatedAt: at, Intent: PlatformIntent{Generation: "intent"}, Policy: PolicySnapshot{Generation: "policy", RuntimeInputMode: RuntimeInputConfigurationV1}, RuntimeSnapshot: RuntimeSnapshot{CapturedAt: &at, Facts: map[string]any{"original": "not stored"}}}
	old, err := Compile(req)
	if err != nil {
		t.Fatal(err)
	}
	req.OmitOriginalInput = true
	c, err := Compile(req)
	if err != nil {
		t.Fatal(err)
	}
	if c.Lineage.CompilerVersion != CompactOnlyInputCompilerVersion || c.RuntimeInput == nil || c.QualityInput == nil ||
		c.Lineage.InputSnapshotDigest != old.Lineage.InputSnapshotDigest || c.Lineage.RuntimeInputDigest != old.Lineage.RuntimeInputDigest ||
		c.ReleaseArtifact.Generation == old.ReleaseArtifact.Generation || c.RuntimeInput.Facts != nil {
		t.Fatal("new contract lost original commitment, changed retained facts or reused old identity")
	}
	for _, a := range []model.PlatformArtifact{c.RouteArtifact, c.DNSArtifact, c.TLSArtifact, c.ReleaseArtifact} {
		if retained, err := ArtifactRetainsOriginalInput(a); err != nil || retained {
			t.Fatal(a.ArtifactKind, err)
		}
		if digest, err := ArtifactRuntimeInputDigest(a); err != nil || digest != c.Lineage.RuntimeInputDigest {
			t.Fatal(err)
		}
	}
	if retained, err := ArtifactRetainsOriginalInput(old.ReleaseArtifact); err != nil || !retained {
		t.Fatal("old history contract released", err)
	}
	replay, err := Compile(req)
	if err != nil || !reflect.DeepEqual(c, replay) {
		t.Fatal("non-deterministic compact-only compilation", err)
	}
	delete(c.ReleaseArtifact.Metadata, QualityEvidenceDigestMetadata)
	if _, err := ArtifactRetainsOriginalInput(c.ReleaseArtifact); err == nil {
		t.Fatal("partial contract accepted")
	}
}

func TestQualityEvidenceProjectionOmitsArbitraryReplayAndPreservesClocks(t *testing.T) {
	at := time.Now().UTC().Add(-time.Hour)
	in := RuntimeSnapshot{IntentGeneration: "i", PolicyGeneration: "p", CapturedAt: &at, Facts: map[string]any{"secret": true},
		DNSSelections: []DNSSelectionObservation{{NodeID: "dns-a", Hostname: "app.example.test", ObservedAt: at, PhysicalEvidence: json.RawMessage(`{"evidence":1}`)}, {NodeID: "dns-b", Hostname: "app.example.test"}},
		Origins:       []OriginObservation{{Ref: "origin-a"}}}
	out := QualityEvidenceInput(in)
	if out.Facts != nil || len(out.Origins) != 0 || len(out.DNSSelections) != 1 || !out.DNSSelections[0].ObservedAt.Equal(at) || !out.CapturedAt.Equal(at) || !reflect.DeepEqual(out.DNSSelections[0], in.DNSSelections[0]) {
		t.Fatal("quality projection leaked replay data or changed evidence")
	}
}
