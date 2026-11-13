package platformconfig

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestConfigurationRuntimeContractIsExplicitAndPreservesOriginalFacts(t *testing.T) {
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	req := CompileRequest{CreatedAt: at, Intent: PlatformIntent{Generation: "intent"}, Policy: PolicySnapshot{Generation: "policy"},
		RuntimeSnapshot: RuntimeSnapshot{CapturedAt: &at, Facts: map[string]any{"replay": strings.Repeat("x", 1<<16)}}}
	legacy, err := Compile(req)
	if err != nil {
		t.Fatal(err)
	}
	if legacy.RuntimeInput != nil || legacy.Lineage.RuntimeInputDigest != "" || legacy.Lineage.CompilerVersion != CompilerVersion {
		t.Fatal("legacy policy silently opted in")
	}
	req.Policy.RuntimeInputMode = RuntimeInputConfigurationV1
	compiled, err := Compile(req)
	if err != nil {
		t.Fatal(err)
	}
	if compiled.RuntimeInput == nil || compiled.RuntimeInput.Facts != nil || compiled.InputSnapshot.Facts["replay"] != req.RuntimeSnapshot.Facts["replay"] ||
		compiled.RuntimeInput.CapturedAt == nil || !compiled.RuntimeInput.CapturedAt.Equal(at) || compiled.Lineage.CompilerVersion != ConfigurationInputCompilerVersion ||
		compiled.Lineage.RuntimeInputDigest == compiled.Lineage.InputSnapshotDigest || compiled.ReleaseArtifact.Generation == legacy.ReleaseArtifact.Generation {
		t.Fatal("runtime input not independent, renewed, or changed original facts")
	}
	replay, err := Compile(req)
	if err != nil || !reflect.DeepEqual(compiled, replay) {
		t.Fatal("runtime contract is not deterministic", err)
	}
	for _, a := range []model.PlatformArtifact{compiled.ReleaseArtifact, compiled.RouteArtifact, compiled.DNSArtifact, compiled.TLSArtifact} {
		if digest, err := ArtifactRuntimeInputDigest(a); err != nil || digest != compiled.Lineage.RuntimeInputDigest {
			t.Fatal("compiled artifact cannot resolve exact runtime input", a.ArtifactKind, err)
		}
	}
	req.Policy.RuntimeInputMode = "future"
	if _, err := Compile(req); err == nil {
		t.Fatal("unknown runtime contract accepted")
	}
}

func TestConfigurationRuntimeProjectionPreservesTypedLeaseAndSelection(t *testing.T) {
	at := time.Now().UTC().Add(-time.Hour)
	in := RuntimeSnapshot{CapturedAt: &at, Facts: map[string]any{"audit": true}, IsolateTrafficAvailability: true,
		DNSSelections: []DNSSelectionObservation{{ObservedAt: at, PhysicalSelection: &model.DNSPhysicalSelection{CapturedAt: at}, PhysicalEvidence: json.RawMessage(`{"original":true}`), RetainedPhysicalEvidence: json.RawMessage(`{"retained":true}`)}},
		Releases:      []ReleaseObservation{{ID: "release", ObservedAt: at}}, RetainedTrafficPartitions: []RetainedTrafficPartition{{ExpiresAt: at}}}
	out := ConfigurationRuntimeInput(in)
	if len(out.DNSSelections[0].RetainedPhysicalEvidence) != 0 || len(in.DNSSelections[0].RetainedPhysicalEvidence) == 0 {
		t.Fatal("audit history leaked into legacy Worker runtime input or mutated the audit source")
	}
	if out.Facts != nil || len(out.DNSSelections[0].PhysicalEvidence) != 0 || len(in.DNSSelections[0].PhysicalEvidence) == 0 ||
		!out.DNSSelections[0].ObservedAt.Equal(at) || !out.DNSSelections[0].PhysicalSelection.CapturedAt.Equal(at) ||
		!out.RetainedTrafficPartitions[0].ExpiresAt.Equal(at) || !out.Releases[0].ObservedAt.Equal(at) || !out.IsolateTrafficAvailability {
		t.Fatal("projection changed typed authority, freshness or caller's evidence")
	}
}

func TestConfigurationRuntimeContractRejectsForgedOrPartialBindings(t *testing.T) {
	compiled, err := Compile(CompileRequest{Intent: PlatformIntent{Generation: "intent"}, Policy: PolicySnapshot{Generation: "policy", RuntimeInputMode: RuntimeInputConfigurationV1}, RuntimeSnapshot: RuntimeSnapshot{Facts: map[string]any{"audit": true}}})
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*model.PlatformArtifact){
		"missing metadata": func(a *model.PlatformArtifact) { delete(a.Metadata, "runtime_input_digest") },
		"missing content":  func(a *model.PlatformArtifact) { delete(a.Content, "lineage") },
		"different digest": func(a *model.PlatformArtifact) {
			a.Metadata["runtime_input_digest"] = "sha256:" + strings.Repeat("a", 64)
		},
		"old compiler":         func(a *model.PlatformArtifact) { a.Metadata["compiler_version"] = CompilerVersion },
		"changed generation":   func(a *model.PlatformArtifact) { a.Generation = "route-old" },
		"unknown kind":         func(a *model.PlatformArtifact) { a.ArtifactKind = model.PlatformArtifactKindPlatformIntent },
		"missing policy optin": func(a *model.PlatformArtifact) { delete(a.Content["policy"].(map[string]any), "runtime_input_mode") },
		"policy altered": func(a *model.PlatformArtifact) {
			a.Content["policy"].(map[string]any)["max_stale_seconds"] = float64(17)
		},
		"unknown lineage": func(a *model.PlatformArtifact) {
			a.Content["lineage"].(map[string]any)["runtime_alternative"] = "ignored"
		},
	} {
		t.Run(name, func(t *testing.T) {
			raw, _ := json.Marshal(compiled.RouteArtifact)
			var a model.PlatformArtifact
			if err := json.Unmarshal(raw, &a); err != nil {
				t.Fatal(err)
			}
			mutate(&a)
			if _, err := ArtifactRuntimeInputDigest(a); err == nil {
				t.Fatal("invalid contract fell back to history")
			}
		})
	}
}
