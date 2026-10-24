package api

import (
	"encoding/json"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
)

func TestRuntimeSummaryPreservesAdmissionAndConfigurationWhileBoundingExport(t *testing.T) {
	now := time.Now().UTC()
	raw := json.RawMessage(`{"captured":"` + strings.Repeat("x", 4<<20) + `"}`)
	facts := []platformconfig.DNSSelectionObservation{}
	for i := 0; i < 36; i++ {
		facts = append(facts, platformconfig.DNSSelectionObservation{NodeID: "dns-a", Hostname: "app.example.test", Type: "A", ObservedAt: now, PhysicalEvidence: raw, PhysicalSelection: &model.DNSPhysicalSelection{}, Candidates: []platformconfig.DNSSelectionCandidate{{EdgeGroupID: "group-a"}}})
	}
	original := platformIntentProjectionResponse{PreviewMode: "runtime_summary", BusinessSnapshotRevision: "revision-a", BusinessSnapshotAt: now, CapturedAt: now, MigrationReady: false, Intent: platformconfig.PlatformIntent{Generation: "intent-a", Scope: "global"}, Policy: platformconfig.PolicySnapshot{Generation: "policy-a", Scope: "global"}, Issues: []platformProjectionIssue{{Code: "preserved-issue"}}, OmittedRuntimeFields: []string{"selected_edge_group", "decision_id", "exclusion_evidence"}, RuntimeSnapshot: platformconfig.RuntimeSnapshot{CapturedAt: &now, DNSSelections: facts, Facts: map[string]any{platformproducer.OfflineRecoveryFact: map[string]any{"captured_at": now, "observations": []any{map[string]any{"edge_id": "edge-a", "admitted": false}}}, "release_readiness": map[string]any{"state": "unavailable"}}}}
	original.OmittedRuntimeFields = append(make([]string, 0, 10), original.OmittedRuntimeFields...)
	wantDigest, err := platformconfig.RuntimeSnapshotDigest(original.RuntimeSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	result := original
	if err := summarizeRuntimePreview(&result); err != nil {
		t.Fatal(err)
	}
	summary := result.RuntimeSummary
	if summary == nil || summary.Schema != runtimePreviewSummarySchema || summary.FullSnapshotDigest != wantDigest || summary.OmittedPhysicalEvidenceCount != 36 || summary.OmittedPhysicalEvidenceBytes != 36*int64(len(raw)) || summary.OmittedPhysicalEvidenceBytes < 128<<20 {
		t.Fatal("incorrect omission accounting", summary)
	}
	encoded, err := json.Marshal(result)
	if err != nil || len(encoded) > 32<<10 {
		t.Fatal("raw evidence expanded summary response", len(encoded), err)
	}
	if !reflect.DeepEqual(original.Intent, result.Intent) || !reflect.DeepEqual(original.Policy, result.Policy) || !reflect.DeepEqual(original.Issues, result.Issues) || !reflect.DeepEqual(original.RuntimeSnapshot.Facts, result.RuntimeSnapshot.Facts) || original.BusinessSnapshotRevision != result.BusinessSnapshotRevision || original.CapturedAt != result.CapturedAt || result.MigrationReady {
		t.Fatal("summary changed configuration or authorization facts")
	}
	for i := range facts {
		if len(facts[i].PhysicalEvidence) != len(raw) || result.RuntimeSnapshot.DNSSelections[i].PhysicalEvidence != nil {
			t.Fatal("response mutation reached shared evidence")
		}
		restored := result.RuntimeSnapshot.DNSSelections[i]
		restored.PhysicalEvidence = raw
		if !reflect.DeepEqual(restored, facts[i]) {
			t.Fatal("summary changed a selection, candidate or observation time")
		}
	}
	if len(original.OmittedRuntimeFields) != 3 || len(result.OmittedRuntimeFields) != 4 || result.OmittedRuntimeFields[3] != runtimePreviewOmittedPhysicalEvidence {
		t.Fatal("omission list not detached")
	}
	// Re-encoding the original capture still contains complete replay evidence.
	digest, err := platformconfig.RuntimeSnapshotDigest(original.RuntimeSnapshot)
	if err != nil || digest != wantDigest {
		t.Fatal("summary rewrote immutable capture", err)
	}
}

func TestRuntimeSummaryInvalidEvidenceCannotBecomeValidByOmission(t *testing.T) {
	result := platformIntentProjectionResponse{RuntimeSnapshot: platformconfig.RuntimeSnapshot{DNSSelections: []platformconfig.DNSSelectionObservation{{PhysicalEvidence: json.RawMessage(`{"broken":`)}}}}
	original := result
	if err := summarizeRuntimePreview(&result); err == nil {
		t.Fatal("invalid replay evidence hidden")
	}
	if !reflect.DeepEqual(result, original) {
		t.Fatal("failed digest partially mutated response")
	}
}

func TestRuntimeSummaryEmptyCaptureIsExplicitAndRequiresAdmin(t *testing.T) {
	_, s, _, admin, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	authority := activateTestProducer(t, s, "summary-fixture", "shadow")
	path := "/v1/admin/platform-config/routes/project?producer_policy_artifact_id=" + authority.ArtifactID + "&view=runtime_summary"
	response := performJSONRequest(t, s, http.MethodGet, path, admin, nil)
	if response.Code != 200 {
		t.Fatal(response.Code, response.Body.String())
	}
	var result platformIntentProjectionResponse
	mustDecodeJSON(t, response, &result)
	if result.PreviewMode != "runtime_summary" || result.RuntimeSummary == nil || result.RuntimeSummary.OmittedPhysicalEvidenceCount != 0 || result.RuntimeSummary.OmittedPhysicalEvidenceBytes != 0 || result.MigrationReady {
		t.Fatal("empty summary lost explicit semantics", result.RuntimeSummary)
	}
	digest, err := platformconfig.RuntimeSnapshotDigest(result.RuntimeSnapshot)
	if err != nil || digest != result.RuntimeSummary.FullSnapshotDigest {
		t.Fatal("empty summary digest mismatch", err)
	}
	if unauthorized := performJSONRequest(t, s, http.MethodGet, path, "invalid-key", nil); unauthorized.Code != 401 {
		t.Fatal("summary relaxed authorization", unauthorized.Code)
	}
	full := performJSONRequest(t, s, http.MethodGet, strings.Replace(path, "runtime_summary", "runtime", 1), admin, nil)
	if full.Code != 200 || strings.Contains(full.Body.String(), `"runtime_summary"`) {
		t.Fatal("full replay contract changed", full.Code)
	}
}
