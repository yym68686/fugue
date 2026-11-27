package operationview

import (
	"encoding/json"
	"strings"
	"testing"

	"fugue/internal/model"
)

func TestEvidenceRedactionRespectsAudienceWithoutChangingStoredPayload(t *testing.T) {
	const fixture = "fixture-value"
	items := []model.OperationEvidence{{
		Summary: "Authorization: Bearer " + fixture,
		Payload: map[string]any{
			"builder_diagnostics": map[string]any{"worker": "fixture-worker", "token": fixture},
			"env":                 map[string]string{"DATABASE_URL": fixture},
			"attempt":             9007199254740993,
		},
	}}

	tenant := RedactEvidence(items, true, false)[0]
	admin := RedactEvidence(items, true, true)[0]
	timeline := RedactTimeline([]model.OperationTimelineEntry{{Payload: items[0].Payload}}, true)[0]
	if tenant.Payload["builder_diagnostics"] != nil || timeline.Payload["builder_diagnostics"] != nil {
		t.Fatal("internal builder diagnostic metadata is visible to the tenant")
	}
	builder, _ := admin.Payload["builder_diagnostics"].(map[string]any)
	if builder["worker"] != "fixture-worker" || builder["token"] != RedactedSecretValue {
		t.Fatal("administrator diagnostic projection did not preserve safe data and redact credentials")
	}
	if tenant.Payload["attempt"] != json.Number("9007199254740993") {
		t.Fatal("redaction changed a diagnostic counter")
	}
	for _, output := range []any{tenant, admin, timeline, RedactEvidence(items, false, true)[0]} {
		data, err := json.Marshal(output)
		if err != nil || strings.Contains(string(data), fixture) {
			t.Fatal("redaction returned an invalid projection or retained the fixture credential")
		}
	}
	if items[0].Payload["env"].(map[string]string)["DATABASE_URL"] != fixture {
		t.Fatal("redaction changed stored evidence")
	}
}

func TestMalformedDebugBundleUsesOnlySafeOperationProjection(t *testing.T) {
	project := func(op model.Operation) model.Operation {
		return model.Operation{ID: op.ID, ErrorMessage: RedactDiagnosticString(op.ErrorMessage)}
	}
	bundle := RedactDebugBundle(model.OperationDebugBundle{
		Operation: model.Operation{ID: "op", ErrorMessage: "PASSWORD=fixture-value"},
		Metadata:  map[string]any{"invalid": make(chan struct{})},
		Evidence:  []model.OperationEvidence{{Message: "fixture-value"}},
	}, project)
	if bundle.Operation.ID != "op" || bundle.Operation.ErrorMessage != "PASSWORD="+RedactedSecretValue ||
		bundle.Metadata != nil || len(bundle.Evidence) != 0 {
		t.Fatalf("malformed bundle bypassed fallback projection: %+v", bundle)
	}
}
