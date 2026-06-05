package observability

import "testing"

func TestMetricLabelPolicy(t *testing.T) {
	if !IsAllowedMetricLabel("app_id") || !IsAllowedMetricLabel("status_class") {
		t.Fatal("expected low-cardinality labels to be allowed")
	}
	for _, key := range []string{"trace_id", "request_id", "email", "Authorization", "database_url"} {
		if !IsDeniedMetricLabel(key) {
			t.Fatalf("expected %s to be denied", key)
		}
		if IsAllowedMetricLabel(key) {
			t.Fatalf("expected %s not to be allowed", key)
		}
	}
}

func TestSanitizeSummaryFieldsDropsSecretsAndCapsOutput(t *testing.T) {
	fields := map[string]string{
		"type":          "interactive",
		"target":        "search",
		"Authorization": "Bearer secret",
		"cookie":        "session=secret",
		"empty":         "",
	}
	clean, warnings := SanitizeSummaryFields(fields, SummaryPolicy{MaxFields: 2, MaxKeyBytes: 16, MaxValueBytes: 32})
	if len(clean) != 2 {
		t.Fatalf("expected two safe fields, got %+v", clean)
	}
	if _, ok := clean["Authorization"]; ok {
		t.Fatalf("secret field was not dropped: %+v", clean)
	}
	if len(warnings) == 0 {
		t.Fatal("expected warnings for dropped secret fields")
	}
}

func TestRedactFieldsPreservesShape(t *testing.T) {
	redacted := RedactFields(map[string]string{
		"status":       "ok",
		"access_token": "secret",
	})
	if redacted["status"] != "ok" {
		t.Fatalf("expected status to be preserved, got %+v", redacted)
	}
	if redacted["access_token"] != "[REDACTED]" {
		t.Fatalf("expected token to be redacted, got %+v", redacted)
	}
}
