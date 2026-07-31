package imagecacheevidence

import "testing"

func TestNormalizeGraphEvidenceIsBounded(t *testing.T) {
	t.Parallel()

	if got := NormalizeGraphStatus(" INCOMPLETE "); got != GraphStatusIncomplete {
		t.Fatalf("incomplete status = %q", got)
	}
	for _, value := range []string{"", "complete"} {
		if got := NormalizeGraphStatus(value); got != GraphStatusComplete {
			t.Fatalf("status %q normalized to %q, want complete", value, got)
		}
	}
	for _, value := range []string{"unknown", "/var/lib/fugue/cache"} {
		if got := NormalizeGraphStatus(value); got != GraphStatusIncomplete {
			t.Fatalf("unknown status %q normalized to %q, want fail-closed incomplete", value, got)
		}
	}
	for _, reason := range []string{ReasonMissingBlob, ReasonMissingChildManifest} {
		if got := NormalizeGraphFailureReason(" " + reason + " "); got != reason {
			t.Fatalf("reason %q normalized to %q", reason, got)
		}
	}
	for _, value := range []string{
		"",
		"missing_manifest",
		"/var/lib/fugue/cache/blob",
		"status=404 body=registry response",
	} {
		if got := NormalizeGraphFailureReason(value); got != "" {
			t.Fatalf("unbounded reason %q normalized to %q, want empty", value, got)
		}
	}
}
