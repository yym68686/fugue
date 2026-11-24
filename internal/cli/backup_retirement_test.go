package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"fugue/internal/model"
)

func TestRecoveredBackupRetirementCommand(t *testing.T) {
	digest := strings.Repeat("a", 64)
	evidence := strings.Repeat("b", 64)
	for _, mode := range []string{"preview", "confirmed", "with-receipt", "missing-reason", "blocked", "unconfirmed-response"} {
		t.Run(mode, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				var req model.RecoveredBackupRetirementRequest
				if r.Method != http.MethodPost || r.URL.Path != "/v1/backups/artifacts/old/retire-recovered" || json.NewDecoder(r.Body).Decode(&req) != nil {
					t.Error("unexpected retirement request")
				}
				if req.ReplacementArtifactID != "new" || req.Reason != "historical point" || (mode == "with-receipt") != (req.RestoreEvidenceSHA256 == evidence) || (mode == "preview") != (req.Confirmation == "") {
					t.Errorf("incorrect request: %+v", req)
				}
				result := model.RecoveredBackupRetirement{ArtifactID: "old", ReplacementArtifactID: "new", Confirmation: digest, Blockers: []string{}}
				if mode == "confirmed" || mode == "with-receipt" {
					result.Retired, result.AuditID = true, "audit-1"
				}
				if mode == "blocked" {
					result.Blockers = []string{"stale confirmation"}
					w.WriteHeader(http.StatusConflict)
				}
				_ = json.NewEncoder(w).Encode(result)
			}))
			defer server.Close()
			args := []string{"--base-url", server.URL, "--token", "fixture", "backup", "artifact", "retire-recovered", "old", "--replacement", "new", "--json"}
			if mode != "missing-reason" {
				args = append(args, "--reason", "historical point")
			}
			if mode != "preview" {
				args = append(args, "--confirm", digest)
			}
			if mode == "with-receipt" {
				args = append(args, "--restore-evidence-sha256", evidence)
			}
			var out, stderr bytes.Buffer
			err := runWithStreams(args, &out, &stderr)
			if (err == nil) != (mode == "preview" || mode == "confirmed" || mode == "with-receipt") {
				t.Fatalf("unexpected error: %v, out %q", err, out.String())
			}
			if (mode == "missing-reason" && calls != 0) || (mode != "missing-reason" && calls != 1) {
				t.Fatalf("unexpected request count %d", calls)
			}
			if mode == "confirmed" && !strings.Contains(out.String(), "audit-1") {
				t.Fatalf("missing audit receipt: %s", out.String())
			}
		})
	}
}
