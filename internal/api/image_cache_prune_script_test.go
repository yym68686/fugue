package api

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestImageCachePruneScriptForwardsGrantAndPartialReceipt(t *testing.T) {
	var server Server
	script := server.nodeUpdaterInstallScript("https://api.invalid")
	_, tail, found := strings.Cut(script, "\nprune_image_cache() {\n")
	if !found {
		t.Fatal("missing prune function")
	}
	body, _, found := strings.Cut(tail, "\n}\n")
	if !found {
		t.Fatal("missing prune function body")
	}
	requestFile := filepath.Join(t.TempDir(), "request.json")
	harness := `
set -eu
json_escape_shell() { printf '%s' "$1"; }
log_task() { :; }
image_cache_api_json() {
  printf '%s' "$2" > "$REQUEST_FILE"
  printf '%s' '{"deleted":false,"planned_delete_bytes":100,"deleted_bytes":0,"execution_status":"partial","skipped_reason":"still_referenced","skipped_blob_bytes":100,"remaining_unreferenced_bytes":20,"remaining_snapshot_valid":true,"candidate_count":2}'
}
prune_image_cache() {
` + body + "\n}\nprune_image_cache\nprintf '%s' \"$FUGUE_NODE_UPDATE_TASK_RESULT_MESSAGE\"\n"
	cmd := exec.Command("bash", "-c", harness)
	cmd.Env = append(os.Environ(), "REQUEST_FILE="+requestFile,
		`FUGUE_NODE_UPDATE_TASK_UNREFERENCED_BLOB_DIGESTS=["sha256:admitted"]`)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("run prune script: %v\n%s", err, out)
	}
	raw, err := os.ReadFile(requestFile)
	if err != nil {
		t.Fatal(err)
	}
	var request struct {
		Digests []string `json:"unreferenced_blob_digests"`
	}
	if err := json.Unmarshal(raw, &request); err != nil || len(request.Digests) != 1 || request.Digests[0] != "sha256:admitted" {
		t.Fatalf("lost orphan grant: %s (%v)", raw, err)
	}
	for _, field := range []string{"execution_status=partial", "skipped_reason=still_referenced", "skipped_blob_bytes=100", "remaining_unreferenced_bytes=20", "remaining_snapshot_valid=true"} {
		if !strings.Contains(string(out), field) {
			t.Fatalf("missing receipt field %s: %s", field, out)
		}
	}
}
