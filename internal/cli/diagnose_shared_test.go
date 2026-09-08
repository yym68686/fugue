package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSharedEvidencePreservesPartialSourcesAndDefiniteRelations(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/operations/op_example":
			fmt.Fprint(w, `{"operation":{"id":"op_example","app_id":"app_example","status":"failed","desired_spec":{"env":{"TOKEN":"synthetic-secret"}}}}`)
		case strings.HasSuffix(r.URL.Path, "/diagnosis"):
			fmt.Fprint(w, `{"diagnosis":{"category":"failed","blocked_by":[{"operation_id":"op_dependency"}]}}`)
		default:
			w.WriteHeader(403)
			fmt.Fprint(w, `{"error":"opaque"}`)
		}
	}))
	defer server.Close()
	var out, stderr bytes.Buffer
	err := runWithStreams([]string{"--base-url", server.URL, "--token", "test", "--json", "diagnose", "operation", "op_example", "--require-complete"}, &out, &stderr)
	if ExitCodeForError(err) != 6 {
		t.Fatal(err)
	}
	var report objectEvidenceReport
	if json.Unmarshal(out.Bytes(), &report) != nil {
		t.Fatal(out.String())
	}
	if report.SchemaVersion != "fugue.evidence.v1" || report.Sources["evidence"].State != "permission_denied" || len(report.Relations) < 1 || strings.Contains(out.String(), "synthetic-secret") {
		t.Fatal(out.String())
	}
}
