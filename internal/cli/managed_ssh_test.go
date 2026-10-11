package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"fugue/internal/model"
)

func TestManagedSSHCLIRequiresExplicitGenerationAndPreservesSpec(test *testing.T) {
	filename := filepath.Join(test.TempDir(), "spec.json")
	raw := []byte(`{"enabled":false,"identity_id":"prepared","container":"workspace","user":"app","working_directory":"/","authorized_key_ids":[],"allow_pty":true,"allow_sftp":true,"max_connections":8,"max_connections_per_ip":4,"max_channels":4}`)
	if err := os.WriteFile(filename, raw, 0o600); err != nil {
		test.Fatal(err)
	}
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/v1/apps":
			_ = json.NewEncoder(writer).Encode(map[string]any{"apps": []model.App{{ID: "workspace", TenantID: "tenant", Name: "workspace"}}})
		case "/v1/apps/workspace/ssh/managed":
			requests++
			var input struct {
				Expected uint64          `json:"expected_generation"`
				Spec     json.RawMessage `json:"spec"`
			}
			if request.Method != http.MethodPut || json.NewDecoder(request.Body).Decode(&input) != nil || input.Expected != 4 || !bytes.Equal(input.Spec, raw) {
				test.Error("CLI changed reviewed intent or CAS generation")
			}
			_ = json.NewEncoder(writer).Encode(map[string]any{"intent": model.ManagedSSHIntent{Generation: 5}})
		default:
			test.Errorf("unexpected operation %s %s", request.Method, request.URL.Path)
		}
	}))
	defer server.Close()
	arguments := []string{"--base-url", server.URL, "--token", "test-token", "app", "ssh", "managed", "apply", "workspace", "--file", filename}
	var output, diagnostic bytes.Buffer
	if err := runWithStreams(arguments, &output, &diagnostic); err == nil || requests != 0 {
		test.Fatal("CLI inferred a write generation")
	}
	if err := runWithStreams(append(arguments, "--expected-generation", "4"), &output, &diagnostic); err != nil || requests != 1 {
		test.Fatal("explicit intent update failed", err, diagnostic.String())
	}
}
