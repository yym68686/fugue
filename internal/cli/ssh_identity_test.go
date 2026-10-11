package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/sshidentity"
)

func TestSSHIdentityCLISeparatesPrepareFromShow(test *testing.T) {
	for _, operation := range []struct{ command, method string }{{"show", http.MethodGet}, {"prepare", http.MethodPost}} {
		test.Run(operation.command, func(test *testing.T) {
			requested := false
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				writer.Header().Set("Content-Type", "application/json")
				switch request.URL.Path {
				case "/v1/apps":
					_ = json.NewEncoder(writer).Encode(map[string]any{"apps": []model.App{{ID: "app_example", TenantID: "tenant_example", Name: "workspace"}}})
				case "/v1/apps/app_example/ssh/identity":
					if request.Method != operation.method {
						test.Errorf("unexpected mutation method %s", request.Method)
					}
					requested = true
					_ = json.NewEncoder(writer).Encode(map[string]any{"identity": model.AppSSHIdentity{ID: "sshidentity_example", Fingerprint: "SHA256:synthetic", Generation: 1}})
				default:
					test.Errorf("unexpected request %s %s", request.Method, request.URL.Path)
					http.NotFound(writer, request)
				}
			}))
			defer server.Close()
			var output, diagnostic bytes.Buffer
			err := runWithStreams([]string{"--base-url", server.URL, "--token", "test-token", "app", "ssh", "identity", operation.command, "workspace"}, &output, &diagnostic)
			if err != nil || !requested || !strings.Contains(output.String(), "activation and client trust require separate verification") {
				test.Fatal("CLI did not preserve identity preparation scope", output.String(), diagnostic.String(), err)
			}
		})
	}
}

func TestSSHTrustPlanPreservesKnownHostsAndRequiresObservation(test *testing.T) {
	keyring, err := sshidentity.NewKeyring("test", map[string][]byte{"test": bytes.Repeat([]byte{11}, 32)})
	if err != nil {
		test.Fatal(err)
	}
	identity, err := keyring.Create("tenant_example", "app_example", time.Now())
	if err != nil {
		test.Fatal(err)
	}
	previous, err := keyring.Create("tenant_example", "old_identity", time.Now())
	if err != nil {
		test.Fatal(err)
	}
	filename := filepath.Join(test.TempDir(), "known_hosts")
	original := "[ssh.example]:23456 " + previous.Identity.PublicKey + "\nunrelated.example " + previous.Identity.PublicKey + "\n"
	if err := os.WriteFile(filename, []byte(original), 0o600); err != nil {
		test.Fatal(err)
	}
	for _, verified := range []bool{false, true} {
		test.Run(fmt.Sprint(verified), func(test *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				writer.Header().Set("Content-Type", "application/json")
				if request.Method != http.MethodGet {
					test.Error("trust plan attempted mutation", request.Method)
				}
				switch request.URL.Path {
				case "/v1/apps":
					_ = json.NewEncoder(writer).Encode(map[string]any{"apps": []model.App{{ID: "app_example", TenantID: "tenant_example", Name: "workspace"}}})
				case "/v1/apps/app_example/ssh/identity":
					_ = json.NewEncoder(writer).Encode(map[string]any{"identity": identity.Identity})
				case "/v1/apps/app_example/ssh":
					_ = json.NewEncoder(writer).Encode(map[string]any{"ssh": model.AppSSHStatus{Ready: verified, IdentityVerified: verified, IdentityID: identity.Identity.ID, Hostname: "ssh.example", PublicPort: 23456, HostKeyFingerprint: identity.Identity.Fingerprint, Observation: &model.AppSSHObservation{ExpiresAt: time.Now().Add(time.Minute)}}})
				default:
					http.NotFound(writer, request)
				}
			}))
			defer server.Close()
			var output, diagnostics bytes.Buffer
			err := runWithStreams([]string{"--base-url", server.URL, "--token", "test-token", "--json", "app", "ssh", "identity", "trust-plan", "workspace", "--known-hosts", filename}, &output, &diagnostics)
			if !verified && err == nil {
				test.Fatal("unverified identity accepted")
			}
			if verified && (err != nil || !strings.Contains(output.String(), "replacement_requires_review") || !strings.Contains(output.String(), identity.Identity.Fingerprint)) {
				test.Fatal("incorrect trust review", output.String(), err)
			}
			after, err := os.ReadFile(filename)
			if err != nil || string(after) != original {
				test.Fatal("trust plan modified client trust", err)
			}
		})
	}
}
