package api

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestInstallersRenderSharedDiscoveryEnv(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 is required to exercise the embedded discovery renderer")
	}
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash is required to exercise the installer functions")
	}
	server := &Server{}
	installers := []struct {
		name, script  string
		updatedFields bool
	}{
		{"join", server.joinClusterInstallScript("https://api.example.test"), false},
		{"updater", server.nodeUpdaterInstallScript("https://api.example.test"), true},
	}
	now := time.Now().UTC().Truncate(time.Second)
	const primary = "synthetic-primary"
	const previous = "synthetic-previous"
	unsigned := model.DiscoveryBundle{
		SchemaVersion: "1.0", Issuer: "test-issuer", Generation: "test-generation",
		GeneratedAt: now, ValidUntil: now.Add(time.Hour), KeyID: "active",
		APIEndpoints: []model.DiscoveryEndpoint{{Name: "public", URL: "https://api.example.test"}},
		Kubernetes:   []model.DiscoveryKubernetesEndpoint{{Name: "cluster-join", Server: "https://cluster.example.test", CAHash: "test-hash"}},
	}
	signed := unsigned
	signed.Signature = signInstallerDiscoveryFixture(t, unsigned, "active", primary)
	signed.Signatures = []model.BundleSignature{{
		KeyID: "retiring", Signature: signInstallerDiscoveryFixture(t, unsigned, "retiring", previous),
		ValidUntil: unsigned.ValidUntil,
	}}
	tampered := signed
	tampered.Generation = "tampered"
	for _, installer := range installers {
		t.Run(installer.name, func(t *testing.T) {
			const start = "render_discovery_env() {\n"
			_, rest, ok := strings.Cut(installer.script, start)
			if !ok {
				t.Fatal("renderer missing")
			}
			body, _, ok := strings.Cut(rest, "\n}\n")
			if !ok || strings.Contains(body, "__FUGUE_DISCOVERY_ENV_PYTHON__") {
				t.Fatal("renderer not fully embedded")
			}
			for _, tc := range []struct {
				name, key, revoked string
				bundle             model.DiscoveryBundle
				wantOK             bool
			}{
				{"primary", primary, "", signed, true},
				{"rotated", "synthetic-next", "active", signed, true},
				{"revoked", "synthetic-next", "active,retiring", signed, false},
				{"tampered", primary, "", tampered, false},
			} {
				t.Run(tc.name, func(t *testing.T) {
					path := filepath.Join(t.TempDir(), "bundle.json")
					data, err := json.Marshal(tc.bundle)
					if err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(path, data, 0600); err != nil {
						t.Fatal(err)
					}
					cmd := exec.Command("bash", "-eu", "-c", start+body+"\n}\nrender_discovery_env \"$1\"\n", "bash", path)
					cmd.Env = []string{
						"PATH=" + os.Getenv("PATH"),
						"FUGUE_BUNDLE_SIGNING_KEY=" + tc.key,
						"FUGUE_BUNDLE_SIGNING_KEY_ID=active",
						"FUGUE_BUNDLE_SIGNING_PREVIOUS_KEY=" + previous,
						"FUGUE_BUNDLE_SIGNING_PREVIOUS_KEY_ID=retiring",
						"FUGUE_BUNDLE_REVOKED_KEY_IDS=" + tc.revoked,
					}
					out, err := cmd.CombinedOutput()
					if !tc.wantOK {
						if err == nil || !strings.Contains(string(out), "signature verification failed") || strings.Contains(string(out), "FUGUE_DISCOVERY_GENERATION=") {
							t.Fatalf("expected rejection without emitted state: %v %s", err, out)
						}
						return
					}
					if err != nil || !strings.Contains(string(out), "FUGUE_DISCOVERY_GENERATION=test-generation\n") || !strings.Contains(string(out), "FUGUE_DISCOVERY_K3S_SERVER=https://cluster.example.test\n") {
						t.Fatalf("unexpected renderer output: %v %s", err, out)
					}
					if got := strings.Contains(string(out), "FUGUE_DISCOVERY_K3S_CA_HASH=test-hash\n"); got != installer.updatedFields {
						t.Fatalf("installer projection changed: %s", out)
					}
				})
			}
		})
	}
}

// Use the installer wire format for this equivalence test. The Go bundle
// signer serializes additional fields, including nil values as null.
func signInstallerDiscoveryFixture(t *testing.T, b model.DiscoveryBundle, keyID, key string) string {
	t.Helper()
	payload := struct {
		SchemaVersion string                              `json:"schema_version"`
		Generation    string                              `json:"generation"`
		GeneratedAt   time.Time                           `json:"generated_at"`
		ValidUntil    time.Time                           `json:"valid_until"`
		Issuer        string                              `json:"issuer"`
		KeyID         string                              `json:"key_id"`
		APIEndpoints  []model.DiscoveryEndpoint           `json:"api_endpoints"`
		Kubernetes    []model.DiscoveryKubernetesEndpoint `json:"kubernetes"`
	}{
		b.SchemaVersion, b.Generation, b.GeneratedAt, b.ValidUntil, b.Issuer, keyID, b.APIEndpoints, b.Kubernetes,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	mac := hmac.New(sha256.New, []byte(key))
	_, _ = mac.Write(data)
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}
