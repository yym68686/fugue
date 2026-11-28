package main

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/releaseguardian"
)

func TestRegistryRehomeDispatchRejectsUnsafeEnrollmentBeforeProviderAccess(t *testing.T) {
	root := t.TempDir()
	profile := filepath.Join(root, "dispatch.json")
	digest := "sha256:" + strings.Repeat("a", 64)
	args := []string{profile, digest, digest, filepath.Join(root, "evidence.json")}
	for _, body := range []string{
		`{}`,
		`{"journalFile":"/private/journal.json","writerKubeconfig":"/private/kubeconfig","writerContext":"writer","archiveFile":"/private/archive.json","archiveDigest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","admissionCABundleFile":"/private/ca.pem","observerPublicKey":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=","actor":"fixture","cutover":{"phase":"native"}}`,
		`{"journalFile":"/private/journal.json","writerKubeconfig":"/private/kubeconfig","writerContext":"writer","archiveFile":"/private/archive.json","archiveDigest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","admissionCABundleFile":"/private/ca.pem","observerPublicKey":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=","actor":"fixture","cutover":{"phase":"frozen"},"unexpected":true}`,
	} {
		if err := os.WriteFile(profile, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := runRegistryRehomeDispatch(t.Context(), args, io.Discard); err == nil ||
			!strings.Contains(err.Error(), "invalid frozen registry rehome dispatcher enrollment") {
			t.Fatalf("invalid enrollment reached provider access: %v", err)
		}
	}
}

func TestRegistryRehomeAdmissionRequiresIPListenerAndExactHTTPSPath(t *testing.T) {
	valid := registryRehomeAdmissionSettings{
		JournalFile: "/private/journal.json", AdmissionCABundleFile: "/private/ca.pem",
		TLSCertificateFile: "/private/tls.crt", TLSPrivateKeyFile: "/private/tls.key",
		ListenAddress: "0.0.0.0:8443", ObserverPublicKey: make([]byte, 32),
		RegistryRehomeKubeEnrollment: releaseguardian.RegistryRehomeKubeEnrollment{
			Cutover:           releaseguardian.KubeDeploymentCutover{Phase: releaseguardian.KubeCutoverFrozen},
			AdmissionEndpoint: "https://webhook.example.test/registry-rehome-admission",
		},
	}
	if err := validateRegistryRehomeAdmissionSettings(valid); err != nil {
		t.Fatalf("valid listener enrollment rejected: %v", err)
	}
	for _, mutate := range []func(*registryRehomeAdmissionSettings){
		func(s *registryRehomeAdmissionSettings) { s.ListenAddress = "webhook.example.test:8443" },
		func(s *registryRehomeAdmissionSettings) { s.AdmissionEndpoint = "https://webhook.example.test/other" },
		func(s *registryRehomeAdmissionSettings) { s.ListenAddress = "0.0.0.0:0" },
	} {
		next := valid
		mutate(&next)
		if err := validateRegistryRehomeAdmissionSettings(next); err == nil {
			t.Fatalf("unsafe admission service profile accepted: %+v", next)
		}
	}
}

func TestRegistryRehomeCommandsRejectInvalidInvocation(t *testing.T) {
	if err := runRegistryRehomeRenderFence(t.Context(), nil, io.Discard); err == nil || !strings.Contains(err.Error(), "registry-rehome-render-fence") {
		t.Fatalf("renderer usage was not enforced: %v", err)
	}
	if err := runRegistryRehomeDispatch(t.Context(), nil, io.Discard); err == nil || !strings.Contains(err.Error(), "registry-rehome-dispatch") {
		t.Fatalf("dispatch usage was not enforced: %v", err)
	}
	if err := runRegistryRehomeAdmission(t.Context(), nil, io.Discard); err == nil || !strings.Contains(err.Error(), "registry-rehome-admission") {
		t.Fatalf("admission usage was not enforced: %v", err)
	}
}

func TestRegistryRehomeWriterUsesOnlyExplicitPrivateStaticCredentials(t *testing.T) {
	path := filepath.Join(t.TempDir(), "writer.yaml")
	profile := `apiVersion: v1
kind: Config
current-context: fixture
contexts:
- name: fixture
  context: {cluster: fixture, user: fixture}
clusters:
- name: fixture
  cluster: {server: "https://provider.example.test"}
users:
- name: fixture
  user: {token: fixture-token}
`
	if err := os.WriteFile(path, []byte(profile), 0o600); err != nil {
		t.Fatal(err)
	}
	if cfg, err := loadRegistryRehomeWriter(path, "fixture"); err != nil || cfg.Host != "https://provider.example.test" {
		t.Fatalf("explicit static writer profile was not loaded: %v", err)
	}
	if _, err := loadRegistryRehomeWriter(path, "not-present"); err == nil {
		t.Fatal("absent context fell back to current-context")
	}
	if err := os.WriteFile(path, []byte(strings.Replace(profile, "token: fixture-token", "tokenFile: /private/token", 1)), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadRegistryRehomeWriter(path, "fixture"); err == nil {
		t.Fatal("writer accepted indirect credentials")
	}
}
