package main

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDNSPolicyDispatchRejectsUnsafeProfilesBeforeProviderAccess(t *testing.T) {
	root := t.TempDir()
	profile := filepath.Join(root, "profile.json")
	token := filepath.Join(root, "token")
	if err := os.WriteFile(token, []byte("fixture-token"), 0o644); err != nil {
		t.Fatal(err)
	}
	digest := "sha256:" + strings.Repeat("a", 64)
	args := []string{profile, digest, digest}
	for _, fixture := range []struct {
		body, want string
	}{
		{`{"journalFile":"/ignored","apiOrigin":"https://example.test","readTokenFile":"` + token + `","artifactKeyFile":"/ignored-key","unknown":true}`, "separate runtime"},
		{`{"journalFile":"/ignored","apiOrigin":"https://example.test","apiOrigin":"https://other.test","readTokenFile":"` + token + `","artifactKeyFile":"/ignored-key"}`, "separate runtime"},
		{`{"journalFile":"/ignored","apiOrigin":"https://example.test","readTokenFile":"` + token + `","artifactKeyFile":"` + token + `"}`, "separate runtime"},
		{`{"journalFile":"/ignored","apiOrigin":"https://example.test","readTokenFile":"` + token + `","artifactKeyFile":"/ignored-key"}`, "not private"},
	} {
		if err := os.WriteFile(profile, []byte(fixture.body), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := runDNSPolicyDispatch(t.Context(), args, io.Discard); err == nil || !strings.Contains(err.Error(), fixture.want) {
			t.Fatalf("expected local profile refusal %q, got %v", fixture.want, err)
		}
	}
}
