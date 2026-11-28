package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestOfflineReleaseArchiveProfileNeedsNoClusterConfigAndRejectsUntrustedInput(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")
	dir := t.TempDir()
	path := filepath.Join(dir, "sealed-release.json")
	raw := []byte(`{"not":"an archive"}`)
	pin := fmt.Sprintf("sha256:%x", sha256.Sum256(raw))
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	check := func(name, input, digest, want string) {
		t.Helper()
		err := run(context.Background(), []string{"verify-release-recovery-archive", input, digest}, &output)
		if err == nil || !strings.Contains(err.Error(), want) || output.Len() != 0 {
			t.Fatalf("%s: output=%q err=%v want=%q", name, output.String(), err, want)
		}
	}
	check("invalid envelope", path, pin, "invalid release recovery archive envelope")
	check("wrong pin", path, "sha256:"+strings.Repeat("f", 64), "independently pinned digest")
	link := filepath.Join(dir, "link")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	check("symlink", link, pin, "archive is unavailable")
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	check("public archive", path, pin, "archive must be private")
}

func TestOfflineRegistryRehomeRejectsUntrustedBundleWithoutClusterConfig(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")
	path := filepath.Join(t.TempDir(), "registry-rehome-recovery.json")
	raw := []byte(`{"schema":"not-a-recovery-bundle"}`)
	pin := fmt.Sprintf("sha256:%x", sha256.Sum256(raw))
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ digest, want string }{
		{pin, "invalid registry rehome recovery bundle"},
		{"sha256:" + strings.Repeat("f", 64), "independently pinned digest"},
	} {
		var output bytes.Buffer
		err := run(t.Context(), []string{"verify-release-registry-rehome", path, tc.digest}, &output)
		if err == nil || !strings.Contains(err.Error(), tc.want) || output.Len() != 0 {
			t.Fatalf("output=%q err=%v want=%q", output.String(), err, tc.want)
		}
	}
}
