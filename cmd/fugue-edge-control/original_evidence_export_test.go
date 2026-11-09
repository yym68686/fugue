package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/edgecontrol"
)

func TestOriginalEvidenceExportReturnsOnlyBoundedSelectionWithoutWrites(t *testing.T) {
	root := t.TempDir()
	group := "cell-neutral"
	payload := []byte(`{"schema":"edge-control-persistent-group-state/v1","edge_group_id":"cell-neutral","revision":1,"digest":""}`)
	hash := sha256.Sum256(payload)
	raw := bytes.Replace(payload, []byte(`"digest":""`), []byte(`"digest":"sha256:`+hex.EncodeToString(hash[:])+`"`), 1)
	statePath := filepath.Join(root, "original.json")
	if err := os.WriteFile(statePath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	key := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x71}, 32))
	keyringPath := filepath.Join(root, group+".json")
	if err := os.WriteFile(keyringPath, []byte(`{"schema":"edge-control-group-bundle-signing-keyring/v1","generation":1,"group":{"edge_group_id":"cell-neutral","primary_key_id":"key-neutral","primary_key":"`+key+`"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	args := []string{"-state-file", statePath, "-edge-group-id", group, "-signing-keyring-dir", root, "-generations", "not-present"}
	filesBefore, _ := os.ReadDir(root)
	var summary bytes.Buffer
	if err := runOriginalEvidenceExport(t.Context(), args, &summary); err != nil {
		t.Fatal(err)
	}
	var report edgecontrol.GroupOriginalExport
	encoded := summary.Bytes()
	if json.Unmarshal(encoded, &report) != nil || report.StateDigest == "" || report.DependencyClosure || report.StateContentsValidated || len(report.Generations) != 1 {
		t.Fatal("export did not retain only diagnostic evidence")
	}
	if strings.Contains(summary.String(), "primary_key") || strings.Contains(summary.String(), key) || len(encoded) > 32<<20 {
		t.Fatal("key leaked or export exceeded its bound")
	}
	for _, bad := range [][]string{nil, append(append([]string(nil), args...), "unexpected"), {"-state-file", statePath, "-edge-group-id", group, "-signing-keyring-dir", root, "-generations", ""}} {
		summary.Reset()
		if err := runOriginalEvidenceExport(t.Context(), bad, &summary); err == nil || summary.Len() != 0 {
			t.Fatal("invalid export disclosed partial evidence", err)
		}
	}
	filesAfter, _ := os.ReadDir(root)
	if len(filesBefore) != len(filesAfter) {
		t.Fatal("export created a file")
	}
}
