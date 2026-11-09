package main

import (
	"os"
	"path/filepath"
	"testing"

	"fugue/internal/store"
)

func TestEncodedBackupFlagsKeepExportAndOfflineDecodeSeparate(t *testing.T) {
	export := map[string]bool{"mode": true, "backup-contract": true}
	if !validEncodedBackupFlags("encoded-backup", export) || validEncodedBackupFlags("decode-backup", export) {
		t.Fatal("incorrect export selection")
	}
	for _, flag := range []string{"apply", "rehearse", "source-verification-contract", "digest", "backup-input", "backup-output"} {
		extra := map[string]bool{"mode": true, "backup-contract": true, flag: true}
		if validEncodedBackupFlags("encoded-backup", extra) {
			t.Fatal("export accepted unrelated flag", flag)
		}
	}
	if !validEncodedBackupFlags("decode-backup", map[string]bool{"mode": true, "backup-contract": true, "backup-input": true, "backup-output": true}) {
		t.Fatal("offline decode rejected exact paths")
	}
}

func TestEncodedBackupFailureLeavesNoOutputAndPreservesExistingFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	in, out := filepath.Join(dir, "encoded.jsonl"), filepath.Join(dir, "input.json")
	if err := os.WriteFile(in, []byte(`incomplete`), 0600); err != nil {
		t.Fatal(err)
	}
	c := store.CompilerInputBackupContract{Schema: store.CompilerInputBackupSchema, Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", ExpectedLogicalBytes: 1, MaxEncodedBytes: 1024}
	if r, err := decodeEncodedBackupFile(t.Context(), c, in, out); err == nil || r.Complete {
		t.Fatal("failed decode accepted")
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 {
		t.Fatal("partial decode file retained", len(entries), err)
	}
	if err = os.WriteFile(out, []byte("retained"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = decodeEncodedBackupFile(t.Context(), c, in, out); err == nil {
		t.Fatal("existing output accepted")
	}
	data, _ := os.ReadFile(out)
	if string(data) != "retained" {
		t.Fatal("existing output changed")
	}
	if err = os.Chmod(in, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err = decodeEncodedBackupFile(t.Context(), c, in, filepath.Join(dir, "other")); err == nil {
		t.Fatal("nonprivate input accepted")
	}
}
