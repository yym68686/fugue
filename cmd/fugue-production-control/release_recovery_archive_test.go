package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWritePrivateReleaseArchiveDoesNotOverwrite(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "archive")
	if err := writePrivateReleaseArchive(dir, []byte("sealed fixture")); err != nil {
		t.Fatal(err)
	}
	for path, mode := range map[string]os.FileMode{dir: 0700, filepath.Join(dir, sealedReleaseArchiveName): 0600} {
		info, err := os.Stat(path)
		if err != nil || info.Mode().Perm() != mode {
			t.Fatalf("archive output not private: info=%v err=%v", info, err)
		}
	}
	if err := writePrivateReleaseArchive(dir, []byte("replacement")); err == nil {
		t.Fatal("existing archive was accepted as a fresh destination")
	}
	raw, err := os.ReadFile(filepath.Join(dir, sealedReleaseArchiveName))
	if err != nil || string(raw) != "sealed fixture" {
		t.Fatalf("existing archive changed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, sealedReleaseArchiveName+".partial")); !os.IsNotExist(err) {
		t.Fatalf("archive has unfinished staging file: %v", err)
	}
}
