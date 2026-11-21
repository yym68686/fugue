package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFrontRepairToolManifestRejectsInvalidArtifactInventories(t *testing.T) {
	const amd = frontRepairToolArtifactPrefix + "amd64"
	const arm = frontRepairToolArtifactPrefix + "arm64"
	source := strings.Repeat("a", 40)
	for _, tt := range []struct {
		name    string
		entries map[string]string
		want    string
	}{
		{"missing architecture", map[string]string{amd: "ordinary"}, "only the two"},
		{"unexpected binary", map[string]string{amd: "ordinary", "unrecognized": "ordinary"}, "foreign or linked"},
		{"linked binary", map[string]string{amd: "ordinary", arm: "symlink"}, "foreign or linked"},
		{"oversized binary", map[string]string{amd: "oversized", arm: "ordinary"}, "Worker tool budget"},
		{"unverified program", map[string]string{amd: "ordinary", arm: "ordinary"}, "not an ELF executable"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			for name, entry := range tt.entries {
				path := filepath.Join(dir, name)
				switch entry {
				case "symlink":
					if err := os.Symlink(amd, path); err != nil {
						t.Fatal(err)
					}
				case "oversized":
					f, err := os.Create(path)
					if err != nil {
						t.Fatal(err)
					}
					resize := f.Truncate(frontRepairToolMaxBytes + 1)
					closed := f.Close()
					if resize != nil || closed != nil {
						t.Fatalf("oversized fixture: %v %v", resize, closed)
					}
				default:
					if err := os.WriteFile(path, []byte("not an executable"), 0600); err != nil {
						t.Fatal(err)
					}
				}
			}
			var out bytes.Buffer
			err := runFrontRepairToolManifest([]string{"--dir", dir, "--source-commit", source}, &out)
			if err == nil || !strings.Contains(err.Error(), tt.want) || out.Len() != 0 {
				t.Fatalf("invalid artifact emitted a manifest: err=%v stdout=%q", err, out.String())
			}
		})
	}
}

func TestFrontRepairToolManifestRejectsNonBuildArguments(t *testing.T) {
	dir := t.TempDir()
	for _, args := range [][]string{
		{"--dir", dir, "--source-commit", "main"},
		{"--dir", dir, "--source-commit", strings.Repeat("a", 40), "--apply"},
		{"--dir", ".", "--source-commit", strings.Repeat("a", 40)},
	} {
		var out bytes.Buffer
		if err := runFrontRepairToolManifest(args, &out); err == nil || out.Len() != 0 {
			t.Fatalf("unexpected manifest for %v: err=%v output=%s", args, err, out.String())
		}
	}
}
