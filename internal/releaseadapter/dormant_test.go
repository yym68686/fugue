package releaseadapter

import (
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestBoundaryBTransactionSeamIsDormant(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate dormant test source")
	}
	repositoryRoot := filepath.Clean(filepath.Join(filepath.Dir(currentFile), "..", ".."))
	for _, relative := range []string{
		"scripts/upgrade_fugue_control_plane.sh",
		".github/workflows/deploy-control-plane.yml",
	} {
		data, err := os.ReadFile(filepath.Join(repositoryRoot, relative))
		if err != nil {
			t.Fatalf("read %s: %v", relative, err)
		}
		for _, forbidden := range []string{
			"fugue/internal/releaseadapter",
			"ReleaseDomainTransactionEnvelope",
			"release-domain-transaction.fugue.dev",
		} {
			if strings.Contains(string(data), forbidden) {
				t.Fatalf("%s unexpectedly activates dormant transaction symbol %q", relative, forbidden)
			}
		}
	}

	err := filepath.WalkDir(repositoryRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "vendor":
				return filepath.SkipDir
			default:
				return nil
			}
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		relative, err := filepath.Rel(repositoryRoot, path)
		if err != nil {
			return err
		}
		if strings.HasPrefix(filepath.ToSlash(relative), "internal/releaseadapter/") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(data), `"fugue/internal/releaseadapter"`) {
			t.Errorf("production source %s imports dormant releaseadapter package", relative)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk repository production sources: %v", err)
	}
}
