package api

import "testing"

func TestNormalizeAppFilesUsesDefaultUniAPIPath(t *testing.T) {
	files, err := normalizeAppFiles("providers: []", nil)
	if err != nil {
		t.Fatalf("normalize app files: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(files))
	}
	if files[0].Path != "/home/api.yaml" {
		t.Fatalf("unexpected file path: %s", files[0].Path)
	}
	if files[0].Mode != 0o600 {
		t.Fatalf("unexpected file mode: %d", files[0].Mode)
	}
}
