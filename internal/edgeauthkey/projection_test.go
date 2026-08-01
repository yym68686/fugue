package edgeauthkey

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadAtomicProjectionRejectsMixedGeneration(t *testing.T) {
	dir := t.TempDir()
	writeProjection := func(generation, key string) string {
		t.Helper()
		data := filepath.Join(dir, ".."+generation)
		if err := os.Mkdir(data, 0o700); err != nil {
			t.Fatal(err)
		}
		for name, value := range map[string]string{KeyFile: key, KeyIDFile: "public-data-plane-release", GenerationFile: generation} {
			if err := os.WriteFile(filepath.Join(data, name), []byte(value+"\n"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		return filepath.Base(data)
	}
	first := writeProjection("generation-00000001", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err := os.Symlink(first, filepath.Join(dir, "..data")); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{KeyFile, KeyIDFile, GenerationFile} {
		if err := os.Symlink(filepath.Join("..data", name), filepath.Join(dir, name)); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := Load(dir)
	if err != nil || snapshot.Generation != "generation-00000001" {
		t.Fatalf("load projection: %+v err=%v", snapshot, err)
	}
	second := writeProjection("generation-00000002", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	if err := os.Remove(filepath.Join(dir, "..data")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(second, filepath.Join(dir, "..data")); err != nil {
		t.Fatal(err)
	}
	rotated, err := Load(dir)
	if err != nil || rotated.Generation != "generation-00000002" || Equal(snapshot, rotated) {
		t.Fatalf("rotation was not distinguished: old=%+v new=%+v err=%v", snapshot, rotated, err)
	}
	// Mixing a new key with the old generation is a semantically different
	// snapshot and cannot validate an envelope signed by either stable tuple.
	if err := os.WriteFile(filepath.Join(dir, second, GenerationFile), []byte("generation-00000001\n"), 0o400); err != nil {
		t.Fatal(err)
	}
	mixed, err := Load(dir)
	if err != nil || Equal(snapshot, mixed) || Equal(rotated, mixed) {
		t.Fatalf("mixed key/generation was accepted as a stable known tuple: %+v err=%v", mixed, err)
	}
}
