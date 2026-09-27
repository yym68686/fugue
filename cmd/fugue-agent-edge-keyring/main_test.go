package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestExplicitKeyGenerationCannotOverwriteOrExposePrivateAuthority(t *testing.T) {
	private := filepath.Join(t.TempDir(), "private.json")
	public := filepath.Join(t.TempDir(), "public.json")
	args := []string{"generate", "-private-file", private, "-key-id", "test-key", "-generation", "1", "-not-before", "2026-01-01T00:00:00Z", "-not-after", "2027-01-01T00:00:00Z"}
	var out bytes.Buffer
	if err := run(args, &out); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "private_key") {
		t.Fatal("command exported private key")
	}
	if err := os.WriteFile(public, out.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(private)
	if err != nil {
		t.Fatal(err)
	}
	info, _ := os.Stat(private)
	if info.Mode().Perm() != 0600 || !bytes.Contains(raw, []byte("private_key")) {
		t.Fatal("private persistence missing or unprotected")
	}
	out.Reset()
	if err := run([]string{"verify", "-private-file", private, "-public-file", public}, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), `"verified":true`) || strings.Contains(out.String(), "key_id") {
		t.Fatal("unexpected verification output")
	}
	if err := run(args, &out); err == nil {
		t.Fatal("explicit generation overwrote an existing root")
	}
	after, _ := os.ReadFile(private)
	if !bytes.Equal(raw, after) {
		t.Fatal("refused generation changed private file")
	}
	var changed map[string]any
	json.Unmarshal([]byte(`{"schema":"fugue.agent-edge-trust/v1","generation":2,"keys":[]}`), &changed)
	b, _ := json.Marshal(changed)
	os.WriteFile(public, b, 0600)
	if err := run([]string{"verify", "-private-file", private, "-public-file", public}, &out); err == nil {
		t.Fatal("foreign declared trust accepted")
	}
}
