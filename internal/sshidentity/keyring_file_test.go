package sshidentity

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadKeyringRejectsUnsafeFilesAndReadsRecovery(test *testing.T) {
	path := filepath.Join(test.TempDir(), "keyring.json")
	if _, err := LoadKeyring(path); !errors.Is(err, ErrKeyUnavailable) {
		test.Fatal("missing file was not rejected", err)
	}
	raw, err := json.Marshal(map[string]any{
		"schema":        "fugue.ssh-identity-keyring/v1",
		"active_key_id": "storage-v1",
		"keys":          map[string]string{"storage-v1": base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{9}, 32))},
	})
	if err != nil {
		test.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		test.Fatal(err)
	}
	keyring, err := LoadKeyring(path)
	if err != nil || !keyring.Configured() {
		test.Fatal("valid keyring not loaded", err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		test.Fatal(err)
	}
	if _, err := LoadKeyring(path); !errors.Is(err, ErrKeyUnavailable) {
		test.Fatal("readable-by-other-users keyring was accepted", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		test.Fatal(err)
	}
	if err := os.WriteFile(path, append(raw, []byte(" {}")...), 0o600); err != nil {
		test.Fatal(err)
	}
	if _, err := LoadKeyring(path); !errors.Is(err, ErrKeyUnavailable) {
		test.Fatal("invalid trailing configuration was accepted", err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		test.Fatal(err)
	}
	if _, err := LoadKeyring(path); err != nil {
		test.Fatal("restoring key configuration required a code change", err)
	}
}
