package entryfailover

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCredentialVaultEncryptsRotatesAndScopesToken(t *testing.T) {
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "key")
	if err := NewVaultKey(keyPath); err != nil {
		t.Fatal(err)
	}
	if err := NewVaultKey(keyPath); err == nil {
		t.Fatal("vault key overwritten")
	}
	key, err := LoadVaultKey(keyPath)
	if err != nil {
		t.Fatal(err)
	}
	vault := CredentialVault{StateDir: filepath.Join(dir, "state"), Key: key}
	p := testPolicy()
	if n, err := vault.Put(p, "secret-one"); err != nil || n != 1 {
		t.Fatalf("initial import %d %v", n, err)
	}
	raw, err := os.ReadFile(vault.path())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "secret-one") {
		t.Fatal("token stored in plaintext")
	}
	if n, err := vault.Put(p, "secret-two"); err != nil || n != 2 {
		t.Fatalf("rotation %d %v", n, err)
	}
	got, err := vault.Token(p)
	if err != nil || got != "secret-two" {
		t.Fatalf("token after rotation %q %v", got, err)
	}
	other := p
	other.TenantID = "another-tenant"
	if _, err = vault.Token(other); err == nil {
		t.Fatal("cross-tenant credential read allowed")
	}
	wrong := vault
	wrong.Key = make([]byte, 32)
	if _, err = wrong.Token(p); err == nil {
		t.Fatal("wrong key decrypted credential")
	}
}
