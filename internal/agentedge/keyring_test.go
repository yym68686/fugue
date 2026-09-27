package agentedge

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestKeyConfigurationSeparatesPrivateAuthorityFromPublicTrust(t *testing.T) {
	_, private, keys, now := grantFixture(t)
	anchor := keys["key-one"]
	k := PrivateKeyring{Schema: PrivateKeyringSchema, Generation: 1, Keys: []PrivateKeyConfig{{PublicKeyConfig: PublicKeyConfig{
		KeyID: "key-one", PublicKey: base64.RawURLEncoding.EncodeToString(anchor.PublicKey), NotBefore: anchor.NotBefore, NotAfter: anchor.NotAfter}, PrivateKey: base64.RawURLEncoding.EncodeToString(private)}}}
	path := filepath.Join(t.TempDir(), "keys.json")
	raw, _ := json.Marshal(k)
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadPrivateKeyring(path)
	if err != nil {
		t.Fatal(err)
	}
	key, _, err := loaded.SigningKey("key-one", now)
	if err != nil || !key.Equal(ed25519.PrivateKey(private)) {
		t.Fatal("private key projection changed", err)
	}
	public, _ := json.Marshal(k.Public())
	if strings.Contains(string(public), "private_key") || strings.Contains(string(public), k.Keys[0].PrivateKey) {
		t.Fatal("public configuration leaked signing authority")
	}
	if err := os.WriteFile(path, public, 0600); err != nil {
		t.Fatal(err)
	}
	trust, err := LoadTrustKeyring(path)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := trust.PublicKeys()
	if err != nil || !verified["key-one"].PublicKey.Equal(anchor.PublicKey) {
		t.Fatal("public trust projection changed", err)
	}
	if _, err := LoadPrivateKeyring(path); err == nil {
		t.Fatal("public trust could mint grants")
	}
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadTrustKeyring(path); err == nil {
		t.Fatal("client loaded a private key configuration")
	}
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadPrivateKeyring(path); err == nil {
		t.Fatal("world-readable private key accepted")
	}
	k.Keys[0].Revoked = true
	k.Keys[0].PrivateKey = ""
	if err := k.Validate(); err != nil {
		t.Fatal("revocation required retaining private material", err)
	}
	if _, _, err := k.SigningKey("key-one", now); err == nil {
		t.Fatal("revoked key can sign")
	}
}

func TestKeyConfigurationRejectsCorruptionAndAmbiguity(t *testing.T) {
	_, private, keys, _ := grantFixture(t)
	key := keys["key-one"]
	k := PrivateKeyring{Schema: PrivateKeyringSchema, Generation: 1, Keys: []PrivateKeyConfig{{PublicKeyConfig: PublicKeyConfig{
		KeyID: "key-one", PublicKey: base64.RawURLEncoding.EncodeToString(key.PublicKey), NotBefore: key.NotBefore, NotAfter: key.NotAfter}, PrivateKey: base64.RawURLEncoding.EncodeToString(private)}}}
	for _, scenario := range []string{"bad pair", "duplicate", "future order", "zero generation", "private missing"} {
		t.Run(scenario, func(t *testing.T) {
			raw, _ := json.Marshal(k)
			var bad PrivateKeyring
			json.Unmarshal(raw, &bad)
			switch scenario {
			case "bad pair":
				bad.Keys[0].PublicKey = base64.RawURLEncoding.EncodeToString(make([]byte, 32))
			case "duplicate":
				bad.Keys = append(bad.Keys, bad.Keys[0])
			case "future order":
				bad.Keys[0].NotAfter = bad.Keys[0].NotBefore
			case "zero generation":
				bad.Generation = 0
			case "private missing":
				bad.Keys[0].PrivateKey = ""
			}
			if bad.Validate() == nil {
				t.Fatal("invalid key configuration accepted")
			}
		})
	}
}
