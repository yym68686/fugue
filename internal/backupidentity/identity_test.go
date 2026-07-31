package backupidentity

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"
)

const (
	testCell       = "backup/app-database/0123456789abcdef"
	testSpecDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
)

func TestBackupObserverIdentityBindsOneExactRunSpecAndCell(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
	keyring := testKeyring()
	token, err := Issue(keyring, testClaims(), now, 5*time.Minute)
	if err != nil {
		t.Fatalf("issue identity: %v", err)
	}
	if !strings.HasPrefix(token, "fugue_bo_v1.") || strings.HasPrefix(token, "fugue_pc_v1.") || strings.HasPrefix(token, "fugue_wk_") {
		t.Fatalf("identity token crossed another credential domain: %q", token)
	}
	claims, err := Parse(keyring, token, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("parse identity: %v", err)
	}
	if claims.CredentialID != CredentialIDForCell(testCell) || claims.RunID != "run-1" ||
		claims.TenantID != "tenant-1" || claims.CellKey != testCell || claims.SpecDigest != testSpecDigest ||
		claims.Permission != PermissionReadRunObservation || claims.TokenID == "" ||
		claims.IssuedAtUnix != now.Unix() || claims.ExpiresAtUnix != now.Add(5*time.Minute).Unix() {
		t.Fatalf("parsed claims drifted: %+v", claims)
	}
}

func TestBackupObserverIdentitySupportsRotationAndExplicitRevocation(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
	old := DeriveKeyring(strings.Repeat("o", 32), "backup-key-1", "", "", nil)
	token, err := Issue(old, testClaims(), now, 5*time.Minute)
	if err != nil {
		t.Fatalf("issue old identity: %v", err)
	}
	rotated := DeriveKeyring(strings.Repeat("n", 32), "backup-key-2", strings.Repeat("o", 32), "backup-key-1", nil)
	if rotated.Keys["backup-key-2"] == strings.Repeat("n", 32) || rotated.Keys["backup-key-1"] == strings.Repeat("o", 32) {
		t.Fatal("source keys were not domain-separated")
	}
	if _, err := Parse(rotated, token, now.Add(time.Minute)); err != nil {
		t.Fatalf("rotation overlap rejected in-flight identity: %v", err)
	}
	revoked := DeriveKeyring(strings.Repeat("n", 32), "backup-key-2", strings.Repeat("o", 32), "backup-key-1", []string{"backup-key-1"})
	if _, ok := revoked.Keys["backup-key-1"]; ok {
		t.Fatal("revoked verification key remained active")
	}
	if _, err := Parse(revoked, token, now.Add(time.Minute)); !errors.Is(err, ErrInvalidIdentity) {
		t.Fatalf("revoked identity error = %v, want invalid", err)
	}
	activeRevoked := DeriveKeyring(strings.Repeat("n", 32), "backup-key-2", "", "", []string{"backup-key-2"})
	if activeRevoked.ActiveKeyID != "" {
		t.Fatalf("revoked active key remained issuable: %+v", activeRevoked)
	}
}

func TestBackupObserverIdentityRejectsExpiredFutureTamperedAndOverbroadClaims(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
	keyring := testKeyring()
	token, err := Issue(keyring, testClaims(), now, time.Minute)
	if err != nil {
		t.Fatalf("issue identity: %v", err)
	}
	if _, err := Parse(keyring, token, now.Add(time.Minute)); !errors.Is(err, ErrExpiredIdentity) {
		t.Fatalf("expired identity error = %v", err)
	}
	tampered := token[:len(token)-1] + differentSuffix(token[len(token)-1:])
	if _, err := Parse(keyring, tampered, now.Add(30*time.Second)); !errors.Is(err, ErrInvalidIdentity) {
		t.Fatalf("tampered identity error = %v", err)
	}
	future, err := Issue(keyring, testClaims(), now.Add(FutureSkew+time.Second), time.Minute)
	if err != nil {
		t.Fatalf("issue future identity: %v", err)
	}
	if _, err := Parse(keyring, future, now); !errors.Is(err, ErrInvalidIdentity) {
		t.Fatalf("future identity error = %v", err)
	}
	for name, mutate := range map[string]func(*Claims){
		"wrong credential":    func(claims *Claims) { claims.CredentialID = "backup-observer:other" },
		"noncanonical run":    func(claims *Claims) { claims.RunID = "Run-1" },
		"noncanonical tenant": func(claims *Claims) { claims.TenantID = "Tenant-1" },
		"wrong cell":          func(claims *Claims) { claims.CellKey = "backup/all/0123456789abcdef" },
		"wrong digest":        func(claims *Claims) { claims.SpecDigest = "sha256:ABC" },
		"wrong permission":    func(claims *Claims) { claims.Permission = "backup.write" },
	} {
		t.Run(name, func(t *testing.T) {
			claims := parsedUnsignedClaims(t, token)
			mutate(&claims)
			if _, err := normalizeClaims(claims); !errors.Is(err, ErrInvalidIdentity) {
				t.Fatalf("overbroad claims error = %v, want invalid", err)
			}
		})
	}
	if _, err := Issue(DeriveKeyring("weak", "backup-key-1", "", "", nil), testClaims(), now, time.Minute); !errors.Is(err, ErrInvalidIdentity) {
		t.Fatalf("weak key issuance error = %v, want invalid", err)
	}
	if _, err := Issue(keyring, testClaims(), now, MaxTTL+time.Second); !errors.Is(err, ErrInvalidIdentity) {
		t.Fatalf("overlong TTL error = %v, want invalid", err)
	}
}

func TestBackupObserverIdentityDependencyBoundary(t *testing.T) {
	t.Parallel()
	command := exec.Command("go", "list", "-f", `{{join .Imports "\n"}}`, ".")
	output, err := command.Output()
	if err != nil {
		t.Fatalf("list identity dependencies: %v", err)
	}
	local := []string{}
	for _, dependency := range strings.Fields(string(output)) {
		if strings.HasPrefix(dependency, "fugue/") {
			local = append(local, dependency)
		}
	}
	if !reflect.DeepEqual(local, []string{}) {
		t.Fatalf("backup identity direct dependency boundary widened: %v", local)
	}
}

func testKeyring() Keyring {
	return DeriveKeyring(strings.Repeat("k", 32), "backup-key-1", "", "", nil)
}

func testClaims() Claims {
	return Claims{
		CredentialID: CredentialIDForCell(testCell), RunID: "run-1", TenantID: "tenant-1",
		CellKey: testCell, SpecDigest: testSpecDigest,
	}
}

func differentSuffix(current string) string {
	if current == "x" {
		return "y"
	}
	return "x"
}

func parsedUnsignedClaims(t *testing.T, token string) Claims {
	t.Helper()
	parts := strings.Split(strings.TrimPrefix(token, identityTokenPrefix), ".")
	if len(parts) != 3 {
		t.Fatalf("unexpected token shape: %q", token)
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("decode token payload: %v", err)
	}
	var claims Claims
	if err := json.Unmarshal(payload, &claims); err != nil {
		t.Fatalf("decode claims: %v", err)
	}
	return claims
}
