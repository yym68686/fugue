package backupmaterializer

import (
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"fugue/internal/backupcontrol"
	"fugue/internal/backupidentity"
	materializercontract "fugue/internal/backupmaterializer/contract"
)

func TestObserverInputBundleBindsOneExactSpecAndCredentialGeneration(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 30, 8, 0, 0, 0, time.UTC)
	keyring := testBundleKeyring()
	spec := testBundleSpec(t)
	bundle, err := IssueObserverInputBundle(keyring, spec, "tenant-1", now)
	if err != nil {
		t.Fatalf("issue input bundle: %v", err)
	}
	if bundle.APIVersion != ObserverInputBundleAPIVersion || bundle.Kind != ObserverInputBundleKind ||
		bundle.Policy != ObserverInputBundlePolicy || bundle.CellKey != spec.CellKey || bundle.RunID != spec.RunID ||
		bundle.SpecDigest != spec.Digest || bundle.CredentialID != backupidentity.CredentialIDForCell(spec.CellKey) ||
		bundle.TokenID == "" || !strings.HasPrefix(bundle.ObserverToken, "fugue_bo_v1.") ||
		bundle.IssuedAt != now || bundle.RenewAfter != now.Add(ObserverIdentityRenewAfter) ||
		bundle.ExpiresAt != now.Add(ObserverIdentityTTL) || !bundle.ObservationOnly || bundle.ProductionMutationAllowed ||
		bundle.Digest == "" || bundle.Digest != DigestObserverInputBundle(bundle) {
		t.Fatalf("issued input bundle drifted: %#v", bundle)
	}
	claims, err := backupidentity.Parse(keyring, bundle.ObserverToken, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("parse issued observer identity: %v", err)
	}
	if claims.RunID != spec.RunID || claims.TenantID != "tenant-1" || claims.CellKey != spec.CellKey ||
		claims.SpecDigest != spec.Digest || claims.CredentialID != bundle.CredentialID || claims.TokenID != bundle.TokenID {
		t.Fatalf("issued token escaped bundle binding: %+v", claims)
	}
	document, err := json.Marshal(bundle)
	if err != nil {
		t.Fatalf("marshal input bundle: %v", err)
	}
	decoded, err := DecodeObserverInputBundle(document, keyring, now.Add(time.Minute))
	if err != nil || !reflect.DeepEqual(decoded, bundle) {
		t.Fatalf("decode input bundle: decoded=%#v err=%v", decoded, err)
	}
	envelope, err := DecodeObserverInputBundleEnvelope(document, now.Add(time.Minute))
	if err != nil || !reflect.DeepEqual(envelope, bundle) {
		t.Fatalf("decode transport envelope: decoded=%#v err=%v", envelope, err)
	}
	reissued, err := IssueObserverInputBundle(keyring, spec, "tenant-1", now)
	if err != nil {
		t.Fatalf("reissue input bundle: %v", err)
	}
	if reissued.TokenID == bundle.TokenID || reissued.ObserverToken == bundle.ObserverToken || reissued.Digest == bundle.Digest {
		t.Fatal("reissuing one spec reused a bearer credential generation")
	}
	for _, forbidden := range []string{"bucket", "endpoint", "objectKey", "secretAccessKey", "credentialEncryptionKey"} {
		if strings.Contains(string(document), forbidden) {
			t.Fatalf("input bundle exposed physical backend material %q: %s", forbidden, document)
		}
	}
}

func TestObserverInputBundleEnvelopeSeparatesTransportValidationFromSigningCapability(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 30, 8, 0, 0, 0, time.UTC)
	keyring := testBundleKeyring()
	bundle, err := IssueObserverInputBundle(keyring, testBundleSpec(t), "tenant-1", now)
	if err != nil {
		t.Fatalf("issue input bundle: %v", err)
	}
	if err := ValidateObserverInputBundleEnvelope(bundle, now.Add(time.Minute)); err != nil {
		t.Fatalf("validate authenticated transport envelope: %v", err)
	}

	// The transport validator deliberately has no signing key. It verifies the
	// complete clear-text token envelope but cannot authenticate the HMAC. The
	// full validator remains the authority whenever a keyring is available.
	parts := strings.Split(strings.TrimPrefix(bundle.ObserverToken, "fugue_bo_v1."), ".")
	if len(parts) != 3 || parts[2] == "" {
		t.Fatalf("issued token shape drifted: %q", bundle.ObserverToken)
	}
	replacement := byte('A')
	if parts[2][0] == replacement {
		replacement = 'B'
	}
	parts[2] = string(replacement) + parts[2][1:]
	tampered := bundle
	tampered.ObserverToken = "fugue_bo_v1." + strings.Join(parts, ".")
	tampered.Digest = DigestObserverInputBundle(tampered)
	if err := ValidateObserverInputBundleEnvelope(tampered, now.Add(time.Minute)); err != nil {
		t.Fatalf("transport envelope unexpectedly acquired signing authority: %v", err)
	}
	if err := ValidateObserverInputBundle(tampered, keyring, now.Add(time.Minute)); !errors.Is(err, ErrObserverInputBundle) {
		t.Fatalf("full validator accepted a tampered HMAC: %v", err)
	}

	future, err := IssueObserverInputBundle(
		keyring,
		testBundleSpec(t),
		"tenant-1",
		now.Add(backupidentity.FutureSkew+time.Second),
	)
	if err != nil {
		t.Fatalf("issue future input bundle: %v", err)
	}
	if err := ValidateObserverInputBundleEnvelope(future, now); !errors.Is(err, ErrObserverInputBundle) {
		t.Fatalf("future envelope error = %v, want invalid bundle", err)
	}
	if err := ValidateObserverInputBundleEnvelope(bundle, bundle.ExpiresAt); !errors.Is(err, ErrObserverInputBundle) {
		t.Fatalf("expired envelope error = %v, want invalid bundle", err)
	}
}

func TestObserverInputBundleRejectsBindingDigestTimeAndCredentialDrift(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 30, 8, 0, 0, 0, time.UTC)
	keyring := testBundleKeyring()
	bundle, err := IssueObserverInputBundle(keyring, testBundleSpec(t), "tenant-1", now)
	if err != nil {
		t.Fatalf("issue input bundle: %v", err)
	}
	tests := map[string]func(*ObserverInputBundle){
		"API version":         func(value *ObserverInputBundle) { value.APIVersion = "backup-materializer.fugue.dev/v2" },
		"policy":              func(value *ObserverInputBundle) { value.Policy = "write-enabled" },
		"production mutation": func(value *ObserverInputBundle) { value.ProductionMutationAllowed = true },
		"cell":                func(value *ObserverInputBundle) { value.CellKey = "backup/registry/0123456789abcdef" },
		"run":                 func(value *ObserverInputBundle) { value.RunID = "other-run" },
		"spec digest": func(value *ObserverInputBundle) {
			value.SpecDigest = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		},
		"credential": func(value *ObserverInputBundle) { value.CredentialID = "backup-observer:other" },
		"token id":   func(value *ObserverInputBundle) { value.TokenID = strings.Repeat("a", 22) },
		"token": func(value *ObserverInputBundle) {
			value.ObserverToken = value.ObserverToken[:len(value.ObserverToken)-1] + "x"
		},
		"renewal": func(value *ObserverInputBundle) { value.RenewAfter = value.RenewAfter.Add(time.Second) },
		"expiry":  func(value *ObserverInputBundle) { value.ExpiresAt = value.ExpiresAt.Add(time.Second) },
		"timezone": func(value *ObserverInputBundle) {
			value.IssuedAt = value.IssuedAt.In(time.FixedZone("offset", 3600))
		},
		"desired spec": func(value *ObserverInputBundle) { value.DesiredSpec.RunID = "other-run" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			candidate := bundle
			mutate(&candidate)
			candidate.Digest = DigestObserverInputBundle(candidate)
			if err := ValidateObserverInputBundle(candidate, keyring, now.Add(time.Minute)); !errors.Is(err, ErrObserverInputBundle) {
				t.Fatalf("drift validation error = %v, want invalid bundle", err)
			}
		})
	}
	if err := ValidateObserverInputBundle(bundle, keyring, time.Time{}); !errors.Is(err, ErrObserverInputBundle) {
		t.Fatalf("zero validation time error = %v", err)
	}
	if err := ValidateObserverInputBundle(bundle, keyring, bundle.ExpiresAt); !errors.Is(err, ErrObserverInputBundle) {
		t.Fatalf("expired bundle validation error = %v", err)
	}
}

func TestObserverInputBundleSupportsSignerRotationAndRevocation(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 30, 8, 0, 0, 0, time.UTC)
	old := backupidentity.DeriveKeyring(strings.Repeat("o", 32), "backup-key-1", "", "", nil)
	bundle, err := IssueObserverInputBundle(old, testBundleSpec(t), "tenant-1", now)
	if err != nil {
		t.Fatalf("issue old input bundle: %v", err)
	}
	rotated := backupidentity.DeriveKeyring(strings.Repeat("n", 32), "backup-key-2", strings.Repeat("o", 32), "backup-key-1", nil)
	if err := ValidateObserverInputBundle(bundle, rotated, now.Add(time.Minute)); err != nil {
		t.Fatalf("rotation overlap rejected in-flight bundle: %v", err)
	}
	revoked := backupidentity.DeriveKeyring(strings.Repeat("n", 32), "backup-key-2", strings.Repeat("o", 32), "backup-key-1", []string{"backup-key-1"})
	if err := ValidateObserverInputBundle(bundle, revoked, now.Add(time.Minute)); !errors.Is(err, ErrObserverInputBundle) {
		t.Fatalf("revoked bundle error = %v, want invalid bundle", err)
	}
	if _, err := IssueObserverInputBundle(backupidentity.Keyring{}, testBundleSpec(t), "tenant-1", now); !errors.Is(err, ErrObserverInputBundle) {
		t.Fatalf("missing signer error = %v, want invalid bundle", err)
	}
}

func TestObserverInputBundleContractAndIdentityPolicyRemainAligned(t *testing.T) {
	t.Parallel()
	if materializercontract.ObserverIdentityFutureSkew != backupidentity.FutureSkew ||
		materializercontract.ObserverIdentityPermission != backupidentity.PermissionReadRunObservation {
		t.Fatalf(
			"wire/identity constants drifted: skew=%s/%s permission=%q/%q",
			materializercontract.ObserverIdentityFutureSkew,
			backupidentity.FutureSkew,
			materializercontract.ObserverIdentityPermission,
			backupidentity.PermissionReadRunObservation,
		)
	}
	for _, kind := range []string{
		backupcontrol.TargetControlPlaneDatabase,
		backupcontrol.TargetAppDatabase,
		backupcontrol.TargetPersistentStorage,
		backupcontrol.TargetDataWorkspace,
		backupcontrol.TargetRegistry,
		backupcontrol.TargetPlatformComponent,
	} {
		cellKey := "backup/" + kind + "/0123456789abcdef"
		if got, want := materializercontract.CredentialIDForCell(cellKey), backupidentity.CredentialIDForCell(cellKey); got == "" || got != want {
			t.Fatalf("wire credential ID for %q = %q, identity policy = %q", cellKey, got, want)
		}
	}
}

func TestObserverInputBundleDecoderIsStrictBoundedAndRedactedByFormatting(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 30, 8, 0, 0, 0, time.UTC)
	keyring := testBundleKeyring()
	bundle, err := IssueObserverInputBundle(keyring, testBundleSpec(t), "tenant-1", now)
	if err != nil {
		t.Fatalf("issue input bundle: %v", err)
	}
	document, err := json.Marshal(bundle)
	if err != nil {
		t.Fatalf("marshal input bundle: %v", err)
	}
	unknown := append([]byte(nil), document[:len(document)-1]...)
	unknown = append(unknown, []byte(`,"unexpected":true}`)...)
	for name, candidate := range map[string][]byte{
		"empty":     nil,
		"unknown":   unknown,
		"trailing":  append(append([]byte(nil), document...), []byte(` {}`)...),
		"oversized": []byte(strings.Repeat("x", MaxObserverInputBundleBytes+1)),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeObserverInputBundle(candidate, keyring, now.Add(time.Minute)); !errors.Is(err, ErrObserverInputBundle) {
				t.Fatalf("decode error = %v, want invalid bundle", err)
			}
		})
	}
	for _, rendered := range []string{bundle.String(), bundle.GoString(), fmt.Sprint(bundle), fmt.Sprintf("%#v", bundle)} {
		if strings.Contains(rendered, bundle.ObserverToken) || !strings.Contains(rendered, "[REDACTED]") {
			t.Fatalf("diagnostic formatting leaked observer token: %q", rendered)
		}
	}
	if !strings.Contains(string(document), bundle.ObserverToken) {
		t.Fatal("private wire payload unexpectedly omitted the observer token")
	}
}

func TestObserverInputBundleDependencyBoundary(t *testing.T) {
	t.Parallel()
	command := exec.Command("go", "list", "-f", `{{join .Imports "\n"}}`, ".")
	output, err := command.Output()
	if err != nil {
		t.Fatalf("list materializer dependencies: %v", err)
	}
	local := []string{}
	for _, dependency := range strings.Fields(string(output)) {
		if strings.HasPrefix(dependency, "fugue/") {
			local = append(local, dependency)
		}
	}
	sort.Strings(local)
	want := []string{
		"fugue/internal/backupcontrol",
		"fugue/internal/backupidentity",
		"fugue/internal/backupmaterializer/contract",
	}
	if !reflect.DeepEqual(local, want) {
		t.Fatalf("backup materializer dependency boundary widened: got=%v want=%v", local, want)
	}

	contractCommand := exec.Command("go", "list", "-f", `{{join .Imports "\n"}}`, "./contract")
	contractOutput, err := contractCommand.Output()
	if err != nil {
		t.Fatalf("list pure materializer contract dependencies: %v", err)
	}
	contractImports := strings.Fields(string(contractOutput))
	sort.Strings(contractImports)
	wantContractImports := []string{
		"bytes",
		"crypto/sha256",
		"encoding/base64",
		"encoding/hex",
		"encoding/json",
		"errors",
		"fmt",
		"fugue/internal/backupcontrol",
		"io",
		"regexp",
		"strings",
		"time",
	}
	if !reflect.DeepEqual(contractImports, wantContractImports) {
		t.Fatalf("pure materializer contract dependency boundary widened: got=%v want=%v", contractImports, wantContractImports)
	}
}

func testBundleKeyring() backupidentity.Keyring {
	return backupidentity.DeriveKeyring(strings.Repeat("k", 32), "backup-key-1", "", "", nil)
}

func testBundleSpec(t *testing.T) backupcontrol.BackupRunSpec {
	t.Helper()
	spec, err := backupcontrol.NewShadowBackupRunSpec(
		"run-1",
		"run-1",
		backupcontrol.BackupTarget{Type: backupcontrol.TargetAppDatabase, ScopeKey: "app/app-1/database"},
		"backend-1",
		"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		4,
		120,
		1800,
	)
	if err != nil {
		t.Fatalf("build test backup spec: %v", err)
	}
	return spec
}
