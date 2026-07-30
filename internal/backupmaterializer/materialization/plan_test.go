package materialization

import (
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"fugue/internal/backupcontrol"
	"fugue/internal/backupidentity"
	"fugue/internal/backupmaterializer"
	materializercontract "fugue/internal/backupmaterializer/contract"
)

func TestBuildSealsOneExactNonExecutableSecretGeneration(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 31, 1, 0, 0, 0, time.UTC)
	bundle := testBundle(t, now)
	plan, err := Build(bundle, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("build Secret plan: %v", err)
	}
	wantCellID := strings.ReplaceAll(strings.TrimPrefix(bundle.CellKey, "backup/"), "/", "-")
	if plan.APIVersion != PlanAPIVersion || plan.Kind != PlanKind || plan.Policy != PlanPolicy ||
		plan.Namespace != SecretNamespace || plan.SecretName != "fugue-backup-observer-"+wantCellID+"-input" ||
		plan.CellKey != bundle.CellKey || plan.CellID != wantCellID || plan.RunID != bundle.RunID ||
		plan.SpecDigest != bundle.SpecDigest || plan.BundleDigest != bundle.Digest ||
		plan.CredentialID != bundle.CredentialID || plan.TokenID != bundle.TokenID || plan.DesiredSpec != bundle.DesiredSpec ||
		plan.SpecKey != SpecDataKey || plan.TokenKey != TokenDataKey ||
		plan.SpecDocumentDigest == "" || plan.ObserverTokenDigest == "" || plan.IssuedAt != bundle.IssuedAt ||
		plan.RenewAfter != bundle.RenewAfter || plan.ExpiresAt != bundle.ExpiresAt ||
		!strings.HasPrefix(plan.IdempotencyKey, "backup-observer-input/"+wantCellID+"/") ||
		!plan.RetainExistingOnFailure || !plan.RequireResourceVersionCAS || !plan.LastKnownGoodRequired ||
		!plan.ObservationOnly || plan.ExecutionAllowed || plan.ProductionMutationAllowed ||
		plan.Digest == "" || plan.Digest != DigestPlan(plan) {
		t.Fatalf("materialization plan drifted: %#v", plan)
	}
	data, err := plan.Data(now.Add(time.Minute))
	if err != nil {
		t.Fatalf("read private Secret data: %v", err)
	}
	decodedSpec, err := backupcontrol.DecodeBackupRunSpec(data.SpecDocument)
	if err != nil || decodedSpec != bundle.DesiredSpec || data.SpecKey != SpecDataKey || data.TokenKey != TokenDataKey ||
		string(data.ObserverToken) != bundle.ObserverToken {
		t.Fatalf("private Secret data drifted: data=%#v spec=%#v err=%v", data, decodedSpec, err)
	}
	data.SpecDocument[0] ^= 0xff
	data.ObserverToken[0] ^= 0xff
	fresh, err := plan.Data(now.Add(time.Minute))
	if err != nil || string(fresh.ObserverToken) != bundle.ObserverToken || fresh.SpecDocument[0] == data.SpecDocument[0] {
		t.Fatalf("returned Secret data aliases immutable plan state: fresh=%#v err=%v", fresh, err)
	}
	rebuilt, err := Build(bundle, now.Add(time.Minute))
	if err != nil || !reflect.DeepEqual(rebuilt, plan) {
		t.Fatalf("same bundle did not produce one idempotent plan: rebuilt=%#v err=%v", rebuilt, err)
	}
}

func TestSecretIdentityIsCanonicalForEveryBackupCellKind(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{
		backupcontrol.TargetControlPlaneDatabase,
		backupcontrol.TargetAppDatabase,
		backupcontrol.TargetPersistentStorage,
		backupcontrol.TargetDataWorkspace,
		backupcontrol.TargetRegistry,
		backupcontrol.TargetPlatformComponent,
	} {
		cellKey := "backup/" + kind + "/0123456789abcdef"
		cellID := kind + "-0123456789abcdef"
		name := secretNameForCell(cellKey)
		identity, err := SecretIdentityForCell(cellKey)
		if got := cellIDForKey(cellKey); got != cellID ||
			err != nil || identity.Namespace != SecretNamespace || identity.SecretName != name ||
			identity.CellKey != cellKey || identity.CellID != cellID ||
			name != "fugue-backup-observer-"+cellID+"-input" || len(name) > 63 || !canonicalName.MatchString(name) {
			t.Fatalf("cell %q produced invalid identity: id=%q name=%q identity=%#v err=%v", cellKey, got, name, identity, err)
		}
	}
	for _, invalid := range []string{"", "backup/app-database/ABC", "backup/unknown/0123456789abcdef"} {
		if cellIDForKey(invalid) != "" || secretNameForCell(invalid) != "" {
			t.Fatalf("invalid cell %q produced a Secret identity", invalid)
		}
		if _, err := SecretIdentityForCell(invalid); !errors.Is(err, ErrPlan) {
			t.Fatalf("invalid cell %q identity error = %v, want invalid plan", invalid, err)
		}
	}
}

func TestPlanRejectsBindingDataPolicyAndTimeDrift(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 31, 1, 0, 0, 0, time.UTC)
	bundle := testBundle(t, now)
	plan, err := Build(bundle, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("build Secret plan: %v", err)
	}
	tests := map[string]func(*Plan){
		"API version":        func(value *Plan) { value.APIVersion = "backup-materialization.fugue.dev/v2" },
		"kind":               func(value *Plan) { value.Kind = "Other" },
		"policy":             func(value *Plan) { value.Policy = "write-any-secret" },
		"namespace":          func(value *Plan) { value.Namespace = "default" },
		"secret name":        func(value *Plan) { value.SecretName = "other" },
		"cell":               func(value *Plan) { value.CellKey = "backup/registry/0123456789abcdef" },
		"cell ID":            func(value *Plan) { value.CellID = "registry-0123456789abcdef" },
		"run":                func(value *Plan) { value.RunID = "other-run" },
		"spec digest":        func(value *Plan) { value.SpecDigest = strings.Replace(value.SpecDigest, "a", "b", 1) },
		"bundle digest":      func(value *Plan) { value.BundleDigest = strings.Replace(value.BundleDigest, "a", "b", 1) },
		"credential":         func(value *Plan) { value.CredentialID = "backup-observer:other" },
		"token ID":           func(value *Plan) { value.TokenID = strings.Repeat("A", 22) },
		"desired spec":       func(value *Plan) { value.DesiredSpec.RunID = "other-run" },
		"spec key":           func(value *Plan) { value.SpecKey = "other.json" },
		"token key":          func(value *Plan) { value.TokenKey = "other-token" },
		"spec digest field":  func(value *Plan) { value.SpecDocumentDigest = strings.Repeat("0", 64) },
		"token digest field": func(value *Plan) { value.ObserverTokenDigest = strings.Repeat("0", 64) },
		"issued time":        func(value *Plan) { value.IssuedAt = value.IssuedAt.Add(time.Second) },
		"renewal time":       func(value *Plan) { value.RenewAfter = value.RenewAfter.Add(time.Second) },
		"expiry time":        func(value *Plan) { value.ExpiresAt = value.ExpiresAt.Add(time.Second) },
		"idempotency":        func(value *Plan) { value.IdempotencyKey = "other" },
		"retain policy":      func(value *Plan) { value.RetainExistingOnFailure = false },
		"CAS policy":         func(value *Plan) { value.RequireResourceVersionCAS = false },
		"LKG policy":         func(value *Plan) { value.LastKnownGoodRequired = false },
		"observation mode":   func(value *Plan) { value.ObservationOnly = false },
		"execution":          func(value *Plan) { value.ExecutionAllowed = true },
		"production mutation": func(value *Plan) {
			value.ProductionMutationAllowed = true
		},
		"spec document": func(value *Plan) {
			value.specDocument = []byte(`{}`)
			value.SpecDocumentDigest = digestBytes(value.specDocument)
		},
		"observer token": func(value *Plan) {
			value.observerToken += "x"
			value.ObserverTokenDigest = digestBytes([]byte(value.observerToken))
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			candidate := plan
			candidate.specDocument = append([]byte(nil), plan.specDocument...)
			mutate(&candidate)
			candidate.Digest = DigestPlan(candidate)
			if err := Validate(candidate, now.Add(time.Minute)); !errors.Is(err, ErrPlan) {
				t.Fatalf("drift error = %v, want invalid plan", err)
			}
			if err := ValidateLastKnownGood(candidate, now.Add(time.Minute)); !errors.Is(err, ErrPlan) {
				t.Fatalf("last-known-good drift error = %v, want invalid plan", err)
			}
			if err := ValidateSealed(candidate); !errors.Is(err, ErrPlan) {
				t.Fatalf("sealed drift error = %v, want invalid plan", err)
			}
		})
	}
	badDigest := plan
	badDigest.Digest = strings.Repeat("0", 64)
	if err := Validate(badDigest, now.Add(time.Minute)); !errors.Is(err, ErrPlan) {
		t.Fatalf("plan digest drift error = %v, want invalid plan", err)
	}
	if err := ValidateLastKnownGood(badDigest, now.Add(time.Minute)); !errors.Is(err, ErrPlan) {
		t.Fatalf("last-known-good digest drift error = %v, want invalid plan", err)
	}
	if err := ValidateSealed(badDigest); !errors.Is(err, ErrPlan) {
		t.Fatalf("sealed digest drift error = %v, want invalid plan", err)
	}
	for name, validationTime := range map[string]time.Time{"zero": {}, "expired": plan.ExpiresAt} {
		t.Run(name, func(t *testing.T) {
			if err := Validate(plan, validationTime); !errors.Is(err, ErrPlan) {
				t.Fatalf("time validation error = %v, want invalid plan", err)
			}
			if err := ValidateLastKnownGood(plan, validationTime); !errors.Is(err, ErrPlan) {
				t.Fatalf("last-known-good time validation error = %v, want invalid plan", err)
			}
			if _, err := plan.Data(validationTime); !errors.Is(err, ErrPlan) {
				t.Fatalf("private data time error = %v, want invalid plan", err)
			}
		})
	}
	if err := ValidateSealed(plan); err != nil {
		t.Fatalf("sealed structural validation incorrectly applied a current lifetime gate: %v", err)
	}
	for name, validationTime := range map[string]time.Time{
		"delivery window elapsed": plan.IssuedAt.Add(materializercontract.MaxObserverInputDeliveryAge + time.Second),
		"renewal due":             plan.RenewAfter,
		"after renewal":           plan.RenewAfter.Add(time.Minute),
	} {
		t.Run(name, func(t *testing.T) {
			if err := Validate(plan, validationTime); !errors.Is(err, ErrPlan) {
				t.Fatalf("apply-window validation error = %v, want invalid plan", err)
			}
			if _, err := plan.Data(validationTime); !errors.Is(err, ErrPlan) {
				t.Fatalf("stale plan unexpectedly exposed private apply data: %v", err)
			}
			if err := ValidateLastKnownGood(plan, validationTime); err != nil {
				t.Fatalf("unexpired materialized LKG was not retainable: %v", err)
			}
		})
	}
	oldBundle := testBundle(t, now.Add(-2*materializercontract.MaxObserverInputDeliveryAge))
	if _, err := Build(oldBundle, now); !errors.Is(err, ErrPlan) {
		t.Fatalf("replayed bundle build error = %v, want invalid plan", err)
	}
}

func TestRestoreSealedRecoversHistoricalGenerationWithoutAuthorizingIt(t *testing.T) {
	t.Parallel()
	issuedAt := time.Date(2026, 7, 31, 2, 0, 0, 0, time.UTC)
	bundle := testBundle(t, issuedAt)
	built, err := Build(bundle, issuedAt.Add(30*time.Second))
	if err != nil {
		t.Fatalf("build fresh plan: %v", err)
	}
	restored, err := RestoreSealed(bundle)
	if err != nil || !reflect.DeepEqual(restored, built) {
		t.Fatalf("restored plan drifted: restored=%#v built=%#v err=%v", restored, built, err)
	}
	if err := ValidateSealed(restored); err != nil {
		t.Fatalf("restored structural binding failed: %v", err)
	}
	if _, err := restored.Data(restored.RenewAfter); !errors.Is(err, ErrPlan) {
		t.Fatalf("restored stale data error = %v, want apply denial", err)
	}
	if err := ValidateLastKnownGood(restored, restored.RenewAfter); err != nil {
		t.Fatalf("restored generation should remain retainable before expiry: %v", err)
	}
	if err := ValidateLastKnownGood(restored, restored.ExpiresAt); !errors.Is(err, ErrPlan) {
		t.Fatalf("restored expired LKG error = %v, want fail closed", err)
	}
	if err := ValidateSealed(restored); err != nil {
		t.Fatalf("current expiry leaked into structural recovery: %v", err)
	}

	tests := map[string]func(*materializercontract.ObserverInputBundle){
		"API version": func(value *materializercontract.ObserverInputBundle) {
			value.APIVersion = "backup-materializer.fugue.dev/v2"
			value.Digest = materializercontract.DigestObserverInputBundle(*value)
		},
		"renewal": func(value *materializercontract.ObserverInputBundle) {
			value.RenewAfter = value.RenewAfter.Add(time.Second)
			value.Digest = materializercontract.DigestObserverInputBundle(*value)
		},
		"token": func(value *materializercontract.ObserverInputBundle) {
			value.ObserverToken += "x"
			value.Digest = materializercontract.DigestObserverInputBundle(*value)
		},
		"digest": func(value *materializercontract.ObserverInputBundle) {
			value.Digest = strings.Repeat("0", 64)
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			candidate := bundle
			mutate(&candidate)
			if _, err := RestoreSealed(candidate); !errors.Is(err, ErrPlan) {
				t.Fatalf("restore drift error = %v, want invalid plan", err)
			}
		})
	}
}

func TestPlanSerializationAndFormattingNeverExposePrivateData(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 31, 1, 0, 0, 0, time.UTC)
	bundle := testBundle(t, now)
	plan, err := Build(bundle, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("build Secret plan: %v", err)
	}
	document, err := json.Marshal(plan)
	if err != nil {
		t.Fatalf("marshal public plan: %v", err)
	}
	if strings.Contains(string(document), bundle.ObserverToken) || strings.Contains(string(document), `"observerToken"`) ||
		strings.Contains(string(document), `"specDocument"`) {
		t.Fatalf("public plan JSON exposed private data: %s", document)
	}
	var decoded Plan
	if err := json.Unmarshal(document, &decoded); err != nil {
		t.Fatalf("decode public plan: %v", err)
	}
	if err := Validate(decoded, now.Add(time.Minute)); !errors.Is(err, ErrPlan) {
		t.Fatalf("public metadata unexpectedly reconstructed private plan: %v", err)
	}
	data, err := plan.Data(now.Add(time.Minute))
	if err != nil {
		t.Fatalf("read private data: %v", err)
	}
	for _, rendered := range []string{
		plan.String(), plan.GoString(), fmt.Sprint(plan), fmt.Sprintf("%#v", plan),
		data.String(), data.GoString(), fmt.Sprint(data), fmt.Sprintf("%#v", data),
	} {
		if strings.Contains(rendered, bundle.ObserverToken) || strings.Contains(rendered, string(data.SpecDocument)) ||
			!strings.Contains(rendered, "[REDACTED]") {
			t.Fatalf("diagnostic formatting exposed private data: %q", rendered)
		}
	}
	invalid := plan
	invalid.ExecutionAllowed = true
	invalid.Digest = DigestPlan(invalid)
	if _, err := invalid.Data(now.Add(time.Minute)); !errors.Is(err, ErrPlan) || strings.Contains(fmt.Sprint(err), bundle.ObserverToken) {
		t.Fatalf("invalid plan error leaked private data or escaped policy: %v", err)
	}
}

func TestPlanDataIsImmutableUnderConcurrentReads(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 31, 1, 0, 0, 0, time.UTC)
	bundle := testBundle(t, now)
	plan, err := Build(bundle, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("build Secret plan: %v", err)
	}
	const readers = 32
	var wait sync.WaitGroup
	errorsFound := make(chan error, readers)
	for range readers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			data, err := plan.Data(now.Add(time.Minute))
			if err == nil && (string(data.ObserverToken) != bundle.ObserverToken || data.SpecKey != SpecDataKey) {
				err = errors.New("private data drifted")
			}
			if len(data.ObserverToken) > 0 {
				data.ObserverToken[0] ^= 0xff
			}
			errorsFound <- err
		}()
	}
	wait.Wait()
	close(errorsFound)
	for err := range errorsFound {
		if err != nil {
			t.Fatalf("concurrent private data read failed: %v", err)
		}
	}
	fresh, err := plan.Data(now.Add(time.Minute))
	if err != nil || string(fresh.ObserverToken) != bundle.ObserverToken {
		t.Fatalf("concurrent caller mutated plan state: data=%#v err=%v", fresh, err)
	}
}

func TestPlanProductionDependencyBoundaryIsPureAndNonExecutable(t *testing.T) {
	t.Parallel()
	command := exec.Command("go", "list", "-deps", ".")
	output, err := command.Output()
	if err != nil {
		t.Fatalf("list materialization dependencies: %v", err)
	}
	local := []string{}
	for _, dependency := range strings.Fields(string(output)) {
		if strings.HasPrefix(dependency, "fugue/") {
			local = append(local, dependency)
		}
		for _, forbidden := range []string{
			"database/sql", "os/exec", "fugue/internal/backupidentity", "fugue/internal/backupmaterializer",
		} {
			if dependency == forbidden {
				t.Fatalf("materialization dependency widened to %q", dependency)
			}
		}
		for _, prefix := range []string{"k8s.io/", "fugue/internal/api", "fugue/internal/store", "fugue/internal/model"} {
			if strings.HasPrefix(dependency, prefix) {
				t.Fatalf("materialization dependency widened to %q", dependency)
			}
		}
	}
	sort.Strings(local)
	want := []string{
		"fugue/internal/backupcontrol",
		"fugue/internal/backupmaterializer/contract",
		"fugue/internal/backupmaterializer/materialization",
	}
	if !reflect.DeepEqual(local, want) {
		t.Fatalf("materialization local dependency closure drifted: got=%v want=%v", local, want)
	}
	directCommand := exec.Command("go", "list", "-f", `{{join .Imports "\n"}}`, ".")
	directOutput, err := directCommand.Output()
	if err != nil {
		t.Fatalf("list direct materialization dependencies: %v", err)
	}
	direct := strings.Fields(string(directOutput))
	sort.Strings(direct)
	wantDirect := []string{
		"crypto/sha256",
		"encoding/hex",
		"encoding/json",
		"errors",
		"fmt",
		"fugue/internal/backupcontrol",
		"fugue/internal/backupmaterializer/contract",
		"regexp",
		"strings",
		"time",
	}
	if !reflect.DeepEqual(direct, wantDirect) {
		t.Fatalf("materialization direct dependency boundary widened: got=%v want=%v", direct, wantDirect)
	}
}

func testBundle(t *testing.T, now time.Time) backupmaterializer.ObserverInputBundle {
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
		t.Fatalf("build backup spec: %v", err)
	}
	keyring := backupidentity.DeriveKeyring(strings.Repeat("k", 32), "backup-key-1", "", "", nil)
	bundle, err := backupmaterializer.IssueObserverInputBundle(keyring, spec, "tenant-1", now)
	if err != nil {
		t.Fatalf("issue observer input bundle: %v", err)
	}
	return bundle
}
