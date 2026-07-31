package reconcile

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
	"fugue/internal/backupmaterializer/materialization"
)

func TestManifestSealsExactPublicSecretShapeWithoutPrivateData(t *testing.T) {
	t.Parallel()
	issuedAt := time.Date(2026, 8, 1, 1, 0, 0, 0, time.UTC)
	plan := testPlan(t, "run-1", issuedAt, issuedAt.Add(30*time.Second))
	manifest, err := BuildManifest(plan, issuedAt.Add(30*time.Second))
	if err != nil {
		t.Fatalf("build manifest: %v", err)
	}
	wantLabels := map[string]string{
		LabelName:      labelNameValue,
		LabelComponent: labelComponentValue,
		LabelManagedBy: labelManagedByValue,
		LabelCellID:    plan.CellID,
	}
	if manifest.APIVersion != ContractAPIVersion || manifest.Kind != ManifestKind || manifest.Policy != ContractPolicy ||
		manifest.Namespace != plan.Namespace || manifest.SecretName != plan.SecretName || manifest.CellKey != plan.CellKey ||
		manifest.CellID != plan.CellID || manifest.SecretType != SecretTypeOpaque || manifest.Immutable ||
		manifest.OwnerReferencesAllowed ||
		!reflect.DeepEqual(manifest.Labels, wantLabels) || manifest.PlanDigest != plan.Digest ||
		manifest.DataDigests[plan.SpecKey] != plan.SpecDocumentDigest ||
		manifest.DataDigests[plan.TokenKey] != plan.ObserverTokenDigest || len(manifest.DataDigests) != 2 ||
		manifest.Annotations[AnnotationPlanDigest] != plan.Digest ||
		manifest.Annotations[AnnotationCellKey] != plan.CellKey ||
		manifest.Annotations[AnnotationRunID] != plan.RunID ||
		manifest.Annotations[AnnotationBundleDigest] != plan.BundleDigest ||
		manifest.Annotations[AnnotationExpiresAt] != plan.ExpiresAt.Format(time.RFC3339) ||
		manifest.Digest == "" || manifest.Digest != DigestManifest(manifest) {
		t.Fatalf("manifest drifted: %#v", manifest)
	}
	data, err := plan.Data(issuedAt.Add(30 * time.Second))
	if err != nil {
		t.Fatalf("read plan data: %v", err)
	}
	document, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	rendered := strings.Join([]string{string(document), fmt.Sprint(manifest), fmt.Sprintf("%#v", manifest)}, "\n")
	if strings.Contains(rendered, string(data.ObserverToken)) || strings.Contains(rendered, string(data.SpecDocument)) {
		t.Fatalf("public manifest exposed private data: %s", rendered)
	}

	manifest.Labels[LabelName] = "mutated"
	manifest.Annotations[AnnotationPlanDigest] = "mutated"
	manifest.DataDigests[plan.SpecKey] = "mutated"
	fresh, err := BuildManifest(plan, issuedAt.Add(30*time.Second))
	if err != nil || fresh.Labels[LabelName] != labelNameValue || fresh.Annotations[AnnotationPlanDigest] != plan.Digest ||
		fresh.DataDigests[plan.SpecKey] != plan.SpecDocumentDigest {
		t.Fatalf("caller mutated a future manifest: manifest=%#v err=%v", fresh, err)
	}
	if _, err := BuildManifest(plan, plan.RenewAfter); !errors.Is(err, ErrReconcile) {
		t.Fatalf("stale apply manifest error = %v, want fail closed", err)
	}
}

func TestSealCurrentRequiresExactManagedMetadataAndData(t *testing.T) {
	t.Parallel()
	issuedAt := time.Date(2026, 8, 1, 2, 0, 0, 0, time.UTC)
	plan := testPlan(t, "run-1", issuedAt, issuedAt.Add(30*time.Second))
	evidence := testEvidence(t, plan, issuedAt.Add(30*time.Second))
	evidence.Labels["admission.example/extra"] = "preserved"
	evidence.Annotations["admission.example/audit"] = "preserved"
	snapshot, err := SealCurrent(plan, evidence)
	if err != nil {
		t.Fatalf("seal current: %v", err)
	}
	manifest, err := BuildManifest(plan, issuedAt.Add(30*time.Second))
	if err != nil {
		t.Fatalf("build manifest: %v", err)
	}
	if snapshot.APIVersion != ContractAPIVersion || snapshot.Kind != SnapshotKind || snapshot.Policy != ContractPolicy ||
		snapshot.Namespace != plan.Namespace || snapshot.SecretName != plan.SecretName || snapshot.CellKey != plan.CellKey ||
		snapshot.CellID != plan.CellID || snapshot.UID != evidence.UID || snapshot.ResourceVersion != evidence.ResourceVersion ||
		snapshot.SecretType != SecretTypeOpaque || snapshot.PlanDigest != plan.Digest ||
		snapshot.ManifestDigest != manifest.Digest || snapshot.SpecDataDigest != plan.SpecDocumentDigest ||
		snapshot.TokenDataDigest != plan.ObserverTokenDigest || snapshot.Immutable || snapshot.OwnerReferenceCount != 0 ||
		snapshot.ExpiresAt != plan.ExpiresAt ||
		snapshot.Digest == "" || ValidateSnapshot(snapshot) != nil {
		t.Fatalf("snapshot drifted: %#v", snapshot)
	}
	privateData, err := plan.Data(issuedAt.Add(30 * time.Second))
	if err != nil {
		t.Fatalf("read private data: %v", err)
	}
	evidenceDocument, err := json.Marshal(evidence)
	if err != nil {
		t.Fatalf("marshal evidence: %v", err)
	}
	snapshotDocument, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	rendered := strings.Join([]string{string(evidenceDocument), string(snapshotDocument), fmt.Sprint(evidence), fmt.Sprintf("%#v", evidence), fmt.Sprint(snapshot), fmt.Sprintf("%#v", snapshot)}, "\n")
	if strings.Contains(rendered, string(privateData.ObserverToken)) || strings.Contains(rendered, string(privateData.SpecDocument)) ||
		!strings.Contains(rendered, "[REDACTED]") || !strings.Contains(rendered, "[OMITTED]") {
		t.Fatalf("diagnostic formatting exposed private data: %s", rendered)
	}
	if err := ValidateSnapshot(snapshot); err != nil {
		t.Fatalf("sealed snapshot should remain structurally valid independent of current expiry: %v", err)
	}

	tests := map[string]func(*SecretEvidence){
		"namespace":       func(value *SecretEvidence) { value.Namespace = "default" },
		"name":            func(value *SecretEvidence) { value.SecretName = "other" },
		"UID empty":       func(value *SecretEvidence) { value.UID = "" },
		"UID control":     func(value *SecretEvidence) { value.UID = "uid\nother" },
		"resource empty":  func(value *SecretEvidence) { value.ResourceVersion = "" },
		"resource space":  func(value *SecretEvidence) { value.ResourceVersion = " 42" },
		"type":            func(value *SecretEvidence) { value.SecretType = "kubernetes.io/tls" },
		"immutable":       func(value *SecretEvidence) { value.Immutable = true },
		"deleting":        func(value *SecretEvidence) { value.DeletionPending = true },
		"owner reference": func(value *SecretEvidence) { value.OwnerReferenceCount = 1 },
		"label missing":   func(value *SecretEvidence) { delete(value.Labels, LabelManagedBy) },
		"label changed":   func(value *SecretEvidence) { value.Labels[LabelCellID] = "other" },
		"annotation":      func(value *SecretEvidence) { value.Annotations[AnnotationPlanDigest] = plan.BundleDigest },
		"spec missing":    func(value *SecretEvidence) { delete(value.Data, plan.SpecKey) },
		"token missing":   func(value *SecretEvidence) { delete(value.Data, plan.TokenKey) },
		"extra data":      func(value *SecretEvidence) { value.Data["other"] = []byte("value") },
		"spec content":    func(value *SecretEvidence) { value.Data[plan.SpecKey][0] ^= 0xff },
		"token content":   func(value *SecretEvidence) { value.Data[plan.TokenKey][0] ^= 0xff },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			candidate := cloneEvidence(testEvidence(t, plan, issuedAt.Add(30*time.Second)))
			mutate(&candidate)
			if _, err := SealCurrent(plan, candidate); !errors.Is(err, ErrReconcile) {
				t.Fatalf("seal error = %v, want fail closed", err)
			}
		})
	}

	mutated := snapshot
	mutated.CellID = "registry-0123456789abcdef"
	mutated.Digest = DigestSnapshot(mutated)
	if err := ValidateSnapshot(mutated); !errors.Is(err, ErrReconcile) {
		t.Fatalf("mutated snapshot error = %v, want fail closed", err)
	}
}

func TestRecoverCurrentRebuildsManagedLKGAfterRestart(t *testing.T) {
	t.Parallel()
	issuedAt := time.Date(2026, 8, 1, 2, 30, 0, 0, time.UTC)
	applyAt := issuedAt.Add(30 * time.Second)
	plan := testPlan(t, "run-1", issuedAt, applyAt)
	evidence := testEvidence(t, plan, applyAt)
	evidence.Labels["admission.example/extra"] = "preserved"
	evidence.Annotations["admission.example/audit"] = "preserved"

	recovered, err := RecoverCurrent(evidence)
	if err != nil {
		t.Fatalf("recover current without in-memory plan: %v", err)
	}
	if recovered.PlanDigest != plan.Digest || recovered.CellKey != plan.CellKey || recovered.UID != evidence.UID ||
		recovered.ResourceVersion != evidence.ResourceVersion || recovered.SpecDataDigest != plan.SpecDocumentDigest ||
		recovered.TokenDataDigest != plan.ObserverTokenDigest || ValidateSnapshot(recovered) != nil {
		t.Fatalf("recovered snapshot drifted: %#v", recovered)
	}
	observation, err := ObserveExisting(plan.CellKey, evidence)
	if err != nil || observation.State != StateManaged || observation.SnapshotDigest != recovered.Digest {
		t.Fatalf("recovered observation drifted: observation=%#v err=%v", observation, err)
	}
	retainAt := plan.RenewAfter.Add(time.Minute)
	decision, err := Decide(plan.CellKey, nil, observation, retainAt)
	if err != nil || decision.Action != ActionRetainLastKnownGood || decision.Reason != ReasonSourceUnavailableRetainLKG ||
		!decision.RetainExisting || !decision.Stable || decision.MutationCandidate || decision.DeleteAllowed {
		t.Fatalf("restart LKG decision drifted: decision=%#v err=%v", decision, err)
	}
	expired, err := Decide(plan.CellKey, nil, observation, plan.ExpiresAt)
	if err != nil || expired.Action != ActionBlock || expired.Reason != ReasonLastKnownGoodExpired ||
		!expired.RetainExisting || expired.DeleteAllowed {
		t.Fatalf("restart expiry decision drifted: decision=%#v err=%v", expired, err)
	}
}

func TestObserveExistingClassifiesForeignAndMalformedWithoutAdoption(t *testing.T) {
	t.Parallel()
	issuedAt := time.Date(2026, 8, 1, 2, 45, 0, 0, time.UTC)
	applyAt := issuedAt.Add(30 * time.Second)
	plan := testPlan(t, "run-1", issuedAt, applyAt)

	foreignEvidence := testEvidence(t, plan, applyAt)
	foreignEvidence.Labels[LabelManagedBy] = "other-controller"
	if _, err := RecoverCurrent(foreignEvidence); !errors.Is(err, ErrReconcile) {
		t.Fatalf("foreign recovery error = %v, want fail closed", err)
	}
	foreign, err := ObserveExisting(plan.CellKey, foreignEvidence)
	if err != nil || foreign.State != StateForeign {
		t.Fatalf("foreign observation drifted: observation=%#v err=%v", foreign, err)
	}
	foreignDecision, err := Decide(plan.CellKey, &plan, foreign, applyAt)
	if err != nil || foreignDecision.Action != ActionBlock || foreignDecision.Reason != ReasonCurrentObjectForeign ||
		foreignDecision.MutationCandidate || foreignDecision.RetainExisting || foreignDecision.DeleteAllowed {
		t.Fatalf("foreign decision drifted: decision=%#v err=%v", foreignDecision, err)
	}

	malformations := map[string]func(*SecretEvidence){
		"namespace": func(value *SecretEvidence) { value.Namespace = "default" },
		"name":      func(value *SecretEvidence) { value.SecretName = "other" },
		"UID":       func(value *SecretEvidence) { value.UID = "uid\nreplacement" },
		"resource version": func(value *SecretEvidence) {
			value.ResourceVersion = " 42"
		},
		"type":      func(value *SecretEvidence) { value.SecretType = "kubernetes.io/tls" },
		"immutable": func(value *SecretEvidence) { value.Immutable = true },
		"deleting":  func(value *SecretEvidence) { value.DeletionPending = true },
		"owner":     func(value *SecretEvidence) { value.OwnerReferenceCount = 1 },
		"cell label": func(value *SecretEvidence) {
			value.Labels[LabelCellID] = "registry-0123456789abcdef"
		},
		"plan API": func(value *SecretEvidence) {
			value.Annotations[AnnotationPlanAPIVersion] = "backup-materialization.fugue.dev/v2"
		},
		"plan policy": func(value *SecretEvidence) {
			value.Annotations[AnnotationPlanPolicy] = "unsafe-upsert"
		},
		"plan digest": func(value *SecretEvidence) {
			value.Annotations[AnnotationPlanDigest] = plan.BundleDigest
		},
		"cell annotation": func(value *SecretEvidence) {
			value.Annotations[AnnotationCellKey] = "backup/registry/0123456789abcdef"
		},
		"run": func(value *SecretEvidence) {
			value.Annotations[AnnotationRunID] = "other-run"
		},
		"spec digest": func(value *SecretEvidence) {
			value.Annotations[AnnotationSpecDigest] = plan.BundleDigest
		},
		"bundle digest": func(value *SecretEvidence) {
			value.Annotations[AnnotationBundleDigest] = plan.SpecDigest
		},
		"credential": func(value *SecretEvidence) {
			value.Annotations[AnnotationCredentialID] = "backup-observer:other"
		},
		"token ID": func(value *SecretEvidence) {
			value.Annotations[AnnotationTokenID] = strings.Repeat("A", 22)
		},
		"issue timestamp": func(value *SecretEvidence) {
			value.Annotations[AnnotationIssuedAt] = issuedAt.Format("2006-01-02T15:04:05+00:00")
		},
		"renew timestamp": func(value *SecretEvidence) {
			value.Annotations[AnnotationRenewAfter] = "invalid"
		},
		"expiry timestamp": func(value *SecretEvidence) {
			delete(value.Annotations, AnnotationExpiresAt)
		},
		"spec digest annotation": func(value *SecretEvidence) {
			value.Annotations[AnnotationSpecDocumentDigest] = plan.ObserverTokenDigest
		},
		"token digest annotation": func(value *SecretEvidence) {
			value.Annotations[AnnotationObserverTokenDigest] = plan.SpecDocumentDigest
		},
		"spec bytes":  func(value *SecretEvidence) { value.Data[materialization.SpecDataKey][0] ^= 0xff },
		"token bytes": func(value *SecretEvidence) { value.Data[materialization.TokenDataKey][0] ^= 0xff },
		"extra data":  func(value *SecretEvidence) { value.Data["other"] = []byte("value") },
		"missing annotations": func(value *SecretEvidence) {
			value.Annotations = nil
		},
		"missing data": func(value *SecretEvidence) { value.Data = nil },
	}
	for name, mutate := range malformations {
		t.Run(name, func(t *testing.T) {
			evidence := cloneEvidence(testEvidence(t, plan, applyAt))
			mutate(&evidence)
			if _, err := RecoverCurrent(evidence); !errors.Is(err, ErrReconcile) {
				t.Fatalf("recovery error = %v, want fail closed", err)
			}
			observation, err := ObserveExisting(plan.CellKey, evidence)
			if err != nil || observation.State != StateMalformed {
				t.Fatalf("malformed observation drifted: observation=%#v err=%v", observation, err)
			}
			decision, err := Decide(plan.CellKey, &plan, observation, applyAt)
			if err != nil || decision.Action != ActionBlock || decision.Reason != ReasonCurrentObjectMalformed ||
				decision.MutationCandidate || decision.RetainExisting || decision.DeleteAllowed {
				t.Fatalf("malformed decision drifted: decision=%#v err=%v", decision, err)
			}
		})
	}

	otherCell := "backup/registry/0123456789abcdef"
	crossCell, err := ObserveExisting(otherCell, testEvidence(t, plan, applyAt))
	if err != nil || crossCell.State != StateMalformed {
		t.Fatalf("cross-cell current object was not isolated: observation=%#v err=%v", crossCell, err)
	}
}

func TestReconcileDecisionMatrixIsCellLocalCASAndLKGOnly(t *testing.T) {
	t.Parallel()
	issuedAt := time.Date(2026, 8, 1, 3, 0, 0, 0, time.UTC)
	applyAt := issuedAt.Add(30 * time.Second)
	oldPlan := testPlan(t, "run-1", issuedAt, applyAt)
	absent, err := ObserveAbsent(oldPlan.CellKey)
	if err != nil {
		t.Fatalf("observe absent: %v", err)
	}

	create, err := Decide(oldPlan.CellKey, &oldPlan, absent, applyAt)
	if err != nil || create.Action != ActionCreateIfAbsent || create.Reason != ReasonDesiredGenerationReady ||
		!create.MutationCandidate || !create.RequireAbsent || create.Stable || create.Blocked ||
		create.RequireResourceVersionCAS || create.DeleteAllowed || create.ExecutionAllowed || create.ProductionMutationAllowed {
		t.Fatalf("create decision drifted: decision=%#v err=%v", create, err)
	}
	blockedEmpty, err := Decide(oldPlan.CellKey, nil, absent, applyAt)
	if err != nil || blockedEmpty.Action != ActionBlock || blockedEmpty.Reason != ReasonSourceUnavailableNoLKG ||
		!blockedEmpty.Blocked || blockedEmpty.RetainExisting || blockedEmpty.DeleteAllowed {
		t.Fatalf("empty decision drifted: decision=%#v err=%v", blockedEmpty, err)
	}

	snapshot, err := SealCurrent(oldPlan, testEvidence(t, oldPlan, applyAt))
	if err != nil {
		t.Fatalf("seal current: %v", err)
	}
	managed, err := ObserveManaged(snapshot)
	if err != nil {
		t.Fatalf("observe managed: %v", err)
	}
	noop, err := Decide(oldPlan.CellKey, &oldPlan, managed, applyAt)
	if err != nil || noop.Action != ActionNoop || noop.Reason != ReasonCurrentGenerationMatches ||
		!noop.Stable || noop.MutationCandidate || noop.RetainExisting || noop.CurrentPlanDigest != oldPlan.Digest ||
		noop.DesiredPlanDigest != oldPlan.Digest || noop.DeleteAllowed {
		t.Fatalf("noop decision drifted: decision=%#v err=%v", noop, err)
	}

	newIssuedAt := issuedAt.Add(2 * time.Minute)
	newApplyAt := newIssuedAt.Add(30 * time.Second)
	newPlan := testPlan(t, "run-1", newIssuedAt, newApplyAt)
	replace, err := Decide(oldPlan.CellKey, &newPlan, managed, newApplyAt)
	if err != nil || replace.Action != ActionReplaceResourceVersionCAS || replace.Reason != ReasonDesiredGenerationChanged ||
		!replace.MutationCandidate || !replace.RequireUIDMatch || !replace.RequireResourceVersionCAS ||
		!replace.RetainExisting || replace.RequireAbsent || replace.ExpectedUID != snapshot.UID ||
		replace.ExpectedResourceVersion != snapshot.ResourceVersion || replace.DeleteAllowed || replace.ExecutionAllowed ||
		replace.ProductionMutationAllowed {
		t.Fatalf("replace decision drifted: decision=%#v err=%v", replace, err)
	}

	afterRenewal := oldPlan.RenewAfter.Add(time.Minute)
	retain, err := Decide(oldPlan.CellKey, nil, managed, afterRenewal)
	if err != nil || retain.Action != ActionRetainLastKnownGood || retain.Reason != ReasonSourceUnavailableRetainLKG ||
		!retain.Stable || !retain.RetainExisting || retain.MutationCandidate || retain.Blocked || retain.DeleteAllowed {
		t.Fatalf("LKG retain decision drifted: decision=%#v err=%v", retain, err)
	}
	expired, err := Decide(oldPlan.CellKey, nil, managed, oldPlan.ExpiresAt)
	if err != nil || expired.Action != ActionBlock || expired.Reason != ReasonLastKnownGoodExpired ||
		!expired.Blocked || !expired.RetainExisting || expired.MutationCandidate || expired.DeleteAllowed {
		t.Fatalf("expired LKG decision drifted: decision=%#v err=%v", expired, err)
	}

	for _, state := range []CurrentState{StateForeign, StateMalformed} {
		obstruction, observeErr := ObserveObstruction(oldPlan.CellKey, state, "uid-obstruction", "77")
		if observeErr != nil {
			t.Fatalf("observe %s obstruction: %v", state, observeErr)
		}
		decision, decideErr := Decide(oldPlan.CellKey, &newPlan, obstruction, newApplyAt)
		wantReason := ReasonCurrentObjectForeign
		if state == StateMalformed {
			wantReason = ReasonCurrentObjectMalformed
		}
		if decideErr != nil || decision.Action != ActionBlock || decision.Reason != wantReason ||
			!decision.Blocked || decision.MutationCandidate || decision.RetainExisting || decision.DeleteAllowed {
			t.Fatalf("%s obstruction decision drifted: decision=%#v err=%v", state, decision, decideErr)
		}
	}
}

func TestReconcileRejectsCrossCellAndContractDrift(t *testing.T) {
	t.Parallel()
	issuedAt := time.Date(2026, 8, 1, 4, 0, 0, 0, time.UTC)
	applyAt := issuedAt.Add(30 * time.Second)
	plan := testPlan(t, "run-1", issuedAt, applyAt)
	absent, err := ObserveAbsent(plan.CellKey)
	if err != nil {
		t.Fatalf("observe absent: %v", err)
	}
	otherCell := "backup/registry/0123456789abcdef"
	if _, err := Decide(otherCell, &plan, absent, applyAt); !errors.Is(err, ErrReconcile) {
		t.Fatalf("cross-cell decision error = %v, want fail closed", err)
	}
	stale := plan
	if _, err := Decide(plan.CellKey, &stale, absent, plan.RenewAfter); !errors.Is(err, ErrReconcile) {
		t.Fatalf("stale desired error = %v, want fail closed", err)
	}
	invalidCell := plan
	invalidCell.CellKey = otherCell
	invalidCell.Digest = materialization.DigestPlan(invalidCell)
	if _, err := Decide(plan.CellKey, &invalidCell, absent, applyAt); !errors.Is(err, ErrReconcile) {
		t.Fatalf("invalid desired error = %v, want fail closed", err)
	}

	badObservation := absent
	badObservation.State = StateManaged
	badObservation.Digest = DigestObservation(badObservation)
	if err := ValidateObservation(badObservation); !errors.Is(err, ErrReconcile) {
		t.Fatalf("observation drift error = %v, want fail closed", err)
	}
	if _, err := ObserveObstruction(plan.CellKey, StateManaged, "uid", "1"); !errors.Is(err, ErrReconcile) {
		t.Fatalf("invalid obstruction state error = %v, want fail closed", err)
	}

	create, err := Decide(plan.CellKey, &plan, absent, applyAt)
	if err != nil {
		t.Fatalf("create decision: %v", err)
	}
	mutations := map[string]func(*Decision){
		"API version":         func(value *Decision) { value.APIVersion = "v2" },
		"kind":                func(value *Decision) { value.Kind = "Other" },
		"policy":              func(value *Decision) { value.Policy = "upsert" },
		"cell":                func(value *Decision) { value.CellKey = otherCell },
		"action":              func(value *Decision) { value.Action = ActionBlock },
		"reason":              func(value *Decision) { value.Reason = ReasonCurrentObjectForeign },
		"desired digest":      func(value *Decision) { value.DesiredPlanDigest = "" },
		"mutation candidate":  func(value *Decision) { value.MutationCandidate = false },
		"stable":              func(value *Decision) { value.Stable = true },
		"blocked":             func(value *Decision) { value.Blocked = true },
		"require absent":      func(value *Decision) { value.RequireAbsent = false },
		"UID match":           func(value *Decision) { value.RequireUIDMatch = true },
		"resource CAS":        func(value *Decision) { value.RequireResourceVersionCAS = true },
		"retain":              func(value *Decision) { value.RetainExisting = true },
		"delete":              func(value *Decision) { value.DeleteAllowed = true },
		"execution":           func(value *Decision) { value.ExecutionAllowed = true },
		"production mutation": func(value *Decision) { value.ProductionMutationAllowed = true },
		"non-canonical time":  func(value *Decision) { value.DecidedAt = value.DecidedAt.Add(time.Nanosecond) },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			candidate := create
			mutate(&candidate)
			candidate.Digest = DigestDecision(candidate)
			if err := ValidateDecision(candidate); !errors.Is(err, ErrReconcile) {
				t.Fatalf("decision drift error = %v, want fail closed: %#v", err, candidate)
			}
		})
	}
	badDigest := create
	badDigest.Digest = strings.Repeat("0", 64)
	if err := ValidateDecision(badDigest); !errors.Is(err, ErrReconcile) {
		t.Fatalf("decision digest error = %v, want fail closed", err)
	}
}

func TestBlockedDecisionShapesAreExact(t *testing.T) {
	t.Parallel()
	issuedAt := time.Date(2026, 8, 1, 5, 0, 0, 0, time.UTC)
	applyAt := issuedAt.Add(30 * time.Second)
	plan := testPlan(t, "run-1", issuedAt, applyAt)
	absent, _ := ObserveAbsent(plan.CellKey)
	noLKG, err := Decide(plan.CellKey, nil, absent, applyAt)
	if err != nil {
		t.Fatalf("decide without LKG: %v", err)
	}
	badNoLKG := noLKG
	badNoLKG.RetainExisting = true
	badNoLKG.Digest = DigestDecision(badNoLKG)
	if err := ValidateDecision(badNoLKG); !errors.Is(err, ErrReconcile) {
		t.Fatalf("no-LKG retention drift error = %v, want fail closed", err)
	}

	foreign, _ := ObserveObstruction(plan.CellKey, StateForeign, "uid-foreign", "9")
	blockedForeign, err := Decide(plan.CellKey, &plan, foreign, applyAt)
	if err != nil {
		t.Fatalf("decide foreign: %v", err)
	}
	badForeign := blockedForeign
	badForeign.CurrentPlanDigest = plan.Digest
	badForeign.Digest = DigestDecision(badForeign)
	if err := ValidateDecision(badForeign); !errors.Is(err, ErrReconcile) {
		t.Fatalf("foreign adoption drift error = %v, want fail closed", err)
	}

	snapshot, _ := SealCurrent(plan, testEvidence(t, plan, applyAt))
	managed, _ := ObserveManaged(snapshot)
	expired, err := Decide(plan.CellKey, nil, managed, plan.ExpiresAt)
	if err != nil {
		t.Fatalf("decide expired: %v", err)
	}
	badExpired := expired
	badExpired.CurrentSnapshotDigest = ""
	badExpired.Digest = DigestDecision(badExpired)
	if err := ValidateDecision(badExpired); !errors.Is(err, ErrReconcile) {
		t.Fatalf("expired LKG evidence drift error = %v, want fail closed", err)
	}
}

func TestReconcileIsDeterministicUnderConcurrentEvaluation(t *testing.T) {
	t.Parallel()
	issuedAt := time.Date(2026, 8, 1, 6, 0, 0, 0, time.UTC)
	applyAt := issuedAt.Add(30 * time.Second)
	plan := testPlan(t, "run-1", issuedAt, applyAt)
	snapshot, err := SealCurrent(plan, testEvidence(t, plan, applyAt))
	if err != nil {
		t.Fatalf("seal current: %v", err)
	}
	managed, err := ObserveManaged(snapshot)
	if err != nil {
		t.Fatalf("observe managed: %v", err)
	}
	want, err := Decide(plan.CellKey, nil, managed, plan.RenewAfter.Add(time.Minute))
	if err != nil {
		t.Fatalf("baseline decision: %v", err)
	}
	const readers = 32
	var wait sync.WaitGroup
	errorsFound := make(chan error, readers)
	for range readers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			got, decideErr := Decide(plan.CellKey, nil, managed, plan.RenewAfter.Add(time.Minute))
			if decideErr == nil && !reflect.DeepEqual(got, want) {
				decideErr = errors.New("decision drifted")
			}
			errorsFound <- decideErr
		}()
	}
	wait.Wait()
	close(errorsFound)
	for err := range errorsFound {
		if err != nil {
			t.Fatalf("concurrent reconcile failed: %v", err)
		}
	}
}

func TestReconcileProductionDependencyBoundaryIsPureAndNonExecutable(t *testing.T) {
	t.Parallel()
	command := exec.Command("go", "list", "-deps", ".")
	output, err := command.Output()
	if err != nil {
		t.Fatalf("list reconcile dependencies: %v", err)
	}
	local := []string{}
	for _, dependency := range strings.Fields(string(output)) {
		if strings.HasPrefix(dependency, "fugue/") {
			local = append(local, dependency)
		}
		for _, forbidden := range []string{
			"database/sql", "os/exec", "net", "net/http", "fugue/internal/backupidentity", "fugue/internal/backupmaterializer",
		} {
			if dependency == forbidden {
				t.Fatalf("reconcile dependency widened to %q", dependency)
			}
		}
		for _, prefix := range []string{"k8s.io/", "fugue/internal/api", "fugue/internal/store", "fugue/internal/model"} {
			if strings.HasPrefix(dependency, prefix) {
				t.Fatalf("reconcile dependency widened to %q", dependency)
			}
		}
	}
	sort.Strings(local)
	want := []string{
		"fugue/internal/backupcontrol",
		"fugue/internal/backupmaterializer/contract",
		"fugue/internal/backupmaterializer/materialization",
		"fugue/internal/backupmaterializer/reconcile",
	}
	if !reflect.DeepEqual(local, want) {
		t.Fatalf("reconcile local dependency closure drifted: got=%v want=%v", local, want)
	}
	directCommand := exec.Command("go", "list", "-f", `{{join .Imports "\n"}}`, ".")
	directOutput, err := directCommand.Output()
	if err != nil {
		t.Fatalf("list direct reconcile dependencies: %v", err)
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
		"fugue/internal/backupmaterializer/materialization",
		"strings",
		"time",
		"unicode",
		"unicode/utf8",
	}
	if !reflect.DeepEqual(direct, wantDirect) {
		t.Fatalf("reconcile direct dependency boundary widened: got=%v want=%v", direct, wantDirect)
	}
}

func testPlan(t *testing.T, runID string, issuedAt, buildAt time.Time) materialization.Plan {
	t.Helper()
	spec, err := backupcontrol.NewShadowBackupRunSpec(
		runID,
		runID,
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
	bundle, err := backupmaterializer.IssueObserverInputBundle(keyring, spec, "tenant-1", issuedAt)
	if err != nil {
		t.Fatalf("issue input bundle: %v", err)
	}
	plan, err := materialization.Build(bundle, buildAt)
	if err != nil {
		t.Fatalf("build materialization plan: %v", err)
	}
	return plan
}

func testEvidence(t *testing.T, plan materialization.Plan, now time.Time) SecretEvidence {
	t.Helper()
	manifest, err := BuildManifest(plan, now)
	if err != nil {
		t.Fatalf("build manifest: %v", err)
	}
	data, err := plan.Data(now)
	if err != nil {
		t.Fatalf("read plan data: %v", err)
	}
	return SecretEvidence{
		Namespace:       manifest.Namespace,
		SecretName:      manifest.SecretName,
		UID:             "01234567-89ab-cdef-0123-456789abcdef",
		ResourceVersion: "42",
		SecretType:      manifest.SecretType,
		Labels:          cloneStringMap(manifest.Labels),
		Annotations:     cloneStringMap(manifest.Annotations),
		Data: map[string][]byte{
			data.SpecKey:  append([]byte(nil), data.SpecDocument...),
			data.TokenKey: append([]byte(nil), data.ObserverToken...),
		},
	}
}

func cloneEvidence(value SecretEvidence) SecretEvidence {
	value.Labels = cloneStringMap(value.Labels)
	value.Annotations = cloneStringMap(value.Annotations)
	clonedData := make(map[string][]byte, len(value.Data))
	for key, data := range value.Data {
		clonedData[key] = append([]byte(nil), data...)
	}
	value.Data = clonedData
	return value
}

func cloneStringMap(value map[string]string) map[string]string {
	cloned := make(map[string]string, len(value))
	for key, item := range value {
		cloned[key] = item
	}
	return cloned
}
