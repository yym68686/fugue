package productioncontrol

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"fugue/internal/storageaudit"
)

func storageAuditShadowFixture() StorageAuditShadow {
	p := storageaudit.Policy{
		Version: 1, Mode: "audit", EffectiveFrom: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		IntervalSeconds: 300, SourcePageRows: 64, HistoryDays: 7, MinimumVerifiedVersions: 3,
		UnusedUploadHours: 24, UnreferencedUploadDays: 30, WarningFreePercent: 20, PressureFreePercent: 15, CriticalFreePercent: 10,
	}
	target := p
	target.SourcePageRows = 96
	v := StorageAuditPolicyVersion{p, "sha256:" + p.Digest()}
	return StorageAuditShadow{
		Schema: StorageAuditShadowSchema, Owner: "worker", ConfigMapResourceVersion: "1",
		Binding: StorageAuditShadowBinding{
			Kubernetes: ParticipationKubernetesBinding{Endpoint: "https://cluster.example.test", CADigest: digest([]byte("ca")), NamespaceUID: "namespace-v1"},
			Namespace:  "control", ConfigMap: "storage", ConfigMapUID: "storage-v1",
		},
		Provider: storageaudit.MaintenanceObserver{SystemID: "123456", Database: "fixture", Role: "audit-observer"},
		Activation: storageaudit.OwnerActivation{
			DeclarationDigest: strings.Repeat("a", 64), PolicyDigest: p.Digest(),
			ActivatedAt: p.EffectiveFrom.Add(time.Hour), WorkerSecretUID: "secret-v1",
		},
		Current: v, Target: StorageAuditPolicyVersion{target, "sha256:" + target.Digest()}, Rollback: v,
	}
}

func TestStorageAuditPolicyApprovalBindsExactShadowAndRollback(t *testing.T) {
	s := storageAuditShadowFixture()
	head := Version{3, s.Current.Digest}
	submission, err := DraftStorageAuditPolicy("cluster-a", "audit-policy-a", head, s)
	if err != nil {
		t.Fatal(err)
	}
	observation, err := InspectStorageAuditPolicy(submission)
	if err != nil || observation != s {
		t.Fatalf("inspect returned %+v, %v", observation, err)
	}
	if _, err := DraftStorageAuditPolicy("cluster-a", "audit-policy-a", Version{3, s.Target.Digest}, s); !errors.Is(err, ErrResourceDrift) {
		t.Fatalf("draft accepted a stale predecessor: %v", err)
	}
	for _, mutate := range []func(*StorageAuditShadow){
		func(s *StorageAuditShadow) { s.ExecutionEvidence = true },
		func(s *StorageAuditShadow) { s.Rollback = s.Target },
		func(s *StorageAuditShadow) { s.Target.Document.HistoryDays++ },
		func(s *StorageAuditShadow) { s.Provider.SystemID = "" },
	} {
		modified := s
		mutate(&modified)
		if _, err := DraftStorageAuditPolicy("cluster-a", "audit-policy-a", head, modified); err == nil {
			t.Fatal("invalid storage shadow was accepted")
		}
	}
	submission.Intent.TargetDigest = submission.Intent.CurrentDigest
	if _, err := InspectStorageAuditPolicy(submission); err == nil {
		t.Fatal("changed intent did not invalidate bind to the shadow")
	}
}

func testPostgresStorageAuditApproval(t *testing.T, ctx context.Context, existing *Journal) {
	t.Helper()
	base, _, key, trusted, now := approvalFixture(t)
	journal, err := NewJournal(existing.db, trusted)
	if err != nil {
		t.Fatal(err)
	}
	journal.now = func() time.Time { return now }
	s := storageAuditShadowFixture()
	submission, err := DraftStorageAuditPolicy(base.ClusterID, "audit-policy-a", Version{1, s.Current.Digest}, s)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := journal.RecordIntent(ctx, "operator-a", submission.Intent, []byte(`{}`)); err == nil {
		t.Fatal("unbound policy bytes entered journal")
	}
	record, err := journal.RecordIntent(ctx, "operator-a", submission.Intent, submission.Payload)
	if err != nil || !record.Created {
		t.Fatalf("storage audit intent: %+v %v", record, err)
	}
	approval, err := SignApproval(submission.Intent, "owner-a", key, now, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	approved, err := journal.RecordApproval(ctx, approval)
	if err != nil || !approved.Created {
		t.Fatalf("storage audit approval: %+v %v", approved, err)
	}
	if claim, created, err := journal.ReserveExecution(ctx, "worker-a", record.Digest, approved.Digest); err == nil ||
		!strings.Contains(err.Error(), "provider-fenced executor") || created || claim.Fence != 0 {
		t.Fatalf("storage audit approval granted generic dispatch: %+v %v %v", claim, created, err)
	}
}
