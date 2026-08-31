package store

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/model"
)

func migrationLedgerFixture(t *testing.T) (*Store, model.App, model.Operation, model.Runtime) {
	t.Helper()
	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := s.CreateTenant("migration-ledger")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "project", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "app", "", model.AppSpec{
		Image: "registry.example/app:v1", Ports: []int{8080}, Replicas: 1, RuntimeID: model.DefaultManagedRuntimeID,
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	target, _, err := s.CreateRuntime(tenant.ID, "target", model.RuntimeTypeExternalOwned, "", nil)
	if err != nil {
		t.Fatalf("create target runtime: %v", err)
	}
	desired := app.Spec
	desired.RuntimeID = target.ID
	op, err := s.CreateOperation(model.Operation{
		TenantID: tenant.ID, Type: model.OperationTypeMigrate, AppID: app.ID,
		TargetRuntimeID: target.ID, DesiredSpec: &desired, RequestedByType: model.ActorTypeAPIKey, RequestedByID: "key_test",
	})
	if err != nil {
		t.Fatalf("create migration: %v", err)
	}
	return s, app, op, target
}

func TestMigrationOperationCreatesNinetyDayLedgerAndFailsClosed(t *testing.T) {
	s, app, op, target := migrationLedgerFixture(t)
	initial, found, err := s.LatestAppMigrationLedger(op.ID)
	if err != nil || !found {
		t.Fatalf("latest migration ledger: found=%v err=%v", found, err)
	}
	if initial.CutoverStatus != model.AppMigrationCutoverPending || !initial.OldArtifactsProtected ||
		initial.OldRuntimeID != app.Spec.RuntimeID || initial.NewRuntimeID != target.ID {
		t.Fatalf("unexpected initial ledger: %+v", initial)
	}
	if initial.RetainUntil.Before(initial.CreatedAt.Add(90 * 24 * time.Hour)) {
		t.Fatalf("ledger retention is shorter than 90 days: %+v", initial)
	}
	claimed, found, err := s.ClaimNextPendingOperation()
	if err != nil || !found || claimed.ID != op.ID {
		t.Fatalf("claim migration: found=%v op=%+v err=%v", found, claimed, err)
	}
	if _, err := s.CompleteAgentOperation(op.ID, target.ID, "", "migrated"); !errors.Is(err, errMigrationCutoverEvidenceMissing) {
		t.Fatalf("completion without verified evidence = %v, want fail-closed", err)
	}
}

func TestVerifiedMigrationLedgerAllowsCompletion(t *testing.T) {
	s, app, op, target := migrationLedgerFixture(t)
	claimed, found, err := s.ClaimNextPendingOperation()
	if err != nil || !found || claimed.ID != op.ID {
		t.Fatalf("claim migration: found=%v op=%+v err=%v", found, claimed, err)
	}
	ready := true
	physical := 1
	verified, err := s.RecordAppMigrationLedger(model.AppMigrationLedger{
		TenantID: op.TenantID, ProjectID: app.ProjectID, AppID: app.ID, OperationID: op.ID,
		OldRuntimeID: op.SourceRuntimeID, NewRuntimeID: target.ID,
		OldClusterID: "cluster-old", NewClusterID: "cluster-new",
		ImageRef: app.Spec.Image, ImageReplicationStatus: model.AppMigrationEvidenceVerified,
		RuntimeObjectStatus: model.AppMigrationEvidenceVerified,
		EndpointRequired:    true, EndpointStatus: model.AppMigrationEvidenceReady, EndpointReady: &ready,
		PhysicalReplicas: &physical, DesiredReplicas: 1, Generation: 2, ObservedGeneration: 2,
		CutoverStatus: model.AppMigrationCutoverVerified, OldArtifactsProtected: true,
	})
	if err != nil {
		t.Fatalf("record verified ledger: %v", err)
	}
	if err := ValidateAppMigrationCutover(verified); err != nil {
		t.Fatalf("verified ledger rejected: %v", err)
	}
	completed, err := s.CompleteAgentOperation(op.ID, target.ID, "", "migrated")
	if err != nil || completed.Status != model.OperationStatusCompleted {
		t.Fatalf("complete verified migration: op=%+v err=%v", completed, err)
	}
}

func TestMigrationArtifactsStayProtectedUntilVerifiedCutover(t *testing.T) {
	s, app, op, target := migrationLedgerFixture(t)
	blocked, reason, err := s.MigrationArtifactsRetirementBlocked(app.ID)
	if err != nil || !blocked || reason == "" {
		t.Fatalf("pending migration must protect artifacts: blocked=%v reason=%q err=%v", blocked, reason, err)
	}
	if err := s.RecordMigrationArtifactRetirementBlocked(app.ID, "target endpoint not ready"); err != nil {
		t.Fatalf("record blocked retirement attempt: %v", err)
	}
	latest, found, err := s.LatestAppMigrationLedger(op.ID)
	if err != nil || !found || latest.CutoverStatus != model.AppMigrationCutoverBlocked || latest.FailureReason != "target endpoint not ready" {
		t.Fatalf("expected blocked ledger event, got found=%v ledger=%+v err=%v", found, latest, err)
	}
	ready := true
	physical := 1
	if _, err := s.RecordAppMigrationLedger(model.AppMigrationLedger{
		TenantID: op.TenantID, ProjectID: app.ProjectID, AppID: app.ID, OperationID: op.ID,
		OldRuntimeID: op.SourceRuntimeID, NewRuntimeID: target.ID, OldClusterID: "old", NewClusterID: "new",
		ImageReplicationStatus: model.AppMigrationEvidenceVerified,
		RuntimeObjectStatus:    model.AppMigrationEvidenceVerified,
		EndpointRequired:       true, EndpointStatus: model.AppMigrationEvidenceReady, EndpointReady: &ready,
		PhysicalReplicas: &physical, DesiredReplicas: 1, Generation: 1, ObservedGeneration: 1,
		CutoverStatus: model.AppMigrationCutoverVerified, OldArtifactsProtected: true,
	}); err != nil {
		t.Fatalf("record verified cutover: %v", err)
	}
	claimed, found, err := s.ClaimNextPendingOperation()
	if err != nil || !found || claimed.ID != op.ID {
		t.Fatalf("claim migration for cutover completion: found=%v op=%+v err=%v", found, claimed, err)
	}
	if completed, completeErr := s.CompleteAgentOperation(op.ID, target.ID, "", "migrated"); completeErr != nil || completed.Status != model.OperationStatusCompleted {
		t.Fatalf("complete migration before releasing artifacts: op=%+v err=%v", completed, completeErr)
	}
	blocked, reason, err = s.MigrationArtifactsRetirementBlocked(app.ID)
	if err != nil || blocked || reason != "" {
		t.Fatalf("verified migration must release retirement gate: blocked=%v reason=%q err=%v", blocked, reason, err)
	}
}

func TestMigrationArtifactRetirementGatesMatchPerAppDecision(t *testing.T) {
	t.Parallel()

	s, app, op, target := migrationLedgerFixture(t)
	gates, err := s.MigrationArtifactRetirementGates()
	if err != nil {
		t.Fatalf("snapshot pending migration gates: %v", err)
	}
	if gate, ok := gates[app.ID]; !ok || !gate.Blocked || gate.Reason == "" {
		t.Fatalf("pending migration missing from gate snapshot: %+v", gates)
	}
	ready := true
	physical := 1
	if _, err := s.RecordAppMigrationLedger(model.AppMigrationLedger{
		TenantID: op.TenantID, ProjectID: app.ProjectID, AppID: app.ID, OperationID: op.ID,
		OldRuntimeID: op.SourceRuntimeID, NewRuntimeID: target.ID, OldClusterID: "old", NewClusterID: "new",
		ImageReplicationStatus: model.AppMigrationEvidenceVerified, RuntimeObjectStatus: model.AppMigrationEvidenceVerified,
		EndpointRequired: true, EndpointStatus: model.AppMigrationEvidenceReady, EndpointReady: &ready,
		PhysicalReplicas: &physical, DesiredReplicas: 1, Generation: 1, ObservedGeneration: 1,
		CutoverStatus: model.AppMigrationCutoverVerified, OldArtifactsProtected: true,
	}); err != nil {
		t.Fatalf("record verified migration ledger: %v", err)
	}
	if claimed, found, err := s.ClaimNextPendingOperation(); err != nil || !found || claimed.ID != op.ID {
		t.Fatalf("claim migration: found=%v operation=%+v err=%v", found, claimed, err)
	}
	if _, err := s.CompleteAgentOperation(op.ID, target.ID, "", "migrated"); err != nil {
		t.Fatalf("complete verified migration: %v", err)
	}
	gates, err = s.MigrationArtifactRetirementGates()
	if err != nil {
		t.Fatalf("snapshot completed migration gates: %v", err)
	}
	if gate, blocked := gates[app.ID]; blocked {
		t.Fatalf("verified completed migration remained blocked: %+v", gate)
	}
}

func TestCompletedMigrationRetirementGateAcceptsHistoricalVerifiedEvidence(t *testing.T) {
	s, app, op, target := migrationLedgerFixture(t)
	ready := true
	physical := 1
	verified, err := s.RecordAppMigrationLedger(model.AppMigrationLedger{
		TenantID: op.TenantID, ProjectID: app.ProjectID, AppID: app.ID, OperationID: op.ID,
		OldRuntimeID: op.SourceRuntimeID, NewRuntimeID: target.ID, OldClusterID: "old", NewClusterID: "new",
		ImageReplicationStatus: model.AppMigrationEvidenceVerified,
		RuntimeObjectStatus:    model.AppMigrationEvidenceVerified,
		EndpointStatus:         model.AppMigrationEvidenceNotApplicable,
		EndpointReady:          &ready, PhysicalReplicas: &physical, DesiredReplicas: 1,
		Generation: 1, ObservedGeneration: 1, CutoverStatus: model.AppMigrationCutoverVerified,
		OldArtifactsProtected: true, ObservedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("record verified migration: %v", err)
	}
	if claimed, found, claimErr := s.ClaimNextPendingOperation(); claimErr != nil || !found || claimed.ID != op.ID {
		t.Fatalf("claim migration: found=%v op=%+v err=%v", found, claimed, claimErr)
	}
	if _, err := s.CompleteAgentOperation(op.ID, target.ID, "", "migrated"); err != nil {
		t.Fatalf("complete migration: %v", err)
	}
	// A later immutable audit snapshot may be old by the time a cleanup sweep
	// runs. Retirement must re-check its structure, but not require a new K8s
	// observation after the operation already completed.
	historical := verified
	historical.ID = ""
	historical.CutoverStatus = model.AppMigrationCutoverCompleted
	historical.ObservedAt = time.Now().UTC().Add(-2 * time.Hour)
	historical.UpdatedAt = time.Now().UTC()
	if _, err := s.RecordAppMigrationLedger(historical); err != nil {
		t.Fatalf("record historical completed ledger: %v", err)
	}
	blocked, reason, err := s.MigrationArtifactsRetirementBlocked(app.ID)
	if err != nil || blocked || reason != "" {
		t.Fatalf("completed historical ledger must release retirement gate: blocked=%v reason=%q err=%v", blocked, reason, err)
	}
}

func TestMigrationLedgerRetentionDoesNotUseThirtyDayDiagnosticLimit(t *testing.T) {
	now := time.Date(2026, time.July, 30, 0, 0, 0, 0, time.UTC)
	items := []model.OperationEvidence{
		{ID: "regular-old", OperationID: "op", Type: model.OperationEvidenceTypeRolloutProgress, CollectedAt: now.Add(-31 * 24 * time.Hour)},
		{ID: "migration-89d", OperationID: "op", Type: model.OperationEvidenceTypeMigrationStarted, CollectedAt: now.Add(-89 * 24 * time.Hour)},
		{ID: "migration-91d", OperationID: "op", Type: model.OperationEvidenceTypeMigrationFailed, CollectedAt: now.Add(-91 * 24 * time.Hour)},
		{ID: "inserted", OperationID: "op", Type: model.OperationEvidenceTypeRolloutProgress, CollectedAt: now},
	}
	retained := retainOperationEvidence(items, items[len(items)-1])
	ids := map[string]bool{}
	for _, item := range retained {
		ids[item.ID] = true
	}
	if ids["regular-old"] || ids["migration-91d"] || !ids["migration-89d"] || !ids["inserted"] {
		t.Fatalf("unexpected retained evidence ids: %+v", ids)
	}
}

func TestMigrationLedgerInternalReaderDoesNotUseDiagnosticPageLimit(t *testing.T) {
	filter := normalizeOperationEvidenceFilter(model.OperationEvidenceFilter{
		Types: []string{
			model.OperationEvidenceTypeMigrationStarted,
			model.OperationEvidenceTypeMigrationCompleted,
			model.OperationEvidenceTypeMigrationFailed,
		},
		IncludeMigrationLedger: true,
		Limit:                  unboundedMigrationEvidenceLimit,
	})
	if filter.Limit != unboundedMigrationEvidenceLimit {
		t.Fatalf("migration ledger reader was silently capped at the diagnostic page limit: %+v", filter)
	}

	ordinary := normalizeOperationEvidenceFilter(model.OperationEvidenceFilter{
		Types: []string{model.OperationEvidenceTypeRolloutProgress},
		Limit: unboundedMigrationEvidenceLimit,
	})
	if ordinary.Limit != defaultOperationEvidenceLimit {
		t.Fatalf("unbounded reads must remain restricted to migration ledgers: %+v", ordinary)
	}
}

func TestMigrationLedgerArchiveSurvivesTenantPurge(t *testing.T) {
	t.Parallel()
	s, app, op, _ := migrationLedgerFixture(t)
	before, found, err := s.LatestAppMigrationLedger(op.ID)
	if err != nil || !found {
		t.Fatalf("read migration ledger before tenant purge: found=%v ledger=%+v err=%v", found, before, err)
	}
	if _, err := s.DeleteTenant(app.TenantID); err != nil {
		t.Fatalf("delete tenant: %v", err)
	}
	after, found, err := s.LatestAppMigrationLedger(op.ID)
	if err != nil || !found {
		t.Fatalf("migration ledger was cascaded with tenant data: found=%v ledger=%+v err=%v", found, after, err)
	}
	if after.ID != before.ID || after.AppID != app.ID || after.OperationID != op.ID {
		t.Fatalf("archived migration ledger changed across tenant purge: before=%+v after=%+v", before, after)
	}
	if after.RetainUntil.Before(after.CreatedAt.Add(90 * 24 * time.Hour)) {
		t.Fatalf("archived ledger retention is shorter than 90 days: %+v", after)
	}
	blocked, reason, err := s.MigrationArtifactsRetirementBlocked(app.ID)
	if err != nil || !blocked || reason == "" {
		t.Fatalf("purged parent records must keep archived migration artifacts protected: blocked=%v reason=%q err=%v", blocked, reason, err)
	}
}

func TestMigrationCutoverGateRejectsMissingOrStaleEvidence(t *testing.T) {
	now := time.Now().UTC()
	base := model.AppMigrationLedger{
		AppID:                  "app",
		OperationID:            "op",
		AssociatedOperationID:  "op",
		NewRuntimeID:           "runtime-new",
		OldClusterID:           "cluster-old",
		NewClusterID:           "cluster-new",
		ImageReplicationStatus: model.AppMigrationEvidenceVerified,
		RuntimeObjectStatus:    model.AppMigrationEvidenceVerified,
		EndpointStatus:         model.AppMigrationEvidenceNotApplicable,
		PhysicalReplicas:       intPointer(1),
		DesiredReplicas:        1,
		Generation:             2,
		ObservedGeneration:     2,
		CutoverStatus:          model.AppMigrationCutoverVerified,
		OldArtifactsProtected:  true,
		OperatorType:           "controller",
		OperatorID:             "controller-1",
		EvidenceSource:         model.OperationEvidenceSourceKubernetesAPI,
		ObservedAt:             now,
	}
	if err := ValidateAppMigrationCutover(base); err != nil {
		t.Fatalf("fresh complete evidence rejected: %v", err)
	}
	missingTimestamp := base
	missingTimestamp.ObservedAt = time.Time{}
	if err := ValidateAppMigrationCutover(missingTimestamp); err == nil {
		t.Fatal("missing observation timestamp must fail closed")
	}
	stale := base
	stale.ObservedAt = now.Add(-16 * time.Minute)
	if err := ValidateAppMigrationCutover(stale); err == nil {
		t.Fatal("stale migration evidence must fail closed")
	}
}

func intPointer(value int) *int { return &value }

func TestLatestMigrationLedgerByAppDoesNotFallBackPastMissingNewestOperation(t *testing.T) {
	s, app, first, target := migrationLedgerFixture(t)
	desired := app.Spec
	desired.RuntimeID = target.ID
	second, err := s.CreateOperation(model.Operation{
		TenantID: app.TenantID, Type: model.OperationTypeMigrate, AppID: app.ID,
		SourceRuntimeID: first.TargetRuntimeID, TargetRuntimeID: target.ID, DesiredSpec: &desired,
	})
	if err != nil {
		t.Fatalf("create second migration: %v", err)
	}
	if err := s.withLockedState(true, func(state *model.State) error {
		filtered := state.OperationEvidence[:0]
		for _, evidence := range state.OperationEvidence {
			if evidence.OperationID != second.ID {
				filtered = append(filtered, evidence)
			}
		}
		state.OperationEvidence = filtered
		archived := state.AppMigrationLedgers[:0]
		for _, ledger := range state.AppMigrationLedgers {
			if ledger.OperationID != second.ID {
				archived = append(archived, ledger)
			}
		}
		state.AppMigrationLedgers = archived
		return nil
	}); err != nil {
		t.Fatalf("remove newest ledger for test: %v", err)
	}
	ledgers, err := s.LatestAppMigrationLedgersByApp()
	if err != nil {
		t.Fatalf("list latest migration ledgers: %v", err)
	}
	latest, ok := ledgers[app.ID]
	if !ok || latest.OperationID != second.ID || latest.CutoverStatus != model.AppMigrationCutoverBlocked || !latest.OldArtifactsProtected {
		t.Fatalf("newest missing ledger must remain blocked, got ok=%v ledger=%+v", ok, latest)
	}
}

func TestMigrationFailureLedgerSurvivesMissingAppRecord(t *testing.T) {
	s, app, op, _ := migrationLedgerFixture(t)
	if err := s.withLockedState(true, func(state *model.State) error {
		apps := state.Apps[:0]
		for _, candidate := range state.Apps {
			if candidate.ID != app.ID {
				apps = append(apps, candidate)
			}
		}
		state.Apps = apps
		return nil
	}); err != nil {
		t.Fatalf("remove app record: %v", err)
	}
	if err := s.recordMigrationFailureLedger(op, "target endpoint missing", model.OperationEvidenceSourceController); err != nil {
		t.Fatalf("record migration failure after app removal: %v", err)
	}
	latest, found, err := s.LatestAppMigrationLedger(op.ID)
	if err != nil || !found || latest.CutoverStatus != model.AppMigrationCutoverFailed || !latest.OldArtifactsProtected {
		t.Fatalf("missing app must not erase migration failure evidence: found=%v ledger=%+v err=%v", found, latest, err)
	}
}
