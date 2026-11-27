package store

import (
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/pgretire"
)

func TestSourceRetirementEvidenceSurvivesDiagnosticPruning(t *testing.T) {
	t.Parallel()
	s := newStoreTestDatabase(t, "")
	const tenant, app, operation = "retirement-tenant", "retirement-app", "retirement-operation"
	seedStoreTestIdentity(t, s, tenant, "retirement-project", app)
	ctx := t.Context()
	now := time.Now().UTC()
	old := now.Add(-365 * 24 * time.Hour)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT INTO fugue_operations
	(id,tenant_id,app_id,type,status,execution_mode,requested_by_type,requested_by_id,created_at,updated_at)
	VALUES ($1,$2,$3,'database-source-retire','completed','managed','system','fixture',$4,$4)`, operation, tenant, app, old); err != nil {
		t.Fatal(err)
	}
	for id, kind := range map[string]string{"intent": pgretire.IntentEvidence, "verified": pgretire.VerifiedEvidence, "ordinary": model.OperationEvidenceTypeRolloutProgress} {
		if _, err := tx.ExecContext(ctx, `INSERT INTO fugue_operation_evidence
		(id,tenant_id,app_id,operation_id,evidence_type,source,severity,confidence,observed_at,collected_at,summary,created_at)
		VALUES ($1,$2,$3,$4,$5,'test','info','high',$6,$6,$1,$6)`, id, tenant, app, operation, kind, old); err != nil {
			t.Fatal(err)
		}
	}
	if err := pruneOperationEvidenceTx(ctx, tx, model.OperationEvidence{
		TenantID: tenant, AppID: app, OperationID: operation, CollectedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	got, err := s.ListOperationEvidence(model.OperationEvidenceFilter{PlatformAdmin: true, OperationID: operation})
	if err != nil || len(got) != 2 {
		t.Fatal("safety evidence was lost or ordinary diagnostics were retained", got, err)
	}
	for _, row := range got {
		if row.Type != pgretire.IntentEvidence && row.Type != pgretire.VerifiedEvidence {
			t.Fatalf("unexpected evidence type %s", row.Type)
		}
	}
}
