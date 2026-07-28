package store

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestAutomationPolicyPostgresIntegration(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("FUGUE_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set FUGUE_TEST_DATABASE_URL to run automation policy Postgres integration test")
	}

	stateStore := New("", databaseURL)
	if err := stateStore.Init(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = stateStore.db.Close()
	})
	suffix := model.NewID("integration")
	tenant, err := stateStore.CreateTenant("automation-pg-integration-" + suffix)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = stateStore.DeleteTenant(tenant.ID)
	})
	project, err := stateStore.CreateProject(tenant.ID, "automation-pg-project-"+suffix, "")
	if err != nil {
		t.Fatal(err)
	}

	policy := testAutomationPolicy(tenant.ID, project.ID, "Postgres Recovery")
	created, err := stateStore.CreateAutomationPolicy(policy)
	if err != nil {
		t.Fatal(err)
	}
	if created.Generation != 1 {
		t.Fatalf("created generation=%d, want 1", created.Generation)
	}
	listed, err := stateStore.ListAutomationPolicies(AutomationPolicyFilter{TenantID: tenant.ID})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || listed[0].ID != created.ID {
		t.Fatalf("listed policies=%+v", listed)
	}

	created.Name = "Postgres Recovery Shadow"
	created.Mode = model.GatePolicyModeShadow
	updated, err := stateStore.UpdateAutomationPolicy(created, tenant.ID, false, 1)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Generation != 2 {
		t.Fatalf("updated generation=%d, want 2", updated.Generation)
	}
	if _, err := stateStore.UpdateAutomationPolicy(updated, tenant.ID, false, 1); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale update error=%v, want conflict", err)
	}
	if _, err := stateStore.DeleteAutomationPolicy(updated.ID, tenant.ID, false, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := stateStore.GetAutomationPolicy(updated.ID, tenant.ID, false); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted policy get error=%v, want not found", err)
	}

	// The test is intentionally independent of the JSON path and confirms the
	// parent composite foreign key rejects a project from another tenant.
	otherTenant, err := stateStore.CreateTenant("automation-pg-other-" + suffix)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = stateStore.DeleteTenant(otherTenant.ID)
	})
	otherProject, err := stateStore.CreateProject(otherTenant.ID, "automation-pg-other-project-"+suffix, "")
	if err != nil {
		t.Fatal(err)
	}
	mismatched := testAutomationPolicy(tenant.ID, otherProject.ID, "Mismatched")
	mismatched.ID = "automation_policy_mismatched_parent"
	mismatched.Generation = 1
	mismatched.CreatedAt = time.Now().UTC()
	mismatched.UpdatedAt = mismatched.CreatedAt
	rulesJSON, metadataJSON, err := marshalAutomationPolicyJSON(mismatched)
	if err != nil {
		t.Fatal(err)
	}
	_, err = stateStore.db.ExecContext(context.Background(), `
INSERT INTO fugue_automation_policies (
	id, tenant_id, project_id, name, description, kind, owner_type,
	scope_type, scope_id, mode, priority, managed, source_ref,
	rules_json, generation, metadata_json, created_at, updated_at
)
VALUES (
	$1, $2, $3, $4, $5, $6, $7,
	$8, $9, $10, $11, $12, $13,
	$14::jsonb, $15, $16::jsonb, $17, $18
)`,
		mismatched.ID, mismatched.TenantID, mismatched.ProjectID, mismatched.Name, mismatched.Description, mismatched.Kind, mismatched.OwnerType,
		mismatched.Scope.Type, mismatched.Scope.ID, mismatched.Mode, mismatched.Priority, mismatched.Managed, mismatched.SourceRef,
		rulesJSON, mismatched.Generation, metadataJSON, mismatched.CreatedAt, mismatched.UpdatedAt,
	)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23503" {
		t.Fatalf("mismatched project database error=%v, want foreign-key violation", err)
	}
}
