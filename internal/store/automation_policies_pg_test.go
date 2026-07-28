package store

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"fugue/internal/model"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestPostgresAutomationPolicyLifecycleUsesParentLocksAndGenerationCAS(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	stateStore := &Store{db: db, databaseURL: "postgres://automation-test", dbReady: true}

	baseTime := time.Date(2026, 7, 28, 8, 0, 0, 0, time.UTC)
	input := testAutomationPolicy("tenant_a", "project_a", "API Recovery")
	input.ID = "automation_policy_test"
	normalized, err := normalizeAutomationPolicyForStore(input)
	if err != nil {
		t.Fatal(err)
	}
	createdRow := normalized
	createdRow.Generation = 1
	createdRow.CreatedAt = baseTime
	createdRow.UpdatedAt = baseTime

	mock.ExpectBegin()
	mock.ExpectQuery(`(?s)SELECT id\s+FROM fugue_tenants\s+WHERE id = \$1\s+FOR KEY SHARE`).
		WithArgs(input.TenantID).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(input.TenantID))
	mock.ExpectQuery(`(?s)SELECT tenant_id, delete_requested_at\s+FROM fugue_projects\s+WHERE id = \$1\s+FOR KEY SHARE`).
		WithArgs(input.ProjectID).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "delete_requested_at"}).AddRow(input.TenantID, nil))
	mock.ExpectQuery(`(?s)INSERT INTO fugue_automation_policies .*RETURNING`).
		WithArgs(
			input.ID, input.TenantID, input.ProjectID, normalized.Name, normalized.Description, normalized.Kind, normalized.OwnerType,
			normalized.Scope.Type, normalized.Scope.ID, normalized.Mode, normalized.Priority, normalized.Managed, normalized.SourceRef,
			sqlmock.AnyArg(), int64(1), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
		).
		WillReturnRows(automationPolicySQLRows(t, createdRow))
	mock.ExpectCommit()

	created, err := stateStore.CreateAutomationPolicy(input)
	if err != nil {
		t.Fatal(err)
	}
	if created.ID != input.ID || created.Generation != 1 {
		t.Fatalf("created policy=%+v", created)
	}

	update := created
	update.Name = "API Recovery Shadow"
	update.Mode = model.GatePolicyModeShadow
	updatedRow := update
	updatedRow.Generation = 2
	updatedRow.UpdatedAt = baseTime.Add(time.Minute)

	mock.ExpectBegin()
	mock.ExpectQuery(`(?s)SELECT .*FROM fugue_automation_policies\s+WHERE id = \$1 AND tenant_id = \$2 FOR UPDATE`).
		WithArgs(created.ID, created.TenantID).
		WillReturnRows(automationPolicySQLRows(t, createdRow))
	mock.ExpectQuery(`(?s)UPDATE fugue_automation_policies\s+SET .*WHERE id = \$1 AND generation = \$14\s+RETURNING`).
		WithArgs(
			created.ID, update.Name, update.Description, update.Kind,
			update.Scope.Type, update.Scope.ID, update.Mode, update.Priority,
			update.SourceRef, sqlmock.AnyArg(), int64(2), sqlmock.AnyArg(),
			sqlmock.AnyArg(), int64(1),
		).
		WillReturnRows(automationPolicySQLRows(t, updatedRow))
	mock.ExpectCommit()

	updated, err := stateStore.UpdateAutomationPolicy(update, created.TenantID, false, 1)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Generation != 2 || updated.Mode != model.GatePolicyModeShadow {
		t.Fatalf("updated policy=%+v", updated)
	}

	mock.ExpectBegin()
	mock.ExpectQuery(`(?s)SELECT .*FROM fugue_automation_policies\s+WHERE id = \$1 AND tenant_id = \$2 FOR UPDATE`).
		WithArgs(updated.ID, updated.TenantID).
		WillReturnRows(automationPolicySQLRows(t, updatedRow))
	mock.ExpectRollback()
	if _, err := stateStore.UpdateAutomationPolicy(updated, updated.TenantID, false, 1); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale update error=%v, want conflict", err)
	}

	mock.ExpectBegin()
	mock.ExpectQuery(`(?s)SELECT .*FROM fugue_automation_policies\s+WHERE id = \$1 AND tenant_id = \$2 FOR UPDATE`).
		WithArgs(updated.ID, updated.TenantID).
		WillReturnRows(automationPolicySQLRows(t, updatedRow))
	mock.ExpectExec(`(?s)DELETE FROM fugue_automation_policies\s+WHERE id = \$1 AND generation = \$2`).
		WithArgs(updated.ID, int64(2)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	removed, err := stateStore.DeleteAutomationPolicy(updated.ID, updated.TenantID, false, 2)
	if err != nil {
		t.Fatal(err)
	}
	if removed.ID != updated.ID || removed.Generation != updated.Generation {
		t.Fatalf("removed policy=%+v", removed)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresAutomationPolicyReadsPreserveTenantAndProjectFilters(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	stateStore := &Store{db: db, databaseURL: "postgres://automation-test", dbReady: true}
	policy := testAutomationPolicy("tenant_a", "project_a", "Recovery")
	policy.ID = "automation_policy_read"
	policy.Generation = 1
	policy.CreatedAt = time.Date(2026, 7, 28, 8, 0, 0, 0, time.UTC)
	policy.UpdatedAt = policy.CreatedAt

	mock.ExpectQuery(`(?s)FROM fugue_automation_policies\s+WHERE owner_type = 'user' AND managed = FALSE AND tenant_id = \$1 AND project_id = \$2`).
		WithArgs(policy.TenantID, policy.ProjectID).
		WillReturnRows(automationPolicySQLRows(t, policy))
	policies, err := stateStore.ListAutomationPolicies(AutomationPolicyFilter{
		TenantID: policy.TenantID, ProjectID: policy.ProjectID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(policies) != 1 || policies[0].ID != policy.ID {
		t.Fatalf("tenant/project list=%+v", policies)
	}

	mock.ExpectQuery(`(?s)FROM fugue_automation_policies\s+WHERE owner_type = 'user' AND managed = FALSE AND project_id = \$1`).
		WithArgs(policy.ProjectID).
		WillReturnRows(automationPolicySQLRows(t, policy))
	policies, err = stateStore.ListAutomationPolicies(AutomationPolicyFilter{
		ProjectID: policy.ProjectID, PlatformAdmin: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(policies) != 1 || policies[0].ID != policy.ID {
		t.Fatalf("admin project list=%+v", policies)
	}

	mock.ExpectQuery(`(?s)FROM fugue_automation_policies\s+WHERE id = \$1`).
		WithArgs(policy.ID).
		WillReturnRows(automationPolicySQLRows(t, policy))
	adminPolicy, err := stateStore.GetAutomationPolicy(policy.ID, "tenant_other", true)
	if err != nil || adminPolicy.ID != policy.ID {
		t.Fatalf("platform-admin get error=%v policy=%+v", err, adminPolicy)
	}

	mock.ExpectQuery(`(?s)FROM fugue_automation_policies\s+WHERE id = \$1 AND tenant_id = \$2`).
		WithArgs(policy.ID, "tenant_other").
		WillReturnRows(automationPolicySQLRows(t))
	if _, err := stateStore.GetAutomationPolicy(policy.ID, "tenant_other", false); !errors.Is(err, ErrNotFound) {
		t.Fatalf("tenant-scoped get error=%v, want not found", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresAutomationPolicyCreateRejectsDeletingProject(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	stateStore := &Store{db: db, databaseURL: "postgres://automation-test", dbReady: true}
	policy := testAutomationPolicy("tenant_a", "project_a", "Recovery")
	deleteRequestedAt := time.Date(2026, 7, 28, 8, 0, 0, 0, time.UTC)

	mock.ExpectBegin()
	mock.ExpectQuery(`(?s)SELECT id\s+FROM fugue_tenants\s+WHERE id = \$1\s+FOR KEY SHARE`).
		WithArgs(policy.TenantID).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(policy.TenantID))
	mock.ExpectQuery(`(?s)SELECT tenant_id, delete_requested_at\s+FROM fugue_projects\s+WHERE id = \$1\s+FOR KEY SHARE`).
		WithArgs(policy.ProjectID).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "delete_requested_at"}).AddRow(policy.TenantID, deleteRequestedAt))
	mock.ExpectRollback()

	if _, err := stateStore.CreateAutomationPolicy(policy); !errors.Is(err, ErrConflict) {
		t.Fatalf("create in deleting project error=%v, want conflict", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func automationPolicySQLRows(t *testing.T, policies ...model.AutomationPolicy) *sqlmock.Rows {
	t.Helper()
	rows := sqlmock.NewRows([]string{
		"id", "tenant_id", "project_id", "name", "description", "kind", "owner_type",
		"scope_type", "scope_id", "mode", "priority", "managed", "source_ref",
		"rules_json", "generation", "metadata_json", "created_at", "updated_at",
	})
	for _, policy := range policies {
		rulesJSON, err := json.Marshal(policy.Rules)
		if err != nil {
			t.Fatal(err)
		}
		metadataJSON, err := json.Marshal(policy.Metadata)
		if err != nil {
			t.Fatal(err)
		}
		var projectID any
		if policy.ProjectID != "" {
			projectID = policy.ProjectID
		}
		rows.AddRow(
			policy.ID, policy.TenantID, projectID, policy.Name, policy.Description, policy.Kind, policy.OwnerType,
			policy.Scope.Type, policy.Scope.ID, policy.Mode, policy.Priority, policy.Managed, policy.SourceRef,
			rulesJSON, policy.Generation, metadataJSON, policy.CreatedAt, policy.UpdatedAt,
		)
	}
	return rows
}
