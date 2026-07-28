package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
)

const automationPolicySelectColumns = `
id, tenant_id, project_id, name, description, kind, owner_type,
scope_type, scope_id, mode, priority, managed, source_ref,
rules_json, generation, metadata_json, created_at, updated_at`

func (s *Store) pgCreateAutomationPolicy(policy model.AutomationPolicy) (model.AutomationPolicy, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.AutomationPolicy{}, fmt.Errorf("begin create automation policy transaction: %w", err)
	}
	defer tx.Rollback()

	if err := pgLockAutomationPolicyParents(ctx, tx, policy); err != nil {
		return model.AutomationPolicy{}, err
	}
	now := time.Now().UTC()
	if policy.ID == "" {
		policy.ID = model.NewID("automation_policy")
	}
	policy.Generation = 1
	policy.CreatedAt = now
	policy.UpdatedAt = now
	rulesJSON, metadataJSON, err := marshalAutomationPolicyJSON(policy)
	if err != nil {
		return model.AutomationPolicy{}, err
	}

	stored, err := scanAutomationPolicy(tx.QueryRowContext(ctx, `
INSERT INTO fugue_automation_policies (
	id, tenant_id, project_id, name, description, kind, owner_type,
	scope_type, scope_id, mode, priority, managed, source_ref,
	rules_json, generation, metadata_json, created_at, updated_at
)
VALUES (
	$1, $2, $3, $4, $5, $6, $7,
	$8, $9, $10, $11, $12, $13,
	$14::jsonb, $15, $16::jsonb, $17, $18
)
RETURNING `+automationPolicySelectColumns,
		policy.ID, policy.TenantID, nullIfEmpty(policy.ProjectID), policy.Name, policy.Description, policy.Kind, policy.OwnerType,
		policy.Scope.Type, policy.Scope.ID, policy.Mode, policy.Priority, policy.Managed, policy.SourceRef,
		rulesJSON, policy.Generation, metadataJSON, policy.CreatedAt, policy.UpdatedAt,
	))
	if err != nil {
		return model.AutomationPolicy{}, mapDBErr(err)
	}
	if err := tx.Commit(); err != nil {
		return model.AutomationPolicy{}, fmt.Errorf("commit create automation policy transaction: %w", err)
	}
	return stored, nil
}

func (s *Store) pgListAutomationPolicies(filter AutomationPolicyFilter) ([]model.AutomationPolicy, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	args := make([]any, 0, 2)
	clauses := []string{"owner_type = 'user'", "managed = FALSE"}
	if filter.TenantID != "" {
		args = append(args, filter.TenantID)
		clauses = append(clauses, fmt.Sprintf("tenant_id = $%d", len(args)))
	}
	if filter.ProjectID != "" {
		args = append(args, filter.ProjectID)
		clauses = append(clauses, fmt.Sprintf("project_id = $%d", len(args)))
	}
	query := `SELECT ` + automationPolicySelectColumns + `
FROM fugue_automation_policies
WHERE ` + strings.Join(clauses, " AND ") + `
ORDER BY tenant_id ASC, project_id ASC NULLS FIRST, priority DESC, lower(name) ASC, id ASC`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list automation policies: %w", err)
	}
	defer rows.Close()

	policies := make([]model.AutomationPolicy, 0)
	for rows.Next() {
		policy, err := scanAutomationPolicy(rows)
		if err != nil {
			return nil, fmt.Errorf("scan automation policy: %w", err)
		}
		policies = append(policies, policy)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate automation policies: %w", err)
	}
	return policies, nil
}

func (s *Store) pgGetAutomationPolicy(id, tenantID string, platformAdmin bool) (model.AutomationPolicy, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `SELECT ` + automationPolicySelectColumns + `
FROM fugue_automation_policies
WHERE id = $1`
	args := []any{id}
	if !platformAdmin {
		args = append(args, tenantID)
		query += ` AND tenant_id = $2`
	}
	policy, err := scanAutomationPolicy(s.db.QueryRowContext(ctx, query, args...))
	if err != nil {
		return model.AutomationPolicy{}, mapDBErr(err)
	}
	return policy, nil
}

func (s *Store) pgUpdateAutomationPolicy(policy model.AutomationPolicy, tenantID string, platformAdmin bool, expectedGeneration int64) (model.AutomationPolicy, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.AutomationPolicy{}, fmt.Errorf("begin update automation policy transaction: %w", err)
	}
	defer tx.Rollback()

	policy, err = normalizeAutomationPolicyForStore(policy)
	if err != nil {
		return model.AutomationPolicy{}, err
	}
	if err := pgLockAutomationPolicyParents(ctx, tx, policy); err != nil {
		return model.AutomationPolicy{}, err
	}
	existing, err := pgGetAutomationPolicyForUpdate(ctx, tx, policy.ID, tenantID, platformAdmin)
	if err != nil {
		return model.AutomationPolicy{}, err
	}
	if existing.Generation != expectedGeneration {
		return model.AutomationPolicy{}, ErrConflict
	}
	if strings.TrimSpace(policy.TenantID) != existing.TenantID ||
		strings.TrimSpace(policy.ProjectID) != existing.ProjectID ||
		strings.TrimSpace(policy.OwnerType) != existing.OwnerType ||
		policy.Managed != existing.Managed ||
		policy.Kind != existing.Kind {
		return model.AutomationPolicy{}, fmt.Errorf("%w: policy identity and ownership fields are immutable", ErrInvalidInput)
	}
	policy.ID = existing.ID
	policy.TenantID = existing.TenantID
	policy.ProjectID = existing.ProjectID
	policy.OwnerType = existing.OwnerType
	policy.Managed = existing.Managed
	policy.CreatedAt = existing.CreatedAt
	policy.Generation = existing.Generation + 1
	policy.UpdatedAt = time.Now().UTC()
	rulesJSON, metadataJSON, err := marshalAutomationPolicyJSON(policy)
	if err != nil {
		return model.AutomationPolicy{}, err
	}

	stored, err := scanAutomationPolicy(tx.QueryRowContext(ctx, `
UPDATE fugue_automation_policies
SET name = $2,
	description = $3,
	kind = $4,
	scope_type = $5,
	scope_id = $6,
	mode = $7,
	priority = $8,
	source_ref = $9,
	rules_json = $10::jsonb,
	generation = $11,
	metadata_json = $12::jsonb,
	updated_at = $13
WHERE id = $1 AND generation = $14
RETURNING `+automationPolicySelectColumns,
		policy.ID, policy.Name, policy.Description, policy.Kind,
		policy.Scope.Type, policy.Scope.ID, policy.Mode, policy.Priority,
		policy.SourceRef, rulesJSON, policy.Generation, metadataJSON,
		policy.UpdatedAt, expectedGeneration,
	))
	if err != nil {
		if err == sql.ErrNoRows {
			return model.AutomationPolicy{}, ErrConflict
		}
		return model.AutomationPolicy{}, mapDBErr(err)
	}
	if err := tx.Commit(); err != nil {
		return model.AutomationPolicy{}, fmt.Errorf("commit update automation policy transaction: %w", err)
	}
	return stored, nil
}

func (s *Store) pgDeleteAutomationPolicy(id, tenantID string, platformAdmin bool, expectedGeneration int64) (model.AutomationPolicy, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.AutomationPolicy{}, fmt.Errorf("begin delete automation policy transaction: %w", err)
	}
	defer tx.Rollback()

	existing, err := pgGetAutomationPolicyForUpdate(ctx, tx, id, tenantID, platformAdmin)
	if err != nil {
		return model.AutomationPolicy{}, err
	}
	if existing.Generation != expectedGeneration {
		return model.AutomationPolicy{}, ErrConflict
	}
	result, err := tx.ExecContext(ctx, `
DELETE FROM fugue_automation_policies
WHERE id = $1 AND generation = $2
`, id, expectedGeneration)
	if err != nil {
		return model.AutomationPolicy{}, fmt.Errorf("delete automation policy: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return model.AutomationPolicy{}, fmt.Errorf("read deleted automation policy row count: %w", err)
	}
	if affected != 1 {
		return model.AutomationPolicy{}, ErrConflict
	}
	if err := tx.Commit(); err != nil {
		return model.AutomationPolicy{}, fmt.Errorf("commit delete automation policy transaction: %w", err)
	}
	return existing, nil
}

func (s *Store) pgDeleteAutomationPoliciesByAppTx(ctx context.Context, tx *sql.Tx, appID string) error {
	if _, err := tx.ExecContext(ctx, `
DELETE FROM fugue_automation_policies
WHERE scope_type = $1 AND scope_id = $2
`, model.AutomationScopeApp, appID); err != nil {
		return fmt.Errorf("delete automation policies for app %s: %w", appID, err)
	}
	return nil
}

func pgGetAutomationPolicyForUpdate(ctx context.Context, tx *sql.Tx, id, tenantID string, platformAdmin bool) (model.AutomationPolicy, error) {
	query := `SELECT ` + automationPolicySelectColumns + `
FROM fugue_automation_policies
WHERE id = $1`
	args := []any{id}
	if !platformAdmin {
		args = append(args, tenantID)
		query += ` AND tenant_id = $2`
	}
	query += ` FOR UPDATE`
	policy, err := scanAutomationPolicy(tx.QueryRowContext(ctx, query, args...))
	if err != nil {
		return model.AutomationPolicy{}, mapDBErr(err)
	}
	return policy, nil
}

func pgLockAutomationPolicyParents(ctx context.Context, tx *sql.Tx, policy model.AutomationPolicy) error {
	var tenantID string
	if err := tx.QueryRowContext(ctx, `
SELECT id
FROM fugue_tenants
WHERE id = $1
FOR KEY SHARE
`, policy.TenantID).Scan(&tenantID); err != nil {
		return mapDBErr(err)
	}
	if policy.ProjectID == "" {
		if policy.Scope.Type == model.AutomationScopeApp {
			return fmt.Errorf("%w: app-scoped automation policies require a project", ErrInvalidInput)
		}
		return nil
	}
	var (
		projectTenantID   string
		deleteRequestedAt sql.NullTime
	)
	if err := tx.QueryRowContext(ctx, `
SELECT tenant_id, delete_requested_at
FROM fugue_projects
WHERE id = $1
FOR KEY SHARE
`, policy.ProjectID).Scan(&projectTenantID, &deleteRequestedAt); err != nil {
		return mapDBErr(err)
	}
	if projectTenantID != policy.TenantID {
		return ErrNotFound
	}
	if deleteRequestedAt.Valid {
		return ErrConflict
	}
	if policy.Scope.Type != model.AutomationScopeApp {
		return nil
	}
	var (
		appTenantID  string
		appProjectID string
		appPhase     string
	)
	if err := tx.QueryRowContext(ctx, `
SELECT tenant_id, project_id, lower(COALESCE(status_json->>'phase', ''))
FROM fugue_apps
WHERE id = $1
FOR KEY SHARE
`, policy.Scope.ID).Scan(&appTenantID, &appProjectID, &appPhase); err != nil {
		return mapDBErr(err)
	}
	if appTenantID != policy.TenantID ||
		appProjectID != policy.ProjectID ||
		appPhase == "deleted" ||
		appPhase == "deleting" {
		return ErrNotFound
	}
	return nil
}

func marshalAutomationPolicyJSON(policy model.AutomationPolicy) ([]byte, []byte, error) {
	rulesJSON, err := marshalJSON(policy.Rules)
	if err != nil {
		return nil, nil, err
	}
	metadata := policy.Metadata
	if metadata == nil {
		metadata = map[string]string{}
	}
	metadataJSON, err := marshalJSON(metadata)
	if err != nil {
		return nil, nil, err
	}
	return rulesJSON, metadataJSON, nil
}

func scanAutomationPolicy(scanner sqlScanner) (model.AutomationPolicy, error) {
	var (
		policy      model.AutomationPolicy
		projectID   sql.NullString
		rulesRaw    []byte
		metadataRaw []byte
	)
	if err := scanner.Scan(
		&policy.ID,
		&policy.TenantID,
		&projectID,
		&policy.Name,
		&policy.Description,
		&policy.Kind,
		&policy.OwnerType,
		&policy.Scope.Type,
		&policy.Scope.ID,
		&policy.Mode,
		&policy.Priority,
		&policy.Managed,
		&policy.SourceRef,
		&rulesRaw,
		&policy.Generation,
		&metadataRaw,
		&policy.CreatedAt,
		&policy.UpdatedAt,
	); err != nil {
		return model.AutomationPolicy{}, err
	}
	rules, err := decodeJSONValue[[]model.AutomationRule](rulesRaw)
	if err != nil {
		return model.AutomationPolicy{}, err
	}
	metadata, err := decodeJSONValue[map[string]string](metadataRaw)
	if err != nil {
		return model.AutomationPolicy{}, err
	}
	policy.ProjectID = projectID.String
	policy.Rules = rules
	policy.Metadata = metadata
	policy.CreatedAt = policy.CreatedAt.UTC()
	policy.UpdatedAt = policy.UpdatedAt.UTC()
	normalized, err := normalizePersistedAutomationPolicy(policy)
	if err != nil {
		return model.AutomationPolicy{}, err
	}
	return cloneAutomationPolicy(normalized), nil
}
