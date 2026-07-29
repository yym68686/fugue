package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
)

const automationActionIntentSelectColumns = `
id, tenant_id, project_id, policy_id, policy_generation, rule_id,
scope_type, scope_id, mode, source, status,
rule_snapshot_json, evidence_json, decision_json,
evidence_hash, idempotency_key, rollback_target,
production_mutation_allowed, expires_at, created_at, updated_at`

func (s *Store) pgCreateAutomationActionIntent(
	intent model.AutomationActionIntent,
) (model.AutomationActionIntent, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.AutomationActionIntent{}, false, fmt.Errorf("begin create automation intent transaction: %w", err)
	}
	defer tx.Rollback()

	existing, err := pgGetAutomationActionIntentByIdempotency(ctx, tx, intent.IdempotencyKey, true)
	switch {
	case err == nil:
		if !automationActionIntentsEquivalent(existing, intent) {
			return model.AutomationActionIntent{}, false, ErrIdempotencyMismatch
		}
		if err := tx.Commit(); err != nil {
			return model.AutomationActionIntent{}, false, fmt.Errorf("commit reused automation intent transaction: %w", err)
		}
		return existing, false, nil
	case err != sql.ErrNoRows:
		return model.AutomationActionIntent{}, false, mapDBErr(err)
	}

	policy, err := scanAutomationPolicy(tx.QueryRowContext(ctx, `
SELECT `+automationPolicySelectColumns+`
FROM fugue_automation_policies
WHERE id = $1
FOR KEY SHARE
`, intent.PolicyID))
	if err != nil {
		return model.AutomationActionIntent{}, false, mapDBErr(err)
	}
	if err := validateAutomationActionIntentPolicy(policy, intent); err != nil {
		return model.AutomationActionIntent{}, false, err
	}

	if intent.ID == "" {
		intent.ID = model.NewID("automation_intent")
	}
	now := time.Now().UTC()
	intent.CreatedAt = now
	intent.UpdatedAt = now
	intent, err = normalizePersistedAutomationActionIntent(intent)
	if err != nil {
		return model.AutomationActionIntent{}, false, err
	}
	ruleJSON, evidenceJSON, decisionJSON, err := marshalAutomationActionIntentJSON(intent)
	if err != nil {
		return model.AutomationActionIntent{}, false, err
	}

	stored, err := scanAutomationActionIntent(tx.QueryRowContext(ctx, `
INSERT INTO fugue_automation_action_intents (
	id, tenant_id, project_id, policy_id, policy_generation, rule_id,
	scope_type, scope_id, mode, source, status,
	rule_snapshot_json, evidence_json, decision_json,
	evidence_hash, idempotency_key, rollback_target,
	production_mutation_allowed, expires_at, created_at, updated_at
)
VALUES (
	$1, $2, $3, $4, $5, $6,
	$7, $8, $9, $10, $11,
	$12::jsonb, $13::jsonb, $14::jsonb,
	$15, $16, $17,
	$18, $19, $20, $21
)
ON CONFLICT (idempotency_key) DO NOTHING
RETURNING `+automationActionIntentSelectColumns,
		intent.ID, intent.TenantID, intent.ProjectID, intent.PolicyID, intent.PolicyGeneration, intent.RuleID,
		intent.Scope.Type, intent.Scope.ID, intent.Mode, intent.Source, intent.Status,
		ruleJSON, evidenceJSON, decisionJSON,
		intent.EvidenceHash, intent.IdempotencyKey, intent.RollbackTarget,
		intent.ProductionMutationAllowed, intent.ExpiresAt, intent.CreatedAt, intent.UpdatedAt,
	))
	if err == sql.ErrNoRows {
		existing, lookupErr := pgGetAutomationActionIntentByIdempotency(ctx, tx, intent.IdempotencyKey, true)
		if lookupErr != nil {
			return model.AutomationActionIntent{}, false, mapDBErr(lookupErr)
		}
		if !automationActionIntentsEquivalent(existing, intent) {
			return model.AutomationActionIntent{}, false, ErrIdempotencyMismatch
		}
		if err := tx.Commit(); err != nil {
			return model.AutomationActionIntent{}, false, fmt.Errorf("commit raced automation intent transaction: %w", err)
		}
		return existing, false, nil
	}
	if err != nil {
		return model.AutomationActionIntent{}, false, mapDBErr(err)
	}
	if err := tx.Commit(); err != nil {
		return model.AutomationActionIntent{}, false, fmt.Errorf("commit create automation intent transaction: %w", err)
	}
	return stored, true, nil
}

func (s *Store) pgListAutomationActionIntents(
	filter AutomationActionIntentFilter,
) ([]model.AutomationActionIntent, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	args := make([]any, 0, 7)
	clauses := make([]string, 0, 6)
	if filter.TenantID != "" {
		args = append(args, filter.TenantID)
		clauses = append(clauses, fmt.Sprintf("tenant_id = $%d", len(args)))
	}
	if filter.ProjectID != "" {
		args = append(args, filter.ProjectID)
		clauses = append(clauses, fmt.Sprintf("project_id = $%d", len(args)))
	}
	if filter.PolicyID != "" {
		args = append(args, filter.PolicyID)
		clauses = append(clauses, fmt.Sprintf("policy_id = $%d", len(args)))
	}
	if filter.AppID != "" {
		args = append(args, filter.AppID)
		clauses = append(clauses, fmt.Sprintf("scope_type = 'app' AND scope_id = $%d", len(args)))
	}
	if filter.Source != "" {
		args = append(args, filter.Source)
		clauses = append(clauses, fmt.Sprintf("source = $%d", len(args)))
	}
	if filter.Status != "" {
		args = append(args, filter.Status)
		clauses = append(clauses, fmt.Sprintf("status = $%d", len(args)))
	}
	query := `SELECT ` + automationActionIntentSelectColumns + `
FROM fugue_automation_action_intents`
	if len(clauses) > 0 {
		query += `
WHERE ` + strings.Join(clauses, " AND ")
	}
	args = append(args, filter.Limit)
	query += fmt.Sprintf(`
ORDER BY created_at DESC, id DESC
LIMIT $%d`, len(args))

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list automation intents: %w", err)
	}
	defer rows.Close()
	intents := make([]model.AutomationActionIntent, 0)
	for rows.Next() {
		intent, err := scanAutomationActionIntent(rows)
		if err != nil {
			return nil, fmt.Errorf("scan automation intent: %w", err)
		}
		intents = append(intents, intent)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate automation intents: %w", err)
	}
	return intents, nil
}

func (s *Store) pgGetAutomationActionIntent(
	id,
	tenantID string,
	platformAdmin bool,
) (model.AutomationActionIntent, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `SELECT ` + automationActionIntentSelectColumns + `
FROM fugue_automation_action_intents
WHERE id = $1`
	args := []any{id}
	if !platformAdmin {
		args = append(args, tenantID)
		query += ` AND tenant_id = $2`
	}
	intent, err := scanAutomationActionIntent(s.db.QueryRowContext(ctx, query, args...))
	if err != nil {
		return model.AutomationActionIntent{}, mapDBErr(err)
	}
	return intent, nil
}

func pgGetAutomationActionIntentByIdempotency(
	ctx context.Context,
	tx *sql.Tx,
	idempotencyKey string,
	forShare bool,
) (model.AutomationActionIntent, error) {
	query := `SELECT ` + automationActionIntentSelectColumns + `
FROM fugue_automation_action_intents
WHERE idempotency_key = $1`
	if forShare {
		query += ` FOR SHARE`
	}
	return scanAutomationActionIntent(tx.QueryRowContext(ctx, query, idempotencyKey))
}

func marshalAutomationActionIntentJSON(
	intent model.AutomationActionIntent,
) ([]byte, []byte, []byte, error) {
	ruleJSON, err := marshalJSON(intent.RuleSnapshot)
	if err != nil {
		return nil, nil, nil, err
	}
	evidenceJSON, err := marshalJSON(intent.Evidence)
	if err != nil {
		return nil, nil, nil, err
	}
	decisionJSON, err := marshalJSON(intent.Decision)
	if err != nil {
		return nil, nil, nil, err
	}
	return ruleJSON, evidenceJSON, decisionJSON, nil
}

func scanAutomationActionIntent(scanner sqlScanner) (model.AutomationActionIntent, error) {
	var (
		intent      model.AutomationActionIntent
		ruleRaw     []byte
		evidenceRaw []byte
		decisionRaw []byte
	)
	if err := scanner.Scan(
		&intent.ID,
		&intent.TenantID,
		&intent.ProjectID,
		&intent.PolicyID,
		&intent.PolicyGeneration,
		&intent.RuleID,
		&intent.Scope.Type,
		&intent.Scope.ID,
		&intent.Mode,
		&intent.Source,
		&intent.Status,
		&ruleRaw,
		&evidenceRaw,
		&decisionRaw,
		&intent.EvidenceHash,
		&intent.IdempotencyKey,
		&intent.RollbackTarget,
		&intent.ProductionMutationAllowed,
		&intent.ExpiresAt,
		&intent.CreatedAt,
		&intent.UpdatedAt,
	); err != nil {
		return model.AutomationActionIntent{}, err
	}
	rule, err := decodeJSONValue[model.AutomationRule](ruleRaw)
	if err != nil {
		return model.AutomationActionIntent{}, err
	}
	evidence, err := decodeJSONValue[model.AutomationEvaluationEvidence](evidenceRaw)
	if err != nil {
		return model.AutomationActionIntent{}, err
	}
	decision, err := decodeJSONValue[model.AutomationEvaluationDecision](decisionRaw)
	if err != nil {
		return model.AutomationActionIntent{}, err
	}
	intent.RuleSnapshot = rule
	intent.Evidence = evidence
	intent.Decision = decision
	intent.ExpiresAt = intent.ExpiresAt.UTC()
	intent.CreatedAt = intent.CreatedAt.UTC()
	intent.UpdatedAt = intent.UpdatedAt.UTC()
	return normalizePersistedAutomationActionIntent(intent)
}
