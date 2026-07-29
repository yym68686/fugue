package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
)

const automationActionDispatchSelectColumns = `
id, intent_id, tenant_id, project_id, policy_id, policy_generation, rule_id,
scope_type, scope_id, action_type, contract_id, trigger_invariant, subject,
source_generation, rollback_target, idempotency_key, wal_hash,
safety_decision_json, status, fencing_token, version, expires_at,
lease_owner, lease_expires_at, cooldown_until, last_error,
created_at, updated_at, claimed_at, completed_at`

func (s *Store) pgCreateAutomationActionDispatch(
	dispatch model.AutomationActionDispatch,
) (model.AutomationActionDispatch, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.AutomationActionDispatch{}, false, fmt.Errorf("begin create automation dispatch transaction: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, dispatch.IdempotencyKey); err != nil {
		return model.AutomationActionDispatch{}, false, fmt.Errorf("lock automation dispatch idempotency key: %w", err)
	}
	intent, err := pgGetAutomationActionIntentByIDTx(ctx, tx, dispatch.IntentID, true)
	if err != nil {
		return model.AutomationActionDispatch{}, false, mapDBErr(err)
	}
	if err := validateAutomationActionDispatchIntent(intent, dispatch); err != nil {
		return model.AutomationActionDispatch{}, false, err
	}

	existing, found, err := pgGetAutomationActionDispatchByIntentTx(ctx, tx, dispatch.IntentID, true)
	if err != nil {
		return model.AutomationActionDispatch{}, false, mapDBErr(err)
	}
	if found {
		if err := tx.Commit(); err != nil {
			return model.AutomationActionDispatch{}, false, fmt.Errorf("commit reused automation dispatch transaction: %w", err)
		}
		return existing, false, nil
	}

	existing, found, err = pgGetAutomationActionDispatchByIdempotencyTx(ctx, tx, dispatch.IdempotencyKey, true)
	if err != nil {
		return model.AutomationActionDispatch{}, false, mapDBErr(err)
	}
	if found {
		if !automationActionDispatchEquivalent(existing, dispatch) {
			return model.AutomationActionDispatch{}, false, ErrIdempotencyMismatch
		}
		if err := tx.Commit(); err != nil {
			return model.AutomationActionDispatch{}, false, fmt.Errorf("commit reused automation dispatch idempotency transaction: %w", err)
		}
		return existing, false, nil
	}

	now := time.Now().UTC().Truncate(time.Microsecond)
	if dispatch.ID == "" {
		dispatch.ID = model.NewID("automation_dispatch")
	}
	dispatch.FencingToken, err = pgNextAutomationActionFencingTokenTx(
		ctx,
		tx,
		automationActionDispatchSubjectKey(dispatch),
		dispatch.TenantID,
		now,
	)
	if err != nil {
		return model.AutomationActionDispatch{}, false, err
	}
	dispatch.Version = 1
	dispatch.CreatedAt = now
	dispatch.UpdatedAt = now
	dispatch.Status = automationActionDispatchInitialStatus(dispatch.SafetyDecision)
	dispatch.WALHash = automationActionDispatchWALHash(dispatch)
	dispatch, err = normalizePersistedAutomationActionDispatch(dispatch)
	if err != nil {
		return model.AutomationActionDispatch{}, false, err
	}
	decisionJSON, err := marshalJSON(dispatch.SafetyDecision)
	if err != nil {
		return model.AutomationActionDispatch{}, false, fmt.Errorf("encode automation dispatch safety decision: %w", err)
	}

	stored, err := scanAutomationActionDispatch(tx.QueryRowContext(ctx, `
INSERT INTO fugue_automation_action_dispatches (
	id, intent_id, tenant_id, project_id, policy_id, policy_generation, rule_id,
	scope_type, scope_id, action_type, contract_id, trigger_invariant, subject,
	source_generation, rollback_target, idempotency_key, wal_hash,
	safety_decision_json, status, fencing_token, version, expires_at,
	lease_owner, lease_expires_at, cooldown_until, last_error,
	created_at, updated_at, claimed_at, completed_at
) VALUES (
	$1, $2, $3, $4, $5, $6, $7,
	$8, $9, $10, $11, $12, $13,
	$14, $15, $16, $17,
	$18::jsonb, $19, $20, $21, $22,
	$23, $24, $25, $26,
	$27, $28, $29, $30
)
ON CONFLICT DO NOTHING
RETURNING `+automationActionDispatchSelectColumns,
		dispatch.ID, dispatch.IntentID, dispatch.TenantID, dispatch.ProjectID, dispatch.PolicyID,
		dispatch.PolicyGeneration, dispatch.RuleID, dispatch.Scope.Type, dispatch.Scope.ID,
		dispatch.ActionType, dispatch.ContractID, dispatch.TriggerInvariant, dispatch.Subject,
		dispatch.SourceGeneration, dispatch.RollbackTarget, dispatch.IdempotencyKey, dispatch.WALHash,
		decisionJSON, dispatch.Status, dispatch.FencingToken, dispatch.Version, dispatch.ExpiresAt,
		dispatch.LeaseOwner, dispatch.LeaseExpiresAt, dispatch.CooldownUntil, dispatch.LastError,
		dispatch.CreatedAt, dispatch.UpdatedAt, dispatch.ClaimedAt, dispatch.CompletedAt,
	))
	if err == sql.ErrNoRows {
		// A concurrent creator may have won the unique intent constraint after
		// the initial lookup. Resolve by intent first so wall-clock fields in a
		// freshly evaluated safety decision cannot turn an exactly-once replay
		// into a false idempotency mismatch.
		existing, found, lookupErr := pgGetAutomationActionDispatchByIntentTx(ctx, tx, dispatch.IntentID, true)
		if lookupErr != nil {
			return model.AutomationActionDispatch{}, false, mapDBErr(lookupErr)
		}
		if !found {
			existing, found, lookupErr = pgGetAutomationActionDispatchByIdempotencyTx(ctx, tx, dispatch.IdempotencyKey, true)
		}
		if lookupErr != nil {
			return model.AutomationActionDispatch{}, false, mapDBErr(lookupErr)
		}
		if !found || (existing.IntentID != dispatch.IntentID &&
			!automationActionDispatchEquivalent(existing, dispatch)) {
			return model.AutomationActionDispatch{}, false, ErrIdempotencyMismatch
		}
		if err := tx.Commit(); err != nil {
			return model.AutomationActionDispatch{}, false, fmt.Errorf("commit raced automation dispatch transaction: %w", err)
		}
		return existing, false, nil
	}
	if err != nil {
		return model.AutomationActionDispatch{}, false, mapDBErr(err)
	}
	if err := tx.Commit(); err != nil {
		return model.AutomationActionDispatch{}, false, fmt.Errorf("commit create automation dispatch transaction: %w", err)
	}
	return stored, true, nil
}

func (s *Store) pgListAutomationActionDispatches(
	filter AutomationActionDispatchFilter,
) ([]model.AutomationActionDispatch, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	args := make([]any, 0, 6)
	clauses := make([]string, 0, 5)
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
	if filter.Status != "" {
		args = append(args, filter.Status)
		clauses = append(clauses, fmt.Sprintf("status = $%d", len(args)))
	}
	query := `SELECT ` + automationActionDispatchSelectColumns + `
FROM fugue_automation_action_dispatches`
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
		return nil, fmt.Errorf("list automation dispatches: %w", err)
	}
	defer rows.Close()
	dispatches := make([]model.AutomationActionDispatch, 0)
	for rows.Next() {
		dispatch, err := scanAutomationActionDispatch(rows)
		if err != nil {
			return nil, fmt.Errorf("scan automation dispatch: %w", err)
		}
		dispatches = append(dispatches, dispatch)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate automation dispatches: %w", err)
	}
	return dispatches, nil
}

func (s *Store) pgGetAutomationActionDispatch(
	id,
	tenantID string,
	platformAdmin bool,
) (model.AutomationActionDispatch, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	query := `SELECT ` + automationActionDispatchSelectColumns + `
FROM fugue_automation_action_dispatches
WHERE id = $1`
	args := []any{id}
	if !platformAdmin {
		args = append(args, tenantID)
		query += ` AND tenant_id = $2`
	}
	dispatch, err := scanAutomationActionDispatch(s.db.QueryRowContext(ctx, query, args...))
	if err != nil {
		return model.AutomationActionDispatch{}, mapDBErr(err)
	}
	return dispatch, nil
}

func (s *Store) pgGetAutomationActionDispatchByIntent(
	intentID string,
) (model.AutomationActionDispatch, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	dispatch, err := scanAutomationActionDispatch(s.db.QueryRowContext(ctx, `
SELECT `+automationActionDispatchSelectColumns+`
FROM fugue_automation_action_dispatches
WHERE intent_id = $1`, intentID))
	if err != nil {
		return model.AutomationActionDispatch{}, mapDBErr(err)
	}
	return dispatch, nil
}

func (s *Store) pgClaimAutomationActionDispatch(
	id,
	owner string,
	now time.Time,
	lease time.Duration,
) (model.AutomationActionDispatch, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.AutomationActionDispatch{}, false, fmt.Errorf("begin claim automation dispatch transaction: %w", err)
	}
	defer tx.Rollback()

	dispatch, err := scanAutomationActionDispatch(tx.QueryRowContext(ctx, `
SELECT `+automationActionDispatchSelectColumns+`
FROM fugue_automation_action_dispatches
WHERE id = $1
FOR UPDATE`, id))
	if err != nil {
		return model.AutomationActionDispatch{}, false, mapDBErr(err)
	}
	if !dispatch.ExpiresAt.After(now) {
		dispatch.Version++
		dispatch.Status = model.AutomationActionDispatchStatusExpired
		dispatch.UpdatedAt = now
		dispatch.LastError = "dispatch ttl expired before claim"
		dispatch.LeaseOwner = ""
		dispatch.LeaseExpiresAt = nil
		dispatch.CompletedAt = timePtr(now)
		dispatch.WALHash = automationActionDispatchWALHash(dispatch)
		if _, err := tx.ExecContext(ctx, `
UPDATE fugue_automation_action_dispatches
SET status = $2, version = $3, updated_at = $4, last_error = $5,
	lease_owner = '', lease_expires_at = NULL, completed_at = $7, wal_hash = $6
WHERE id = $1`, dispatch.ID, dispatch.Status, dispatch.Version, now, dispatch.LastError, dispatch.WALHash, dispatch.CompletedAt); err != nil {
			return model.AutomationActionDispatch{}, false, mapDBErr(err)
		}
		if err := tx.Commit(); err != nil {
			return model.AutomationActionDispatch{}, false, fmt.Errorf("commit expired automation dispatch: %w", err)
		}
		return dispatch, false, nil
	}
	if dispatch.Status != model.AutomationActionDispatchStatusReady &&
		!(dispatch.Status == model.AutomationActionDispatchStatusClaimed ||
			dispatch.Status == model.AutomationActionDispatchStatusExecuting) {
		if err := tx.Commit(); err != nil {
			return model.AutomationActionDispatch{}, false, fmt.Errorf("commit non-claimable automation dispatch: %w", err)
		}
		return model.AutomationActionDispatch{}, false, nil
	}
	if dispatch.CooldownUntil != nil && dispatch.CooldownUntil.After(now) {
		if err := tx.Commit(); err != nil {
			return model.AutomationActionDispatch{}, false, fmt.Errorf("commit cooldown automation dispatch: %w", err)
		}
		return model.AutomationActionDispatch{}, false, nil
	}
	if (dispatch.Status == model.AutomationActionDispatchStatusClaimed ||
		dispatch.Status == model.AutomationActionDispatchStatusExecuting) &&
		dispatch.LeaseExpiresAt != nil && dispatch.LeaseExpiresAt.After(now) {
		if err := tx.Commit(); err != nil {
			return model.AutomationActionDispatch{}, false, fmt.Errorf("commit active automation dispatch lease: %w", err)
		}
		return model.AutomationActionDispatch{}, false, nil
	}

	var currentToken int64
	if err := tx.QueryRowContext(ctx, `
SELECT last_token
FROM fugue_automation_action_fencing
WHERE subject_key = $1
FOR UPDATE`, automationActionDispatchSubjectKey(dispatch)).Scan(&currentToken); err != nil {
		if err != sql.ErrNoRows {
			return model.AutomationActionDispatch{}, false, mapDBErr(err)
		}
		// A missing token row is corruption or an incomplete migration. Treat
		// the dispatch as stale and hold it; never recreate a token implicitly
		// during claim because that could resurrect an old writer.
		currentToken = 0
	}
	if currentToken != dispatch.FencingToken {
		dispatch.Status = model.AutomationActionDispatchStatusHeld
		dispatch.Version++
		dispatch.UpdatedAt = now
		dispatch.LastError = "dispatch fencing token is stale"
		dispatch.LeaseOwner = ""
		dispatch.LeaseExpiresAt = nil
		dispatch.WALHash = automationActionDispatchWALHash(dispatch)
		if _, err := tx.ExecContext(ctx, `
UPDATE fugue_automation_action_dispatches
SET status = $2, version = $3, updated_at = $4, last_error = $5,
	lease_owner = '', lease_expires_at = NULL, wal_hash = $6
WHERE id = $1 AND version = $7`,
			dispatch.ID, dispatch.Status, dispatch.Version, now, dispatch.LastError, dispatch.WALHash, dispatch.Version-1,
		); err != nil {
			return model.AutomationActionDispatch{}, false, mapDBErr(err)
		}
		if err := tx.Commit(); err != nil {
			return model.AutomationActionDispatch{}, false, fmt.Errorf("commit stale automation dispatch: %w", err)
		}
		return dispatch, false, nil
	}

	token, err := pgNextAutomationActionFencingTokenTx(
		ctx,
		tx,
		automationActionDispatchSubjectKey(dispatch),
		dispatch.TenantID,
		now,
	)
	if err != nil {
		return model.AutomationActionDispatch{}, false, err
	}
	leaseExpires := now.Add(lease)
	nextVersion := dispatch.Version + 1
	nextWAL := dispatch
	nextWAL.FencingToken = token
	nextWAL.Version = nextVersion
	nextWAL.Status = model.AutomationActionDispatchStatusClaimed
	nextWAL.LeaseOwner = owner
	nextWAL.LeaseExpiresAt = &leaseExpires
	nextWAL.ClaimedAt = timePtr(now)
	nextWAL.CompletedAt = nil
	nextWAL.UpdatedAt = now
	nextWAL.LastError = ""
	nextWAL.WALHash = automationActionDispatchWALHash(nextWAL)
	updated, err := scanAutomationActionDispatch(tx.QueryRowContext(ctx, `
UPDATE fugue_automation_action_dispatches
SET status = $2, fencing_token = $3, version = $4, lease_owner = $5,
	lease_expires_at = $6, claimed_at = $7, completed_at = NULL,
	updated_at = $8, last_error = '', wal_hash = $9
WHERE id = $1 AND version = $10
RETURNING `+automationActionDispatchSelectColumns,
		dispatch.ID, nextWAL.Status, nextWAL.FencingToken, nextWAL.Version, nextWAL.LeaseOwner,
		nextWAL.LeaseExpiresAt, nextWAL.ClaimedAt, nextWAL.UpdatedAt,
		nextWAL.WALHash, dispatch.Version,
	))
	if err != nil {
		return model.AutomationActionDispatch{}, false, mapDBErr(err)
	}
	if err := tx.Commit(); err != nil {
		return model.AutomationActionDispatch{}, false, fmt.Errorf("commit claim automation dispatch: %w", err)
	}
	return updated, true, nil
}

func (s *Store) pgValidateAutomationActionDispatchFence(
	id string,
	fencingToken,
	version int64,
	now time.Time,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var (
		subjectKey string
		status     string
		rowToken   int64
		rowVersion int64
		expires    time.Time
		lease      sql.NullTime
		lastToken  int64
	)
	err := s.db.QueryRowContext(ctx, `
SELECT d.tenant_id || E'\n' || d.scope_type || E'\n' || d.scope_id || E'\n' ||
	d.action_type || E'\n' || d.subject,
	d.status, d.fencing_token, d.version, d.expires_at, d.lease_expires_at,
	COALESCE((
		SELECT f.last_token
		FROM fugue_automation_action_fencing f
		WHERE f.subject_key = d.tenant_id || E'\n' || d.scope_type || E'\n' || d.scope_id || E'\n' ||
			d.action_type || E'\n' || d.subject
	), 0)
FROM fugue_automation_action_dispatches d
WHERE d.id = $1`,
		id,
	).Scan(&subjectKey, &status, &rowToken, &rowVersion, &expires, &lease, &lastToken)
	if err != nil {
		return mapDBErr(err)
	}
	if status != model.AutomationActionDispatchStatusClaimed &&
		status != model.AutomationActionDispatchStatusExecuting {
		return ErrConflict
	}
	if rowToken != fencingToken || rowVersion != version ||
		!expires.After(now) || !lease.Valid || !lease.Time.After(now) ||
		lastToken != fencingToken || strings.TrimSpace(subjectKey) == "" {
		return ErrConflict
	}
	return nil
}

func pgGetAutomationActionIntentByIDTx(
	ctx context.Context,
	tx *sql.Tx,
	id string,
	forUpdate bool,
) (model.AutomationActionIntent, error) {
	query := `SELECT ` + automationActionIntentSelectColumns + `
FROM fugue_automation_action_intents
WHERE id = $1`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	return scanAutomationActionIntent(tx.QueryRowContext(ctx, query, id))
}

func pgGetAutomationActionDispatchByIntentTx(
	ctx context.Context,
	tx *sql.Tx,
	intentID string,
	forUpdate bool,
) (model.AutomationActionDispatch, bool, error) {
	query := `SELECT ` + automationActionDispatchSelectColumns + `
FROM fugue_automation_action_dispatches
WHERE intent_id = $1`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	dispatch, err := scanAutomationActionDispatch(tx.QueryRowContext(ctx, query, intentID))
	if err == sql.ErrNoRows {
		return model.AutomationActionDispatch{}, false, nil
	}
	if err != nil {
		return model.AutomationActionDispatch{}, false, err
	}
	return dispatch, true, nil
}

func pgGetAutomationActionDispatchByIdempotencyTx(
	ctx context.Context,
	tx *sql.Tx,
	idempotencyKey string,
	forUpdate bool,
) (model.AutomationActionDispatch, bool, error) {
	query := `SELECT ` + automationActionDispatchSelectColumns + `
FROM fugue_automation_action_dispatches
WHERE idempotency_key = $1`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	dispatch, err := scanAutomationActionDispatch(tx.QueryRowContext(ctx, query, idempotencyKey))
	if err == sql.ErrNoRows {
		return model.AutomationActionDispatch{}, false, nil
	}
	if err != nil {
		return model.AutomationActionDispatch{}, false, err
	}
	return dispatch, true, nil
}

func pgNextAutomationActionFencingTokenTx(
	ctx context.Context,
	tx *sql.Tx,
	subjectKey string,
	tenantID string,
	now time.Time,
) (int64, error) {
	if strings.TrimSpace(subjectKey) == "" || strings.TrimSpace(tenantID) == "" {
		return 0, ErrInvalidInput
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_automation_action_fencing (subject_key, tenant_id, last_token, updated_at)
VALUES ($1, $2, 0, $3)
	ON CONFLICT (subject_key) DO NOTHING`, subjectKey, tenantID, now); err != nil {
		return 0, mapDBErr(err)
	}
	var token int64
	if err := tx.QueryRowContext(ctx, `
UPDATE fugue_automation_action_fencing
SET last_token = last_token + 1, updated_at = $2
WHERE subject_key = $1 AND last_token < 9223372036854775807
RETURNING last_token`, subjectKey, now).Scan(&token); err != nil {
		if err == sql.ErrNoRows {
			return 0, fmt.Errorf("%w: automation fencing token exhausted", ErrConflict)
		}
		return 0, mapDBErr(err)
	}
	return token, nil
}

func scanAutomationActionDispatch(scanner sqlScanner) (model.AutomationActionDispatch, error) {
	var (
		dispatch      model.AutomationActionDispatch
		safetyRaw     []byte
		leaseExpires  sql.NullTime
		cooldownUntil sql.NullTime
		claimedAt     sql.NullTime
		completedAt   sql.NullTime
	)
	if err := scanner.Scan(
		&dispatch.ID, &dispatch.IntentID, &dispatch.TenantID, &dispatch.ProjectID,
		&dispatch.PolicyID, &dispatch.PolicyGeneration, &dispatch.RuleID,
		&dispatch.Scope.Type, &dispatch.Scope.ID, &dispatch.ActionType,
		&dispatch.ContractID, &dispatch.TriggerInvariant, &dispatch.Subject,
		&dispatch.SourceGeneration, &dispatch.RollbackTarget, &dispatch.IdempotencyKey,
		&dispatch.WALHash, &safetyRaw, &dispatch.Status, &dispatch.FencingToken,
		&dispatch.Version, &dispatch.ExpiresAt, &dispatch.LeaseOwner,
		&leaseExpires, &cooldownUntil, &dispatch.LastError, &dispatch.CreatedAt,
		&dispatch.UpdatedAt, &claimedAt, &completedAt,
	); err != nil {
		return model.AutomationActionDispatch{}, err
	}
	safety, err := decodeJSONValue[model.ActionSafetyDecision](safetyRaw)
	if err != nil {
		return model.AutomationActionDispatch{}, err
	}
	dispatch.SafetyDecision = safety
	if leaseExpires.Valid {
		dispatch.LeaseExpiresAt = timePtr(leaseExpires.Time)
	}
	if cooldownUntil.Valid {
		dispatch.CooldownUntil = timePtr(cooldownUntil.Time)
	}
	if claimedAt.Valid {
		dispatch.ClaimedAt = timePtr(claimedAt.Time)
	}
	if completedAt.Valid {
		dispatch.CompletedAt = timePtr(completedAt.Time)
	}
	return normalizePersistedAutomationActionDispatch(dispatch)
}
