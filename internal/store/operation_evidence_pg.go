package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
)

const operationEvidenceSelectColumns = `id, tenant_id, project_id, app_id, operation_id, release_attempt_id, evidence_type, source, severity, confidence, subject_kind, subject_name, subject_namespace, subject_uid, observed_at, collected_at, summary, message, reason, exit_code, started_at, finished_at, container_name, pod_name, deployment_name, replica_set_name, node_name, redaction_status, payload_json, payload_version, created_at`
const releaseAttemptSelectColumns = `id, tenant_id, project_id, app_id, trigger_type, trigger_actor_type, trigger_actor_id, source_operation_id, root_operation_id, image_ref, target_digest, previous_digest, desired_source_json, status, confidence, failure_operation_id, failure_evidence_id, summary, started_at, finished_at, created_at, updated_at`
const releaseStepSelectColumns = `id, tenant_id, release_attempt_id, operation_id, step_type, status, started_at, finished_at, summary, evidence_id, payload_json, created_at`

func (s *Store) pgRecordOperationEvidence(evidence model.OperationEvidence) (model.OperationEvidence, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.OperationEvidence{}, mapDBErr(err)
	}
	defer tx.Rollback()

	payloadJSON, err := marshalJSON(evidence.Payload)
	if err != nil {
		return model.OperationEvidence{}, err
	}
	out, err := scanOperationEvidence(tx.QueryRowContext(ctx, `
INSERT INTO fugue_operation_evidence (
	id, tenant_id, project_id, app_id, operation_id, release_attempt_id, evidence_type, source,
	severity, confidence, subject_kind, subject_name, subject_namespace, subject_uid,
	observed_at, collected_at, summary, message, reason, exit_code, started_at, finished_at,
	container_name, pod_name, deployment_name, replica_set_name, node_name, redaction_status,
	payload_json, payload_version, created_at
) VALUES (
	$1, $2, $3, $4, $5, $6, $7, $8,
	$9, $10, $11, $12, $13, $14,
	$15, $16, $17, $18, $19, $20, $21, $22,
	$23, $24, $25, $26, $27, $28,
	$29, $30, $31
)
RETURNING `+operationEvidenceSelectColumns,
		evidence.ID, evidence.TenantID, evidence.ProjectID, evidence.AppID, evidence.OperationID, evidence.ReleaseAttemptID, evidence.Type, evidence.Source,
		evidence.Severity, evidence.Confidence, evidence.SubjectKind, evidence.SubjectName, evidence.SubjectNamespace, evidence.SubjectUID,
		evidence.ObservedAt, evidence.CollectedAt, evidence.Summary, evidence.Message, evidence.Reason, evidence.ExitCode, evidence.StartedAt, evidence.FinishedAt,
		evidence.ContainerName, evidence.PodName, evidence.DeploymentName, evidence.ReplicaSetName, evidence.NodeName, evidence.RedactionStatus,
		payloadJSON, evidence.PayloadVersion, evidence.CreatedAt))
	if err != nil {
		return model.OperationEvidence{}, mapDBErr(err)
	}
	if err := pruneOperationEvidenceTx(ctx, tx, evidence); err != nil {
		return model.OperationEvidence{}, err
	}
	if err := tx.Commit(); err != nil {
		return model.OperationEvidence{}, mapDBErr(err)
	}
	return out, nil
}

func (s *Store) pgListOperationEvidence(filter model.OperationEvidenceFilter) ([]model.OperationEvidence, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clauses := []string{}
	args := []any{}
	if !filter.PlatformAdmin {
		args = append(args, strings.TrimSpace(filter.TenantID))
		clauses = append(clauses, fmt.Sprintf("tenant_id = $%d", len(args)))
	} else if strings.TrimSpace(filter.TenantID) != "" {
		args = append(args, strings.TrimSpace(filter.TenantID))
		clauses = append(clauses, fmt.Sprintf("tenant_id = $%d", len(args)))
	}
	if filter.OperationID != "" {
		args = append(args, filter.OperationID)
		clauses = append(clauses, fmt.Sprintf("operation_id = $%d", len(args)))
	}
	if filter.AppID != "" {
		args = append(args, filter.AppID)
		clauses = append(clauses, fmt.Sprintf("app_id = $%d", len(args)))
	}
	if filter.ReleaseAttemptID != "" {
		args = append(args, filter.ReleaseAttemptID)
		clauses = append(clauses, fmt.Sprintf("release_attempt_id = $%d", len(args)))
	}
	if types := normalizedOperationFilterValues(filter.Types); len(types) > 0 {
		clauses = append(clauses, fmt.Sprintf("lower(evidence_type) IN (%s)", sqlPlaceholderList(len(args)+1, len(types))))
		for _, value := range types {
			args = append(args, value)
		}
	}
	if severities := normalizedOperationFilterValues(filter.Severities); len(severities) > 0 {
		clauses = append(clauses, fmt.Sprintf("lower(severity) IN (%s)", sqlPlaceholderList(len(args)+1, len(severities))))
		for _, value := range severities {
			args = append(args, value)
		}
	}
	if filter.Since != nil {
		args = append(args, filter.Since.UTC())
		clauses = append(clauses, fmt.Sprintf("collected_at >= $%d", len(args)))
	}
	query := `SELECT ` + operationEvidenceSelectColumns + ` FROM fugue_operation_evidence`
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	query += " ORDER BY collected_at DESC, id DESC"
	args = append(args, filter.Limit)
	query += fmt.Sprintf(" LIMIT $%d", len(args))

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()
	items, err := scanOperationEvidenceRows(rows)
	if err != nil {
		return nil, err
	}
	sortOperationEvidenceOldestFirst(items)
	return items, nil
}

func scanOperationEvidenceRows(rows *sql.Rows) ([]model.OperationEvidence, error) {
	out := []model.OperationEvidence{}
	for rows.Next() {
		item, err := scanOperationEvidence(rows)
		if err != nil {
			return nil, mapDBErr(err)
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, mapDBErr(err)
	}
	return out, nil
}

func scanOperationEvidence(scanner sqlScanner) (model.OperationEvidence, error) {
	var item model.OperationEvidence
	var payloadJSON []byte
	var exitCode sql.NullInt64
	var startedAt sql.NullTime
	var finishedAt sql.NullTime
	if err := scanner.Scan(
		&item.ID,
		&item.TenantID,
		&item.ProjectID,
		&item.AppID,
		&item.OperationID,
		&item.ReleaseAttemptID,
		&item.Type,
		&item.Source,
		&item.Severity,
		&item.Confidence,
		&item.SubjectKind,
		&item.SubjectName,
		&item.SubjectNamespace,
		&item.SubjectUID,
		&item.ObservedAt,
		&item.CollectedAt,
		&item.Summary,
		&item.Message,
		&item.Reason,
		&exitCode,
		&startedAt,
		&finishedAt,
		&item.ContainerName,
		&item.PodName,
		&item.DeploymentName,
		&item.ReplicaSetName,
		&item.NodeName,
		&item.RedactionStatus,
		&payloadJSON,
		&item.PayloadVersion,
		&item.CreatedAt,
	); err != nil {
		return model.OperationEvidence{}, err
	}
	if exitCode.Valid {
		value := int(exitCode.Int64)
		item.ExitCode = &value
	}
	if startedAt.Valid {
		t := startedAt.Time.UTC()
		item.StartedAt = &t
	}
	if finishedAt.Valid {
		t := finishedAt.Time.UTC()
		item.FinishedAt = &t
	}
	if len(payloadJSON) > 0 {
		if err := json.Unmarshal(payloadJSON, &item.Payload); err != nil {
			return model.OperationEvidence{}, err
		}
	}
	if item.Payload == nil {
		item.Payload = map[string]any{}
	}
	return item, nil
}

func pruneOperationEvidenceTx(ctx context.Context, tx *sql.Tx, inserted model.OperationEvidence) error {
	cutoffBase := inserted.CollectedAt
	if cutoffBase.IsZero() {
		cutoffBase = time.Now().UTC()
	}
	cutoff := cutoffBase.UTC().Add(-operationEvidenceRetentionWindow)
	if operationID := strings.TrimSpace(inserted.OperationID); operationID != "" {
		if err := pruneOperationEvidenceScopeTx(ctx, tx, "operation_id", operationID, cutoff, operationEvidenceRetentionLimitPerOperation); err != nil {
			return err
		}
	}
	if appID := strings.TrimSpace(inserted.AppID); appID != "" {
		return pruneOperationEvidenceScopeTx(ctx, tx, "app_id", appID, cutoff, operationEvidenceRetentionLimitPerApp)
	}
	if tenantID := strings.TrimSpace(inserted.TenantID); tenantID != "" {
		return pruneOperationEvidenceTenantWithoutAppTx(ctx, tx, tenantID, cutoff)
	}
	return nil
}

func pruneOperationEvidenceScopeTx(ctx context.Context, tx *sql.Tx, column, value string, cutoff time.Time, limit int) error {
	if strings.TrimSpace(value) == "" || limit <= 0 {
		return nil
	}
	if column != "operation_id" && column != "app_id" {
		return ErrInvalidInput
	}
	_, err := tx.ExecContext(ctx, fmt.Sprintf(`
DELETE FROM fugue_operation_evidence
WHERE %s = $1
  AND (
	collected_at < $2
	OR id NOT IN (
		SELECT id
		FROM fugue_operation_evidence
		WHERE %s = $1
		ORDER BY collected_at DESC, id DESC
		LIMIT $3
	)
  )
`, column, column), value, cutoff, limit)
	return mapDBErr(err)
}

func pruneOperationEvidenceTenantWithoutAppTx(ctx context.Context, tx *sql.Tx, tenantID string, cutoff time.Time) error {
	_, err := tx.ExecContext(ctx, `
DELETE FROM fugue_operation_evidence
WHERE tenant_id = $1
  AND app_id = ''
  AND (
	collected_at < $2
	OR id NOT IN (
		SELECT id
		FROM fugue_operation_evidence
		WHERE tenant_id = $1
		  AND app_id = ''
		ORDER BY collected_at DESC, id DESC
		LIMIT $3
	)
  )
`, tenantID, cutoff, operationEvidenceRetentionLimitPerTenant)
	return mapDBErr(err)
}

func (s *Store) pgCreateReleaseAttempt(attempt model.ReleaseAttempt) (model.ReleaseAttempt, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	desiredJSON, err := marshalJSON(attempt.DesiredSource)
	if err != nil {
		return model.ReleaseAttempt{}, err
	}
	out, err := scanReleaseAttempt(s.db.QueryRowContext(ctx, `
INSERT INTO fugue_release_attempts (
	id, tenant_id, project_id, app_id, trigger_type, trigger_actor_type, trigger_actor_id,
	source_operation_id, root_operation_id, image_ref, target_digest, previous_digest,
	desired_source_json, status, confidence, failure_operation_id, failure_evidence_id,
	summary, started_at, finished_at, created_at, updated_at
) VALUES (
	$1, $2, $3, $4, $5, $6, $7,
	$8, $9, $10, $11, $12,
	$13, $14, $15, $16, $17,
	$18, $19, $20, $21, $22
)
RETURNING `+releaseAttemptSelectColumns,
		attempt.ID, attempt.TenantID, attempt.ProjectID, attempt.AppID, attempt.TriggerType, attempt.TriggerActorType, attempt.TriggerActorID,
		attempt.SourceOperationID, attempt.RootOperationID, attempt.ImageRef, attempt.TargetDigest, attempt.PreviousDigest,
		desiredJSON, attempt.Status, attempt.Confidence, attempt.FailureOperationID, attempt.FailureEvidenceID,
		attempt.Summary, attempt.StartedAt, attempt.FinishedAt, attempt.CreatedAt, attempt.UpdatedAt))
	return out, mapDBErr(err)
}

func (s *Store) pgUpdateReleaseAttempt(attempt model.ReleaseAttempt) (model.ReleaseAttempt, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	desiredJSON, err := marshalJSON(attempt.DesiredSource)
	if err != nil {
		return model.ReleaseAttempt{}, err
	}
	out, err := scanReleaseAttempt(s.db.QueryRowContext(ctx, `
UPDATE fugue_release_attempts
SET tenant_id = $2,
	project_id = $3,
	app_id = $4,
	trigger_type = $5,
	trigger_actor_type = $6,
	trigger_actor_id = $7,
	source_operation_id = $8,
	root_operation_id = $9,
	image_ref = $10,
	target_digest = $11,
	previous_digest = $12,
	desired_source_json = $13,
	status = $14,
	confidence = $15,
	failure_operation_id = $16,
	failure_evidence_id = $17,
	summary = $18,
	started_at = $19,
	finished_at = $20,
	updated_at = $21
WHERE id = $1
RETURNING `+releaseAttemptSelectColumns,
		attempt.ID, attempt.TenantID, attempt.ProjectID, attempt.AppID, attempt.TriggerType, attempt.TriggerActorType, attempt.TriggerActorID,
		attempt.SourceOperationID, attempt.RootOperationID, attempt.ImageRef, attempt.TargetDigest, attempt.PreviousDigest,
		desiredJSON, attempt.Status, attempt.Confidence, attempt.FailureOperationID, attempt.FailureEvidenceID,
		attempt.Summary, attempt.StartedAt, attempt.FinishedAt, attempt.UpdatedAt))
	return out, mapDBErr(err)
}

func (s *Store) pgGetReleaseAttempt(id string) (model.ReleaseAttempt, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	out, err := scanReleaseAttempt(s.db.QueryRowContext(ctx, `SELECT `+releaseAttemptSelectColumns+` FROM fugue_release_attempts WHERE id = $1`, id))
	return out, mapDBErr(err)
}

func (s *Store) pgListReleaseAttempts(filter model.ReleaseAttemptFilter) ([]model.ReleaseAttempt, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clauses := []string{}
	args := []any{}
	if !filter.PlatformAdmin {
		args = append(args, strings.TrimSpace(filter.TenantID))
		clauses = append(clauses, fmt.Sprintf("tenant_id = $%d", len(args)))
	} else if strings.TrimSpace(filter.TenantID) != "" {
		args = append(args, strings.TrimSpace(filter.TenantID))
		clauses = append(clauses, fmt.Sprintf("tenant_id = $%d", len(args)))
	}
	if filter.AppID != "" {
		args = append(args, filter.AppID)
		clauses = append(clauses, fmt.Sprintf("app_id = $%d", len(args)))
	}
	if filter.Status != "" {
		args = append(args, filter.Status)
		clauses = append(clauses, fmt.Sprintf("status = $%d", len(args)))
	}
	query := `SELECT ` + releaseAttemptSelectColumns + ` FROM fugue_release_attempts`
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	query += " ORDER BY started_at DESC, id DESC"
	args = append(args, filter.Limit)
	query += fmt.Sprintf(" LIMIT $%d", len(args))
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()
	return scanReleaseAttemptRows(rows)
}

func (s *Store) pgFindReleaseAttemptForOperation(operationID string) (model.ReleaseAttempt, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	item, err := scanReleaseAttempt(s.db.QueryRowContext(ctx, `
SELECT `+releaseAttemptSelectColumns+`
FROM fugue_release_attempts ra
WHERE ra.source_operation_id = $1
   OR ra.root_operation_id = $1
   OR ra.failure_operation_id = $1
   OR EXISTS (
       SELECT 1
       FROM fugue_release_steps rs
       WHERE rs.release_attempt_id = ra.id
         AND rs.operation_id = $1
   )
ORDER BY ra.started_at DESC, ra.id DESC
LIMIT 1
`, operationID))
	if err != nil {
		if err == sql.ErrNoRows {
			return model.ReleaseAttempt{}, false, nil
		}
		return model.ReleaseAttempt{}, false, mapDBErr(err)
	}
	return item, true, nil
}

func scanReleaseAttemptRows(rows *sql.Rows) ([]model.ReleaseAttempt, error) {
	out := []model.ReleaseAttempt{}
	for rows.Next() {
		item, err := scanReleaseAttempt(rows)
		if err != nil {
			return nil, mapDBErr(err)
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, mapDBErr(err)
	}
	return out, nil
}

func scanReleaseAttempt(scanner sqlScanner) (model.ReleaseAttempt, error) {
	var item model.ReleaseAttempt
	var desiredJSON []byte
	var finishedAt sql.NullTime
	if err := scanner.Scan(
		&item.ID,
		&item.TenantID,
		&item.ProjectID,
		&item.AppID,
		&item.TriggerType,
		&item.TriggerActorType,
		&item.TriggerActorID,
		&item.SourceOperationID,
		&item.RootOperationID,
		&item.ImageRef,
		&item.TargetDigest,
		&item.PreviousDigest,
		&desiredJSON,
		&item.Status,
		&item.Confidence,
		&item.FailureOperationID,
		&item.FailureEvidenceID,
		&item.Summary,
		&item.StartedAt,
		&finishedAt,
		&item.CreatedAt,
		&item.UpdatedAt,
	); err != nil {
		return model.ReleaseAttempt{}, err
	}
	if len(desiredJSON) > 0 {
		if err := json.Unmarshal(desiredJSON, &item.DesiredSource); err != nil {
			return model.ReleaseAttempt{}, err
		}
	}
	if item.DesiredSource == nil {
		item.DesiredSource = map[string]any{}
	}
	if finishedAt.Valid {
		t := finishedAt.Time.UTC()
		item.FinishedAt = &t
	}
	return item, nil
}

func (s *Store) pgRecordReleaseStep(step model.ReleaseStep) (model.ReleaseStep, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	payloadJSON, err := marshalJSON(step.Payload)
	if err != nil {
		return model.ReleaseStep{}, err
	}
	out, err := scanReleaseStep(s.db.QueryRowContext(ctx, `
INSERT INTO fugue_release_steps (
	id, tenant_id, release_attempt_id, operation_id, step_type, status, started_at,
	finished_at, summary, evidence_id, payload_json, created_at
) VALUES (
	$1, $2, $3, $4, $5, $6, $7,
	$8, $9, $10, $11, $12
)
RETURNING `+releaseStepSelectColumns,
		step.ID, step.TenantID, step.ReleaseAttemptID, step.OperationID, step.Type, step.Status, step.StartedAt,
		step.FinishedAt, step.Summary, step.EvidenceID, payloadJSON, step.CreatedAt))
	return out, mapDBErr(err)
}

func (s *Store) pgListReleaseSteps(tenantID string, platformAdmin bool, releaseAttemptID string) ([]model.ReleaseStep, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	args := []any{strings.TrimSpace(releaseAttemptID)}
	query := `SELECT rs.` + strings.ReplaceAll(releaseStepSelectColumns, ", ", ", rs.") + `
FROM fugue_release_steps rs
JOIN fugue_release_attempts ra ON ra.id = rs.release_attempt_id
WHERE rs.release_attempt_id = $1`
	if !platformAdmin {
		args = append(args, strings.TrimSpace(tenantID))
		query += fmt.Sprintf(" AND ra.tenant_id = $%d", len(args))
	}
	query += " ORDER BY rs.started_at ASC, rs.id ASC"
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()
	return scanReleaseStepRows(rows)
}

func scanReleaseStepRows(rows *sql.Rows) ([]model.ReleaseStep, error) {
	out := []model.ReleaseStep{}
	for rows.Next() {
		item, err := scanReleaseStep(rows)
		if err != nil {
			return nil, mapDBErr(err)
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, mapDBErr(err)
	}
	return out, nil
}

func scanReleaseStep(scanner sqlScanner) (model.ReleaseStep, error) {
	var item model.ReleaseStep
	var payloadJSON []byte
	var finishedAt sql.NullTime
	if err := scanner.Scan(
		&item.ID,
		&item.TenantID,
		&item.ReleaseAttemptID,
		&item.OperationID,
		&item.Type,
		&item.Status,
		&item.StartedAt,
		&finishedAt,
		&item.Summary,
		&item.EvidenceID,
		&payloadJSON,
		&item.CreatedAt,
	); err != nil {
		return model.ReleaseStep{}, err
	}
	if finishedAt.Valid {
		t := finishedAt.Time.UTC()
		item.FinishedAt = &t
	}
	if len(payloadJSON) > 0 {
		if err := json.Unmarshal(payloadJSON, &item.Payload); err != nil {
			return model.ReleaseStep{}, err
		}
	}
	if item.Payload == nil {
		item.Payload = map[string]any{}
	}
	return item, nil
}

func (s *Store) pgCountOperationEvidenceMetricGroups() ([]OperationEvidenceRecordMetricCount, []OperationEvidenceCaptureMetricCount, []OperationEvidenceRolloutFailureMetricCount, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	recordRows, err := s.db.QueryContext(ctx, `
SELECT evidence_type, severity, confidence, COUNT(*)
FROM fugue_operation_evidence
GROUP BY evidence_type, severity, confidence
ORDER BY evidence_type, severity, confidence
`)
	if err != nil {
		return nil, nil, nil, mapDBErr(err)
	}
	defer recordRows.Close()
	records := []OperationEvidenceRecordMetricCount{}
	for recordRows.Next() {
		var item OperationEvidenceRecordMetricCount
		if err := recordRows.Scan(&item.Type, &item.Severity, &item.Confidence, &item.Count); err != nil {
			return nil, nil, nil, mapDBErr(err)
		}
		records = append(records, item)
	}
	if err := recordRows.Err(); err != nil {
		return nil, nil, nil, mapDBErr(err)
	}

	captureTypes := operationEvidenceCaptureMetricTypes()
	captureArgs := make([]any, 0, len(captureTypes)+1)
	captureArgs = append(captureArgs, model.OperationEvidenceTypeCollectorError)
	for _, value := range captureTypes {
		captureArgs = append(captureArgs, value)
	}
	captureRows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
SELECT CASE WHEN evidence_type = $1 THEN 'error' ELSE 'success' END AS result, COUNT(*)
FROM fugue_operation_evidence
WHERE evidence_type IN (%s)
GROUP BY result
ORDER BY result
`, sqlPlaceholderList(2, len(captureTypes))), captureArgs...)
	if err != nil {
		return nil, nil, nil, mapDBErr(err)
	}
	defer captureRows.Close()
	captures := []OperationEvidenceCaptureMetricCount{}
	for captureRows.Next() {
		var item OperationEvidenceCaptureMetricCount
		if err := captureRows.Scan(&item.Result, &item.Count); err != nil {
			return nil, nil, nil, mapDBErr(err)
		}
		captures = append(captures, item)
	}
	if err := captureRows.Err(); err != nil {
		return nil, nil, nil, mapDBErr(err)
	}

	rolloutTypes := operationEvidenceRolloutFailureMetricTypes()
	rolloutArgs := make([]any, 0, len(rolloutTypes))
	for _, value := range rolloutTypes {
		rolloutArgs = append(rolloutArgs, value)
	}
	rolloutRows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
SELECT evidence_type, confidence, COUNT(*)
FROM fugue_operation_evidence
WHERE evidence_type IN (%s)
GROUP BY evidence_type, confidence
ORDER BY evidence_type, confidence
`, sqlPlaceholderList(1, len(rolloutTypes))), rolloutArgs...)
	if err != nil {
		return nil, nil, nil, mapDBErr(err)
	}
	defer rolloutRows.Close()
	rolloutByKey := map[string]OperationEvidenceRolloutFailureMetricCount{}
	for rolloutRows.Next() {
		var evidenceType, confidence string
		var count int64
		if err := rolloutRows.Scan(&evidenceType, &confidence, &count); err != nil {
			return nil, nil, nil, mapDBErr(err)
		}
		reason := rolloutFailureMetricReason(evidenceType)
		if reason == "" {
			continue
		}
		key := reason + "\x00" + confidence
		item := rolloutByKey[key]
		item.Reason = reason
		item.Confidence = confidence
		item.Count += count
		rolloutByKey[key] = item
	}
	if err := rolloutRows.Err(); err != nil {
		return nil, nil, nil, mapDBErr(err)
	}
	return records, captures, operationEvidenceRolloutMetricCounts(rolloutByKey), nil
}

func (s *Store) pgCountReleaseAttemptMetricGroups() ([]ReleaseAttemptMetricCount, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, `
SELECT trigger_type, status, COUNT(*)
FROM fugue_release_attempts
GROUP BY trigger_type, status
ORDER BY trigger_type, status
`)
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()
	items := []ReleaseAttemptMetricCount{}
	for rows.Next() {
		var item ReleaseAttemptMetricCount
		if err := rows.Scan(&item.TriggerType, &item.Status, &item.Count); err != nil {
			return nil, mapDBErr(err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, mapDBErr(err)
	}
	return items, nil
}

func (s *Store) pgCountReleaseEvidenceResearchGroups() (ReleaseEvidenceResearchSummary, []MigrationEvidenceMetricCount, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	summary := ReleaseEvidenceResearchSummary{WindowSeconds: int64(releaseEvidenceResearchFollowupWindow.Seconds())}
	if err := s.db.QueryRowContext(ctx, `
SELECT
	COUNT(*) FILTER (WHERE trigger_type = $1),
	COUNT(*) FILTER (WHERE trigger_type IN ($2, $3)),
	COUNT(*) FILTER (
		WHERE trigger_type = $1
		  AND EXISTS (
			SELECT 1
			FROM fugue_release_attempts followup
			WHERE followup.app_id = fugue_release_attempts.app_id
			  AND followup.trigger_type IN ($2, $3)
			  AND followup.started_at >= fugue_release_attempts.started_at
			  AND followup.started_at <= fugue_release_attempts.started_at + ($4::interval)
		  )
	)
FROM fugue_release_attempts
`, model.ReleaseAttemptTriggerEnvPatch, model.ReleaseAttemptTriggerImageTrackingAuto, model.ReleaseAttemptTriggerImageTrackingManualSync, fmt.Sprintf("%d seconds", summary.WindowSeconds)).Scan(
		&summary.EnvPatchAttempts,
		&summary.TrackingSyncAttempts,
		&summary.EnvPatchThenTrackingSyncAttempts,
	); err != nil {
		return ReleaseEvidenceResearchSummary{}, nil, mapDBErr(err)
	}

	rows, err := s.db.QueryContext(ctx, `
WITH log_evidence AS (
	SELECT confidence, lower(summary || E'\n' || message || E'\n' || payload_json::text) AS haystack
	FROM fugue_operation_evidence
	WHERE evidence_type IN ($1, $2)
)
SELECT 'schema_or_migration_log' AS signal, confidence, COUNT(*)
FROM log_evidence
WHERE haystack LIKE '%migration%' OR haystack LIKE '%schema%'
GROUP BY confidence
UNION ALL
SELECT 'sqlstate_log' AS signal, confidence, COUNT(*)
FROM log_evidence
WHERE haystack LIKE '%sqlstate%'
GROUP BY confidence
UNION ALL
SELECT 'deadlock_log' AS signal, confidence, COUNT(*)
FROM log_evidence
WHERE haystack LIKE '%deadlock%'
GROUP BY confidence
ORDER BY signal, confidence
`, model.OperationEvidenceTypeRolloutPreviousLogs, model.OperationEvidenceTypeRolloutCurrentLogs)
	if err != nil {
		return ReleaseEvidenceResearchSummary{}, nil, mapDBErr(err)
	}
	defer rows.Close()
	migrationCounts := []MigrationEvidenceMetricCount{}
	for rows.Next() {
		var item MigrationEvidenceMetricCount
		if err := rows.Scan(&item.Signal, &item.Confidence, &item.Count); err != nil {
			return ReleaseEvidenceResearchSummary{}, nil, mapDBErr(err)
		}
		migrationCounts = append(migrationCounts, item)
	}
	if err := rows.Err(); err != nil {
		return ReleaseEvidenceResearchSummary{}, nil, mapDBErr(err)
	}
	return summary, migrationCounts, nil
}

func operationEvidenceCaptureMetricTypes() []string {
	return []string{
		model.OperationEvidenceTypeCollectorError,
		model.OperationEvidenceTypeRolloutPodFailure,
		model.OperationEvidenceTypeRolloutContainerTerminated,
		model.OperationEvidenceTypeRolloutPreviousLogs,
		model.OperationEvidenceTypeRolloutCurrentLogs,
		model.OperationEvidenceTypeRolloutKubernetesEvent,
		model.OperationEvidenceTypeRolloutDeploymentSnapshot,
		model.OperationEvidenceTypeRolloutReplicaSetSnapshot,
		model.OperationEvidenceTypeRolloutPodSnapshot,
		model.OperationEvidenceTypeImagePullFailure,
		model.OperationEvidenceTypeSchedulerFailure,
		model.OperationEvidenceTypeVolumeMountFailure,
		model.OperationEvidenceTypeReadinessProbeFailure,
		model.OperationEvidenceTypeLivenessProbeFailure,
		model.OperationEvidenceTypeStartupProbeFailure,
	}
}

func operationEvidenceRolloutFailureMetricTypes() []string {
	return []string{
		model.OperationEvidenceTypeRolloutPodFailure,
		model.OperationEvidenceTypeRolloutContainerTerminated,
		model.OperationEvidenceTypeRolloutTimeout,
		model.OperationEvidenceTypeImagePullFailure,
		model.OperationEvidenceTypeSchedulerFailure,
		model.OperationEvidenceTypeVolumeMountFailure,
		model.OperationEvidenceTypeReadinessProbeFailure,
		model.OperationEvidenceTypeLivenessProbeFailure,
		model.OperationEvidenceTypeStartupProbeFailure,
	}
}
