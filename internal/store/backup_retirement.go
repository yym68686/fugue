package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

const (
	recoveredBackupRetirementAction = "backup.artifact.retire_recovered"
	recoveredBackupMinAge           = 7 * 24 * time.Hour
	replacementBackupMaxAge         = 72 * time.Hour
	minRetainedRecoveryPoints       = 3
)

type recoveredBackupRetirementState struct {
	Artifact, Replacement model.BackupArtifact
	Run, ReplacementRun   model.BackupRun
	HasAlias, Restoring   bool
	NewerReplacement      bool
	RetainedCandidates    []model.BackupArtifact
}

// RecoveredBackupRetirement evaluates a preview or commits a reviewed
// protected->deleted transition. The caller separately observes remote
// identities before submitting a confirmation. No remote writes occur here.
func (s *Store) RecoveredBackupRetirement(ctx context.Context, id string, req model.RecoveredBackupRetirementRequest, actor model.Principal) (model.RecoveredBackupRetirement, error) {
	if !actor.IsPlatformAdmin() || actor.ActorID == "" || actor.ActorType == "" ||
		id == "" || len(id) > 128 || req.ReplacementArtifactID == "" ||
		len(req.ReplacementArtifactID) > 128 || id == req.ReplacementArtifactID ||
		len(req.Reason) > 512 || (req.RestoreEvidenceSHA256 != "" && !backupRetirementDigestValid(req.RestoreEvidenceSHA256)) ||
		(req.Confirmation != "" && !backupRetirementDigestValid(req.Confirmation)) {
		return model.RecoveredBackupRetirement{}, ErrInvalidInput
	}
	return s.pgRecoveredBackupRetirement(ctx, id, req, actor)
}

func (s *Store) pgRecoveredBackupRetirement(ctx context.Context, id string, req model.RecoveredBackupRetirementRequest, actor model.Principal) (model.RecoveredBackupRetirement, error) {
	var result model.RecoveredBackupRetirement
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	preview := req.Confirmation == ""
	options := &sql.TxOptions{ReadOnly: preview, Isolation: sql.LevelRepeatableRead}
	if !preview {
		options.Isolation = sql.LevelSerializable
	}
	tx, err := s.db.BeginTx(ctx, options)
	if err != nil {
		return result, err
	}
	defer tx.Rollback()
	lock := ""
	if !preview {
		lock = " FOR UPDATE"
	}
	rows, err := tx.QueryContext(ctx, backupArtifactSelectSQL()+" WHERE id IN ($1,$2) ORDER BY id"+lock, id, req.ReplacementArtifactID)
	if err != nil {
		return result, mapDBErr(err)
	}
	var input recoveredBackupRetirementState
	for rows.Next() {
		a, scanErr := scanBackupArtifact(rows)
		if scanErr != nil {
			rows.Close()
			return result, scanErr
		}
		if a.ID == id {
			input.Artifact = a
		} else {
			input.Replacement = a
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return result, mapDBErr(err)
	}
	a, b := input.Artifact, input.Replacement
	if a.ID == "" || b.ID == "" {
		return result, ErrNotFound
	}
	// New restore plans lock the same artifact row. Existing plans, even terminal
	// ones, are excluded because restore-run creation may reuse a plan.
	for _, entry := range []struct {
		id  string
		run *model.BackupRun
	}{{a.RunID, &input.Run}, {b.RunID, &input.ReplacementRun}} {
		if entry.id == "" {
			continue
		}
		*entry.run, err = scanBackupRun(tx.QueryRowContext(ctx, backupRunSelectSQL()+" WHERE id=$1", entry.id))
		if err != nil && !errors.Is(err, ErrNotFound) {
			return result, err
		}
	}
	m := a.Manifest.Metadata
	err = tx.QueryRowContext(ctx, `
SELECT EXISTS (
	SELECT 1 FROM fugue_backup_artifacts other WHERE other.id <> $1 AND other.kind='longhorn-snapshot'
	AND (($2 <> '' AND other.manifest_json->'metadata'->>'longhorn_backup_url'=$2)
		OR ($3 <> '' AND $4 <> '' AND other.manifest_json->'metadata'->>'volume_name'=$3
			AND other.manifest_json->'metadata'->>'snapshot_name'=$4))
), EXISTS (
	SELECT 1 FROM fugue_backup_restore_plans p WHERE p.artifact_id=$1
	UNION ALL
	SELECT 1 FROM fugue_backup_restore_runs r WHERE r.artifact_id=$1
)`, id, m["longhorn_backup_url"], m["volume_name"], m["snapshot_name"]).Scan(&input.HasAlias, &input.Restoring)
	if err != nil {
		return result, mapDBErr(err)
	}
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM fugue_backup_artifacts
WHERE policy_id=$1 AND status='active' AND protected=FALSE AND (created_at>$2 OR (created_at=$2 AND id>$3)))`,
		b.PolicyID, b.CreatedAt, b.ID).Scan(&input.NewerReplacement); err != nil {
		return result, mapDBErr(err)
	}
	input.RetainedCandidates, err = pgRetirementRetainedCandidates(ctx, tx, a, b)
	if err != nil {
		return result, err
	}
	now := time.Now().UTC()
	result, err = reviewRecoveredBackupRetirement(now, input, req, actor)
	if err != nil || preview || len(result.Blockers) != 0 {
		return result, err
	}
	updated, err := tx.ExecContext(ctx, `
UPDATE fugue_backup_artifacts SET protected=FALSE, status='deleted', deleted_at=$2
WHERE id=$1 AND protected=TRUE AND status='active'`, id, now)
	if err != nil {
		return result, mapDBErr(err)
	}
	if count, err := updated.RowsAffected(); err != nil || count != 1 {
		return result, ErrConflict
	}
	event := recoveredBackupRetirementAudit(now, result, req, actor)
	if err := s.pgAppendAuditEventTx(ctx, tx, event); err != nil {
		return result, err
	}
	if err := tx.Commit(); err != nil {
		return result, mapDBErr(err)
	}
	result.Retired, result.AuditID = true, event.ID
	return result, nil
}

func pgRetirementRetainedCandidates(ctx context.Context, tx *sql.Tx, old, replacement model.BackupArtifact) ([]model.BackupArtifact, error) {
	rows, err := tx.QueryContext(ctx, backupArtifactSelectSQL()+`
 WHERE policy_id=$1 AND backend_id=$2 AND kind='longhorn-snapshot' AND status='active'
   AND created_at>$3 AND id<>$4
   AND manifest_json->'metadata'->>'volume_name'=$5
   AND manifest_json->'metadata'->>'backup_target_name'=$6
   AND manifest_json->'metadata'->>'backup_target_url'=$7
   AND EXISTS (SELECT 1 FROM fugue_backup_runs r WHERE r.id=fugue_backup_artifacts.run_id AND r.status='succeeded')`,
		replacement.PolicyID, replacement.BackendID, old.CreatedAt, replacement.ID,
		replacement.Manifest.Metadata["volume_name"], replacement.Manifest.Metadata["backup_target_name"], replacement.Manifest.Metadata["backup_target_url"])
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()
	var candidates []model.BackupArtifact
	for rows.Next() {
		a, err := scanBackupArtifact(rows)
		if err != nil {
			return nil, err
		}
		candidates = append(candidates, a)
	}
	if err := rows.Err(); err != nil {
		return nil, mapDBErr(err)
	}
	var eligible []model.BackupArtifact
	for _, a := range candidates {
		run, err := scanBackupRun(tx.QueryRowContext(ctx, backupRunSelectSQL()+" WHERE id=$1", a.RunID))
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if isRetirementRetainedCandidate(old, replacement, a, run) {
			eligible = append(eligible, a)
		}
	}
	return eligible, nil
}

func isRetirementRetainedCandidate(old, replacement, a model.BackupArtifact, run model.BackupRun) bool {
	m, rm := a.Manifest.Metadata, replacement.Manifest.Metadata
	return a.ID != replacement.ID && a.Status == model.BackupArtifactStatusActive && a.PolicyID == replacement.PolicyID &&
		a.BackendID == replacement.BackendID && a.CreatedAt.After(old.CreatedAt) && backupRetirementIdentity(a, run) &&
		run.Status == model.BackupRunStatusSucceeded && m["volume_name"] == rm["volume_name"] &&
		m["backup_target_name"] == rm["backup_target_name"] && m["backup_target_url"] == rm["backup_target_url"]
}

func retirementRetainedSet(in recoveredBackupRetirementState, now time.Time) []model.BackupArtifact {
	sort.Slice(in.RetainedCandidates, func(i, j int) bool {
		a, b := in.RetainedCandidates[i], in.RetainedCandidates[j]
		if a.CreatedAt.Equal(b.CreatedAt) {
			return a.ID > b.ID
		}
		return a.CreatedAt.After(b.CreatedAt)
	})
	retained := []model.BackupArtifact{in.Replacement}
	for _, a := range in.RetainedCandidates {
		if a.CreatedAt.After(now) || backupRetirementAlias(in.Artifact, a) || a.RunID == in.Artifact.RunID {
			continue
		}
		distinct := true
		for _, b := range retained {
			if a.RunID == b.RunID || backupRetirementAlias(b, a) || a.Manifest.Metadata["longhorn_backup_name"] == b.Manifest.Metadata["longhorn_backup_name"] {
				distinct = false
				break
			}
		}
		if distinct {
			retained = append(retained, a)
			if len(retained) == minRetainedRecoveryPoints {
				break
			}
		}
	}
	return retained
}

func reviewRecoveredBackupRetirement(now time.Time, in recoveredBackupRetirementState, req model.RecoveredBackupRetirementRequest, actor model.Principal) (model.RecoveredBackupRetirement, error) {
	a, b, r, replacementRun := in.Artifact, in.Replacement, in.Run, in.ReplacementRun
	result := model.RecoveredBackupRetirement{ArtifactID: a.ID, ReplacementArtifactID: b.ID,
		Blockers: []string{}, RetainedArtifactIDs: []string{}, RestoreEvidenceStatus: "not_provided"}
	if req.RestoreEvidenceSHA256 != "" {
		result.RestoreEvidenceStatus = "operator_attested"
	}
	block := func(condition bool, message string) {
		if condition {
			result.Blockers = append(result.Blockers, message)
		}
	}
	switch a.Manifest.Metadata["reconciliation_source"] {
	case "completed-engine-reconciliation", "completed-longhorn-orphan-manifest":
	default:
		block(true, "artifact is not a recovered Longhorn backup")
	}
	block(!a.Protected || a.Status != model.BackupArtifactStatusActive || !backupRetirementIdentity(a, r), "artifact is not an active, protected control-plane Longhorn backup with exact ownership")
	block(r.Status != model.BackupRunStatusFailed && r.Status != model.BackupRunStatusCanceled, "owning run is not failed or canceled")
	block(a.CreatedAt.IsZero() || a.CreatedAt.After(now.Add(-recoveredBackupMinAge)), "recovered backup has not completed the seven-day minimum age")
	block((r.LockedUntil != nil && r.LockedUntil.After(now)) || (r.NextRetryAt != nil && r.NextRetryAt.After(now)), "owning run has an active lease or future retry")
	block(in.HasAlias, "another artifact refers to the same remote backup or local snapshot")
	block(in.Restoring, "artifact has a restore plan or run reference")
	block(in.NewerReplacement || b.Protected, "replacement must be the newest unprotected active artifact of the policy")
	block(!backupRetirementIdentity(b, replacementRun) || b.Status != model.BackupArtifactStatusActive ||
		replacementRun.Status != model.BackupRunStatusSucceeded || b.PolicyID != a.PolicyID ||
		b.BackendID != a.BackendID || !b.CreatedAt.After(a.CreatedAt) || b.CreatedAt.Before(now.Add(-replacementBackupMaxAge)) || b.CreatedAt.After(now),
		"replacement must be an active, successful backup under the same policy and backend and no older than 72 hours")
	am, bm := a.Manifest.Metadata, b.Manifest.Metadata
	block(am["volume_name"] != bm["volume_name"] || am["backup_target_url"] != bm["backup_target_url"] ||
		am["backup_target_name"] != bm["backup_target_name"] || am["longhorn_backup_url"] == bm["longhorn_backup_url"] ||
		am["longhorn_backup_name"] == bm["longhorn_backup_name"] ||
		am["snapshot_name"] == bm["snapshot_name"], "replacement must be a distinct recovery point for the same Longhorn volume and repository")
	retained := retirementRetainedSet(in, now)
	block(len(retained) < minRetainedRecoveryPoints, "at least three distinct newer successful recovery points in the same volume and repository must remain")
	block(strings.TrimSpace(req.Reason) == "", "a retirement reason is required")
	if len(result.Blockers) != 0 {
		return result, nil
	}
	for _, point := range retained {
		result.RetainedArtifactIDs = append(result.RetainedArtifactIDs, point.ID)
	}
	binding, err := json.Marshal([]any{"fugue.recovered-backup-retirement/v2", actor.ActorType, actor.ActorID, a, retained, r, replacementRun, req.Reason, req.RestoreEvidenceSHA256})
	if err != nil {
		return result, err
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(binding))
	if req.Confirmation != "" && req.Confirmation != digest {
		result.Blockers = append(result.Blockers, "eligibility or evidence changed since preview; preview again")
		return result, nil
	}
	result.Confirmation = digest
	return result, nil
}

func backupRetirementIdentity(a model.BackupArtifact, run model.BackupRun) bool {
	if run.ID == "" || a.RunID != run.ID || a.PolicyID == "" || a.PolicyID != run.PolicyID ||
		a.BackendID == "" || a.BackendID != run.BackendID || a.Kind != model.BackupArtifactKindLonghornSnapshot ||
		a.Target.Type != model.BackupTargetControlPlaneDatabase || run.Target.Type != model.BackupTargetControlPlaneDatabase ||
		run.Target.Engine != model.BackupEngineLonghornSnapshot || a.TenantID != "" || a.ProjectID != "" || a.AppID != "" ||
		a.Manifest.Format != "longhorn-remote-backup" || a.Manifest.RunID != a.RunID {
		return false
	}
	for _, name := range []string{"longhorn_backup_name", "longhorn_backup_url", "backup_target_name", "backup_target_url", "volume_name", "snapshot_name"} {
		if strings.TrimSpace(a.Manifest.Metadata[name]) == "" {
			return false
		}
	}
	return true
}

func backupRetirementAlias(a, b model.BackupArtifact) bool {
	m, other := a.Manifest.Metadata, b.Manifest.Metadata
	return b.Kind == model.BackupArtifactKindLonghornSnapshot &&
		((m["longhorn_backup_url"] != "" && m["longhorn_backup_url"] == other["longhorn_backup_url"]) ||
			(m["volume_name"] != "" && m["snapshot_name"] != "" && m["volume_name"] == other["volume_name"] && m["snapshot_name"] == other["snapshot_name"]))
}

func backupRetirementDigestValid(digest string) bool {
	if len(digest) != 2*sha256.Size || digest != strings.ToLower(digest) {
		return false
	}
	_, err := hex.DecodeString(digest)
	return err == nil
}

func recoveredBackupRetirementAudit(now time.Time, result model.RecoveredBackupRetirement, req model.RecoveredBackupRetirementRequest, actor model.Principal) model.AuditEvent {
	return model.AuditEvent{ID: model.NewID("audit"), CreatedAt: now, ActorType: actor.ActorType, ActorID: actor.ActorID,
		Action: recoveredBackupRetirementAction, TargetType: "backup_artifact", TargetID: result.ArtifactID,
		Metadata: map[string]string{
			"replacement_artifact_id": result.ReplacementArtifactID, "restore_evidence_sha256": req.RestoreEvidenceSHA256,
			"evidence_source": result.RestoreEvidenceStatus, "retained_artifact_ids": strings.Join(result.RetainedArtifactIDs, ","),
			"reason": req.Reason, "confirmation": req.Confirmation,
		}}
}
