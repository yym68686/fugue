package store

import (
	"context"
	"errors"
	"fugue/internal/model"
	"sort"
	"time"
)

// ReconcileBackupRetention retries retention independently of new successful
// backups. Disabled policies keep their history unless an administrator
// explicitly reconciles that policy. Manual backups have no implicit expiry.
func (s *Store) ReconcileBackupRetention(policyID string) error {
	if s.usingDatabase() {
		return s.pgApplyBackupRetention(policyID)
	}
	return s.withLockedState(true, func(state *model.State) error {
		if findBackupPolicy(state, policyID) < 0 {
			return ErrNotFound
		}
		applyBackupRetentionInState(state, policyID)
		return nil
	})
}

func (s *Store) ReconcileEnabledBackupRetention() error {
	policies, err := s.ListBackupPolicies(BackupPolicyFilter{PlatformAdmin: true, Limit: 10001})
	if err != nil {
		return err
	}
	if len(policies) >= 10001 {
		return errors.New("policy retention inventory exceeds safety bound")
	}
	var failures []error
	for _, p := range policies {
		if err := s.ReconcileBackupRetention(p.ID); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

func retentionExpired(policy model.BackupPolicy, rank int, created, now time.Time) bool {
	keep := policy.Retention.ProtectLatest
	if rank < keep {
		return false
	}
	count := policy.RetainCount
	if count <= 0 {
		count = policy.Retention.RetainCount
	}
	return (count > 0 && rank >= count) || (policy.Retention.RetainDays > 0 && created.Before(now.Add(-time.Duration(policy.Retention.RetainDays)*24*time.Hour)))
}

// Restore creation also locks the artifact row, so it cannot race an expiry.
const retentionEligibleSQL = `status='active' AND protected=FALSE
 AND NOT EXISTS (SELECT 1 FROM fugue_backup_restore_plans p WHERE p.artifact_id=fugue_backup_artifacts.id AND p.status NOT IN ('succeeded','failed','canceled'))
 AND NOT EXISTS (SELECT 1 FROM fugue_backup_restore_runs r WHERE r.artifact_id=fugue_backup_artifacts.id AND r.status NOT IN ('succeeded','failed','canceled'))`

func (s *Store) PreviewBackupRetention(policyID string) ([]string, error) {
	ids := []string{}
	if s.usingDatabase() {
		p, err := s.pgGetBackupPolicy(policyID, "", true)
		if err != nil {
			return nil, err
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		rows, err := s.db.QueryContext(ctx, `SELECT id,created_at FROM fugue_backup_artifacts WHERE policy_id=$1 AND `+retentionEligibleSQL+` ORDER BY created_at DESC,id DESC`, p.ID)
		if err != nil {
			return nil, mapDBErr(err)
		}
		defer rows.Close()
		rank, now := 0, time.Now().UTC()
		for rows.Next() {
			var id string
			var created time.Time
			if err := rows.Scan(&id, &created); err != nil {
				return nil, err
			}
			if retentionExpired(p, rank, created, now) {
				ids = append(ids, id)
			}
			rank++
		}
		return ids, mapDBErr(rows.Err())
	}
	err := s.withLockedState(false, func(st *model.State) error {
		i := findBackupPolicy(st, policyID)
		if i < 0 {
			return ErrNotFound
		}
		artifacts := []model.BackupArtifact{}
		for _, a := range st.BackupArtifacts {
			if a.PolicyID == policyID && a.Status == "active" && !a.Protected && !backupArtifactHasInFlightRestore(st, a.ID) {
				artifacts = append(artifacts, a)
			}
		}
		sort.Slice(artifacts, func(i, j int) bool {
			if artifacts[i].CreatedAt.Equal(artifacts[j].CreatedAt) {
				return artifacts[i].ID > artifacts[j].ID
			}
			return artifacts[i].CreatedAt.After(artifacts[j].CreatedAt)
		})
		for rank, a := range artifacts {
			if retentionExpired(st.BackupPolicies[i], rank, a.CreatedAt, time.Now().UTC()) {
				ids = append(ids, a.ID)
			}
		}
		return nil
	})
	return ids, err
}

func (s *Store) pgReconcileRetentionCandidates(policy model.BackupPolicy) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Policy writers take an exclusive row lock. Keep the retention decision
	// consistent with the policy until all expiry transitions commit.
	policy, err = scanBackupPolicy(tx.QueryRowContext(ctx, backupPolicySelectSQL()+` WHERE id=$1 FOR SHARE`, policy.ID))
	if err != nil {
		return mapDBErr(err)
	}
	rows, err := tx.QueryContext(ctx, `SELECT id,created_at FROM fugue_backup_artifacts WHERE policy_id=$1 AND `+retentionEligibleSQL+` ORDER BY created_at DESC,id DESC`, policy.ID)
	if err != nil {
		return mapDBErr(err)
	}
	ids := []string{}
	rank := 0
	now := time.Now().UTC()
	for rows.Next() {
		var id string
		var created time.Time
		if err = rows.Scan(&id, &created); err != nil {
			rows.Close()
			return err
		}
		if retentionExpired(policy, rank, created, now) {
			ids = append(ids, id)
		}
		rank++
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, id := range ids {
		var lockedID string
		if err := tx.QueryRowContext(ctx, `SELECT id FROM fugue_backup_artifacts WHERE id=$1 FOR UPDATE`, id).Scan(&lockedID); err != nil {
			return mapDBErr(err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE fugue_backup_artifacts SET status='expired',deleted_at=$2 WHERE id=$1 AND `+retentionEligibleSQL, id, now); err != nil {
			return mapDBErr(err)
		}
	}
	return tx.Commit()
}
