package store

import (
	"context"
	"time"

	"fugue/internal/model"
)

// LonghornBackupHasOtherReferences fails closed when another live/protected
// artifact, or an in-flight restore, refers to the same remote backup URL.
// Backend IDs alone are insufficient: two backends can share a repository.
func (s *Store) LonghornBackupHasOtherReferences(artifact model.BackupArtifact) (bool, error) {
	backupURL := artifact.Manifest.Metadata["longhorn_backup_url"]
	if artifact.Kind != model.BackupArtifactKindLonghornSnapshot || backupURL == "" {
		return true, ErrInvalidInput
	}
	if s.usingDatabase() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		var referenced bool
		err := s.db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM fugue_backup_artifacts a
			WHERE a.kind = 'longhorn-snapshot' AND a.manifest_json->'metadata'->>'longhorn_backup_url' = $1
			AND ((a.id <> $2 AND (a.protected = TRUE OR a.status NOT IN ('deleted', 'expired')))
			OR EXISTS (SELECT 1 FROM fugue_backup_restore_plans p WHERE p.artifact_id = a.id AND p.status NOT IN ('succeeded', 'failed', 'canceled'))
			OR EXISTS (SELECT 1 FROM fugue_backup_restore_runs r WHERE r.artifact_id = a.id AND r.status NOT IN ('succeeded', 'failed', 'canceled'))))`, backupURL, artifact.ID).Scan(&referenced)
		return referenced, mapDBErr(err)
	}
	referenced := false
	err := s.withLockedState(false, func(state *model.State) error {
		for _, other := range state.BackupArtifacts {
			if other.Kind != model.BackupArtifactKindLonghornSnapshot || other.Manifest.Metadata["longhorn_backup_url"] != backupURL {
				continue
			}
			if (other.ID != artifact.ID && (other.Protected || (other.Status != model.BackupArtifactStatusDeleted && other.Status != model.BackupArtifactStatusExpired))) || backupArtifactHasInFlightRestore(state, other.ID) {
				referenced = true
				break
			}
		}
		return nil
	})
	return referenced, err
}
