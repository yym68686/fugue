package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
)

const (
	backupInventoryCheckpointPrefix = "backup-inventory/v3/"
	backupInventoryOwnerKey         = "backup-inventory-writer/v1"
)

// BackupInventoryCheckpoints is the sole write port for backup inventory.
// Callers supply a namespace digest and cannot select any other checkpoint.
type BackupInventoryCheckpoints struct{ store *Store }

func (s *Store) BackupInventoryCheckpointWriter() BackupInventoryCheckpoints {
	return BackupInventoryCheckpoints{store: s}
}

func (b BackupInventoryCheckpoints) Read(ctx context.Context, namespace [sha256.Size]byte) (ObservationPages, []byte, error) {
	return b.store.readObservationPages(ctx, backupInventoryKey(namespace))
}

func (b BackupInventoryCheckpoints) Update(ctx context.Context, namespace [sha256.Size]byte, pages ObservationPageUpdate, legacy func([]byte) ([]byte, error)) (bool, error) {
	return b.store.updateObservationPages(ctx, backupInventoryKey(namespace), pages, legacy, admitAPIBackupInventoryWriter)
}

// Hold the shared owner fence until the page and its cursor commit. The API
// remains the sole writer; a future handoff must take the exclusive form of
// the same lock. A missing owner preserves the pre-migration API ownership.
func admitAPIBackupInventoryWriter(ctx context.Context, tx *sql.Tx) (bool, error) {
	var held bool
	if err := tx.QueryRowContext(ctx, `SELECT pg_try_advisory_xact_lock_shared(hashtextextended($1,0))`, backupInventoryOwnerKey).Scan(&held); err != nil || !held {
		return false, err
	}
	var owner string
	err := tx.QueryRowContext(ctx, `SELECT value FROM fugue_meta WHERE key=$1`, observationCheckpointKey(backupInventoryOwnerKey)).Scan(&owner)
	if errors.Is(err, sql.ErrNoRows) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	switch owner {
	case "api":
		return true, nil
	case "worker":
		return false, nil
	default:
		return false, errors.New("unknown backup inventory writer")
	}
}

func backupInventoryKey(namespace [sha256.Size]byte) string {
	return backupInventoryCheckpointPrefix + hex.EncodeToString(namespace[:])
}
