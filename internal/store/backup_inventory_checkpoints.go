package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
)

const backupInventoryCheckpointPrefix = "backup-inventory/v3/"

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
	return b.store.updateObservationPages(ctx, backupInventoryKey(namespace), pages, legacy)
}

func backupInventoryKey(namespace [sha256.Size]byte) string {
	return backupInventoryCheckpointPrefix + hex.EncodeToString(namespace[:])
}
