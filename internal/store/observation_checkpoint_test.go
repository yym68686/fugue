package store

import (
	"context"
	"crypto/sha256"
	"errors"
	"path/filepath"
	"testing"
)

func TestGenericObservationWriterCannotModifyBackupInventory(t *testing.T) {
	key := backupInventoryKey(sha256.Sum256([]byte("backup namespace")))
	// Validate scope before touching a database or invoking a foreign callback.
	ok, err := (&Store{}).UpdateObservationCheckpoint(t.Context(), key, func([]byte) ([]byte, error) {
		t.Fatal("generic writer entered the dedicated backup namespace")
		return nil, nil
	})
	if ok || !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected scope rejection, got %v, %v", ok, err)
	}
}

func TestObservationCheckpointRestartLockAndCanceledWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "postgres-fixture")
	a, b := newStoreTestDatabase(t, path), newStoreTestDatabase(t, path)
	ok, err := a.UpdateObservationCheckpoint(t.Context(), "scan", func(raw []byte) ([]byte, error) {
		if len(raw) != 0 {
			t.Fatal("unexpected initial value")
		}
		acquired, e := b.UpdateObservationCheckpoint(t.Context(), "scan", func([]byte) ([]byte, error) { t.Fatal("concurrent callback ran"); return nil, nil })
		if e != nil || acquired {
			t.Fatalf("lock: %v %v", acquired, e)
		}
		return []byte(`{"page":1}`), nil
	})
	if !ok || err != nil {
		t.Fatal(ok, err)
	}
	raw, err := b.ReadObservationCheckpoint(t.Context(), "scan")
	if err != nil || string(raw) != `{"page":1}` {
		t.Fatal(string(raw), err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	_, err = b.UpdateObservationCheckpoint(ctx, "scan", func([]byte) ([]byte, error) { cancel(); return []byte(`{"page":2}`), nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	raw, _ = a.ReadObservationCheckpoint(t.Context(), "scan")
	if string(raw) != `{"page":1}` {
		t.Fatal("canceled update changed cursor")
	}
}
