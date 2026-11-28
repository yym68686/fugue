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
	for _, k := range []string{key, backupInventoryOwnerKey} {
		ok, err := (&Store{}).UpdateObservationCheckpoint(t.Context(), k, func([]byte) ([]byte, error) {
			t.Fatal("generic writer entered the dedicated backup namespace")
			return nil, nil
		})
		if ok || !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("expected scope rejection, got %v, %v", ok, err)
		}
	}
}

func TestPostgresBackupInventoryFencePreservesCursor(t *testing.T) {
	for _, profile := range []string{"legacy", "legacy-without-page-schema", "paged"} {
		t.Run(profile, func(t *testing.T) {
			s := newStoreTestDatabase(t, "")
			ctx := t.Context()
			s.storagePolicy.value.CheckpointPages = profile == "paged"
			if profile == "legacy-without-page-schema" {
				if _, err := s.db.ExecContext(ctx, `ALTER TABLE fugue_observation_checkpoint_heads RENAME TO fixture_checkpoint_heads`); err != nil {
					t.Fatal(err)
				}
			}
			checkpoint := s.BackupInventoryCheckpointWriter()
			namespace := sha256.Sum256([]byte("backup namespace"))
			verifyTransferBlocked := func() {
				var held bool
				err := s.db.QueryRowContext(ctx, `SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))`, backupInventoryOwnerKey).Scan(&held)
				if err != nil || held {
					t.Fatalf("exclusive owner lock during page: held=%t err=%v", held, err)
				}
			}
			ok, err := checkpoint.Update(ctx, namespace, func(ObservationPages, []byte) (ObservationPages, error) {
				verifyTransferBlocked()
				return ObservationPages{Metadata: []byte("next")}, nil
			}, func([]byte) ([]byte, error) {
				verifyTransferBlocked()
				return []byte("next"), nil
			})
			if !ok || err != nil {
				t.Fatalf("initial API page: accepted=%t err=%v", ok, err)
			}
			func() {
				tx, err := s.db.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				var held bool
				if err := tx.QueryRowContext(ctx, `SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))`, backupInventoryOwnerKey).Scan(&held); err != nil || !held {
					t.Fatalf("owner fence did not release: held=%t err=%v", held, err)
				}
				if _, err := tx.ExecContext(ctx, `INSERT INTO fugue_meta(key,value) VALUES($1,'worker')`, observationCheckpointKey(backupInventoryOwnerKey)); err != nil {
					t.Fatal(err)
				}
				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}
			}()
			ok, err = checkpoint.Update(ctx, namespace, func(ObservationPages, []byte) (ObservationPages, error) {
				t.Fatal("former writer entered paged callback")
				return ObservationPages{}, nil
			}, func([]byte) ([]byte, error) {
				t.Fatal("former writer entered legacy callback")
				return nil, nil
			})
			if ok || err != nil {
				t.Fatalf("former writer accepted=%t err=%v", ok, err)
			}
			pages, legacy, err := checkpoint.Read(ctx, namespace)
			if profile == "paged" {
				legacy = pages.Metadata
			}
			if err != nil || string(legacy) != "next" {
				t.Fatalf("cursor after owner change: %q, err=%v", legacy, err)
			}
		})
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
