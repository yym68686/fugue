package store

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"fugue/internal/platformconfig"
)

func TestAdmissionWriterContentionPreservesBudgetAndDoesNotAbortTransaction(t *testing.T) {
	db, _, _ := hotInventoryTestDatabase(t)
	holder, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Rollback()
	if _, err := holder.ExecContext(t.Context(), `SELECT pg_advisory_xact_lock(315609238744290)`); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(t.Context(), `SET LOCAL lock_timeout='50ms'`); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if err := waitCompilerInputAdmissionWriter(t.Context(), tx); stringSQLState(err) != "55P03" {
		t.Fatal("contention did not retain the existing retry classification", err)
	}
	if elapsed := time.Since(started); elapsed < 40*time.Millisecond || elapsed > time.Second {
		t.Fatal("stricter caller lock budget was ignored", elapsed)
	}
	if _, err := tx.ExecContext(t.Context(), `SELECT 1`); err != nil {
		t.Fatal("ordinary contention aborted the database transaction", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := waitCompilerInputAdmissionWriter(ctx, tx); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled acquisition continued waiting", err)
	}
}

func TestAppendWriterAllowsCommittedReadersButSerializesWritersAndMaintenance(t *testing.T) {
	for _, chunks := range []bool{false, true} {
		t.Run(fmt.Sprintf("chunks=%t", chunks), func(t *testing.T) {
			s := archiveTestStore(t)
			s.storagePolicy.value.CompilerInputChunks = chunks
			s.storagePolicy.separated = compilerInputCodecPolicy{Version: 1, Enabled: chunks}
			snapshot := platformconfig.RuntimeSnapshot{IntentGeneration: fmt.Sprintf("append-reader-%t-%d", chunks, time.Now().UnixNano()), PolicyGeneration: "policy"}
			digest, _ := platformconfig.RuntimeSnapshotDigest(snapshot)
			if err := s.EnsurePlatformCompilerInputContext(t.Context(), snapshot, digest); err != nil {
				t.Fatal(err)
			}
			guard, err := s.db.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer guard.Rollback()
			table := "fugue_platform_input_manifests"
			if _, err = guard.Exec("LOCK TABLE " + table + " IN SHARE MODE"); err != nil {
				t.Fatal(err)
			}
			next := snapshot
			next.IntentGeneration += "-new"
			nextDigest, _ := platformconfig.RuntimeSnapshotDigest(next)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- s.EnsurePlatformCompilerInputContext(ctx, next, nextDigest) }()
			for {
				var held bool
				err = s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks q JOIN pg_locks b USING(pid)
 WHERE q.locktype='advisory' AND q.classid=73483 AND q.objid=2156932323 AND q.granted
 AND b.locktype='advisory' AND b.classid=73483 AND b.objid=2156932322 AND b.mode='ShareLock' AND b.granted)`).Scan(&held)
				if err != nil {
					t.Fatal("append did not enter its protected write", err)
				}
				if held {
					break
				}
				select {
				case err := <-done:
					t.Fatal("fixture did not pause the actual append writer", err)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				case <-time.After(5 * time.Millisecond):
				}
			}
			reader, err := s.db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Rollback()
			if err = waitCompilerInputAdmissionWriter(ctx, reader); err != nil {
				t.Fatal("append blocked committed input admission", err)
			}
			got, err := s.GetPlatformCompilerInputContext(ctx, digest)
			if err != nil || got.IntentGeneration != snapshot.IntentGeneration {
				t.Fatal("committed input became unreadable during append", err)
			}
			var maintenance bool
			if err = guard.QueryRow(`SELECT pg_try_advisory_xact_lock(315609238744290)`).Scan(&maintenance); err != nil || maintenance {
				t.Fatal("maintenance bypassed live writer/readers", maintenance, err)
			}
			queued := next
			queued.IntentGeneration += "-queued"
			queuedDigest, _ := platformconfig.RuntimeSnapshotDigest(queued)
			queueCtx, queueCancel := context.WithTimeout(ctx, 100*time.Millisecond)
			defer queueCancel()
			if err = s.EnsurePlatformCompilerInputContext(queueCtx, queued, queuedDigest); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("append writers no longer serialized within caller budget", err)
			}
			failure := compilerInputWriterStats.diagnosticSnapshot()["last_failure"].(compilerInputWriterFailure)
			if failure.Phase != "append_queue" {
				t.Fatal("append queue was confused with lifecycle contention", failure)
			}
			if err = reader.Rollback(); err != nil {
				t.Fatal(err)
			}
			if err = guard.Commit(); err != nil {
				t.Fatal(err)
			}
			if err = <-done; err != nil {
				t.Fatal("append failed after its write was released", err)
			}
			if _, err = s.GetPlatformCompilerInputContext(ctx, nextDigest); err != nil {
				t.Fatal("verified append was not committed", err)
			}
		})
	}
}

func TestAdmissionWriterAcquiresAfterReleaseAndHoldsUntilRollback(t *testing.T) {
	db, _, _ := hotInventoryTestDatabase(t)
	holder, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Rollback()
	if _, err := holder.ExecContext(t.Context(), `SELECT pg_advisory_xact_lock(315609238744290)`); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	result := make(chan error, 1)
	go func() { result <- waitCompilerInputAdmissionWriter(t.Context(), tx) }()
	select {
	case err := <-result:
		t.Fatal("admission passed a live exclusive writer", err)
	case <-time.After(50 * time.Millisecond):
	}
	if err := holder.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatal("admission did not resume after release", err)
	}
	other, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Rollback()
	var acquired bool
	if err := other.QueryRowContext(t.Context(), `SELECT pg_try_advisory_xact_lock(315609238744290)`).Scan(&acquired); err != nil || acquired {
		t.Fatal("admission did not hold its fence until rollback", acquired, err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := other.QueryRowContext(t.Context(), `SELECT pg_try_advisory_xact_lock(315609238744290)`).Scan(&acquired); err != nil || !acquired {
		t.Fatal("admission leaked its fence", acquired, err)
	}
}
