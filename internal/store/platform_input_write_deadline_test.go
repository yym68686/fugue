package store

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"fugue/internal/platformconfig"
)

func TestCompilerInputWriterQueueRespectsCallerDeadline(t *testing.T) {
	s := boundedContentTestStore(t)
	locker, err := s.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer locker.Rollback()
	if _, err = locker.ExecContext(t.Context(), `SELECT pg_advisory_xact_lock(315609238744290)`); err != nil {
		t.Fatal(err)
	}
	snapshot := platformconfig.RuntimeSnapshot{IntentGeneration: fmt.Sprintf("deadline-%d", time.Now().UnixNano()), PolicyGeneration: "policy"}
	digest, err := platformconfig.RuntimeSnapshotDigest(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.EnsurePlatformCompilerInputContext(ctx, snapshot, digest) }()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("queued writer ignored caller deadline", err)
		}
	case <-time.After(2 * time.Second):
		_ = locker.Rollback()
		<-done
		t.Fatal("cancelled writer remained in the database queue")
	}
	var count int
	if err = s.db.QueryRowContext(t.Context(), `SELECT count(*) FROM fugue_platform_input_manifests WHERE content_hash=$1`, digest).Scan(&count); err != nil || count != 0 {
		t.Fatal("cancelled writer persisted an input", count, err)
	}
}

func TestCompilerInputWriterSurvivesQueueBeyondLegacyDeadline(t *testing.T) {
	s := boundedContentTestStore(t)
	locker, err := s.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer locker.Rollback()
	if _, err = locker.ExecContext(t.Context(), `SELECT pg_advisory_xact_lock(315609238744290)`); err != nil {
		t.Fatal(err)
	}
	snapshot := platformconfig.RuntimeSnapshot{IntentGeneration: fmt.Sprintf("queued-write-%d", time.Now().UnixNano()), PolicyGeneration: "policy"}
	digest, err := platformconfig.RuntimeSnapshotDigest(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.EnsurePlatformCompilerInputContext(ctx, snapshot, digest) }()
	select {
	case err := <-done:
		t.Fatal("writer stopped before the valid queue was released", err)
	case <-time.After(31 * time.Second):
	}
	if err = locker.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err = <-done; err != nil {
		t.Fatal("bounded queued write failed", err)
	}
	stored, err := s.GetPlatformCompilerInput(digest)
	if err != nil {
		t.Fatal(err)
	}
	if actual, err := platformconfig.RuntimeSnapshotDigest(stored); err != nil || actual != digest {
		t.Fatal("queued write changed the verified input", err)
	}
}
