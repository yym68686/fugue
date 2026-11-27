package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"time"

	"fugue/internal/storageaudit"
)

const (
	storageLifecycleReaderAcquire = `SELECT pg_try_advisory_lock_shared(hashtextextended($1, 0))`
	storageLifecycleReaderRelease = `SELECT pg_advisory_unlock_shared(hashtextextended($1, 0))`
	storageLifecycleReaderOwner   = `SELECT owner FROM fugue_storage_audit.lifecycle_status_v1`
)

// EnableStorageLifecycleHandoff is part of startup configuration. Only enable
// it once the capability is installed and the API has SELECT on its status view.
func (s *Store) EnableStorageLifecycleHandoff() { s.lifecycle.handoff = true }

// Use the already-required process-local storage policy refresh to update the
// diagnostic cache. HTTP diagnostics and metrics do not query PostgreSQL.
func (s *Store) refreshStorageLifecycleStatus(ctx context.Context) {
	if !s.lifecycle.handoff || s.db == nil || ctx.Err() != nil {
		return
	}
	p, err := storageaudit.ReadProgress(ctx, s.db)
	if err != nil {
		s.recordStorageLifecycleStatusFailure()
		return
	}
	s.lifecycle.mu.Lock()
	defer s.lifecycle.mu.Unlock()
	if p.Owner == "worker" {
		s.lifecycle.workerOwned = true
		s.lifecycle.cursor = storageLifecycleCursor{}
		s.lifecycle.observation = p.Observation()
	} else if s.lifecycle.workerOwned {
		s.lifecycle.observation.PolicyReadFailed = true
		s.lifecycle.observation.LastRunFailed = true
		s.lifecycle.observation.FailureCategory = "owner_status_unavailable"
	}
}

func (s *Store) recordStorageLifecycleStatusFailure() {
	s.lifecycle.mu.Lock()
	defer s.lifecycle.mu.Unlock()
	s.lifecycle.observation.PolicyReadFailed = true
	s.lifecycle.observation.LastRunFailed = true
	s.lifecycle.observation.FailureCategory = "owner_status_unavailable"
}

// The session-level lock must precede the owner SELECT and the separate
// repeatable-read scan, all on one physical session. Taking a transaction lock
// in a statement of the RR scan could observe a pre-lock owner snapshot. The
// worker and the handoff acquire this same key exclusively before doing work.
func (s *Store) acquireStorageLifecycleReader(ctx context.Context) (*sql.Conn, func(), error) {
	probe, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	conn, err := s.db.Conn(probe)
	if err != nil {
		return nil, nil, err
	}
	discard := func() {
		// A cancelled query does not prove that PostgreSQL did not acquire
		// the lock. Never return a session with unknown lock state to the pool.
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		_ = conn.Close()
	}
	var held bool
	if err = conn.QueryRowContext(probe, storageLifecycleReaderAcquire, storageaudit.OwnershipLock).Scan(&held); err != nil {
		discard()
		return nil, nil, err
	}
	if !held {
		_ = conn.Close()
		return nil, nil, nil
	}
	release := func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		var unlocked bool
		if err := conn.QueryRowContext(cleanup, storageLifecycleReaderRelease, storageaudit.OwnershipLock).Scan(&unlocked); err != nil || !unlocked {
			discard()
			return
		}
		_ = conn.Close()
	}
	var owner string
	if err = conn.QueryRowContext(probe, storageLifecycleReaderOwner).Scan(&owner); err != nil {
		release()
		return nil, nil, err
	}
	switch owner {
	case "api":
		return conn, release, nil
	case "worker":
		s.lifecycle.mu.Lock()
		if !s.lifecycle.workerOwned {
			s.lifecycle.workerOwned = true
			s.lifecycle.cursor = storageLifecycleCursor{}
			s.lifecycle.observation.PolicyReadFailed = true
			s.lifecycle.observation.LastRunFailed = true
			s.lifecycle.observation.FailureCategory = "owner_status_unavailable"
		}
		s.lifecycle.mu.Unlock()
		release()
		return nil, nil, nil
	default:
		release()
		return nil, nil, errors.New("unrecognized storage lifecycle owner")
	}
}
