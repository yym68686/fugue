package store

import (
	"context"
	"fmt"
	"strings"
)

// WithAdvisoryLock runs fn only when this process acquires the named global
// advisory lock. Postgres locks are session-scoped, so the lock and unlock must
// use the same pinned connection.
func (s *Store) WithAdvisoryLock(ctx context.Context, name string, fn func() error) (bool, error) {
	if s == nil {
		return false, nil
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return false, ErrInvalidInput
	}
	if fn == nil {
		fn = func() error { return nil }
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if !s.usingDatabase() {
		if !s.advisoryLockMu.TryLock() {
			return false, nil
		}
		defer s.advisoryLockMu.Unlock()
		return true, fn()
	}
	if err := s.ensureDatabaseReady(); err != nil {
		return false, err
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return false, fmt.Errorf("pin postgres connection for advisory lock: %w", err)
	}
	defer conn.Close()

	var acquired bool
	if err := conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock(hashtextextended($1, 0))`, name).Scan(&acquired); err != nil {
		return false, fmt.Errorf("acquire advisory lock %q: %w", name, err)
	}
	if !acquired {
		return false, nil
	}
	defer conn.ExecContext(context.Background(), `SELECT pg_advisory_unlock(hashtextextended($1, 0))`, name)

	if err := fn(); err != nil {
		return true, err
	}
	return true, nil
}
