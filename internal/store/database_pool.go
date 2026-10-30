package store

import "time"

// ConfigureDatabaseConcurrency bounds physical database connections independently
// of worker concurrency. Excess callers wait in database/sql with their own
// query deadlines instead of competing for PostgreSQL's reserved connections.
func (s *Store) ConfigureDatabaseConcurrency(maxOpen int) {
	if s != nil && s.db != nil && maxOpen > 0 {
		s.db.SetMaxOpenConns(maxOpen)
	}
}

// ConfigureDatabasePool retains established connections for concurrent read
// bursts. It leaves the database/sql concurrency limit unchanged.
func (s *Store) ConfigureDatabasePool(maxIdle int, maxIdleTime time.Duration) {
	if s == nil || s.db == nil {
		return
	}
	s.db.SetMaxIdleConns(max(0, maxIdle))
	s.db.SetConnMaxIdleTime(maxIdleTime)
}
