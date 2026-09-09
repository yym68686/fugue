package store

import "time"

// ConfigureDatabasePool retains established connections for concurrent read
// bursts. It leaves the database/sql concurrency limit unchanged.
func (s *Store) ConfigureDatabasePool(maxIdle int, maxIdleTime time.Duration) {
	if s == nil || s.db == nil {
		return
	}
	s.db.SetMaxIdleConns(max(0, maxIdle))
	s.db.SetConnMaxIdleTime(maxIdleTime)
}
