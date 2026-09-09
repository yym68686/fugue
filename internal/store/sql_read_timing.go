package store

import (
	"context"
	"database/sql"
	"time"
)

// SQLReadTiming separates acquiring a connection (including opening one),
// waiting for the query result, transferring rows and decoding them.
type SQLReadTiming struct {
	Acquire, Query, Rows, Decode time.Duration
}

type sqlReadQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func acquireSQLRead(ctx context.Context, db *sql.DB, timing *SQLReadTiming) (sqlReadQueryer, func(), error) {
	if timing == nil {
		return db, func() {}, nil
	}
	started := time.Now()
	conn, err := db.Conn(ctx)
	timing.Acquire += time.Since(started)
	if err != nil {
		return nil, func() {}, err
	}
	return conn, func() { _ = conn.Close() }, nil
}

// DatabaseReadStats exposes counters only, never queries or credentials.
func (s *Store) DatabaseReadStats() sql.DBStats {
	if s.db == nil {
		return sql.DBStats{}
	}
	return s.db.Stats()
}
