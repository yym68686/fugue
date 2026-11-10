package store

import (
	"context"
	"database/sql"
	"sync"
)

// pendingRootNativeTx owns the explicitly acquired connection as well as the
// transaction. A transaction started on sql.Conn does not return that Conn to
// its pool by itself, including after context cancellation has rolled it back.
type pendingRootNativeTx struct {
	*sql.Tx
	conn   *sql.Conn
	cancel context.CancelFunc
	once   sync.Once
}

func (tx *pendingRootNativeTx) release() {
	tx.once.Do(func() {
		_ = tx.conn.Close()
		tx.cancel()
	})
}

func (tx *pendingRootNativeTx) Commit() error {
	err := tx.Tx.Commit()
	tx.release()
	return err
}

func (tx *pendingRootNativeTx) Rollback() error {
	err := tx.Tx.Rollback()
	tx.release()
	return err
}
