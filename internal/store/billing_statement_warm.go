package store

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/stdlib"
)

// WarmBillingStatements prepares query descriptions only. It neither executes
// billing SQL nor reads balances, takes ledger locks or opens a transaction.
// Retaining each connection until the round ends ensures distinct connections.
func (s *Store) WarmBillingStatements(ctx context.Context, connections int) error {
	if s == nil || s.db == nil || connections <= 0 {
		return nil
	}
	connections = min(connections, 16)
	if maxOpen := s.db.Stats().MaxOpenConnections; maxOpen > 0 {
		connections = min(connections, maxOpen)
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	held := make([]*sql.Conn, connections)
	errors := make([]error, connections)
	var wg sync.WaitGroup
	for i := range connections {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := s.db.Conn(ctx)
			if err != nil {
				errors[i] = err
				return
			}
			held[i] = conn
			errors[i] = conn.Raw(func(driver any) error {
				pg, ok := driver.(*stdlib.Conn)
				if !ok {
					return fmt.Errorf("statement warming requires PostgreSQL")
				}
				for _, query := range []string{billingSnapshotInputsSQL, billingSnapshotLocksSQL, billingSnapshotPersistSQL, billingSnapshotEventsSQL} {
					// pgx uses SQL as the map key and a derived server name. The
					// normal Query/Exec path reuses this exact description and
					// retains its existing parameter encoding and query mode.
					if _, err := pg.Conn().Prepare(ctx, query, query); err != nil {
						return err
					}
				}
				return nil
			})
		}()
	}
	wg.Wait()
	for _, conn := range held {
		if conn != nil {
			_ = conn.Close()
		}
	}
	for _, err := range errors {
		if err != nil {
			return err
		}
	}
	return nil
}

// RunBillingStatementWarmer is best effort and does not gate HTTP readiness.
// The configured count bounds connection use, including during a DB failover.
func (s *Store) RunBillingStatementWarmer(ctx context.Context, connections int, report func(error)) {
	if connections <= 0 {
		return
	}
	for ctx.Err() == nil {
		err := s.WarmBillingStatements(ctx, connections)
		if report != nil {
			report(err)
		}
		timer := time.NewTimer(time.Minute)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}
