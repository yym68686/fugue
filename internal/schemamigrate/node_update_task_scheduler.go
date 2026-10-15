package schemamigrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// Online access path for the fleet scheduler; unrelated historical tasks and
// their payload/log bodies are not scheduler inputs.
var nodeTaskSchedulerIndexes = []struct{ name, definition, create string }{
	{"idx_fugue_node_update_tasks_type_updater_created", "CREATE INDEX idx_fugue_node_update_tasks_type_updater_created ON public.fugue_node_update_tasks USING btree (task_type, node_updater_id, created_at DESC, id DESC)", "CREATE INDEX CONCURRENTLY idx_fugue_node_update_tasks_type_updater_created ON fugue_node_update_tasks (task_type,node_updater_id,created_at DESC,id DESC)"},
}

func MigrateNodeUpdateTaskScheduler(ctx context.Context, databaseURL string) error {
	databaseURL, err := normalizeDatabaseURL(databaseURL)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	db, err := openDatabase(databaseURL)
	if err != nil {
		return err
	}
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	for _, setting := range []string{`SET maintenance_work_mem='32MB'`, `SET max_parallel_maintenance_workers=0`, `SET lock_timeout='2s'`} {
		if _, err := conn.ExecContext(ctx, setting); err != nil {
			return err
		}
	}
	const lockID = int64(315609238744306)
	if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, lockID); err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, _ = conn.ExecContext(cleanup, `SELECT pg_advisory_unlock($1)`, lockID)
	}()
	for _, index := range nodeTaskSchedulerIndexes {
		inspect := func() (bool, bool, error) {
			var definition string
			var valid bool
			err := conn.QueryRowContext(ctx, `SELECT pg_get_indexdef(c.oid),i.indisvalid FROM pg_class c JOIN pg_index i ON i.indexrelid=c.oid WHERE c.oid=to_regclass($1)`, index.name).Scan(&definition, &valid)
			if errors.Is(err, sql.ErrNoRows) {
				return false, false, nil
			}
			if err != nil {
				return false, false, err
			}
			if definition != index.definition {
				return false, true, fmt.Errorf("node task scheduler index %s has an unexpected definition; refusing replacement", index.name)
			}
			return valid, true, nil
		}
		valid, exists, err := inspect()
		if err != nil {
			return err
		}
		if valid {
			continue
		}
		if exists {
			if _, err := conn.ExecContext(ctx, "DROP INDEX CONCURRENTLY "+index.name); err != nil {
				return err
			}
		}
		if _, err := conn.ExecContext(ctx, index.create); err != nil {
			return fmt.Errorf("create node task scheduler index %s: %w", index.name, err)
		}
		valid, _, err = inspect()
		if err != nil {
			return err
		}
		if !valid {
			return fmt.Errorf("node task scheduler index %s is not valid", index.name)
		}
	}
	return nil
}
