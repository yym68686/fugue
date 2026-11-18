package schemamigrate

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

const platformHistoryFKIndexLock = int64(315609238744335)

type platformHistoryFKIndex struct {
	name, table, column string
	nullable            bool
}

var platformHistoryFKIndexes = [...]platformHistoryFKIndex{
	{"idx_fugue_platform_artifacts_content_ref_hash", "fugue_platform_artifacts", "content_ref_hash", true},
	{"idx_fugue_platform_lkg_history_artifact_id", "fugue_platform_lkg_snapshot_history", "artifact_id", false},
	{"idx_fugue_platform_messages_artifact_id", "fugue_platform_release_messages", "artifact_id", false},
}

func (i platformHistoryFKIndex) definition(concurrently bool) string {
	prefix := "CREATE INDEX "
	if concurrently {
		prefix += "CONCURRENTLY "
	}
	s := prefix + i.name + " ON public." + i.table + " USING btree (" + i.column + ")"
	if i.nullable {
		// Equality in the FK trigger cannot match NULL references.
		s += " WHERE (" + i.column + " IS NOT NULL)"
	}
	return s
}

// MigratePlatformHistoryFKIndexes adds access paths for existing foreign-key
// checks. This never validates, removes, or changes the constraints or history.
func MigratePlatformHistoryFKIndexes(ctx context.Context, databaseURL string) error {
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
	for _, setting := range []string{
		`SET search_path = pg_catalog, public`,
		`SET maintenance_work_mem = '32MB'`,
		`SET max_parallel_maintenance_workers = 0`,
		`SET lock_timeout = '2s'`,
	} {
		if _, err := conn.ExecContext(ctx, setting); err != nil {
			return fmt.Errorf("configure platform history index migration: %w", err)
		}
	}
	if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, platformHistoryFKIndexLock); err != nil {
		return fmt.Errorf("serialize platform history index migration: %w", err)
	}
	// Session settings and the lock disappear when the dedicated DB closes.
	for _, index := range platformHistoryFKIndexes {
		ready, exists, err := inspectPlatformHistoryFKIndex(ctx, conn, index)
		if err != nil {
			return err
		}
		if ready {
			continue
		}
		if exists {
			// A canceled online build can leave a matching, unusable index.
			// Never remove a differently defined object with the same name.
			if _, err := conn.ExecContext(ctx, "DROP INDEX CONCURRENTLY public."+index.name); err != nil {
				return fmt.Errorf("drop incomplete platform history index %s: %w", index.name, err)
			}
		}
		if _, err := conn.ExecContext(ctx, index.definition(true)); err != nil {
			return fmt.Errorf("create platform history index %s: %w", index.name, err)
		}
		ready, _, err = inspectPlatformHistoryFKIndex(ctx, conn, index)
		if err != nil {
			return err
		}
		if !ready {
			return fmt.Errorf("platform history index %s is not usable", index.name)
		}
	}
	return nil
}

func inspectPlatformHistoryFKIndex(ctx context.Context, conn platformArtifactIndexQueryer, index platformHistoryFKIndex) (ready, exists bool, err error) {
	var definition sql.NullString
	err = conn.QueryRowContext(ctx, `
SELECT c.oid IS NOT NULL,
       CASE WHEN c.relkind = 'i' THEN pg_get_indexdef(c.oid) END,
       COALESCE(i.indisvalid AND i.indisready AND i.indislive, false)
FROM (SELECT to_regclass($1) AS oid) name
LEFT JOIN pg_class c ON c.oid = name.oid
LEFT JOIN pg_index i ON i.indexrelid = c.oid`, "public."+index.name).Scan(&exists, &definition, &ready)
	if err != nil {
		return false, false, fmt.Errorf("inspect platform history index %s: %w", index.name, err)
	}
	if !exists {
		return false, false, nil
	}
	if !definition.Valid || definition.String != index.definition(false) {
		return false, true, fmt.Errorf("platform history index %s has an unexpected definition; refusing replacement", index.name)
	}
	return ready, true, nil
}
