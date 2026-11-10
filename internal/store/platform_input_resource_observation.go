package store

import (
	"context"
	"database/sql"
	"time"
)

// PendingRootDatabaseResources is a read-only sample, not a reservation or a
// whole-operation bound. WAL counters are cluster-wide; temp counters belong to
// the database and can reset independently. Never attribute their deltas to one
// operation or turn a sample into Proven=true.
type PendingRootDatabaseResources struct {
	ObservedAt         time.Time                     `json:"observed_at"`
	Database           string                        `json:"database"`
	DatabaseOID        uint32                        `json:"database_oid"`
	Schema             string                        `json:"schema"`
	ServerAddress      string                        `json:"server_address"`
	ServerPort         int                           `json:"server_port"`
	PostmasterStarted  time.Time                     `json:"postmaster_started_at"`
	DataDirectory      string                        `json:"-"` // may be hidden from an ordinary role
	InRecovery         bool                          `json:"in_recovery"`
	ReadOnly           bool                          `json:"read_only"`
	TempFilesForbidden bool                          `json:"sample_temp_files_forbidden"`
	DatabaseBytes      uint64                        `json:"database_bytes"`
	TempBytes          uint64                        `json:"database_temp_bytes"`
	TempFiles          uint64                        `json:"database_temp_files"`
	DatabaseStatsReset *time.Time                    `json:"database_stats_reset"`
	WALBytes           string                        `json:"cluster_wal_bytes"`
	WALStatsReset      time.Time                     `json:"cluster_wal_stats_reset"`
	LedgerRelations    []PendingRootResourceRelation `json:"ledger_relations"`
}

type PendingRootResourceRelation struct {
	Name           string `json:"name"`
	OID            uint32 `json:"oid"`
	Filenode       uint32 `json:"filenode"`
	AllocatedBytes uint64 `json:"allocated_bytes"`
}

// ReadPendingRootDatabaseResources uses the Store's actual connection pool and
// does not call Init, install schema, create a pool, or read any input body.
// The caller owns the encompassing cross-source deadline.
func (s *Store) ReadPendingRootDatabaseResources(ctx context.Context) (PendingRootDatabaseResources, error) {
	var out PendingRootDatabaseResources
	if s == nil || s.db == nil || ctx == nil || ctx.Err() != nil {
		return out, ErrConflict
	}
	tx, err := beginPendingRootPolicyTx(ctx, s.db, true)
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	var reset sql.NullTime
	err = tx.QueryRowContext(ctx, `SELECT clock_timestamp(),current_database(),d.oid,current_schema(),
 COALESCE(host(inet_server_addr()),''),COALESCE(inet_server_port(),0),pg_postmaster_start_time(),
 COALESCE((SELECT setting FROM pg_settings WHERE name='data_directory'),''),pg_is_in_recovery(),current_setting('transaction_read_only')='on',
 current_setting('temp_file_limit')='0',pg_database_size(d.oid),s.temp_bytes,s.temp_files,s.stats_reset,
 w.wal_bytes::text,w.stats_reset
 FROM pg_database d JOIN pg_stat_database s ON s.datid=d.oid CROSS JOIN pg_stat_wal w
 WHERE d.datname=current_database()`).Scan(&out.ObservedAt, &out.Database, &out.DatabaseOID, &out.Schema,
		&out.ServerAddress, &out.ServerPort, &out.PostmasterStarted, &out.DataDirectory, &out.InRecovery,
		&out.ReadOnly, &out.TempFilesForbidden, &out.DatabaseBytes, &out.TempBytes, &out.TempFiles,
		&reset, &out.WALBytes, &out.WALStatsReset)
	if err != nil {
		return out, err
	}
	if reset.Valid {
		out.DatabaseStatsReset = &reset.Time
	}
	if !out.ReadOnly || !out.TempFilesForbidden || out.DatabaseOID == 0 || out.Schema == "" || out.InRecovery {
		return out, ErrConflict
	}
	// Point lookups only. An absent ledger is observable without installing it;
	// relation identity and allocation include indexes/TOAST when present.
	out.LedgerRelations = make([]PendingRootResourceRelation, 0, 4)
	for _, name := range resourceRelations() {
		var oid, filenode sql.NullInt64
		var allocated sql.NullInt64
		if err := tx.QueryRowContext(ctx, `SELECT c.oid,pg_relation_filenode(c.oid),pg_total_relation_size(c.oid)
 FROM (SELECT to_regclass(quote_ident(current_schema())||'.'||quote_ident($1)) AS oid) r
 LEFT JOIN pg_class c ON c.oid=r.oid`, name).Scan(&oid, &filenode, &allocated); err != nil {
			return out, err
		}
		row := PendingRootResourceRelation{Name: name}
		if oid.Valid && filenode.Valid && allocated.Valid && oid.Int64 > 0 && filenode.Int64 > 0 && allocated.Int64 >= 0 {
			row.OID, row.Filenode, row.AllocatedBytes = uint32(oid.Int64), uint32(filenode.Int64), uint64(allocated.Int64)
		} else if oid.Valid || filenode.Valid || allocated.Valid {
			return out, ErrConflict
		}
		out.LedgerRelations = append(out.LedgerRelations, row)
	}
	if err := tx.Commit(); err != nil {
		return out, err
	}
	return out, nil
}
