package store

import (
	"context"
	"database/sql"
	"time"
)

// Aggregate wall-clock measurements, not PostgreSQL server execution times.
// Query return and Next can both include server work and transport. Never
// serialize SQL, identities, driver errors, connection strings or content.
type CompilerInputHotReadCost struct {
	Queries        int64 `json:"queries"`
	Rows           int64 `json:"rows"`
	QueryReturnUS  int64 `json:"query_return_us"`
	RowsNextUS     int64 `json:"rows_next_us"`
	DecodeUS       int64 `json:"decode_us"`
	SourceBytes    int64 `json:"source_bytes"`
	TransportBytes int64 `json:"transport_bytes"`
}

type CompilerInputHotCost struct {
	RootInventoryUS int64                    `json:"root_inventory_us"`
	TraversalUS     int64                    `json:"traversal_us"`
	ResolverUS      int64                    `json:"resolver_us"`
	Artifacts       CompilerInputHotReadCost `json:"artifacts"`
	Inputs          CompilerInputHotReadCost `json:"inputs"`
	Ledger          CompilerInputHotReadCost `json:"ledger"`
}

func queryCompilerInputHotRows(ctx context.Context, db inputQueryer, cost *CompilerInputHotReadCost, query string, args ...any) (*sql.Rows, error) {
	if cost == nil {
		return db.QueryContext(ctx, query, args...)
	}
	started := time.Now()
	rows, err := db.QueryContext(ctx, query, args...)
	cost.Queries++
	cost.QueryReturnUS += time.Since(started).Microseconds()
	return rows, err
}

func nextCompilerInputHotRow(rows *sql.Rows, cost *CompilerInputHotReadCost) bool {
	if cost == nil {
		return rows.Next()
	}
	started := time.Now()
	next := rows.Next()
	cost.RowsNextUS += time.Since(started).Microseconds()
	if next {
		cost.Rows++
	}
	return next
}
