package schemamigrate

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
)

const EdgeQualityRollupWriterCheckpoint = "edge-quality-rollup-writer/v1"

// EnsureEdgeQualityRollupOwnerProjection is a provisioning operation for the
// database schema owner. The observation worker never runs it, and only its
// single-row projection (not the underlying metadata table) is granted.
func EnsureEdgeQualityRollupOwnerProjection(ctx context.Context, db *sql.DB) error {
	digest := sha256.Sum256([]byte(EdgeQualityRollupWriterCheckpoint))
	statement := fmt.Sprintf(`CREATE OR REPLACE VIEW fugue_edge_quality_rollup_owner
WITH (security_barrier = true) AS
SELECT value AS owner
FROM fugue_meta
WHERE key = 'observation/v1/%x'`, digest)
	_, err := db.ExecContext(ctx, statement)
	return err
}
