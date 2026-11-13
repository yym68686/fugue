package store

import (
	"context"
	"fugue/internal/platformconfig"
)

// Construct historical persisted bytes explicitly. Production writers never
// create this representation; compatibility tests still need authentic old
// data to exercise archive reads, pin transfer and old-schema recovery.
func (s *Store) ensureLegacyCompilerInputFixture(snapshot platformconfig.RuntimeSnapshot, digest string) error {
	var exists, fenced bool
	if err := s.db.QueryRow(`SELECT EXISTS(SELECT 1 FROM fugue_platform_input_manifests WHERE content_hash=$1)`, digest).Scan(&exists); err != nil {
		return err
	}
	if err := s.db.QueryRow(`SELECT writes_fenced FROM fugue_platform_input_archive_control WHERE singleton`).Scan(&fenced); err != nil {
		return err
	}
	if err := s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
		return err
	}
	if exists || fenced {
		return nil
	}
	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, statement := range []string{
		`INSERT INTO fugue_platform_input_chunks SELECT * FROM fugue_platform_input_hot_chunks WHERE content_hash=$1 ON CONFLICT DO NOTHING`,
		`DELETE FROM fugue_platform_input_hot_chunks WHERE content_hash=$1`,
		`UPDATE fugue_platform_input_manifests SET storage_location='legacy' WHERE content_hash=$1 AND format IN ('runtime-snapshot-json-v1','runtime-snapshot-json-chunk-codec-v2')`,
	} {
		if _, err := tx.Exec(statement, digest); err != nil {
			return err
		}
	}
	return tx.Commit()
}
