package productioncontrol

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
)

const recoveryDomain = "fugue.production.journal-recovery.v1"

var snapshotPattern = regexp.MustCompile(`^[0-9A-F]{8}-[0-9A-F]{8}-[1-9][0-9]{0,9}$`)

type recoveryTable struct {
	name string
	rows string
}

// Keep this inventory exhaustive. A new journal table or a change to the
// canonical row representation needs an explicit recovery format migration.
var recoveryTables = [...]recoveryTable{
	{"approvals", `ORDER BY digest COLLATE pg_catalog."C"`},
	{"audit", `ORDER BY kind COLLATE pg_catalog."C", subject_digest COLLATE pg_catalog."C"`},
	{"dns_cutovers", `ORDER BY cluster_id COLLATE pg_catalog."C", phase COLLATE pg_catalog."C"`},
	{"execution_claims", `ORDER BY cluster_id COLLATE pg_catalog."C", resource_id COLLATE pg_catalog."C", fence`},
	{"execution_launches", `ORDER BY intent_digest COLLATE pg_catalog."C"`},
	{"execution_results", `ORDER BY intent_digest COLLATE pg_catalog."C"`},
	{"intents", `ORDER BY digest COLLATE pg_catalog."C"`},
	{"participation_topologies", `ORDER BY cluster_id COLLATE pg_catalog."C", generation`},
	{"resource_baselines", `ORDER BY cluster_id COLLATE pg_catalog."C", resource_id COLLATE pg_catalog."C"`},
}

type JournalRecoveryTable struct {
	Name   string `json:"name"`
	Rows   int64  `json:"rows"`
	Digest string `json:"digest"`
}

type JournalRecoveryCheckpoint struct {
	Format  string                 `json:"format"`
	Cluster string                 `json:"cluster"`
	Tables  []JournalRecoveryTable `json:"tables"`
	Digest  string                 `json:"digest"`
}

// RecoveryCheckpoint fingerprints the entire production journal within one
// read-only MVCC snapshot. For a logical backup, supply the same exported
// PostgreSQL snapshot used by pg_dump. This proves data identity only.
func (j *Journal) RecoveryCheckpoint(ctx context.Context, cluster, snapshot string) (JournalRecoveryCheckpoint, error) {
	if !tokenPattern.MatchString(cluster) || (snapshot != "" && !snapshotPattern.MatchString(snapshot)) {
		return JournalRecoveryCheckpoint{}, errors.New("invalid recovery cluster or exported PostgreSQL snapshot")
	}
	tx, err := j.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return JournalRecoveryCheckpoint{}, fmt.Errorf("open read-only recovery snapshot: %w", err)
	}
	defer tx.Rollback()
	if snapshot != "" {
		// PostgreSQL's SET TRANSACTION SNAPSHOT requires a literal. The
		// validated snapshot identifier contains only hex, digits and hyphens.
		if _, err := tx.ExecContext(ctx, `SET TRANSACTION SNAPSHOT '`+snapshot+`'`); err != nil {
			return JournalRecoveryCheckpoint{}, fmt.Errorf("import PostgreSQL recovery snapshot: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `SET LOCAL search_path TO pg_catalog; SET LOCAL TimeZone TO 'UTC';
		SET LOCAL DateStyle TO 'ISO, YMD'; SET LOCAL bytea_output TO 'hex'; SET LOCAL row_security TO off;
		SET LOCAL statement_timeout TO '4min';`); err != nil {
		return JournalRecoveryCheckpoint{}, fmt.Errorf("configure recovery snapshot: %w", err)
	}
	if err := verifyRecoveryTableInventory(ctx, tx); err != nil {
		return JournalRecoveryCheckpoint{}, err
	}

	checkpoint := JournalRecoveryCheckpoint{Format: recoveryDomain, Cluster: cluster}
	for _, table := range recoveryTables {
		sum, err := readRecoveryTable(ctx, tx, table)
		if err != nil {
			return JournalRecoveryCheckpoint{}, fmt.Errorf("fingerprint recovery table %s: %w", table.name, err)
		}
		checkpoint.Tables = append(checkpoint.Tables, sum)
	}
	binding := struct {
		Cluster string                 `json:"cluster"`
		Tables  []JournalRecoveryTable `json:"tables"`
	}{Cluster: cluster, Tables: checkpoint.Tables}
	encoded, err := signedBytes(recoveryDomain, binding)
	if err != nil {
		return JournalRecoveryCheckpoint{}, err
	}
	checkpoint.Digest = digest(encoded)
	if err := tx.Commit(); err != nil {
		return JournalRecoveryCheckpoint{}, fmt.Errorf("finish recovery snapshot: %w", err)
	}
	return checkpoint, nil
}

func verifyRecoveryTableInventory(ctx context.Context, tx *sql.Tx) error {
	rows, err := tx.QueryContext(ctx, `SELECT c.relname, c.relkind = 'r' AND c.relpersistence = 'p'
		AND NOT EXISTS (SELECT FROM pg_catalog.pg_inherits i WHERE c.oid IN (i.inhparent, i.inhrelid))
		FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
		WHERE n.nspname='fugue_control' AND c.relkind IN ('r','p','f','v','m','S')
		ORDER BY c.relname::text COLLATE pg_catalog."C"`)
	if err != nil {
		return err
	}
	defer rows.Close()
	var index int
	for rows.Next() {
		var name string
		var permanentTable bool
		if err := rows.Scan(&name, &permanentTable); err != nil {
			return err
		}
		if index >= len(recoveryTables) || name != recoveryTables[index].name || !permanentTable {
			return errors.New("production journal recovery table inventory does not match checkpoint format")
		}
		index++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if index != len(recoveryTables) {
		return errors.New("production journal recovery table inventory is incomplete")
	}
	return nil
}

func readRecoveryTable(ctx context.Context, tx *sql.Tx, table recoveryTable) (JournalRecoveryTable, error) {
	rows, err := tx.QueryContext(ctx, `SELECT pg_catalog.to_jsonb(r)::text
		FROM ONLY fugue_control.`+table.name+` AS r `+table.rows)
	if err != nil {
		return JournalRecoveryTable{}, err
	}
	defer rows.Close()
	result := JournalRecoveryTable{Name: table.name}
	hash := sha256.New()
	for rows.Next() {
		var row []byte
		if err := rows.Scan(&row); err != nil {
			return JournalRecoveryTable{}, err
		}
		// JSON escapes embedded line endings, so each newline delimits
		// exactly one canonical row including its nulls and binary values.
		_, _ = hash.Write(row)
		_, _ = hash.Write([]byte{'\n'})
		result.Rows++
	}
	if err := rows.Err(); err != nil {
		return JournalRecoveryTable{}, err
	}
	result.Digest = "sha256:" + hex.EncodeToString(hash.Sum(nil))
	return result, nil
}

// VerifyRecoveryCheckpoint compares the restored data with a digest retained
// outside the backup. It cannot issue or recover production execution rights.
func (j *Journal) VerifyRecoveryCheckpoint(ctx context.Context, cluster, expected string) (JournalRecoveryCheckpoint, error) {
	if !digestPattern.MatchString(expected) {
		return JournalRecoveryCheckpoint{}, errors.New("recovery requires a pinned SHA-256 checkpoint digest")
	}
	checkpoint, err := j.RecoveryCheckpoint(ctx, cluster, "")
	if err != nil {
		return JournalRecoveryCheckpoint{}, err
	}
	if checkpoint.Digest != expected {
		return JournalRecoveryCheckpoint{}, errors.New("restored production journal differs from the pinned checkpoint")
	}
	return checkpoint, nil
}
