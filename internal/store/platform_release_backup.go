package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"io"
	"strings"
	"time"

	"fugue/internal/schemamigrate"
)

const PlatformReleaseBackupSchema = "fugue.platform-release-backup/v1"

// Selection is explicit and bounded. This is a snapshot of persisted release
// facts, not a signature, activation, retention decision or restore ticket.
type PlatformReleaseBackupContract struct {
	Schema          string   `json:"schema"`
	ReleaseIDs      []string `json:"release_ids"`
	MaxEncodedBytes int64    `json:"max_encoded_bytes"`
}

func (c PlatformReleaseBackupContract) Validate() error {
	if c.Schema != PlatformReleaseBackupSchema || len(c.ReleaseIDs) < 1 || len(c.ReleaseIDs) > 32 || c.MaxEncodedBytes < 1 || c.MaxEncodedBytes > 32<<20 {
		return ErrInvalidInput
	}
	seen := map[string]bool{}
	for _, id := range c.ReleaseIDs {
		if len(id) < 1 || len(id) > 256 || strings.TrimSpace(id) != id || strings.ContainsAny(id, "\x00\r\n") || seen[id] {
			return ErrInvalidInput
		}
		seen[id] = true
	}
	return nil
}

type PlatformReleaseBackup struct {
	Schema     string    `json:"schema"`
	ObservedAt time.Time `json:"observed_at"`
	Snapshot   string    `json:"database_snapshot"`
	// Rows retain all persisted columns, including raw JSON scope/evidence,
	// rather than projecting away unknown nested fields through a model type.
	Rows       []json.RawMessage `json:"rows"`
	RowsSHA256 string            `json:"rows_sha256"`
}

type PlatformReleaseBackupReport struct {
	Schema       string    `json:"schema"`
	ReadOnly     bool      `json:"read_only"`
	ObservedAt   time.Time `json:"observed_at"`
	Snapshot     string    `json:"database_snapshot"`
	Records      int       `json:"records"`
	EncodedBytes int64     `json:"encoded_bytes"`
	SHA256       string    `json:"sha256"`
	Complete     bool      `json:"complete"`
}

// Every column in the persisted release contract is included. Future columns
// require an explicit versioned update, never silent omission from a backup.
const releaseBackupColumns = `id,artifact_id,artifact_kind,scope_key,scope_json,generation,
release_channel,status,lane_key,fencing_token,version,idempotency_key,
candidate_generation,serving_unverified_generation,verified_lkg_generation,
pinned_rollback_generation,verification_state,verification_evidence_json,verified_at,
rollback_target_generation,canary_rule_ref,override_mode,override_expires_at,
bypassed_invariants_json,reason,released_by_type,released_by_id,released_at,created_at,updated_at`

// Bound unescaped field bytes before transferring/allocating a row in Go.
// One MiB of source text plus fixed scalar/field overhead fits the independent
// 8 MiB encoded-row ceiling even under JSON's worst-case sixfold escaping.
const releaseBackupSize = `1024::bigint +
octet_length(id)+octet_length(artifact_id)+octet_length(artifact_kind)+octet_length(scope_key)+octet_length(scope_json::text)+octet_length(generation)+
octet_length(release_channel)+octet_length(status)+octet_length(lane_key)+octet_length(idempotency_key)+
octet_length(candidate_generation)+octet_length(serving_unverified_generation)+octet_length(verified_lkg_generation)+
octet_length(pinned_rollback_generation)+octet_length(verification_state)+octet_length(verification_evidence_json::text)+
octet_length(rollback_target_generation)+octet_length(canary_rule_ref)+octet_length(override_mode)+
octet_length(bypassed_invariants_json::text)+octet_length(reason)+octet_length(released_by_type)+octet_length(released_by_id)`

// RunPlatformReleaseBackup uses the normal reader barrier and one read-only
// repeatable-read snapshot. Missing or oversized exact records fail the whole
// selection. No body is emitted until commit and release of the DB barrier.
func RunPlatformReleaseBackup(ctx context.Context, databaseURL string, c PlatformReleaseBackupContract, out io.Writer) (PlatformReleaseBackupReport, error) {
	r := PlatformReleaseBackupReport{Schema: PlatformReleaseBackupSchema, ReadOnly: true}
	if c.Validate() != nil || out == nil {
		return r, ErrInvalidInput
	}
	db, err := openPostgresDatabase(databaseURL)
	if err != nil {
		return r, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return runPlatformReleaseBackup(ctx, db, c, out)
}

func runPlatformReleaseBackup(ctx context.Context, db *sql.DB, c PlatformReleaseBackupContract, out io.Writer) (r PlatformReleaseBackupReport, result error) {
	r = PlatformReleaseBackupReport{Schema: PlatformReleaseBackupSchema, ReadOnly: true}
	if c.Validate() != nil || out == nil {
		return r, ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	s := &Store{db: db, compilerInputArchiveReadLeaseEnabled: true}
	tx, finish, err := s.BeginCompilerInputReadLease(ctx)
	if err != nil {
		return r, err
	}
	defer finish()
	for _, statement := range []string{`SET LOCAL statement_timeout='5s'`, `SET LOCAL lock_timeout='250ms'`, `SET LOCAL work_mem='1MB'`, `SET LOCAL max_parallel_workers_per_gather=0`, `SET LOCAL jit=off`} {
		if _, err = tx.ExecContext(ctx, statement); err != nil {
			return r, err
		}
	}
	if err = schemamigrate.EnforceCompilerInputZeroTemp(ctx, tx); err != nil {
		return r, err
	}
	var coherent bool
	if err = tx.QueryRowContext(ctx, `SELECT transaction_timestamp(),pg_current_snapshot()::text,current_setting('transaction_isolation')='repeatable read' AND current_setting('transaction_read_only')='on'`).Scan(&r.ObservedAt, &r.Snapshot, &coherent); err != nil {
		return r, err
	}
	if !coherent {
		return r, ErrConflict
	}
	var extraColumns int
	columns := strings.Split(strings.ReplaceAll(releaseBackupColumns, "\n", ""), ",")
	if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM pg_attribute WHERE attrelid='fugue_platform_artifact_releases'::regclass AND attnum>0 AND NOT attisdropped AND NOT (attname=ANY($1::text[]))`, columns).Scan(&extraColumns); err != nil {
		return r, err
	}
	if extraColumns != 0 {
		return r, ErrConflict
	}
	b := PlatformReleaseBackup{Schema: c.Schema, ObservedAt: r.ObservedAt, Snapshot: r.Snapshot}
	var total int64
	for _, id := range c.ReleaseIDs {
		var raw sql.NullString
		var size sql.NullInt64
		query := `SELECT CASE WHEN (` + releaseBackupSize + `)<=$2 THEN row_to_json(r)::text END, (` + releaseBackupSize + `) FROM (SELECT ` + releaseBackupColumns + ` FROM fugue_platform_artifact_releases WHERE id=$1) r`
		if err = tx.QueryRowContext(ctx, query, id, 1<<20).Scan(&raw, &size); err != nil {
			return r, mapDBErr(err)
		}
		if !raw.Valid || !size.Valid || size.Int64 > 1<<20 || len(raw.String) > 8<<20 {
			return r, errSharedInputBudget
		}
		row, err := canonicalReleaseBackupRow([]byte(raw.String), id, false)
		if err != nil {
			return r, err
		}
		total += int64(len(row))
		if total > c.MaxEncodedBytes {
			return r, errSharedInputBudget
		}
		b.Rows = append(b.Rows, row)
	}
	rows, err := json.Marshal(b.Rows)
	if err != nil {
		return r, err
	}
	b.RowsSHA256 = sharedHash(sha256.Sum256(rows))
	wire, err := json.Marshal(b)
	if err != nil {
		return r, err
	}
	wire = append(wire, '\n')
	if int64(len(wire)) > c.MaxEncodedBytes {
		return r, errSharedInputBudget
	}
	if err = tx.Commit(); err != nil {
		return r, err
	}
	finish() // A slow or canceled receiver cannot retain the source DB lease.
	type written struct {
		n   int
		err error
	}
	done := make(chan written, 1)
	go func() { n, err := out.Write(wire); done <- written{n, err} }()
	select {
	case <-ctx.Done():
		return r, ctx.Err()
	case v := <-done:
		if v.err != nil {
			return r, v.err
		}
		if v.n != len(wire) {
			return r, io.ErrShortWrite
		}
	}
	r.Records, r.EncodedBytes, r.SHA256, r.Complete = len(b.Rows), int64(len(wire)), sharedHash(sha256.Sum256(wire)), true
	return r, nil
}

func canonicalReleaseBackupRow(raw []byte, id string, requireCanonical bool) ([]byte, error) {
	if len(raw) == 0 || len(raw) > 8<<20 {
		return nil, errSharedInputBudget
	}
	var fields map[string]any
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if d.Decode(&fields) != nil || d.Decode(new(any)) != io.EOF || fields["id"] != id {
		return nil, ErrConflict
	}
	columns := strings.Split(strings.ReplaceAll(releaseBackupColumns, "\n", ""), ",")
	if len(fields) != len(columns) {
		return nil, ErrConflict
	}
	for _, key := range columns {
		if _, ok := fields[key]; !ok {
			return nil, ErrConflict
		}
	}
	canonical, err := json.Marshal(fields)
	if err != nil || len(canonical) > 8<<20 {
		return nil, errSharedInputBudget
	}
	if requireCanonical && !bytes.Equal(canonical, raw) {
		return nil, ErrConflict
	}
	return canonical, nil
}

// DecodePlatformReleaseBackup verifies the complete bounded transport and
// exact selection offline. A checksum is not source authority or permission
// to activate these historical facts. Rows keep the persisted column names.
func DecodePlatformReleaseBackup(in io.Reader, c PlatformReleaseBackupContract) (PlatformReleaseBackup, error) {
	var b PlatformReleaseBackup
	if c.Validate() != nil || in == nil {
		return b, ErrInvalidInput
	}
	wire, err := io.ReadAll(io.LimitReader(in, c.MaxEncodedBytes+1))
	if err != nil {
		return b, err
	}
	if len(wire) == 0 || int64(len(wire)) > c.MaxEncodedBytes || wire[len(wire)-1] != '\n' {
		return b, ErrConflict
	}
	d := json.NewDecoder(bytes.NewReader(wire))
	d.DisallowUnknownFields()
	if d.Decode(&b) != nil || d.Decode(new(any)) != io.EOF {
		return PlatformReleaseBackup{}, ErrConflict
	}
	canonical, err := json.Marshal(b)
	if err != nil || !bytes.Equal(append(canonical, '\n'), wire) || b.Schema != c.Schema || b.ObservedAt.IsZero() || b.Snapshot == "" || len(b.Rows) != len(c.ReleaseIDs) {
		return PlatformReleaseBackup{}, ErrConflict
	}
	for i, row := range b.Rows {
		if _, err = canonicalReleaseBackupRow(row, c.ReleaseIDs[i], true); err != nil {
			return PlatformReleaseBackup{}, err
		}
	}
	rows, err := json.Marshal(b.Rows)
	if err != nil || sharedHash(sha256.Sum256(rows)) != b.RowsSHA256 {
		return PlatformReleaseBackup{}, ErrConflict
	}
	return b, nil
}
