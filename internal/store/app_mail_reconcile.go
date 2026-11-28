package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"fugue/internal/model"
)

const appMailReconcileTableSQL = `CREATE TABLE IF NOT EXISTS fugue_app_mail_reconcile (
	app_id TEXT PRIMARY KEY REFERENCES fugue_app_mail_bindings(app_id) ON DELETE CASCADE,
	generation BIGINT NOT NULL CHECK(generation > 0),
	next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count >= 0),
	last_success_at TIMESTAMPTZ,
	last_result TEXT NOT NULL DEFAULT '' CHECK(last_result IN ('','synced','provider_unavailable','metadata_unavailable','desired_changed')),
	lease_id TEXT,
	lease_expires_at TIMESTAMPTZ,
	CHECK ((lease_id IS NULL) = (lease_expires_at IS NULL))
)`

const appMailReconcileIndexSQL = `CREATE INDEX IF NOT EXISTS idx_fugue_app_mail_reconcile_due
	ON fugue_app_mail_reconcile(next_attempt_at, app_id)`

// Enqueue in the same transaction as intent, including writes from still
// running older API binaries. DNS-only updates are not new lifecycle intent.
// A new generation remains due but does not steal an in-flight lease.
const appMailReconcileEnqueueSQL = `CREATE OR REPLACE FUNCTION fugue_enqueue_app_mail_reconcile()
	RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
	BEGIN
		INSERT INTO fugue_app_mail_reconcile(app_id,generation) VALUES(NEW.app_id,NEW.generation)
		ON CONFLICT(app_id) DO UPDATE SET generation=EXCLUDED.generation,next_attempt_at=now(),
			attempt_count=0,last_success_at=NULL,last_result=''
		WHERE fugue_app_mail_reconcile.generation < EXCLUDED.generation;
		RETURN NULL;
	END
	$$`

const appMailReconcileTriggerSQL = `CREATE OR REPLACE TRIGGER fugue_app_mail_reconcile_enqueue
	AFTER INSERT OR UPDATE OF generation ON fugue_app_mail_bindings
	FOR EACH ROW EXECUTE FUNCTION fugue_enqueue_app_mail_reconcile()`

// This key is not set by schema bootstrap or either component. The production
// handoff must explicitly settle both live and rollback revisions first.
const appMailReconcileOwnerKey = "app-mail-reconcile/owner/v1"
const appMailLegacyPeriodicLock = "app-mail-reconcile"
const appMailReconcileSeedKey = "app-mail-reconcile/seed/v1"

type AppMailReconcileLease struct {
	Binding   model.AppMailBinding
	ID        string
	ExpiresAt time.Time
}

type AppMailReconcileResult string

const (
	AppMailReconcileSynced              AppMailReconcileResult = "synced"
	AppMailReconcileProviderUnavailable AppMailReconcileResult = "provider_unavailable"
	AppMailReconcileMetadataUnavailable AppMailReconcileResult = "metadata_unavailable"
	AppMailReconcileDesiredChanged      AppMailReconcileResult = "desired_changed"
)

type AppMailReconcileStatus struct {
	Generation    uint64
	NextAttemptAt time.Time
	AttemptCount  int
	LastSuccessAt *time.Time
	LastResult    AppMailReconcileResult
}

type AppMailReconcileControl struct {
	Owner  string
	Seeded bool
}

type appMailReconcileSeed struct {
	After    string `json:"after,omitempty"`
	Complete bool   `json:"complete"`
}

// AppMailStandbyWitness selects at most one existing live binding. It never
// claims work, changes the binding, or returns credentials.
func (s *Store) AppMailStandbyWitness(ctx context.Context) (model.AppMailBinding, error) {
	var binding model.AppMailBinding
	var appID string
	var generation uint64
	var raw []byte
	if err := s.db.QueryRowContext(ctx, `SELECT app_id,generation,body
		FROM fugue_app_mail_bindings WHERE body->>'state'='active'
		ORDER BY app_id LIMIT 1`).Scan(&appID, &generation, &raw); err != nil {
		return binding, mapDBErr(err)
	}
	if err := json.Unmarshal(raw, &binding); err != nil {
		return binding, err
	}
	if binding.AppID != appID || binding.Generation != generation || binding.State != "active" {
		return model.AppMailBinding{}, ErrConflict
	}
	return binding, nil
}

// PrepareAppMailReconciliation advances at most one bounded initialization
// page. Its database cursor and work entries commit together; leader changes
// or cancellation cannot skip unfinished bindings. This never sets the owner.
func (s *Store) PrepareAppMailReconciliation(ctx context.Context) (AppMailReconcileControl, error) {
	control, seed, err := readAppMailReconcileControl(ctx, s.db)
	if err != nil || seed.Complete {
		return control, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return control, err
	}
	defer tx.Rollback()
	var acquired bool
	err = tx.QueryRowContext(ctx, `SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))`, appMailReconcileSeedKey).Scan(&acquired)
	if err != nil || !acquired {
		return control, err
	}
	control, seed, err = readAppMailReconcileControl(ctx, tx)
	if err != nil || seed.Complete {
		return control, err
	}
	seed.After, err = seedAppMailReconciliation(ctx, tx, seed.After, 100)
	if err != nil {
		return control, err
	}
	seed.Complete = seed.After == ""
	raw, err := json.Marshal(seed)
	if err != nil {
		return control, err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO fugue_meta(key,value) VALUES($1,$2)
		ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value,updated_at=now()`, appMailReconcileSeedKey, string(raw))
	if err != nil {
		return control, err
	}
	if err = tx.Commit(); err != nil {
		return control, err
	}
	control.Seeded = seed.Complete
	return control, nil
}

func readAppMailReconcileControl(ctx context.Context, db appMailReconcileQueryer) (AppMailReconcileControl, appMailReconcileSeed, error) {
	var control AppMailReconcileControl
	var seed appMailReconcileSeed
	var raw string
	var trigger bool
	err := db.QueryRowContext(ctx, `
		SELECT COALESCE((SELECT value FROM fugue_meta WHERE key=$1),'api'),
			COALESCE((SELECT value FROM fugue_meta WHERE key=$2),''),
			EXISTS (
				SELECT 1 FROM pg_trigger
				WHERE tgrelid='fugue_app_mail_bindings'::regclass
					AND tgname='fugue_app_mail_reconcile_enqueue' AND tgenabled IN ('O','A')
					AND tgfoid='fugue_enqueue_app_mail_reconcile()'::regprocedure
			)`, appMailReconcileOwnerKey, appMailReconcileSeedKey).Scan(&control.Owner, &raw, &trigger)
	if err != nil {
		return AppMailReconcileControl{}, seed, err
	}
	if !trigger || (control.Owner != "api" && control.Owner != "controller") {
		return AppMailReconcileControl{}, seed, errors.New("mail reconciliation owner or enqueue trigger is invalid")
	}
	if raw != "" && (json.Unmarshal([]byte(raw), &seed) != nil || seed.Complete == (seed.After != "")) {
		return AppMailReconcileControl{}, seed, errors.New("mail reconciliation seed checkpoint is invalid")
	}
	control.Seeded = seed.Complete
	return control, seed, nil
}

// WithLegacyAppMailReconciliation gates both the periodic legacy writer and
// every foreground binding PUT. The transaction keeps the shared handoff
// fence from releasing merely because the HTTP caller disconnected; the
// caller still cancels its own bounded request. Transfer takes the exclusive
// lock in the same transaction as setting the owner. The separate periodic
// lock is the exact PostgreSQL key taken by pre-compatibility API binaries.
func (s *Store) WithLegacyAppMailReconciliation(ctx context.Context, periodic bool, fn func() error) (bool, error) {
	if fn == nil {
		return false, ErrInvalidInput
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	fenceCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 45*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(fenceCtx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(fenceCtx, `SET LOCAL lock_timeout='2s'`); err != nil {
		return false, err
	}
	if periodic {
		var acquired bool
		err = tx.QueryRowContext(fenceCtx, `SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))`, appMailLegacyPeriodicLock).Scan(&acquired)
		if err != nil || !acquired {
			return false, err
		}
	}
	if _, err = tx.ExecContext(fenceCtx, `SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0))`, appMailReconcileOwnerKey); err != nil {
		return false, err
	}
	// This is a subsequent READ COMMITTED statement: a reader admitted
	// behind the exclusive transition must see the newly committed owner.
	var owner string
	if err = tx.QueryRowContext(fenceCtx, `SELECT COALESCE((SELECT value FROM fugue_meta WHERE key=$1),'api')`, appMailReconcileOwnerKey).Scan(&owner); err != nil {
		return false, err
	}
	switch owner {
	case "controller":
		return false, nil
	case "api":
	default:
		return false, errors.New("unknown app mail reconciliation owner")
	}
	if err = ctx.Err(); err != nil {
		return false, err
	}
	if err = fn(); err != nil {
		return true, err
	}
	return true, tx.Commit()
}

// SeedAppMailReconciliation installs work for pre-existing bindings in
// bounded pages. A caller checkpoints the returned cursor between pages.
// A concurrent generation change wins through the enqueue trigger.
func (s *Store) SeedAppMailReconciliation(ctx context.Context, after string, limit int) (string, error) {
	return seedAppMailReconciliation(ctx, s.db, after, limit)
}

func seedAppMailReconciliation(ctx context.Context, db appMailReconcileQueryer, after string, limit int) (string, error) {
	if limit < 1 || limit > 100 {
		return after, ErrInvalidInput
	}
	var cursor string
	err := db.QueryRowContext(ctx, `
		WITH page AS MATERIALIZED (
			SELECT app_id,generation FROM fugue_app_mail_bindings WHERE app_id>$1 ORDER BY app_id LIMIT $2
		), seeded AS (
			INSERT INTO fugue_app_mail_reconcile(app_id,generation)
			SELECT app_id,generation FROM page ON CONFLICT(app_id) DO NOTHING
		)
		SELECT CASE WHEN count(*)=$2 THEN max(app_id) ELSE '' END FROM page`, after, limit).Scan(&cursor)
	if err != nil {
		return after, err
	}
	return cursor, nil
}

// ClaimAppMailReconciliation takes one due generation using the database
// clock. The executor must use a deadline safely inside this one-minute
// lease and Mailflare must fence replay after an ambiguous HTTP result.
func (s *Store) ClaimAppMailReconciliation(ctx context.Context) (AppMailReconcileLease, bool, error) {
	id := model.NewID("maillease")
	var out AppMailReconcileLease
	var raw []byte
	var appID string
	var generation uint64
	err := s.db.QueryRowContext(ctx, `
		WITH due AS MATERIALIZED (
			SELECT work.app_id,work.generation,binding.body
			FROM fugue_app_mail_reconcile work
			JOIN fugue_app_mail_bindings binding
				ON binding.app_id=work.app_id AND binding.generation=work.generation
			WHERE work.next_attempt_at <= statement_timestamp()
				AND (work.lease_expires_at IS NULL OR work.lease_expires_at <= statement_timestamp())
				AND EXISTS (SELECT 1 FROM fugue_meta WHERE key=$1 AND value='controller')
			ORDER BY work.next_attempt_at,work.app_id LIMIT 1 FOR UPDATE OF work SKIP LOCKED
		)
		UPDATE fugue_app_mail_reconcile work SET lease_id=$2,lease_expires_at=statement_timestamp()+interval '1 minute'
		FROM due WHERE work.app_id=due.app_id
		RETURNING due.app_id,due.body,due.generation,work.lease_expires_at`, appMailReconcileOwnerKey, id).Scan(&appID, &raw, &generation, &out.ExpiresAt)
	if errors.Is(err, sql.ErrNoRows) {
		return out, false, nil
	}
	if err != nil {
		return out, false, err
	}
	out.ID = id
	if err = json.Unmarshal(raw, &out.Binding); err != nil {
		return out, false, err
	}
	if out.Binding.AppID != appID || out.Binding.Generation != generation {
		return out, false, errors.New("mail reconciliation binding does not match generation")
	}
	return out, true, nil
}

const requireAppMailReconcileLeaseSQL = `
	SELECT true FROM fugue_app_mail_reconcile
	WHERE app_id=$1 AND generation=$2 AND lease_id=$3 AND lease_expires_at>clock_timestamp()
		AND EXISTS (SELECT 1 FROM fugue_meta WHERE key=$4 AND value='controller')`

type appMailReconcileQueryer interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func requireAppMailReconciliationLease(ctx context.Context, db appMailReconcileQueryer, query string, lease AppMailReconcileLease) error {
	if lease.ID == "" || lease.Binding.AppID == "" || lease.Binding.Generation == 0 {
		return ErrInvalidInput
	}
	var valid bool
	err := db.QueryRowContext(ctx, query, lease.Binding.AppID, lease.Binding.Generation, lease.ID, appMailReconcileOwnerKey).Scan(&valid)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrConflict
	}
	return err
}

func lockAppMailReconciliationLease(ctx context.Context, tx *sql.Tx, lease AppMailReconcileLease) error {
	// Keep the work row locked in the transaction that commits a binding or
	// its DNS observation. A replacement claim cannot result in two writers.
	return requireAppMailReconciliationLease(ctx, tx, requireAppMailReconcileLeaseSQL+` FOR UPDATE`, lease)
}

// CheckAppMailReconciliationLease is the last local check before starting a
// bounded provider request. Mailflare still fences ambiguous network effects.
func (s *Store) CheckAppMailReconciliationLease(ctx context.Context, lease AppMailReconcileLease) error {
	return requireAppMailReconciliationLease(ctx, s.db, requireAppMailReconcileLeaseSQL, lease)
}

// RevokeAppMailReconciliation commits a tombstone only with the exact current
// claim. The generation trigger schedules its provider PUT as new work.
func (s *Store) RevokeAppMailReconciliation(ctx context.Context, lease AppMailReconcileLease) error {
	binding := lease.Binding
	binding.State = "revoked"
	_, err := s.saveAppMailBinding(ctx, binding, lease.Binding.Generation, &lease)
	return err
}

func (s *Store) SaveAppMailReconciliationDNS(ctx context.Context, lease AppMailReconcileLease, records []model.AppMailDNSRecord) error {
	return s.saveAppMailProviderDNS(ctx, lease.Binding, records, &lease)
}

// FinishAppMailReconciliation never records progress for a superseded or
// expired lease. It releases a still-owned lease even if a new generation
// arrived in flight; the new generation was already enqueued atomically.
// Persist only bounded result codes, never provider response bodies, tokens,
// or one-time credential results.
func (s *Store) FinishAppMailReconciliation(ctx context.Context, lease AppMailReconcileLease, result AppMailReconcileResult) (bool, error) {
	switch result {
	case AppMailReconcileSynced, AppMailReconcileProviderUnavailable, AppMailReconcileMetadataUnavailable, AppMailReconcileDesiredChanged:
	default:
		return false, ErrInvalidInput
	}
	if lease.ID == "" || lease.Binding.AppID == "" || lease.Binding.Generation == 0 {
		return false, ErrInvalidInput
	}
	var current bool
	err := s.db.QueryRowContext(ctx, `
		UPDATE fugue_app_mail_reconcile
		SET lease_id=NULL,lease_expires_at=NULL,
			last_result=CASE WHEN generation=$2 THEN $4 ELSE last_result END,
			last_success_at=CASE WHEN generation=$2 AND $4='synced' THEN statement_timestamp() ELSE last_success_at END,
			attempt_count=CASE WHEN generation<>$2 THEN attempt_count WHEN $4='synced' THEN 0 ELSE least(attempt_count+1,6) END,
			next_attempt_at=CASE
				WHEN generation<>$2 THEN next_attempt_at
				WHEN $4='synced' THEN statement_timestamp()+interval '30 seconds'
				ELSE statement_timestamp()+least(interval '5 minutes', interval '15 seconds'*(1 << least(attempt_count,5)))
			END
		WHERE app_id=$1 AND lease_id=$3 AND lease_expires_at>statement_timestamp()
			AND EXISTS (SELECT 1 FROM fugue_meta WHERE key=$5 AND value='controller')
		RETURNING generation=$2`, lease.Binding.AppID, lease.Binding.Generation, lease.ID, result, appMailReconcileOwnerKey).Scan(&current)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return current, err
}

func (s *Store) GetAppMailReconciliation(ctx context.Context, appID string) (AppMailReconcileStatus, error) {
	var status AppMailReconcileStatus
	err := s.db.QueryRowContext(ctx, `
		SELECT generation,next_attempt_at,attempt_count,last_success_at,last_result
		FROM fugue_app_mail_reconcile WHERE app_id=$1`, appID).
		Scan(&status.Generation, &status.NextAttemptAt, &status.AttemptCount, &status.LastSuccessAt, &status.LastResult)
	return status, mapDBErr(err)
}
