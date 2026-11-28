package store

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformsafety"
)

// The closed allowlist is deliberately not configurable: configuration cannot
// turn retention into deletion of intent, artifacts, uploads or business state.
type ObservationRetentionPolicy struct {
	Version               int  `json:"version"`
	Enabled               bool `json:"enabled"`
	IntervalSeconds       int  `json:"interval_seconds"`
	BatchRows             int  `json:"batch_rows"`
	HeartbeatDays         int  `json:"heartbeat_days"`
	HeartbeatRowsPerChain int  `json:"heartbeat_rows_per_chain"`
	ProbeDays             int  `json:"probe_days"`
	ProbeRowsPerTable     int  `json:"probe_rows_per_table"`
}

type observationRetentionState struct {
	mu                    sync.Mutex
	policy                ObservationRetentionPolicy
	observedAt            time.Time
	configError           bool
	lastError             bool
	lastFailureStage      string
	lastFailureCategories []string
	last                  map[string]int64
	total                 map[string]int64
	chainCursor           string
}

func ParseObservationRetentionPolicy(raw []byte) (ObservationRetentionPolicy, error) {
	var p ObservationRetentionPolicy
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if len(raw) > 4096 || d.Decode(&p) != nil || d.Decode(new(any)) != io.EOF || p.Version != 1 ||
		p.IntervalSeconds < 30 || p.IntervalSeconds > 3600 || p.BatchRows < 1 || p.BatchRows > 1000 ||
		p.HeartbeatDays < 1 || p.HeartbeatDays > 90 || p.HeartbeatRowsPerChain < 1000 || p.HeartbeatRowsPerChain > 100000 ||
		p.ProbeDays < 7 || p.ProbeDays > 90 || p.ProbeRowsPerTable < 10000 || p.ProbeRowsPerTable > 10000000 {
		return p, errors.New("invalid observation retention policy")
	}
	return p, nil
}

func (s *Store) RunObservationRetention(ctx context.Context, storagePolicyPath string, keys bundleauth.Keyring) {
	if storagePolicyPath == "" || s.db == nil {
		return
	}
	path := filepath.Join(filepath.Dir(storagePolicyPath), "observation-retention.json")
	var lastRun time.Time
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		var p ObservationRetentionPolicy
		f, err := os.Open(path)
		if err == nil {
			raw, readErr := io.ReadAll(io.LimitReader(f, 4097))
			f.Close()
			err = readErr
			if err == nil {
				p, err = ParseObservationRetentionPolicy(raw)
			}
		}
		s.retention.mu.Lock()
		s.retention.configError = err != nil
		if err == nil {
			s.retention.policy = p
		}
		p = s.retention.policy
		s.retention.mu.Unlock()
		if p.Enabled && time.Since(lastRun) >= time.Duration(p.IntervalSeconds)*time.Second {
			lastRun = time.Now().UTC()
			counts, runErr := s.pruneSignedHeartbeatHistory(ctx, p, keys, lastRun)
			s.retention.mu.Lock()
			s.retention.observedAt = lastRun
			s.retention.lastError = runErr != nil
			s.retention.lastFailureCategories = observationRetentionFailureCategories(runErr)
			s.retention.lastFailureStage = ""
			var failure *retentionFailure
			if errors.As(runErr, &failure) {
				s.retention.lastFailureStage = failure.stage
			} else if runErr != nil {
				s.retention.lastFailureStage = "database_or_budget"
			}
			s.retention.last = counts
			if s.retention.total == nil {
				s.retention.total = map[string]int64{}
			}
			for k, v := range counts {
				s.retention.total[k] += v
			}
			s.retention.mu.Unlock()
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Store) retentionObservation() any {
	s.retention.mu.Lock()
	defer s.retention.mu.Unlock()
	last, total := map[string]int64{}, map[string]int64{}
	for k, v := range s.retention.last {
		last[k] = v
	}
	for k, v := range s.retention.total {
		total[k] = v
	}
	return map[string]any{"policy": s.retention.policy, "last_run_at": s.retention.observedAt, "policy_read_failed": s.retention.configError,
		"last_run_failed": s.retention.lastError, "last_failure_stage": s.retention.lastFailureStage,
		"last_failure_categories": append([]string(nil), s.retention.lastFailureCategories...), "last_deleted_rows": last, "process_deleted_rows": total,
		"diagnostic_job_ttl_seconds": 3600, "minimum_heartbeat_history_seconds": 86400,
		"minimum_probe_history_seconds": 604800, "capacity_semantics": "row budgets apply only after minimum protected history; physical bytes need vacuum/reuse"}
}

type retentionFailure struct {
	stage string
	cause error
}

func (e *retentionFailure) Error() string { return "observation retention failed at " + e.stage }
func (e *retentionFailure) Unwrap() error { return e.cause }

const (
	// Keep the legacy timer lock so rolling back cannot start a second
	// heartbeat pass while this API is still using the database.
	observationRetentionLockID         = int64(315609238744311)
	observationSampleRetentionOwnerKey = "observation-sample-retention-writer/v1"
	observationSampleRetentionDemand   = "observation-sample-retention-demand/v1/"
	networkSampleRetention             = "network"
	performanceSampleRetention         = "performance"
	resourceUsageSampleRetention       = "resource-usage"
)

var errUnrecognizedSampleRetentionWriter = errors.New("unrecognized sample retention writer")

// Read ownership under the foreground transaction's shared cutover lock.
func apiOwnsObservationSampleRetention(ctx context.Context, tx *sql.Tx) (bool, error) {
	var owner string
	err := tx.QueryRowContext(ctx, `SELECT value FROM fugue_meta WHERE key=$1`, observationCheckpointKey(observationSampleRetentionOwnerKey)).Scan(&owner)
	if errors.Is(err, sql.ErrNoRows) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if owner != "worker" {
		return false, errUnrecognizedSampleRetentionWriter
	}
	return false, nil
}

func beginObservationSampleIngestion(ctx context.Context, db *sql.DB) (*sql.Tx, error) {
	// The owner must be read from a fresh snapshot after acquiring the
	// shared lock, even if the connection has a different default isolation.
	return db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
}

// Admission still inserts samples when the exclusive transfer lock is held.
// Existing ingestion paths share this lock only for their optional delete;
// none can carry stale API ownership across a transfer and commit a delete.
// The handoff must acquire it exclusively, as well as the old timer lock.
func apiMayPruneDuringSampleIngestion(ctx context.Context, tx *sql.Tx, target string, pruneBefore time.Time) (bool, error) {
	key := observationCheckpointKey(observationSampleRetentionOwnerKey)
	var locked bool
	if err := tx.QueryRowContext(ctx, `SELECT pg_try_advisory_xact_lock_shared(hashtextextended($1,0))`, key).Scan(&locked); err != nil || !locked {
		return false, err
	}
	allowed, err := apiOwnsObservationSampleRetention(ctx, tx)
	if errors.Is(err, errUnrecognizedSampleRetentionWriter) {
		// A corrupt owner must stop deletion, not stop collection. The
		// independent sample worker reports the invalid value.
		return false, nil
	}
	if err != nil || allowed {
		return allowed, err
	}
	// With the independent writer selected, submit the cutoff durably in
	// the same transaction as ingestion. Do not turn the foreground's
	// one-hour/seven-day cutoffs into a continuously advancing wall-clock
	// deadline when a producer is idle. Flooring the demand to a minute
	// deliberately retains up to one extra minute, never less.
	cutoff := pruneBefore.UTC().Truncate(time.Minute).Unix()
	key = observationCheckpointKey(observationSampleRetentionDemand + target)
	_, err = tx.ExecContext(ctx, `INSERT INTO fugue_meta AS demand (key,value)
SELECT $1,$2::bigint::text
WHERE NOT EXISTS (SELECT 1 FROM fugue_meta WHERE key=$1 AND
  CASE WHEN value ~ '^-?[0-9]{1,12}$' THEN value::bigint END >= $2::bigint)
  AND pg_try_advisory_xact_lock(hashtextextended($1,0))
ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value
WHERE COALESCE(CASE WHEN demand.value ~ '^-?[0-9]{1,12}$' THEN demand.value::bigint END < $2::bigint,true)`, key, cutoff)
	return false, err
}

func (s *Store) pruneSignedHeartbeatHistory(ctx context.Context, p ObservationRetentionPolicy, keys bundleauth.Keyring, now time.Time) (map[string]int64, error) {
	raw, _ := json.Marshal(p)
	if _, err := ParseObservationRetentionPolicy(raw); err != nil {
		return nil, err
	}
	counts := map[string]int64{}
	if !p.Enabled || s.db == nil {
		return counts, nil
	}
	ctx, cancel := context.WithTimeout(ctx, 12*time.Second)
	defer cancel()
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return counts, err
	}
	defer conn.Close()
	var locked bool
	if err = conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock($1)`, observationRetentionLockID).Scan(&locked); err != nil || !locked {
		return counts, err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if _, err := conn.ExecContext(cleanup, `SELECT pg_advisory_unlock($1)`, observationRetentionLockID); err != nil {
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		}
	}()
	deleted, err := s.pruneHeartbeatHistories(ctx, conn, p, keys, now)
	counts["heartbeat_history"] = deleted
	if err != nil {
		return counts, &retentionFailure{stage: "heartbeat_history", cause: err}
	}
	return counts, nil
}

func retentionTx(ctx context.Context, conn *sql.Conn) (*sql.Tx, error) {
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, `SET LOCAL lock_timeout='100ms'; SET LOCAL statement_timeout='2s'`); err != nil {
		tx.Rollback()
		return nil, err
	}
	return tx, nil
}

const heartbeatRetentionAnchorPrefix = "observation-retention/heartbeat-boundary/"
const auditObservationColumns = `id,tenant_id,actor_type,actor_id,action,target_type,target_id,metadata_json,chain_id,chain_sequence,previous_hash,event_hash,provenance_json,created_at`

// Both normal and shadow consumers use the same signed heartbeat chain
// writer. Bind the closed event kinds to that writer's exact target identity;
// sharing its chain namespace must never make unrelated audit events eligible.
func heartbeatRetentionEventAllowed(event model.AuditEvent, chain string) bool {
	if event.TargetID == "" || event.ChainID != chain || chain != "platform-consumer-heartbeat:"+event.TargetID {
		return false
	}
	if strings.HasPrefix(event.TargetID, "shadow_") {
		return event.Action == "platform_consumer.shadow_observed" && event.TargetType == "platform_shadow_consumer"
	}
	return event.Action == "platform_consumer.heartbeat_accepted" && event.TargetType == "platform_consumer"
}

// Include the partial index predicate explicitly: a generic prepared plan
// cannot infer that the parameter can never be an empty chain ID.
const heartbeatRetentionSelectSQL = `SELECT ` + auditObservationColumns + ` FROM fugue_audit_events WHERE chain_id=$1 AND chain_id<>'' AND chain_sequence>0 ORDER BY chain_sequence LIMIT $2`

func (s *Store) pruneHeartbeatHistories(ctx context.Context, conn *sql.Conn, p ObservationRetentionPolicy, keys bundleauth.Keyring, now time.Time) (int64, error) {
	s.retention.mu.Lock()
	cursor := s.retention.chainCursor
	s.retention.mu.Unlock()
	rows, err := conn.QueryContext(ctx, `SELECT chain_id FROM fugue_security_audit_chain_state WHERE chain_id LIKE 'platform-consumer-heartbeat:%' AND chain_id>$1 ORDER BY chain_id LIMIT 8`, cursor)
	if err != nil {
		return 0, err
	}
	var chains []string
	for rows.Next() {
		var chain string
		if err = rows.Scan(&chain); err != nil {
			rows.Close()
			return 0, err
		}
		chains = append(chains, chain)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, err
	}
	if len(chains) == 0 {
		s.retention.mu.Lock()
		s.retention.chainCursor = ""
		s.retention.mu.Unlock()
		return 0, nil
	}
	var deleted int64
	var failures []error
	for _, chain := range chains {
		n, e := pruneHeartbeatChain(ctx, conn, chain, p, keys, now)
		deleted += n
		if e != nil {
			failures = append(failures, e)
		}
		s.retention.mu.Lock()
		s.retention.chainCursor = chain
		s.retention.mu.Unlock()
	}
	return deleted, errors.Join(failures...)
}

func pruneHeartbeatChain(ctx context.Context, conn *sql.Conn, chain string, p ObservationRetentionPolicy, keys bundleauth.Keyring, now time.Time) (int64, error) {
	if !strings.HasPrefix(chain, "platform-consumer-heartbeat:") {
		return 0, errors.New("only heartbeat audit chains may be retained")
	}
	tx, err := retentionTx(ctx, conn)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	var head int64
	if err = tx.QueryRowContext(ctx, `SELECT last_sequence FROM fugue_security_audit_chain_state WHERE chain_id=$1 FOR UPDATE SKIP LOCKED`, chain).Scan(&head); errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	} else if err != nil {
		return 0, err
	}
	var anchor model.AuditEvent
	var raw string
	err = tx.QueryRowContext(ctx, `SELECT value FROM fugue_meta WHERE key=$1`, heartbeatRetentionAnchorPrefix+chain).Scan(&raw)
	if err == nil {
		if json.Unmarshal([]byte(raw), &anchor) != nil || !heartbeatRetentionEventAllowed(anchor, chain) || platformsafety.VerifyTamperEvidentAuditEvent(anchor, keys) != nil {
			return 0, &observationRetentionIntegrityError{category: "anchor_integrity"}
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}
	rows, err := tx.QueryContext(ctx, heartbeatRetentionSelectSQL, chain, p.BatchRows)
	if err != nil {
		return 0, err
	}
	var events []model.AuditEvent
	for rows.Next() {
		event, e := scanAuditEvent(rows)
		if e != nil {
			rows.Close()
			return 0, e
		}
		events = append(events, event)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, err
	}
	boundary := anchor
	ids := []string{}
	for _, event := range events {
		// Preserve the head used for replay protection and current runtime facts.
		if event.ChainSequence >= head || !event.CreatedAt.Before(now.Add(-24*time.Hour)) ||
			(!event.CreatedAt.Before(now.Add(-time.Duration(p.HeartbeatDays)*24*time.Hour)) && event.ChainSequence > head-int64(p.HeartbeatRowsPerChain)) {
			break
		}
		if !heartbeatRetentionEventAllowed(event, chain) || event.ChainSequence != boundary.ChainSequence+1 || event.PreviousHash != boundary.EventHash || platformsafety.VerifyTamperEvidentAuditEvent(event, keys) != nil {
			return 0, &observationRetentionIntegrityError{category: "prefix_integrity"}
		}
		boundary = event
		ids = append(ids, event.ID)
	}
	if len(ids) == 0 {
		return 0, nil
	}
	encoded, err := json.Marshal(boundary)
	if err != nil {
		return 0, err
	}
	// Keep the original signed last-deleted event as a verifiable suffix anchor,
	// atomically with the prefix deletion. Never rewrite signatures or sequence.
	if _, err = tx.ExecContext(ctx, `INSERT INTO fugue_meta(key,value,updated_at) VALUES($1,$2,$3) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value,updated_at=EXCLUDED.updated_at`, heartbeatRetentionAnchorPrefix+chain, string(encoded), now); err != nil {
		return 0, err
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM fugue_audit_events WHERE id=ANY($1) AND chain_id=$2 AND action=$3 AND target_type=$4 AND target_id=$5`, ids, chain, boundary.Action, boundary.TargetType, boundary.TargetID)
	if err != nil {
		return 0, err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	if n != int64(len(ids)) {
		return 0, &observationRetentionIntegrityError{category: "prefix_changed"}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return n, nil
}
