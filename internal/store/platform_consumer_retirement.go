package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"fugue/internal/inputarchive"
	"fugue/internal/schemamigrate"
	"github.com/jackc/pgx/v5/pgconn"
)

var ErrPlatformConsumerRetired = errors.New("cannot re-enroll a permanently retired consumer")

type PlatformConsumerRetirementTarget struct {
	ConsumerID   string `json:"consumer_id"`
	ArtifactKind string `json:"artifact_kind"`
	ScopeKey     string `json:"scope_key"`
}

// RetiredPlatformConsumerTargets looks up only exact enrollment keys. It
// neither grants membership nor replaces the transactional retirement fence.
func (s *Store) RetiredPlatformConsumerTargets(ctx context.Context, targets []PlatformConsumerRetirementTarget) (map[PlatformConsumerRetirementTarget]bool, error) {
	// One global recovery capture has at most 64 edges and two route kinds.
	if len(targets) > 128 {
		return nil, ErrInvalidInput
	}
	if len(targets) == 0 || !s.usingDatabase() {
		return nil, nil
	}
	data, err := json.Marshal(targets)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, `
SELECT retired.consumer_id, retired.artifact_kind, retired.scope_key
FROM jsonb_to_recordset($1::jsonb) AS wanted(consumer_id text, artifact_kind text, scope_key text)
JOIN fugue_platform_consumer_retirements retired USING (consumer_id, artifact_kind, scope_key)`, string(data))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	retired := make(map[PlatformConsumerRetirementTarget]bool)
	for rows.Next() {
		var target PlatformConsumerRetirementTarget
		if err := rows.Scan(&target.ConsumerID, &target.ArtifactKind, &target.ScopeKey); err != nil {
			return nil, err
		}
		retired[target] = true
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return retired, nil
}

// The deployed guard shares SQLSTATE 40001 with real serialization failures.
// Classify its specific permanent rejection before mapDBErr loses the cause.
func mapExpectedConsumerInsertError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "40001" && pgErr.Message == ErrPlatformConsumerRetired.Error() {
		return ErrPlatformConsumerRetired
	}
	return mapDBErr(err)
}

type PlatformConsumerRetirementContract struct {
	Version                    int                                `json:"version"`
	Actor                      string                             `json:"actor"`
	Reason                     string                             `json:"reason"`
	PermanentlyAbandonRecovery bool                               `json:"permanently_abandon_recovery"`
	Targets                    []PlatformConsumerRetirementTarget `json:"targets"`
}

func (c PlatformConsumerRetirementContract) Validate() error {
	if c.Version != 1 || !c.PermanentlyAbandonRecovery || len(c.Targets) < 1 || len(c.Targets) > 32 ||
		strings.TrimSpace(c.Actor) == "" || len(c.Actor) > 256 || strings.TrimSpace(c.Reason) == "" || len(c.Reason) > 2048 {
		return ErrInvalidInput
	}
	seen := make(map[PlatformConsumerRetirementTarget]bool)
	for _, t := range c.Targets {
		if t.ConsumerID == "" || len(t.ConsumerID) > 512 || strings.TrimSpace(t.ConsumerID) != t.ConsumerID ||
			t.ArtifactKind == "" || len(t.ArtifactKind) > 128 || NormalizePlatformArtifactKind(t.ArtifactKind) != t.ArtifactKind ||
			t.ScopeKey == "" || len(t.ScopeKey) > 512 || strings.ToLower(strings.TrimSpace(t.ScopeKey)) != t.ScopeKey || seen[t] {
			return ErrInvalidInput
		}
		for _, value := range []string{t.ConsumerID, t.ArtifactKind, t.ScopeKey} {
			for _, r := range value {
				if r < 33 || r > 126 {
					return ErrInvalidInput
				}
			}
		}
		seen[t] = true
	}
	return nil
}

type PlatformConsumerRetirementResult struct {
	PlatformConsumerRetirementTarget
	InstanceID                 string   `json:"instance_id"`
	SnapshotSHA256             string   `json:"snapshot_sha256"`
	AlreadyRetired             bool     `json:"already_retired"`
	RetainedShadowExpectations []string `json:"retained_shadow_expectations"`
}

type PlatformConsumerRetirementReport struct {
	Action              string                             `json:"action"`
	PlanSHA256          string                             `json:"plan_sha256"`
	Targets             []PlatformConsumerRetirementResult `json:"targets"`
	Applied             bool                               `json:"applied"`
	Retired             int                                `json:"retired"`
	HistoryDeleted      bool                               `json:"history_deleted"`
	NodeExecutionFenced bool                               `json:"node_execution_fenced"`
}

type consumerRetirementSnapshot struct {
	Target             PlatformConsumerRetirementTarget `json:"target"`
	Consumer           json.RawMessage                  `json:"consumer"`
	Shadow             json.RawMessage                  `json:"shadow"`
	ShadowExpectations []string                         `json:"retained_shadow_expectations"`
	retired            bool
	plan               string
}

// Operator-only maintenance, not automatic expiry. Preview is strictly read
// only. Apply binds the complete immutable facts, actor, reason and target set
// to the preview hash, and commits all identities or none. The audit ledger is
// not a restorable retention root. This neither deletes shared history nor
// proves that a disconnected process has stopped executing cached data.
func RunPlatformConsumerRetirement(ctx context.Context, databaseURL string, c PlatformConsumerRetirementContract, apply bool, expectedPlan string) (PlatformConsumerRetirementReport, error) {
	if err := c.Validate(); err != nil {
		return PlatformConsumerRetirementReport{}, err
	}
	if apply {
		enabled, err := inputarchive.MigrationEnabledFromEnv()
		if err != nil || !enabled || !inputDigestPattern.MatchString(expectedPlan) {
			return PlatformConsumerRetirementReport{}, ErrInvalidInput
		}
	} else if expectedPlan != "" {
		return PlatformConsumerRetirementReport{}, ErrInvalidInput
	}
	db, err := openPostgresDatabase(databaseURL)
	if err != nil {
		return PlatformConsumerRetirementReport{}, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return runPlatformConsumerRetirement(ctx, db, c, apply, expectedPlan)
}

func runPlatformConsumerRetirement(ctx context.Context, db *sql.DB, c PlatformConsumerRetirementContract, apply bool, expectedPlan string) (PlatformConsumerRetirementReport, error) {
	r := PlatformConsumerRetirementReport{Action: "consumer-retire"}
	if err := c.Validate(); err != nil || apply && !inputDigestPattern.MatchString(expectedPlan) || !apply && expectedPlan != "" {
		return r, ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	options := &sql.TxOptions{ReadOnly: !apply}
	if !apply {
		options.Isolation = sql.LevelRepeatableRead
	}
	tx, err := db.BeginTx(ctx, options)
	if err != nil {
		return r, err
	}
	defer tx.Rollback()
	for _, q := range []string{`SET LOCAL lock_timeout='250ms'`, `SET LOCAL statement_timeout='5s'`, `SET LOCAL work_mem='1MB'`, `SET LOCAL TIME ZONE 'UTC'`} {
		if _, err = tx.ExecContext(ctx, q); err != nil {
			return r, err
		}
	}
	if err = schemamigrate.EnforceCompilerInputZeroTemp(ctx, tx); err != nil {
		return r, err
	}
	if err = verifyPlatformConsumerRetirementSchema(ctx, tx); err != nil {
		return r, err
	}
	if apply {
		// Every serving/shadow/expectation/activation writer holds the shared
		// side until commit. No byte reads or network requests under this lock.
		var epoch int64
		if err = tx.QueryRowContext(ctx, `SELECT epoch FROM fugue_platform_consumer_retirement_epoch WHERE singleton FOR UPDATE`).Scan(&epoch); err != nil {
			return r, err
		}
	}
	c.Targets = append([]PlatformConsumerRetirementTarget(nil), c.Targets...)
	sort.Slice(c.Targets, func(i, j int) bool {
		a, b := c.Targets[i], c.Targets[j]
		return a.ConsumerID < b.ConsumerID || a.ConsumerID == b.ConsumerID && (a.ArtifactKind < b.ArtifactKind || a.ArtifactKind == b.ArtifactKind && a.ScopeKey < b.ScopeKey)
	})
	snapshots := make([]consumerRetirementSnapshot, 0, len(c.Targets))
	for _, target := range c.Targets {
		snapshot, err := readConsumerRetirementSnapshot(ctx, tx, target, c)
		if err != nil {
			return r, fmt.Errorf("exact consumer retirement facts unavailable: %w", err)
		}
		var active bool
		if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM (
 SELECT id FROM fugue_platform_artifact_releases WHERE status='active' AND release_channel<>'shadow'
 UNION SELECT active_release_id FROM fugue_platform_release_lanes WHERE active_release_id<>'' AND release_channel<>'shadow'
 ) active CROSS JOIN LATERAL (
 SELECT 1 FROM fugue_platform_expected_consumer_sets e WHERE e.artifact_release_id=active.id AND e.artifact_release_id<>''
 AND e.artifact_kind=$2 AND e.scope_key=$3 AND e.consumers_json @> jsonb_build_array(jsonb_build_object('consumer_id',$1::text)) OFFSET 0
 ) member)`, target.ConsumerID, target.ArtifactKind, target.ScopeKey).Scan(&active); err != nil {
			return r, err
		}
		if active {
			return r, fmt.Errorf("consumer still enrolled in an active serving expectation; change topology separately: %w", ErrConflict)
		}
		// Shadow enrollment is not serving membership. Explicit identity
		// abandonment may stop its observations, but never releases the shadow
		// publication's independent retention roots. Surface these exact owners.
		rows, err := tx.QueryContext(ctx, `SELECT DISTINCT member.id FROM (
 SELECT id FROM fugue_platform_artifact_releases WHERE status='active' AND release_channel='shadow'
 UNION SELECT active_release_id FROM fugue_platform_release_lanes WHERE active_release_id<>'' AND release_channel='shadow'
 ) active CROSS JOIN LATERAL (
 SELECT id FROM fugue_platform_expected_consumer_sets e WHERE e.artifact_release_id=active.id AND e.artifact_release_id<>''
 AND e.artifact_kind=$2 AND e.scope_key=$3 AND e.consumers_json @> jsonb_build_array(jsonb_build_object('consumer_id',$1::text)) OFFSET 0
 ) member ORDER BY member.id LIMIT 129`, target.ConsumerID, target.ArtifactKind, target.ScopeKey)
		if err != nil {
			return r, err
		}
		snapshot.ShadowExpectations = []string{}
		for rows.Next() {
			var id string
			if err = rows.Scan(&id); err != nil {
				rows.Close()
				return r, err
			}
			snapshot.ShadowExpectations = append(snapshot.ShadowExpectations, id)
		}
		err = rows.Err()
		rows.Close()
		if err != nil || len(snapshot.ShadowExpectations) > 128 {
			return r, errors.Join(err, ErrConflict)
		}
		snapshots = append(snapshots, snapshot)
		var fact struct {
			ID string `json:"id"`
		}
		if err = json.Unmarshal(snapshot.Consumer, &fact); err != nil || fact.ID == "" {
			return r, ErrConflict
		}
		r.Targets = append(r.Targets, PlatformConsumerRetirementResult{PlatformConsumerRetirementTarget: target, InstanceID: fact.ID, SnapshotSHA256: retirementSHA(snapshot.Consumer), AlreadyRetired: snapshot.retired, RetainedShadowExpectations: snapshot.ShadowExpectations})
	}
	var database, schema string
	if err = tx.QueryRowContext(ctx, `SELECT current_database(),current_schema()`).Scan(&database, &schema); err != nil {
		return r, err
	}
	body, err := json.Marshal(struct {
		Database  string                             `json:"database"`
		Schema    string                             `json:"schema"`
		Contract  PlatformConsumerRetirementContract `json:"contract"`
		Snapshots []consumerRetirementSnapshot       `json:"snapshots"`
	}{database, schema, c, snapshots})
	if err != nil {
		return r, err
	}
	r.PlanSHA256 = retirementSHA(body)
	if apply && r.PlanSHA256 != expectedPlan {
		return r, fmt.Errorf("retirement preview changed; no identities retired: %w", ErrConflict)
	}
	for _, snapshot := range snapshots {
		if snapshot.retired {
			if snapshot.plan != r.PlanSHA256 {
				return r, fmt.Errorf("identity belongs to a different retirement plan: %w", ErrConflict)
			}
			continue
		}
		if !apply {
			continue
		}
		t := snapshot.Target
		if _, err = tx.ExecContext(ctx, `INSERT INTO fugue_platform_consumer_retirements
 (consumer_id,artifact_kind,scope_key,actor,reason,plan_sha256,consumer_snapshot,shadow_snapshot,recovery_permanently_abandoned)
 VALUES($1,$2,$3,$4,$5,$6,$7::jsonb,$8::jsonb,true)`, t.ConsumerID, t.ArtifactKind, t.ScopeKey, c.Actor, c.Reason, r.PlanSHA256, string(snapshot.Consumer), string(snapshot.Shadow)); err != nil {
			return r, err
		}
		result, err := tx.ExecContext(ctx, `DELETE FROM fugue_platform_consumer_instances WHERE consumer_id=$1 AND artifact_kind=$2 AND scope_key=$3 AND to_jsonb(fugue_platform_consumer_instances)=$4::jsonb`, t.ConsumerID, t.ArtifactKind, t.ScopeKey, string(snapshot.Consumer))
		if err != nil {
			return r, err
		}
		changed, err := result.RowsAffected()
		if err != nil || changed != 1 {
			return r, ErrConflict
		}
		if _, err = tx.ExecContext(ctx, `DELETE FROM fugue_platform_shadow_consumers WHERE consumer_id=$1 AND artifact_kind=$2 AND scope_key=$3 AND to_jsonb(fugue_platform_shadow_consumers)=$4::jsonb`, t.ConsumerID, t.ArtifactKind, t.ScopeKey, string(snapshot.Shadow)); err != nil {
			return r, err
		}
	}
	if err = tx.Commit(); err != nil {
		return r, err
	}
	if apply {
		r.Applied, r.Retired = true, len(snapshots)
	}
	return r, nil
}

func retirementSHA(raw []byte) string {
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func readConsumerRetirementSnapshot(ctx context.Context, tx *sql.Tx, target PlatformConsumerRetirementTarget, c PlatformConsumerRetirementContract) (consumerRetirementSnapshot, error) {
	s := consumerRetirementSnapshot{Target: target, Shadow: json.RawMessage("null")}
	var actor, reason string
	err := tx.QueryRowContext(ctx, `SELECT consumer_snapshot,shadow_snapshot,plan_sha256,actor,reason FROM fugue_platform_consumer_retirements WHERE consumer_id=$1 AND artifact_kind=$2 AND scope_key=$3`, target.ConsumerID, target.ArtifactKind, target.ScopeKey).Scan(&s.Consumer, &s.Shadow, &s.plan, &actor, &reason)
	if err == nil {
		if actor != c.Actor || reason != c.Reason {
			return s, ErrConflict
		}
		s.retired = true
		return s, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return s, err
	}
	if err = tx.QueryRowContext(ctx, `SELECT to_jsonb(c) FROM fugue_platform_consumer_instances c WHERE consumer_id=$1 AND artifact_kind=$2 AND scope_key=$3`, target.ConsumerID, target.ArtifactKind, target.ScopeKey).Scan(&s.Consumer); err != nil {
		return s, err
	}
	err = tx.QueryRowContext(ctx, `SELECT to_jsonb(c) FROM fugue_platform_shadow_consumers c WHERE consumer_id=$1 AND artifact_kind=$2 AND scope_key=$3`, target.ConsumerID, target.ArtifactKind, target.ScopeKey).Scan(&s.Shadow)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return s, err
	}
	return s, nil
}

func verifyPlatformConsumerRetirementSchema(ctx context.Context, tx *sql.Tx) error {
	var ready bool
	// Missing relations, disabled triggers or missing singleton fail closed.
	err := tx.QueryRowContext(ctx, `SELECT
 (SELECT count(*) FROM pg_trigger WHERE NOT tgisinternal AND tgenabled='A' AND
 ((tgrelid IN ('fugue_platform_consumer_retirement_epoch'::regclass,'fugue_platform_consumer_retirements'::regclass) AND tgname IN ('fugue_consumer_retirement_control','fugue_consumer_retirement_truncate'))
 OR (tgrelid='fugue_platform_consumer_retirements'::regclass AND tgname='fugue_consumer_retirement_insert')
 OR (tgrelid IN ('fugue_platform_consumer_instances'::regclass,'fugue_platform_shadow_consumers'::regclass,'fugue_platform_expected_consumer_sets'::regclass,'fugue_platform_artifact_releases'::regclass,'fugue_platform_release_lanes'::regclass) AND tgname='fugue_retired_consumer_root')))=10
 AND (SELECT count(*)=1 FROM fugue_platform_consumer_retirement_epoch WHERE singleton)`).Scan(&ready)
	if err != nil || !ready {
		return fmt.Errorf("consumer retirement schema is not ready: %w", errors.Join(err, ErrConflict))
	}
	return nil
}
