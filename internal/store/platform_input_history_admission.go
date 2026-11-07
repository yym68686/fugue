package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"time"

	"fugue/internal/inputarchive"
	"fugue/internal/schemamigrate"
)

// Read the persisted phase on every ordinary admission, never a process cache
// or environment flag. Missing/invalid state fails closed. Even a capability
// database becomes strict while fenced/retired; removing a fence cannot undo
// the independently persisted enforced phase.
func compilerInputCapabilityPhase(ctx context.Context, tx *sql.Tx, lock bool) (bool, error) {
	query := `SELECT a.phase,a.plan_sha256,a.enforced_at,c.writes_fenced,c.retirement_started_at
 FROM fugue_platform_input_history_admission a CROSS JOIN fugue_platform_input_archive_control c
 WHERE a.singleton AND c.singleton`
	if lock {
		query += ` FOR SHARE OF a,c`
	}
	var phase, plan string
	var enforcedAt, retiredAt sql.NullTime
	var fenced bool
	if err := tx.QueryRowContext(ctx, query).Scan(&phase, &plan, &enforcedAt, &fenced, &retiredAt); err != nil {
		return false, errors.Join(err, ErrConflict)
	}
	if phase == "capability" && plan == "" && !enforcedAt.Valid {
		return !fenced && !retiredAt.Valid, nil
	}
	if phase != "enforced" || !compilerInputRetirementPlanHash.MatchString(plan) || !enforcedAt.Valid {
		return false, ErrConflict
	}
	return false, nil
}

// This process-owned preparation covers the complete native retained census,
// signed closure and current full-readback pins in one bounded RR snapshot.
// It does not assert external inventory, old-process drainage, rollback binary
// compatibility, resource budgets, or retirement readiness. No HTTP/CLI command
// currently enables this transition; the formal release coordinator must gate
// those separate prerequisites before exposing a production activation path.
type compilerInputHistoryEnforcement struct {
	proof *compilerInputActivationProof
	pins  []compilerInputPinBinding
}

func (s *Store) prepareCompilerInputHistoryEnforcement(ctx context.Context) (*compilerInputHistoryEnforcement, error) {
	proof, err := s.prepareCompilerInputProof(ctx, nil, nil, nil, nil, nil, false, true)
	if err != nil {
		return nil, err
	}
	proof, pins, err := s.prepareCompilerInputActivationPins(ctx, proof)
	if err != nil {
		return nil, err
	}
	return &compilerInputHistoryEnforcement{proof, pins}, nil
}

func canonicalCompilerInputHistoryRoots(roots []inputarchive.HotRoot) []inputarchive.HotRoot {
	copy := append([]inputarchive.HotRoot(nil), roots...)
	sort.Slice(copy, func(i, j int) bool {
		a, _ := json.Marshal(copy[i])
		b, _ := json.Marshal(copy[j])
		return string(a) < string(b)
	})
	return copy
}

// This switch owns the entire short commit. Shared ordinary admissions drain
// before the exclusive barriers; root-table SHARE locks catch old clients and
// phantom roots as well. No input body, signature decoding or network I/O is
// performed under these locks. Drift/timeouts abort without changing phase.
func (s *Store) enforcePreparedCompilerInputHistory(ctx context.Context, prepared *compilerInputHistoryEnforcement) (string, error) {
	if s == nil || prepared == nil || prepared.proof == nil {
		return "", ErrInvalidInput
	}
	p := prepared.proof
	if p.store != s || p.database != s.db || !p.fullInventory || p.ordinary || p.capabilityOnly ||
		!p.closure.MetadataComplete || !time.Now().Before(p.expires) || len(p.inputs) != len(prepared.pins) {
		return "", ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	if err := setCompilerInputAdmissionTimeouts(ctx, tx); err != nil {
		return "", err
	}
	if err := schemamigrate.EnforceCompilerInputZeroTemp(ctx, tx); err != nil {
		return "", err
	}
	for _, query := range []string{
		`SELECT pg_advisory_xact_lock(hashtextextended('fugue.compiler-input-gc.v1',0))`,
		`SELECT pg_advisory_xact_lock(315609238744290)`,
		`LOCK TABLE fugue_platform_artifacts,fugue_platform_input_manifests,fugue_platform_artifact_releases,
 fugue_platform_release_lanes,fugue_platform_lkg_snapshots,fugue_platform_lkg_snapshot_history,
 fugue_platform_consumer_instances,fugue_platform_shadow_consumers,fugue_platform_expected_consumer_sets,
 fugue_platform_release_messages,fugue_node_update_tasks,fugue_platform_input_archive_leases,
 fugue_platform_input_local_pins IN SHARE MODE`,
	} {
		if _, err := tx.ExecContext(ctx, query); err != nil {
			return "", err
		}
	}
	var phase string
	if err := tx.QueryRowContext(ctx, `SELECT phase FROM fugue_platform_input_history_admission WHERE singleton FOR UPDATE`).Scan(&phase); err != nil {
		return "", err
	}
	if phase != "capability" {
		return "", ErrConflict // no replay or replacement of the enabling plan
	}
	epoch, err := readInputArchiveEpoch(ctx, tx, -1)
	if err != nil || epoch != p.sourceEpoch {
		return "", errors.Join(err, ErrConflict)
	}
	var horizon int64
	if err := tx.QueryRowContext(ctx, `SELECT pg_snapshot_xmax(pg_current_snapshot())::text::bigint`).Scan(&horizon); err != nil {
		return "", err
	}
	if horizon < p.horizon || horizon-p.horizon >= 1<<31 {
		return "", ErrConflict
	}
	roots, err := compilerInputHotRoots(ctx, tx)
	if err != nil || !reflect.DeepEqual(canonicalCompilerInputHistoryRoots(roots), canonicalCompilerInputHistoryRoots(p.closure.Roots)) {
		return "", errors.Join(err, ErrConflict)
	}
	if err := schemamigrate.VerifyCompilerInputLocalPins(ctx, tx); err != nil {
		return "", err
	}
	pins := append([]compilerInputPinBinding(nil), prepared.pins...)
	sort.Slice(pins, func(i, j int) bool { return pins[i].Key < pins[j].Key })
	seen := map[string]bool{}
	for _, pin := range pins {
		if seen[pin.Digest] || !compilerInputPinKeyPattern.MatchString(pin.Key) || pin.Token < 1 || !inputDigestPattern.MatchString(pin.Digest) {
			return "", ErrInvalidInput
		}
		seen[pin.Digest] = true
	}
	for _, digest := range p.inputs {
		if !seen[digest] {
			return "", ErrInvalidInput
		}
	}
	for start := 0; start < len(pins); start += 32 {
		if err := checkCompilerInputVerifiedPins(ctx, tx, pins[start:min(start+32, len(pins))], epoch); err != nil {
			return "", err
		}
	}
	witnesses := make([][]any, 0, len(p.tuples))
	for _, original := range p.tuples {
		current := original
		if err := readCompilerInputActivationTuple(ctx, tx, &current, true); err != nil || current != original {
			return "", errors.Join(err, ErrConflict)
		}
		witnesses = append(witnesses, []any{original.table, original.key, original.xmin, original.ctid, original.relation, original.filenode})
	}
	hash := sha256.New()
	err = json.NewEncoder(hash).Encode(struct {
		Closure        inputarchive.HotClosure
		Tuples         [][]any
		Pins           []compilerInputPinBinding
		Epoch, Horizon int64
		Keyring        uint64
		Expires        time.Time
	}{p.closure, witnesses, pins, p.sourceEpoch, p.horizon, p.keyringRevision, p.expires})
	if err != nil {
		return "", err
	}
	plan := hex.EncodeToString(hash.Sum(nil))
	s.platformArtifactKeyringMu.RLock()
	defer s.platformArtifactKeyringMu.RUnlock()
	if s.platformArtifactKeyringRevision != p.keyringRevision || !time.Now().Before(p.expires) {
		return "", ErrConflict
	}
	result, err := tx.ExecContext(ctx, `UPDATE fugue_platform_input_history_admission SET phase='enforced',plan_sha256=$1,enforced_at=now() WHERE singleton AND phase='capability'`, plan)
	if err != nil {
		return "", err
	}
	if n, err := result.RowsAffected(); err != nil || n != 1 {
		return "", errors.Join(err, ErrConflict)
	}
	if err := tx.Commit(); err != nil {
		return "", err
	}
	return plan, nil
}
