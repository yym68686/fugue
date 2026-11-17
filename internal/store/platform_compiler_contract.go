package store

import (
	"context"
	"database/sql"
	"errors"
	"sort"
	"time"

	"fugue/internal/platformconfig"
	"fugue/internal/schemamigrate"
)

var ErrCompilerContractRetired = errors.New("compiler contract retired")

type compilerContractState struct {
	Epoch   int64  `json:"epoch"`
	Phase   string `json:"phase"`
	Version string `json:"compiler_version"`
}

func readCompilerContract(ctx context.Context, tx *sql.Tx, lock bool) (compilerContractState, error) {
	// Additive rollout: old databases and disposable legacy fixtures can read
	// until the schema lane installs the ledger. An installed but corrupt ledger
	// is never interpreted as observe mode.
	var installed bool
	if err := tx.QueryRowContext(ctx, `SELECT to_regclass('fugue_platform_compiler_contract') IS NOT NULL`).Scan(&installed); err != nil {
		return compilerContractState{}, err
	}
	if !installed {
		return compilerContractState{Epoch: -1, Phase: "observe"}, nil
	}
	q := `SELECT epoch,phase,compiler_version FROM fugue_platform_compiler_contract WHERE singleton`
	if lock {
		q += ` FOR SHARE`
	}
	var state compilerContractState
	err := tx.QueryRowContext(ctx, q).Scan(&state.Epoch, &state.Phase, &state.Version)
	if err != nil {
		return state, err
	}
	if state.Epoch < 0 || state.Version != platformconfig.CompactOnlyInputCompilerVersion || state.Phase != "observe" && state.Phase != "enforced" {
		return state, ErrConflict
	}
	return state, nil
}

type CompilerContractBlocker struct {
	ArtifactID   string   `json:"artifact_id"`
	ArtifactKind string   `json:"artifact_kind"`
	Scope        string   `json:"scope"`
	Generation   string   `json:"generation"`
	Version      string   `json:"compiler_version"`
	Roots        []string `json:"roots"`
}

// A readiness report is deliberately NOT a deletion authorization. Its native
// graph includes inactive/offline consumer generations, latest-three rollback
// generations, unexpired LKGs, tasks, pins and archive/external owners.
type CompilerContractReadiness struct {
	Schema             string                    `json:"schema"`
	ObservedAt         time.Time                 `json:"observed_at"`
	State              compilerContractState     `json:"state"`
	Native             CompilerInputHotInventory `json:"native"`
	Blockers           []CompilerContractBlocker `json:"legacy_dependencies"`
	DeletionAuthorized bool                      `json:"deletion_authorized"`
	FailureStage       string                    `json:"failure_stage,omitempty"`
}

func RunCompilerContractReadiness(ctx context.Context, databaseURL string) (report CompilerContractReadiness, err error) {
	report.Schema = "fugue.compiler-contract-readiness/v1"
	db, err := openPostgresDatabase(databaseURL)
	if err != nil {
		return report, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return report, err
	}
	defer tx.Rollback()
	for _, q := range []string{`SET LOCAL statement_timeout='5s'`, `SET LOCAL lock_timeout='250ms'`, `SET LOCAL work_mem='1MB'`, `SET LOCAL max_parallel_workers_per_gather=0`, `SET LOCAL jit=off`} {
		if _, err = tx.ExecContext(ctx, q); err != nil {
			return report, err
		}
	}
	if err = schemamigrate.EnforceCompilerInputZeroTemp(ctx, tx); err != nil {
		return report, err
	}
	report.State, err = readCompilerContract(ctx, tx, false)
	if err != nil {
		return report, err
	}
	report.Native, err = compilerInputHotInventoryFromSnapshot(ctx, tx)
	report.ObservedAt = report.Native.ObservedAt
	if err != nil {
		report.FailureStage = HotInventoryFailureStage(err)
		return report, err
	}
	if !report.Native.Closure.MetadataComplete || len(report.Native.Closure.Blockers) > 0 {
		report.FailureStage = "dependency-closure"
		return report, ErrConflict
	}
	ids, roots := []string{}, map[string][]string{}
	for _, node := range report.Native.Closure.Nodes {
		if node.Identity.Kind == "artifact" {
			ids = append(ids, node.Identity.ID)
			roots[node.Identity.ID] = node.Roots
		}
	}
	for start := 0; start < len(ids); start += 32 {
		rows, e := tx.QueryContext(ctx, `SELECT id,artifact_kind,scope_key,generation,metadata_json->>'compiler_version' FROM fugue_platform_artifacts WHERE id=ANY($1::text[]) AND COALESCE(metadata_json->>'compiler_version','') NOT IN ('',$2)`, ids[start:min(start+32, len(ids))], platformconfig.CompactOnlyInputCompilerVersion)
		if e != nil {
			return report, e
		}
		for rows.Next() {
			var b CompilerContractBlocker
			if e = rows.Scan(&b.ArtifactID, &b.ArtifactKind, &b.Scope, &b.Generation, &b.Version); e != nil {
				break
			}
			b.Roots = append([]string(nil), roots[b.ArtifactID]...)
			sort.Strings(b.Roots)
			report.Blockers = append(report.Blockers, b)
		}
		if e = errors.Join(e, rows.Err(), rows.Close()); e != nil {
			return report, e
		}
	}
	sort.Slice(report.Blockers, func(i, j int) bool { return report.Blockers[i].ArtifactID < report.Blockers[j].ArtifactID })
	return report, tx.Commit()
}
