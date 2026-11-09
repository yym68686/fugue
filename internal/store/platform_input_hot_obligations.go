package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"fugue/internal/inputarchive"
	"fugue/internal/schemamigrate"
)

// These are original registry fields, not inferred expiry or an attestation of
// actual execution. In particular heartbeat age never expires a consumer root.
type compilerInputRootState struct {
	ReleaseID       string     `json:"release_id,omitempty"`
	ReleaseSetID    string     `json:"release_set_id,omitempty"`
	ConsumerID      string     `json:"consumer_id,omitempty"`
	ArtifactKind    string     `json:"artifact_kind,omitempty"`
	ScopeKey        string     `json:"scope_key,omitempty"`
	Generation      string     `json:"generation,omitempty"`
	State           string     `json:"state,omitempty"`
	ExpiresAt       *time.Time `json:"expires_at"`
	LastHeartbeatAt *time.Time `json:"last_heartbeat_at,omitempty"`
}

type CompilerInputRootObligation struct {
	Root inputarchive.HotRoot `json:"root"`
	compilerInputRootState
	// Rule describes when THIS root leaves the native set, never when the
	// identity becomes disposable. All other roots/sources still apply.
	Rule string `json:"root_exit_rule"`
}

type CompilerInputObservedObligation struct {
	Reference            inputarchive.HotReference `json:"reference"`
	ObservedRoots        int                       `json:"observed_roots"`
	StateRootProtection  bool                      `json:"state_root_protection"`
	LatestTimeRootExpiry *time.Time                `json:"latest_observed_time_root_expiry"`
	StrongestRootKnown   bool                      `json:"strongest_root_known"`
	DisposalAuthorized   bool                      `json:"disposal_authorized"`
}

type CompilerInputHotObligations struct {
	Schema                string                            `json:"schema"`
	ObservedAt            time.Time                         `json:"observed_at"`
	Snapshot              string                            `json:"database_snapshot"`
	CoherentMetadata      bool                              `json:"coherent_metadata"`
	RootInventoryComplete bool                              `json:"root_inventory_complete"`
	TraversalComplete     bool                              `json:"traversal_complete"`
	PathsComplete         bool                              `json:"observed_paths_complete"`
	AllSourcesComplete    bool                              `json:"all_sources_complete"`
	BodyVerification      bool                              `json:"body_verification"`
	RetirementAuthorized  bool                              `json:"retirement_authorized"`
	PlanSHA256            string                            `json:"plan_sha256"`
	FailureStage          string                            `json:"failure_stage,omitempty"`
	FailureReason         string                            `json:"failure_reason,omitempty"`
	UncoveredSources      []string                          `json:"uncovered_sources"`
	Targets               []inputarchive.HotReference       `json:"targets"`
	Roots                 []CompilerInputRootObligation     `json:"roots"`
	ObservedNodes         []inputarchive.HotNode            `json:"observed_nodes"`
	Paths                 []inputarchive.HotObligationPath  `json:"observed_paths"`
	Obligations           []CompilerInputObservedObligation `json:"observed_obligations"`
	Progress              inputarchive.HotClosureProgress   `json:"progress"`
	Cost                  CompilerInputHotCost              `json:"cost"`
}

const compilerInputObligationOutputLimit = 32 << 20

// RunCompilerInputHotObligations is a diagnostic, not an H/C/D/U census. It
// uses the existing native root predicates, resolver and fixed RR/SQL/lock/
// memory/temp/graph budgets. There is no original-input or object-store fetch.
func RunCompilerInputHotObligations(ctx context.Context, databaseURL string, targets []inputarchive.HotReference) (CompilerInputHotObligations, error) {
	raw, err := json.Marshal(targets)
	if err != nil {
		return CompilerInputHotObligations{}, ErrInvalidInput
	}
	if _, err = inputarchive.DecodeHotObligationTargets(string(raw)); err != nil {
		return CompilerInputHotObligations{}, ErrInvalidInput
	}
	db, err := openPostgresDatabase(databaseURL)
	if err != nil {
		return CompilerInputHotObligations{}, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return CompilerInputHotObligations{}, err
	}
	defer tx.Rollback()
	for _, statement := range []string{`SET LOCAL statement_timeout='5s'`, `SET LOCAL lock_timeout='250ms'`, `SET LOCAL work_mem='1MB'`, `SET LOCAL max_parallel_workers_per_gather=0`, `SET LOCAL jit=off`} {
		if _, err = tx.ExecContext(ctx, statement); err != nil {
			return CompilerInputHotObligations{}, err
		}
	}
	if err = schemamigrate.EnforceCompilerInputZeroTemp(ctx, tx); err != nil {
		return CompilerInputHotObligations{}, err
	}
	report, err := compilerInputHotObligationsSnapshot(ctx, tx, targets)
	if err != nil {
		return report, err
	}
	if err = tx.Commit(); err != nil {
		report.TraversalComplete, report.PathsComplete = false, false
		report.FailureStage, report.FailureReason = "snapshot-commit", "snapshot-unavailable"
		return report, err
	}
	return report, nil
}

func compilerInputHotObligationsSnapshot(ctx context.Context, tx *sql.Tx, targets []inputarchive.HotReference) (report CompilerInputHotObligations, resultErr error) {
	report = CompilerInputHotObligations{Schema: "fugue.compiler-input-root-obligations/v1", Targets: targets,
		Roots: []CompilerInputRootObligation{}, ObservedNodes: []inputarchive.HotNode{}, Paths: []inputarchive.HotObligationPath{},
		UncoveredSources: []string{"external-generation-ledger-validation", "artifact-signatures-and-local-original-bytes", "persistent-reactivation-admission", "manual-input-pins-registry"}}
	// Reserve envelope space; every appended metadata object consumes its exact
	// JSON size before admission. Refusal preserves only the bounded prefix.
	outputBytes := len(rawJSON(targets)) + 65536
	admit := func(value any) error {
		raw, err := json.Marshal(value)
		if err != nil || len(raw) > compilerInputObligationOutputLimit-outputBytes-1 {
			return ErrConflict
		}
		outputBytes += len(raw) + 1
		return nil
	}
	defer func() {
		if resultErr != nil && report.FailureStage == "" {
			report.FailureStage, report.FailureReason = HotInventoryFailureStage(resultErr), inputarchive.HotClosureFailureReason(resultErr)
		}
	}()
	if err := tx.QueryRowContext(ctx, `SELECT transaction_timestamp(),pg_current_snapshot()::text,current_setting('transaction_isolation')='repeatable read' AND current_setting('transaction_read_only')='on'`).Scan(&report.ObservedAt, &report.Snapshot, &report.CoherentMetadata); err != nil {
		return report, err
	}
	if !report.CoherentMetadata {
		return report, ErrConflict
	}
	started := time.Now()
	roots, err := compilerInputHotRootsObserved(ctx, tx, func(root inputarchive.HotRoot, state compilerInputRootState) error {
		item := CompilerInputRootObligation{Root: root, compilerInputRootState: state, Rule: compilerInputRootExitRule(root.Source)}
		if err := admit(item); err != nil {
			return err
		}
		report.Roots = append(report.Roots, item)
		return nil
	})
	report.Cost.RootInventoryUS = time.Since(started).Microseconds()
	if err != nil {
		return report, err
	}
	report.RootInventoryComplete = true
	var pinsInstalled, indexed bool
	if err = tx.QueryRowContext(ctx, `SELECT to_regclass('fugue_platform_input_local_pins') IS NOT NULL,to_regclass('fugue_platform_input_hot_dependency_index_v1') IS NOT NULL`).Scan(&pinsInstalled, &indexed); err != nil {
		return report, err
	}
	if pinsInstalled {
		if err = schemamigrate.VerifyCompilerInputLocalPins(ctx, tx); err != nil {
			return report, err
		}
		report.UncoveredSources = report.UncoveredSources[:len(report.UncoveredSources)-1]
	}
	if indexed {
		if err = schemamigrate.VerifyCompilerInputHotIndex(ctx, tx); err != nil {
			return report, &compilerInputHotInventoryError{"dependency-index", err}
		}
	}
	started = time.Now()
	closure, graphErr := inputarchive.PlanHotClosureBatchedDiagnostic(ctx, roots,
		inputarchive.HotClosureBudget{Roots: compilerInputHotRootLimit, Nodes: 32768, Edges: 262144, Bindings: 524288},
		func(ctx context.Context, refs []inputarchive.HotReference) (map[inputarchive.HotReference]inputarchive.HotNode, error) {
			started := time.Now()
			nodes, err := resolveCompilerInputHotBatchWithIndex(ctx, tx, refs, &report.Cost, indexed)
			report.Cost.ResolverUS += time.Since(started).Microseconds()
			return nodes, err
		}, func(progress inputarchive.HotClosureProgress) { report.Progress = progress }, func(node inputarchive.HotNode) error {
			// Strip representation estimates/verification flags: these entries
			// expose only observed identities, declared hashes and parent edges.
			node = inputarchive.HotNode{Identity: node.Identity, ContentHash: node.ContentHash, References: node.References}
			if err := admit(node); err != nil {
				return err
			}
			report.ObservedNodes = append(report.ObservedNodes, node)
			return nil
		})
	report.Cost.TraversalUS = time.Since(started).Microseconds()
	report.TraversalComplete = graphErr == nil && closure.MetadataComplete
	for _, target := range targets {
		report.Obligations = append(report.Obligations, CompilerInputObservedObligation{Reference: target})
	}
	report.PathsComplete, err = inputarchive.TraceHotObligationPaths(ctx, roots, report.ObservedNodes, targets, 524288, func(path inputarchive.HotObligationPath) error {
		if err := admit(path); err != nil {
			return err
		}
		report.Paths = append(report.Paths, path)
		root := report.Roots[path.RootIndex]
		for i := range report.Obligations {
			item := &report.Obligations[i]
			if item.Reference != path.Target {
				continue
			}
			item.ObservedRoots++
			// A timestamp on current-LKG or an expired heartbeat does not
			// override its independent, state-based retention predicate.
			timeRoot := root.Root.Source == "unexpired-lkg" || root.Root.Source == "archive-owner" || root.Root.Source == "active-message"
			if !timeRoot || root.ExpiresAt == nil {
				item.StateRootProtection = true
			} else if item.LatestTimeRootExpiry == nil || root.ExpiresAt.After(*item.LatestTimeRootExpiry) {
				expires := *root.ExpiresAt
				item.LatestTimeRootExpiry = &expires
			}
		}
		return nil
	})
	if graphErr != nil {
		report.FailureStage, report.FailureReason = "dependency-graph", inputarchive.HotClosureFailureReason(graphErr)
	} else if err != nil {
		report.FailureStage, report.FailureReason = "observed-paths", "diagnostic-budget-or-deadline"
	} else if !closure.MetadataComplete {
		report.FailureStage, report.FailureReason = "dependency-graph", "unresolved-or-conflicting-identities"
		graphErr = ErrConflict
	}
	return report, errors.Join(graphErr, err)
}

func compilerInputRootExitRule(source string) string {
	switch {
	case source == "active-message":
		return "message-expires-or-associated-release-not-active; null-expiry-has-no-time-exit"
	case source == "unexpired-lkg", source == "archive-owner":
		return "original-expiry; repeated-lkg-events-use-latest-expiry-for-exact-artifact-and-hash"
	case source == "rollback-history":
		return "outside-latest-three-distinct-verified-generations; expiry-alone-does-not-release"
	case source == "current-lkg":
		return "verified-current-lkg-replaced; expiry-alone-does-not-release"
	case strings.HasPrefix(source, "consumer-"), strings.HasPrefix(source, "shadow-consumer-"):
		return "registered-role-replaced-or-lawfully-retired; heartbeat-age-does-not-release"
	case source == "local-input-pin":
		return "pin-explicitly-released; pending-and-verified-retain"
	case source == "pending-external-root":
		return "append-only-pending-retention; publication-success-crash-or-heartbeat-age-never-releases"
	case source == "active-task":
		return "task-no-longer-pending-or-running"
	default:
		return "native-active-root-predicate-no-longer-matches"
	}
}

func rawJSON(value any) []byte {
	raw, _ := json.Marshal(value)
	return raw
}
