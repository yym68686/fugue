package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"fugue/internal/inputarchive"
	"fugue/internal/model"
	"fugue/internal/schemamigrate"
)

const compilerInputHotRootLimit = 16384
const compilerInputHotArtifactBytes = 8 << 20

type compilerInputHotInventoryError struct {
	stage string
	cause error
}

func (e *compilerInputHotInventoryError) Error() string {
	return "hot inventory incomplete at " + e.stage
}
func (e *compilerInputHotInventoryError) Unwrap() error { return e.cause }

// HotInventoryFailureStage returns only operator-defined stages, never SQL,
// connection strings or arbitrary database/transport error messages.
func HotInventoryFailureStage(err error) string {
	var failure *compilerInputHotInventoryError
	if errors.As(err, &failure) {
		return failure.stage
	}
	return "connection-or-snapshot"
}

// This is a coherent, bounded metadata inventory, not retirement authority.
// Full local byte verification and the persistent reactivation admission
// barrier are separate gates and are deliberately never inferred here.
type CompilerInputHotInventory struct {
	Action                string                          `json:"action"`
	ObservedAt            time.Time                       `json:"observed_at"`
	Snapshot              string                          `json:"database_snapshot"`
	CoherentMetadata      bool                            `json:"coherent_metadata"`
	RootInventoryComplete bool                            `json:"root_inventory_complete"`
	TraversalPerformed    bool                            `json:"traversal_performed"`
	RootCounts            map[string]int                  `json:"root_counts"`
	Progress              inputarchive.HotClosureProgress `json:"progress"`
	Cost                  CompilerInputHotCost            `json:"cost"`
	BodyVerification      bool                            `json:"body_verification"`
	ReactivationProtected bool                            `json:"reactivation_protected"`
	RetirementAuthorized  bool                            `json:"retirement_authorized"`
	UncoveredSources      []string                        `json:"uncovered_sources"`
	Closure               inputarchive.HotClosure         `json:"closure"`
}

func RunCompilerInputHotInventory(ctx context.Context, databaseURL string) (CompilerInputHotInventory, error) {
	return runCompilerInputHotInventory(ctx, databaseURL, false)
}

// RunCompilerInputHotRootsInventory is the complete bounded database-root
// census without the graph traversal. It is useful before sizing a full graph;
// it never confers dependency completeness, byte verification or retirement.
func RunCompilerInputHotRootsInventory(ctx context.Context, databaseURL string) (CompilerInputHotInventory, error) {
	return runCompilerInputHotInventory(ctx, databaseURL, true)
}

func runCompilerInputHotInventory(ctx context.Context, databaseURL string, rootsOnly bool) (CompilerInputHotInventory, error) {
	db, err := openPostgresDatabase(databaseURL)
	if err != nil {
		return CompilerInputHotInventory{}, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return CompilerInputHotInventory{}, err
	}
	defer tx.Rollback()
	// Use the same fixed admin-owned transaction-local guard as activation.
	// No arbitrary SET privilege or superuser role is granted to the caller.
	for _, statement := range []string{`SET LOCAL statement_timeout='5s'`, `SET LOCAL lock_timeout='250ms'`, `SET LOCAL work_mem='1MB'`, `SET LOCAL max_parallel_workers_per_gather=0`, `SET LOCAL jit=off`} {
		if _, err = tx.ExecContext(ctx, statement); err != nil {
			return CompilerInputHotInventory{}, err
		}
	}
	if err = schemamigrate.EnforceCompilerInputZeroTemp(ctx, tx); err != nil {
		return CompilerInputHotInventory{}, err
	}
	report, err := compilerInputHotInventorySnapshot(ctx, tx, rootsOnly)
	if err != nil {
		return report, err
	}
	if err = tx.Commit(); err != nil {
		return CompilerInputHotInventory{}, err
	}
	return report, nil
}

func compilerInputHotInventoryFromSnapshot(ctx context.Context, tx *sql.Tx) (CompilerInputHotInventory, error) {
	return compilerInputHotInventorySnapshot(ctx, tx, false)
}

func compilerInputHotInventorySnapshot(ctx context.Context, tx *sql.Tx, rootsOnly bool) (CompilerInputHotInventory, error) {
	report := CompilerInputHotInventory{Action: "archive-hot-inventory", RootCounts: map[string]int{}, UncoveredSources: []string{"manual-input-pins-registry", "external-generation-ledger-validation", "artifact-signatures-and-local-original-bytes", "persistent-reactivation-admission"}}
	if rootsOnly {
		report.Action = "archive-hot-roots"
	}
	if err := tx.QueryRowContext(ctx, `SELECT transaction_timestamp(),pg_current_snapshot()::text,current_setting('transaction_isolation')='repeatable read' AND current_setting('transaction_read_only')='on'`).Scan(&report.ObservedAt, &report.Snapshot, &report.CoherentMetadata); err != nil {
		return report, err
	}
	if !report.CoherentMetadata {
		return report, fmt.Errorf("hot inventory requires one read-only repeatable-read snapshot: %w", ErrConflict)
	}
	rootStarted := time.Now()
	roots, err := compilerInputHotRoots(ctx, tx)
	report.Cost.RootInventoryUS = time.Since(rootStarted).Microseconds()
	if err != nil {
		return report, err
	}
	var pinsInstalled bool
	if err = tx.QueryRowContext(ctx, `SELECT to_regclass('fugue_platform_input_local_pins') IS NOT NULL`).Scan(&pinsInstalled); err != nil {
		return report, err
	}
	if pinsInstalled {
		if err = schemamigrate.VerifyCompilerInputLocalPins(ctx, tx); err != nil {
			return report, err
		}
		report.UncoveredSources = report.UncoveredSources[1:]
	}
	report.RootInventoryComplete = true
	for _, root := range roots {
		report.RootCounts[root.Source]++
	}
	if rootsOnly {
		report.Closure.Roots = roots
		return report, nil
	}
	var indexed bool
	if err = tx.QueryRowContext(ctx, `SELECT to_regclass('fugue_platform_input_hot_dependency_index_v1') IS NOT NULL`).Scan(&indexed); err != nil {
		return report, err
	}
	if indexed {
		if err = schemamigrate.VerifyCompilerInputHotIndex(ctx, tx); err != nil {
			return report, &compilerInputHotInventoryError{"dependency-index", err}
		}
	}
	report.TraversalPerformed = true
	traversalStarted := time.Now()
	report.Closure, err = inputarchive.PlanHotClosureBatchedObserved(ctx, roots,
		inputarchive.HotClosureBudget{Roots: compilerInputHotRootLimit, Nodes: 32768, Edges: 262144, Bindings: 524288},
		func(ctx context.Context, refs []inputarchive.HotReference) (map[inputarchive.HotReference]inputarchive.HotNode, error) {
			started := time.Now()
			nodes, resolveErr := resolveCompilerInputHotBatchWithIndex(ctx, tx, refs, &report.Cost, indexed)
			report.Cost.ResolverUS += time.Since(started).Microseconds()
			return nodes, resolveErr
		}, func(progress inputarchive.HotClosureProgress) { report.Progress = progress })
	report.Cost.TraversalUS = time.Since(traversalStarted).Microseconds()
	if err != nil {
		return report, &compilerInputHotInventoryError{"dependency-graph", err}
	}
	return report, err
}

func compilerInputHotRoots(ctx context.Context, tx *sql.Tx) ([]inputarchive.HotRoot, error) {
	// Historical messages/expected sets cannot reactivate an input by
	// themselves. Current lanes, active releases, observed consumer identities
	// and selected expectations retain it; future activation needs admission.
	const direct = `WITH roots(source,id,kind,identity,artifact_kind,scope_key,expected_hash) AS (
 SELECT 'active-lane',lane_key,'release',active_release_id,'','','' FROM fugue_platform_release_lanes WHERE active_release_id<>''
 UNION ALL SELECT 'active-release',id,'release',id,'','','' FROM fugue_platform_artifact_releases WHERE status='active'
 UNION ALL SELECT 'current-lkg',id,'artifact',artifact_id,'','',content_hash FROM fugue_platform_lkg_snapshots
 UNION ALL SELECT 'unexpired-lkg',artifact_id,'artifact',artifact_id,'','',content_hash FROM fugue_platform_lkg_snapshot_history WHERE expires_at>transaction_timestamp() GROUP BY artifact_id,content_hash
 UNION ALL SELECT 'consumer-'||g.role,c.id,'generation',g.generation,c.artifact_kind,c.scope_key,''
 FROM fugue_platform_consumer_instances c CROSS JOIN LATERAL (VALUES
 ('desired',c.desired_generation),('actual',c.actual_generation),('candidate',c.candidate_generation),('lkg',c.lkg_generation)) g(role,generation) WHERE g.generation<>''
 UNION ALL SELECT 'consumer-parent',id,'artifact',release_set_id,'','','' FROM fugue_platform_consumer_instances WHERE release_set_id<>''
 UNION ALL SELECT 'consumer-expectation',id,'expectation',expected_consumer_set_id,'','','' FROM fugue_platform_consumer_instances WHERE expected_consumer_set_id<>''
 UNION ALL SELECT 'shadow-consumer-'||g.role,jsonb_build_array(c.consumer_id,c.artifact_kind,c.scope_key)::text,'generation',g.generation,c.artifact_kind,c.scope_key,''
 FROM fugue_platform_shadow_consumers c CROSS JOIN LATERAL (VALUES
 ('desired',COALESCE(c.body->>'desired_generation','')),('actual',COALESCE(c.body->>'actual_generation','')),
 ('candidate',COALESCE(c.body->>'candidate_generation','')),('lkg',COALESCE(c.body->>'lkg_generation',''))) g(role,generation) WHERE g.generation<>''
 UNION ALL SELECT 'shadow-consumer-parent',jsonb_build_array(consumer_id,artifact_kind,scope_key)::text,'artifact',body->>'release_set_id','','','' FROM fugue_platform_shadow_consumers WHERE COALESCE(body->>'release_set_id','')<>''
 UNION ALL SELECT 'shadow-consumer-expectation',jsonb_build_array(consumer_id,artifact_kind,scope_key)::text,'expectation',body->>'expected_consumer_set_id','','','' FROM fugue_platform_shadow_consumers WHERE COALESCE(body->>'expected_consumer_set_id','')<>''
 UNION ALL SELECT 'active-expectation',e.id,'expectation',e.id,'','','' FROM fugue_platform_artifact_releases r
 CROSS JOIN LATERAL (SELECT id FROM fugue_platform_expected_consumer_sets WHERE artifact_release_id=r.id AND artifact_release_id<>'' OFFSET 0) e WHERE r.status='active'
 UNION ALL SELECT 'active-message',m.id,'artifact',m.artifact_id,'','','' FROM fugue_platform_release_messages m
 WHERE (m.expires_at IS NULL OR m.expires_at>transaction_timestamp()) AND EXISTS(SELECT 1 FROM fugue_platform_artifact_releases r WHERE r.id=m.release_id AND r.status='active')
 UNION ALL SELECT 'active-task',id,'task',id,'','','' FROM fugue_node_update_tasks WHERE status IN ('pending','running')
 UNION ALL SELECT 'archive-owner',content_hash,'input',content_hash,'','',content_hash FROM fugue_platform_input_archive_leases WHERE expires_at>transaction_timestamp()
) SELECT source,id,kind,identity,artifact_kind,scope_key,expected_hash FROM roots WHERE source=$2 LIMIT $1`
	roots := []inputarchive.HotRoot{}
	// Evaluate each source independently: a final sort over a UNION of all
	// historical tables otherwise defeats the output limit and spends one
	// statement budget on the combined history. PostgreSQL prunes the other
	// constant source branches; graph canonicalization supplies stable order.
	for _, source := range []string{"active-lane", "active-release", "current-lkg", "unexpired-lkg",
		"consumer-desired", "consumer-actual", "consumer-candidate", "consumer-lkg", "consumer-parent", "consumer-expectation",
		"shadow-consumer-desired", "shadow-consumer-actual", "shadow-consumer-candidate", "shadow-consumer-lkg", "shadow-consumer-parent", "shadow-consumer-expectation",
		"active-expectation", "active-message", "active-task", "archive-owner"} {
		rows, err := tx.QueryContext(ctx, direct, compilerInputHotRootLimit-len(roots)+1, source)
		if err != nil {
			return nil, &compilerInputHotInventoryError{"direct-" + source, err}
		}
		for rows.Next() {
			var root inputarchive.HotRoot
			if err = rows.Scan(&root.Source, &root.ID, &root.Reference.Kind, &root.Reference.ID, &root.Reference.ArtifactKind, &root.Reference.ScopeKey, &root.Reference.ExpectedHash); err != nil {
				break
			}
			roots = append(roots, root)
		}
		if err == nil {
			err = rows.Err()
		}
		if closeErr := rows.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			return nil, &compilerInputHotInventoryError{"direct-" + source, err}
		}
		if len(roots) > compilerInputHotRootLimit {
			return nil, &compilerInputHotInventoryError{"root-budget-" + source, ErrConflict}
		}
	}
	// Pending verification conservatively retains its source just like a
	// verified pin. Released tombstones are audit records, not retention roots.
	var pinsInstalled bool
	if err := tx.QueryRowContext(ctx, `SELECT to_regclass('fugue_platform_input_local_pins') IS NOT NULL`).Scan(&pinsInstalled); err != nil {
		return nil, err
	}
	if pinsInstalled {
		rows, err := tx.QueryContext(ctx, `SELECT pin_key,content_hash FROM fugue_platform_input_local_pins WHERE state IN ('pending','verified') LIMIT $1`, compilerInputHotRootLimit-len(roots)+1)
		if err != nil {
			return nil, &compilerInputHotInventoryError{"local-pins", err}
		}
		for rows.Next() {
			var key, digest string
			if err = rows.Scan(&key, &digest); err != nil {
				break
			}
			roots = append(roots, inputarchive.HotRoot{Source: "local-input-pin", ID: key, Reference: inputarchive.HotReference{Kind: "input", ID: digest, ExpectedHash: digest}})
		}
		if err == nil {
			err = rows.Err()
		}
		if closeErr := rows.Close(); err == nil {
			err = closeErr
		}
		if err != nil || len(roots) > compilerInputHotRootLimit {
			return nil, &compilerInputHotInventoryError{"local-pins", errors.Join(err, ErrConflict)}
		}
	}
	// Retain three distinct verified generations, in the existing rollback
	// policy's verification-time order. Repeated verification events and legacy
	// zero sequence values must not hide an older configuration. Each lateral
	// lookup uses the scope/verification-time index; excessive work times out
	// instead of returning a truncated proof.
	// Enumerate scope keys with indexed seeks. DISTINCT over the entire LKG
	// history scans every verification event before it can inspect three
	// generations, even when only a few scopes exist.
	const history = `WITH RECURSIVE history_scopes AS (
 (SELECT artifact_kind,scope_key FROM fugue_platform_lkg_snapshot_history ORDER BY artifact_kind,scope_key LIMIT 1)
 UNION ALL
 SELECT h.artifact_kind,h.scope_key FROM history_scopes s CROSS JOIN LATERAL (
  SELECT artifact_kind,scope_key FROM fugue_platform_lkg_snapshot_history
  WHERE (artifact_kind,scope_key)>(s.artifact_kind,s.scope_key) ORDER BY artifact_kind,scope_key LIMIT 1
 ) h
), scopes AS (
 SELECT artifact_kind,scope_key FROM fugue_platform_lkg_snapshots
 UNION SELECT artifact_kind,scope_key FROM history_scopes
), history AS (
 SELECT s.artifact_kind,s.scope_key,h.artifact_id,h.content_hash,ARRAY[h.generation] AS seen,1 AS depth
 FROM scopes s CROSS JOIN LATERAL (
  SELECT artifact_id,content_hash,generation FROM fugue_platform_lkg_snapshot_history
  WHERE artifact_kind=s.artifact_kind AND scope_key=s.scope_key ORDER BY updated_at DESC,created_at DESC,id DESC LIMIT 1
 ) h
 UNION ALL
 SELECT w.artifact_kind,w.scope_key,h.artifact_id,h.content_hash,array_append(w.seen,h.generation),w.depth+1
 FROM history w CROSS JOIN LATERAL (
  SELECT artifact_id,content_hash,generation FROM fugue_platform_lkg_snapshot_history
  WHERE artifact_kind=w.artifact_kind AND scope_key=w.scope_key AND NOT(generation=ANY(w.seen))
  ORDER BY updated_at DESC,created_at DESC,id DESC LIMIT 1
 ) h WHERE w.depth<3
) SELECT DISTINCT artifact_id,content_hash FROM history ORDER BY artifact_id,content_hash LIMIT $1`
	rows, err := tx.QueryContext(ctx, history, compilerInputHotRootLimit-len(roots)+1)
	if err != nil {
		return nil, &compilerInputHotInventoryError{"rollback-history", err}
	}
	for rows.Next() {
		var id, hash string
		if err = rows.Scan(&id, &hash); err != nil {
			break
		}
		roots = append(roots, inputarchive.HotRoot{Source: "rollback-history", ID: id, Reference: inputarchive.HotReference{Kind: "artifact", ID: id, ExpectedHash: hash}})
	}
	if err == nil {
		err = rows.Err()
	}
	if closeErr := rows.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return nil, &compilerInputHotInventoryError{"rollback-history", err}
	}
	if len(roots) > compilerInputHotRootLimit {
		return nil, &compilerInputHotInventoryError{"rollback-root-budget", ErrConflict}
	}
	return roots, nil
}

func resolveCompilerInputHotNode(ctx context.Context, tx inputQueryer, ref inputarchive.HotReference) (inputarchive.HotNode, error) {
	node := inputarchive.HotNode{Identity: ref, References: []inputarchive.HotReference{}}
	node.Identity.ExpectedHash = ""
	add := func(kind, id, artifactKind, scope string) {
		if id != "" {
			node.References = append(node.References, inputarchive.HotReference{Kind: kind, ID: id, ArtifactKind: artifactKind, ScopeKey: scope})
		}
	}
	switch ref.Kind {
	case "generation":
		var id string
		if err := tx.QueryRowContext(ctx, `SELECT id,content_hash FROM fugue_platform_artifacts WHERE artifact_kind=$1 AND scope_key=$2 AND generation=$3`, ref.ArtifactKind, ref.ScopeKey, ref.ID).Scan(&id, &node.ContentHash); err != nil {
			return node, err
		}
		return compilerInputHotGenerationNode(ref, id, node.ContentHash), nil
	case "release":
		var id, kind, scope, generation string
		var candidate, serving, lkg, pinned, rollback string
		if err := tx.QueryRowContext(ctx, `SELECT artifact_id,artifact_kind,scope_key,generation,candidate_generation,serving_unverified_generation,verified_lkg_generation,pinned_rollback_generation,rollback_target_generation FROM fugue_platform_artifact_releases WHERE id=$1`, ref.ID).Scan(&id, &kind, &scope, &generation, &candidate, &serving, &lkg, &pinned, &rollback); err != nil {
			return node, err
		}
		return compilerInputHotReleaseNode(ref, id, kind, scope, []string{generation, candidate, serving, lkg, pinned, rollback}), nil
	case "expectation":
		var parent, release, kind, scope, generation string
		if err := tx.QueryRowContext(ctx, `SELECT release_set_id,artifact_release_id,artifact_kind,scope_key,expected_generation FROM fugue_platform_expected_consumer_sets WHERE id=$1`, ref.ID).Scan(&parent, &release, &kind, &scope, &generation); err != nil {
			return node, err
		}
		add("artifact", parent, "", "")
		add("release", release, "", "")
		add("generation", generation, kind, scope)
	case "artifact":
		var artifact model.PlatformArtifact
		var content, metadata []byte
		artifact.ID = ref.ID
		err := tx.QueryRowContext(ctx, `SELECT artifact_kind,scope_key,content_hash,left(content_json::text,$2),left(metadata_json::text,16385) FROM fugue_platform_artifacts WHERE id=$1`, ref.ID, compilerInputHotArtifactBytes+1).Scan(&artifact.ArtifactKind, &artifact.ScopeKey, &artifact.ContentHash, &content, &metadata)
		if err != nil {
			return node, err
		}
		if len(content) > compilerInputHotArtifactBytes || len(metadata) > 16<<10 || json.Unmarshal(content, &artifact.Content) != nil || json.Unmarshal(metadata, &artifact.Metadata) != nil {
			return node, ErrConflict
		}
		return compilerInputArtifactHotNode(artifact)
	case "input":
		if !inputDigestPattern.MatchString(ref.ID) {
			return node, ErrConflict
		}
		node.ContentHash = ref.ID
		m, err := readInputManifest(ctx, tx, ref.ID)
		if errors.Is(err, sql.ErrNoRows) {
			node.Format, node.Location = "legacy-inline", "artifact-contents"
			err = tx.QueryRowContext(ctx, `SELECT size_bytes FROM fugue_platform_artifact_contents WHERE content_hash=$1`, ref.ID).Scan(&node.LogicalBytes)
			return node, err
		}
		if err != nil {
			return node, err
		}
		node.Format, node.Location, node.LogicalBytes = m.Format, m.Location, m.Size
		if m.Format == inputFormatV1 || m.Format == inputFormatCodecV2 {
			table, err := compilerInputChunksTable(m.Location)
			if err != nil {
				return node, err
			}
			var count, first, last int64
			if err := tx.QueryRowContext(ctx, `SELECT count(*),COALESCE(min(ordinal),-1),COALESCE(max(ordinal),-1),COALESCE(sum(octet_length(data)),0) FROM `+table+` WHERE content_hash=$1`, ref.ID).Scan(&count, &first, &last, &node.EncodedBytes); err != nil {
				return node, err
			}
			if count != int64(m.Count) || first != 0 || last != count-1 {
				return node, ErrConflict
			}
			node.NeedsLocalCopy = m.Location == "legacy"
		}
	case "task":
		var payload []byte
		var taskType string
		if err := tx.QueryRowContext(ctx, `SELECT task_type,left(COALESCE(payload_json,'{}'::jsonb)::text,65537) FROM fugue_node_update_tasks WHERE id=$1 AND status IN ('pending','running')`, ref.ID).Scan(&taskType, &payload); err != nil {
			return node, err
		}
		resolved, err := compilerInputTaskHotNode(ctx, tx, taskType, payload)
		if err != nil {
			return node, err
		}
		node.References = resolved.References
	default:
		return node, ErrInvalidInput
	}
	return node, nil
}

func compilerInputTaskHotNode(ctx context.Context, tx inputQueryer, taskType string, payload []byte) (inputarchive.HotNode, error) {
	var value any
	if len(payload) > 65536 || json.Unmarshal(payload, &value) != nil {
		return inputarchive.HotNode{}, ErrConflict
	}
	refs, err := compilerInputHotTaskReferences(value)
	if err != nil {
		return inputarchive.HotNode{}, err
	}
	implicit, err := compilerInputTaskConfigurationReferences(ctx, tx, taskType, value)
	if err != nil {
		return inputarchive.HotNode{}, err
	}
	return inputarchive.HotNode{References: append(refs, implicit...)}, nil
}

func compilerInputHotTaskReferences(value any) ([]inputarchive.HotReference, error) {
	refs := []inputarchive.HotReference{}
	visited := 0
	var walk func(any, int) error
	walk = func(value any, depth int) error {
		visited++
		if depth > 32 || visited > 4096 || len(refs) > 256 {
			return ErrConflict
		}
		switch v := value.(type) {
		case map[string]string:
			// NodeUpdateTask's public model uses string values, whereas the
			// database JSON decoder uses map[string]any. Apply the identical
			// dependency rules to both; a typed map must not bypass admission.
			object := make(map[string]any, len(v))
			for key, child := range v {
				object[key] = child
			}
			return walk(object, depth+1)
		case []any:
			for _, child := range v {
				if err := walk(child, depth+1); err != nil {
					return err
				}
			}
		case map[string]any:
			// Generation identity is meaningful only with its exact kind and
			// scope. Do not infer an artifact ID from a display generation.
			if generation, exists := v["generation"]; exists {
				kind, hasKind := v["artifact_kind"]
				scope, hasScope := v["scope_key"]
				if hasKind || hasScope {
					g, gOK := generation.(string)
					k, kOK := kind.(string)
					s, sOK := scope.(string)
					if !gOK || !kOK || !sOK || g == "" || k == "" || s == "" || len(g) > 512 || len(k) > 128 || len(s) > 512 {
						return ErrConflict
					}
					refs = append(refs, inputarchive.HotReference{Kind: "generation", ID: g, ArtifactKind: k, ScopeKey: s})
				}
			}
			for key, child := range v {
				kind := ""
				switch key {
				case "input_snapshot_digest":
					kind = "input"
				case "artifact_id", "release_set_id", "route_artifact_id", "tls_artifact_id", "dns_artifact_id", "policy_artifact_id":
					kind = "artifact"
				case "release_id", "artifact_release_id":
					kind = "release"
				}
				if kind != "" {
					id, ok := child.(string)
					if !ok || len(id) > 512 || kind == "input" && id != "" && !inputDigestPattern.MatchString(id) {
						return ErrConflict
					}
					if id != "" {
						refs = append(refs, inputarchive.HotReference{Kind: kind, ID: id})
					}
				}
				if err := walk(child, depth+1); err != nil {
					return err
				}
				// Public task payloads encode structured arguments as strings.
				// Decode only the explicit JSON argument convention; arbitrary
				// text/log strings are not treated as guessed dependencies.
				if strings.HasSuffix(key, "_json") {
					raw, ok := child.(string)
					if !ok || len(raw) > 65536 {
						return ErrConflict
					}
					var nested any
					if json.Unmarshal([]byte(raw), &nested) != nil {
						return ErrConflict
					}
					if err := walk(nested, depth+1); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}
	err := walk(value, 0)
	if len(refs) > 256 {
		return nil, ErrConflict
	}
	return refs, err
}
