package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/inputarchive"
	"fugue/internal/model"
	"fugue/internal/schemamigrate"
	"fugue/internal/testfixture/celldns"
)

func hotInventoryTestDatabase(t *testing.T) (*sql.DB, string, string) {
	t.Helper()
	dsn := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("requires disposable local PostgreSQL")
	}
	u, err := url.Parse(dsn)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.Contains(u.Path, "fugue_test") {
		t.Fatal("requires disposable loopback fugue_test database")
	}
	base, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = base.Close() })
	schema := model.NewID("hot_inventory_test")
	quoted := quotePostgresIntegrationIdentifier(t, schema)
	if _, err = base.Exec("CREATE SCHEMA " + quoted); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := base.Exec("DROP SCHEMA " + quoted + " CASCADE"); err != nil {
			t.Error(err)
		}
	})
	address := postgresIntegrationURLWithSearchPath(t, dsn, schema)
	db, err := sql.Open("pgx", address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	// Use the real schema and additive migrations, not a second hand-written
	// model that could hide a misspelled production column or table.
	for _, statement := range []string{postgresMetaSchemaStatement, postgresSchemaBatch(), schemamigrate.BoundedContentSQL, schemamigrate.CompilerInputArchiveSQL, schemamigrate.PlatformShadowConsumersSQL} {
		if _, err = db.Exec(statement); err != nil {
			t.Fatal("disposable hot-inventory schema", err)
		}
	}
	if _, err = db.Exec(schemamigrate.CompilerInputZeroTempSQL); err != nil {
		t.Fatal("disposable admin zero-temp installation", err)
	}
	return db, address, schema
}

func seedHotInventoryHistory(t *testing.T, db *sql.DB) string {
	t.Helper()
	raw := []byte(`{"a":1}`)
	sum := sha256.Sum256(raw)
	digest := "sha256:" + hex.EncodeToString(sum[:])
	for i := 1; i <= 4; i++ {
		id := fmt.Sprintf("artifact-neutral-%d", i)
		if _, err := db.Exec(`INSERT INTO fugue_platform_artifacts(id,artifact_kind,scope_key,generation,status,content_hash,content_json,metadata_json,created_at,updated_at)
 VALUES($1,'edge_route_bundle','scope-neutral',$1,'validated',$2,'{}',jsonb_build_object('input_snapshot_digest',$3::text),now(),now())`, id, "sha256:"+strings.Repeat("a", 64), digest); err != nil {
			t.Fatal(err)
		}
		// Three re-verifications of generation 1 all precede generation 2.
		// Every generation has the legitimate predecessor's zero sequence.
		events := 1
		if i == 1 {
			events = 3
		}
		for event := 0; event < events; event++ {
			if _, err := db.Exec(`INSERT INTO fugue_platform_lkg_snapshot_history(id,artifact_id,artifact_kind,scope_key,generation,content_hash,expires_at,created_at,updated_at)
 VALUES($1,$2,'edge_route_bundle','scope-neutral',$2,$3,now()-interval '1 day',now()-($4*interval '1 hour'),now()-($4*interval '1 hour'))`, fmt.Sprintf("history-neutral-%d-%d", i, event), id, "sha256:"+strings.Repeat("a", 64), i*4+event); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := db.Exec(`INSERT INTO fugue_platform_lkg_snapshots(id,artifact_id,artifact_kind,scope_key,generation,content_hash,expires_at,created_at,updated_at) SELECT 'lkg-neutral',id,artifact_kind,scope_key,generation,content_hash,now()+interval '1 day',now(),now() FROM fugue_platform_artifacts WHERE id='artifact-neutral-1'`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO fugue_platform_input_manifests(content_hash,format,size_bytes,chunk_size,chunk_count,created_at) VALUES($1,$2,$3,$4,1,now());`, digest, inputFormatV1, len(raw), inputChunkBytes); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO fugue_platform_input_chunks(content_hash,ordinal,chunk_hash,data) VALUES($1,0,$1,$2)`, digest, raw); err != nil {
		t.Fatal(err)
	}
	return digest
}

func hotInventoryHasRoot(report CompilerInputHotInventory, source, id string) bool {
	for _, root := range report.Closure.Roots {
		if root.Source == source && root.Reference.ID == id {
			return true
		}
	}
	return false
}

func TestHotInventoryPostgresUsesOneSnapshotAndDistinctRollbackGenerations(t *testing.T) {
	db, address, _ := hotInventoryTestDatabase(t)
	digest := seedHotInventoryHistory(t, db)
	tx, err := db.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	// Establish the read snapshot, then change the serving root concurrently.
	var version string
	if err = tx.QueryRow(`SELECT pg_current_snapshot()::text`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`UPDATE fugue_platform_lkg_snapshots SET artifact_id='artifact-neutral-4',generation='artifact-neutral-4'`); err != nil {
		t.Fatal(err)
	}
	report, err := compilerInputHotInventoryFromSnapshot(t.Context(), tx)
	if err != nil || !report.CoherentMetadata || report.Snapshot != version || !report.Closure.MetadataComplete || report.Closure.RequiredLocalCopyBytes != 7 {
		t.Fatal("coherent metadata/unique input accounting failed", report, err)
	}
	if !hotInventoryHasRoot(report, "current-lkg", "artifact-neutral-1") || hotInventoryHasRoot(report, "current-lkg", "artifact-neutral-4") {
		t.Fatal("roots from different snapshots were combined", report)
	}
	for i := 1; i <= 3; i++ {
		if !hotInventoryHasRoot(report, "rollback-history", fmt.Sprintf("artifact-neutral-%d", i)) {
			t.Fatal("repeat verification/zero sequence hid an older rollback generation", report)
		}
	}
	if hotInventoryHasRoot(report, "rollback-history", "artifact-neutral-4") || report.BodyVerification || report.ReactivationProtected || report.RetirementAuthorized || report.Closure.LocalCopiesVerified || len(report.UncoveredSources) == 0 {
		t.Fatal("metadata inventory granted full hot verification or retirement", report)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	next, err := RunCompilerInputHotInventory(t.Context(), address)
	if err != nil || !hotInventoryHasRoot(next, "current-lkg", "artifact-neutral-4") || next.Closure.PlanSHA256 == report.Closure.PlanSHA256 {
		t.Fatal("changed hot root did not invalidate the plan", next, err)
	}
	// An active release pins a generation older than the minimum history set.
	if _, err = db.Exec(`INSERT INTO fugue_platform_artifact_releases(id,artifact_id,artifact_kind,scope_key,generation,release_channel,status,pinned_rollback_generation,rollback_target_generation,candidate_generation,serving_unverified_generation,verified_lkg_generation,released_at,created_at,updated_at)
 VALUES('release-neutral','artifact-neutral-1','edge_route_bundle','scope-neutral','artifact-neutral-1','stable','active','artifact-neutral-4','artifact-neutral-3','artifact-neutral-2','artifact-neutral-2','artifact-neutral-3',now(),now(),now())`); err != nil {
		t.Fatal(err)
	}
	next, err = RunCompilerInputHotInventory(t.Context(), address)
	if err != nil || !next.Closure.MetadataComplete {
		t.Fatal(next, err)
	}
	found := map[string]bool{}
	for _, node := range next.Closure.Nodes {
		if node.Identity.Kind == "generation" {
			found[node.Identity.ID] = true
		}
		if node.Identity.Kind == "input" && node.Identity.ID != digest {
			t.Fatal("invented an input identity", node)
		}
	}
	for i := 1; i <= 4; i++ {
		if !found[fmt.Sprintf("artifact-neutral-%d", i)] {
			t.Fatal("release activation or rollback identity omitted", found)
		}
	}
}

func TestHotInventoryPostgresRejectsPartialOrUntrustedProof(t *testing.T) {
	db, address, _ := hotInventoryTestDatabase(t)
	digest := seedHotInventoryHistory(t, db)
	if _, err := db.Exec(`UPDATE fugue_platform_lkg_snapshots SET content_hash=$1`, "sha256:"+strings.Repeat("b", 64)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DELETE FROM fugue_platform_input_chunks WHERE content_hash=$1`, digest); err != nil {
		t.Fatal(err)
	}
	report, err := RunCompilerInputHotInventory(t.Context(), address)
	if err != nil || report.Closure.MetadataComplete || report.Closure.LocalCopiesVerified || len(report.Closure.Blockers) != 2 {
		t.Fatal("LKG hash conflict or incomplete input coverage was hidden", report, err)
	}
	// Incorrect transaction isolation must fail before collecting any roots.
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if got, err := compilerInputHotInventoryFromSnapshot(t.Context(), tx); err == nil || got.CoherentMetadata || got.Closure.PlanSHA256 != "" {
		t.Fatal("ordinary transaction was exposed as coherent proof", got, err)
	}
	// The root collector must refuse before traversing an oversized set.
	if _, err := db.Exec(`INSERT INTO fugue_platform_release_lanes(lane_key,artifact_kind,scope_key,release_channel,active_release_id,updated_at)
 SELECT 'lane-neutral-'||n,'edge_route_bundle','scope-neutral','stable','release-neutral',now() FROM generate_series(1,$1) n`, compilerInputHotRootLimit+1); err != nil {
		t.Fatal(err)
	}
	if got, err := RunCompilerInputHotInventory(t.Context(), address); err == nil || got.Closure.MetadataComplete || got.Closure.PlanSHA256 != "" {
		t.Fatal("root budget overflow produced a partial proof", got, err)
	}
}

func TestHotInventoryPostgresRootCensusDoesNotPretendToTraverseOrVerify(t *testing.T) {
	db, address, _ := hotInventoryTestDatabase(t)
	seedHotInventoryHistory(t, db)
	// A real inventory can exceed 4,096 roots. The separate root census must
	// retain them all without pretending that their dependencies were checked.
	if _, err := db.Exec(`INSERT INTO fugue_platform_release_lanes(lane_key,artifact_kind,scope_key,release_channel,active_release_id,updated_at)
 SELECT 'lane-neutral-'||n,'edge_route_bundle','scope-neutral','stable','release-not-yet-resolved',now() FROM generate_series(1,5000) n`); err != nil {
		t.Fatal(err)
	}
	report, err := RunCompilerInputHotRootsInventory(t.Context(), address)
	if err != nil || !report.CoherentMetadata || !report.RootInventoryComplete || report.RootCounts["active-lane"] != 5000 || len(report.Closure.Roots) < 5000 {
		t.Fatal("root census was truncated", report, err)
	}
	if report.Action != "archive-hot-roots" || report.TraversalPerformed || report.Closure.MetadataComplete || report.Closure.LocalCopiesVerified || report.BodyVerification || report.ReactivationProtected || report.RetirementAuthorized || report.Closure.PlanSHA256 != "" {
		t.Fatal("a root census was exposed as a graph or retirement proof", report)
	}
	full, err := RunCompilerInputHotInventory(t.Context(), address)
	if err != nil || !full.RootInventoryComplete || !full.TraversalPerformed || full.Closure.MetadataComplete || len(full.Closure.Blockers) != 1 {
		t.Fatal("unresolved root was silently accepted by the full traversal", full, err)
	}
}

func TestHotInventoryFailureStageDoesNotIncludeDriverText(t *testing.T) {
	err := &compilerInputHotInventoryError{stage: "direct-active-lane", cause: fmt.Errorf("synthetic private DSN")}
	if HotInventoryFailureStage(err) != "direct-active-lane" || strings.Contains(err.Error(), "private") || HotInventoryFailureStage(fmt.Errorf("synthetic private DSN")) != "connection-or-snapshot" {
		t.Fatal("operator diagnostic exposed error details")
	}
}

func TestHotInventoryPostgresUnexpiredEventsPreserveHashConflictsAndSnapshot(t *testing.T) {
	db, _, _ := hotInventoryTestDatabase(t)
	seedHotInventoryHistory(t, db)
	if _, err := db.Exec(`UPDATE fugue_platform_lkg_snapshot_history SET expires_at=now()+interval '1 day' WHERE artifact_id='artifact-neutral-1'`); err != nil {
		t.Fatal(err)
	}
	// Same artifact with two expected hashes must remain two roots, while
	// duplicate verification events of the same binding count only once.
	other := "sha256:" + strings.Repeat("b", 64)
	if _, err := db.Exec(`UPDATE fugue_platform_lkg_snapshot_history SET content_hash=$1 WHERE id='history-neutral-1-2'`, other); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	roots, err := compilerInputUnexpiredLKGRoots(t.Context(), tx, compilerInputHotRootLimit)
	if err != nil || len(roots) != 2 || roots[0].Reference.ExpectedHash == roots[1].Reference.ExpectedHash {
		t.Fatal("dedup collapsed a conflicting binding or retained expired rows", roots, err)
	}
	if _, err := db.Exec(`UPDATE fugue_platform_lkg_snapshot_history SET expires_at=now()+interval '1 day' WHERE artifact_id='artifact-neutral-4'`); err != nil {
		t.Fatal(err)
	}
	again, err := compilerInputUnexpiredLKGRoots(t.Context(), tx, compilerInputHotRootLimit)
	if err != nil || len(again) != 2 {
		t.Fatal("unexpired census crossed snapshots", again, err)
	}
	limited, err := compilerInputUnexpiredLKGRoots(t.Context(), tx, 1)
	if err != nil || len(limited) != 2 {
		t.Fatal("distinct-root overflow sentinel disappeared", limited, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	fresh, err := db.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Rollback()
	roots, err = compilerInputUnexpiredLKGRoots(t.Context(), fresh, compilerInputHotRootLimit)
	if err != nil || len(roots) != 3 {
		t.Fatal("fresh snapshot failed to retain newly unexpired history", roots, err)
	}
}

func TestHotInventoryPostgresStreamsLargeUnexpiredHistoryWithinOriginalGuards(t *testing.T) {
	db, address, _ := hotInventoryTestDatabase(t)
	insert := func(first, last, events int) {
		t.Helper()
		if _, err := db.Exec(`INSERT INTO fugue_platform_artifacts(id,artifact_kind,scope_key,generation,status,content_hash,content_json,metadata_json,created_at,updated_at)
 SELECT 'artifact-neutral-'||lpad(n::text,64,'0'),'edge_route_bundle','scope-neutral',n::text,'validated','sha256:'||repeat('a',64),'{}','{}',now(),now() FROM generate_series($1::int,$2::int) n`, first, last); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO fugue_platform_lkg_snapshot_history(id,artifact_id,artifact_kind,scope_key,generation,content_hash,expires_at,created_at,updated_at)
 SELECT 'history-neutral-'||n||'-'||event,'artifact-neutral-'||lpad(n::text,64,'0'),'edge_route_bundle','scope-neutral',n::text,'sha256:'||repeat('a',64),now()+interval '1 day',now(),now() FROM generate_series($1::int,$2::int) n CROSS JOIN generate_series(1,$3::int) event`, first, last, events); err != nil {
			t.Fatal(err)
		}
	}
	insert(1, 8000, 4)
	started := time.Now()
	report, err := RunCompilerInputHotRootsInventory(t.Context(), address)
	if err != nil || !report.RootInventoryComplete || report.RootCounts["unexpired-lkg"] != 8000 || report.RootCounts["rollback-history"] != 3 || len(report.Closure.Roots) != 8003 || report.RetirementAuthorized || report.Closure.MetadataComplete {
		t.Fatal("large deduplicated census refused or lost a retention authority", report.RootCounts, err)
	}
	t.Logf("32000 unexpired verification events, 8000 retained bindings under SQL5s/temp0/workmem1MiB: %s", time.Since(started))
	insert(8001, compilerInputHotRootLimit+1, 1)
	report, err = RunCompilerInputHotRootsInventory(t.Context(), address)
	if err == nil || HotInventoryFailureStage(err) != "root-budget-unexpired-lkg" || report.RootInventoryComplete || report.Closure.PlanSHA256 != "" || len(report.Closure.Roots) != 0 {
		t.Fatal("overflow was treated as a complete retained set", report.RootCounts, err)
	}
}

func TestHotInventoryPostgresBatchMatchesSingleIdentityValidation(t *testing.T) {
	db, _, _ := hotInventoryTestDatabase(t)
	digest := seedHotInventoryHistory(t, db)
	tx, err := db.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	refs := []inputarchive.HotReference{
		{Kind: "artifact", ID: "artifact-neutral-1"},
		{Kind: "input", ID: digest},
		{Kind: "generation", ID: "artifact-neutral-1", ArtifactKind: "edge_route_bundle", ScopeKey: "scope-neutral"},
		{Kind: "artifact", ID: "artifact-missing-neutral"},
		{Kind: "input", ID: "sha256:" + strings.Repeat("f", 64)},
	}
	batch, err := resolveCompilerInputHotBatch(t.Context(), tx, refs)
	if err != nil || len(batch) != 3 {
		t.Fatal("bounded metadata batch coverage changed", batch, err)
	}
	for _, ref := range refs {
		single, err := resolveCompilerInputHotNode(t.Context(), tx, ref)
		if err != nil {
			if _, found := batch[ref]; found {
				t.Fatal("batch hid a missing identity", ref)
			}
			continue
		}
		if !reflect.DeepEqual(batch[ref], single) {
			t.Fatal("batch and single-identity validation disagree", ref, batch[ref], single)
		}
	}
	var cost CompilerInputHotCost
	observed, err := resolveCompilerInputHotBatchObserved(t.Context(), tx, refs, &cost)
	if err != nil || !reflect.DeepEqual(batch, observed) || cost.Artifacts.Queries != 1 || cost.Artifacts.Rows != 1 || cost.Inputs.Queries != 2 || cost.Inputs.Rows != 1 || cost.Ledger.Queries != 1 || cost.Ledger.Rows != 1 || cost.Artifacts.SourceBytes < 1 || cost.Artifacts.TransportBytes < 1 {
		t.Fatal("aggregate observation changed resolution or hid query/row coverage", cost, err)
	}
	decode := cost.Artifacts.DecodeByKind[model.PlatformArtifactKindEdgeRouteBundle]
	if len(cost.Artifacts.DecodeByKind) != 1 || decode == nil || decode.Attempts != 1 || decode.Rejected != 0 || decode.SourceBytes != cost.Artifacts.SourceBytes || decode.TransportBytes != cost.Artifacts.TransportBytes {
		t.Fatal("decode breakdown lost artifact or source/transport accounting", cost.Artifacts)
	}
	// A statement-level failure must abort the batch. It cannot turn every
	// affected input into a successful "not found" inventory.
	if _, err = tx.Exec(`SET LOCAL statement_timeout='1ms'`); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(`SELECT pg_sleep(0.02)`); err == nil {
		t.Fatal("failed to exercise transaction-abort handling")
	}
	if _, err = resolveCompilerInputHotBatch(t.Context(), tx, refs); err == nil {
		t.Fatal("aborted transaction was treated as missing graph identities")
	}
}

func TestHotInventoryPostgresLargeRouteDecodeMatchesSingle(t *testing.T) {
	db, _, _ := hotInventoryTestDatabase(t)
	req, _, _ := celldns.SourceAuthorizedRequest(t, false)
	for _, base := range []model.PlatformArtifact{req.CellRoutePublications[0].Route, req.CellRoutePublications[0].TLS} {
		for _, variant := range []string{"valid", "null-lineage", "bad-lineage", "ignored-overflow"} {
			a := base
			a.ID = "neutral-large-" + a.ArtifactKind + "-" + variant
			// Keep a substantial unrelated object tree in the real JSONB row.
			content, err := json.Marshal(base.Content)
			if err != nil {
				t.Fatal(err)
			}
			a.Content = nil
			if err = json.Unmarshal(content, &a.Content); err != nil {
				t.Fatal(err)
			}
			view := make([]any, 1000)
			for i := range view {
				view[i] = map[string]any{"host": "neutral.example.test", "ports": []int{80, 443}, "enabled": true}
			}
			a.Content["ignored_view"] = view
			if variant == "null-lineage" {
				a.Content["lineage"] = nil
			} else if variant == "bad-lineage" {
				a.Content["lineage"] = map[string]any{"input_snapshot_digest": "invalid"}
			}
			content, err = json.Marshal(a.Content)
			if err != nil {
				t.Fatal(err)
			}
			if variant == "ignored-overflow" {
				content = append(content[:len(content)-1], []byte(`,"numeric_overflow":1e400}`)...)
			}
			metadata, err := json.Marshal(a.Metadata)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = db.Exec(`INSERT INTO fugue_platform_artifacts(id,artifact_kind,scope_key,generation,status,content_hash,content_json,metadata_json,created_at,updated_at)
 VALUES($1,$2,$3,$1,'validated',$4,$5,$6,now(),now())`, a.ID, a.ArtifactKind, a.ScopeKey, a.ContentHash, content, metadata); err != nil {
				t.Fatal(err)
			}
			tx, err := db.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = tx.Exec(`SET LOCAL statement_timeout='5s'; SET LOCAL lock_timeout='250ms'; SET LOCAL work_mem='1MB'; SET LOCAL temp_file_limit=0`); err != nil {
				t.Fatal(err)
			}
			ref := inputarchive.HotReference{Kind: "artifact", ID: a.ID}
			want, singleErr := resolveCompilerInputHotNode(t.Context(), tx, ref)
			var cost CompilerInputHotCost
			got, batchErr := resolveCompilerInputHotBatchObserved(t.Context(), tx, []inputarchive.HotReference{ref}, &cost)
			_ = tx.Rollback()
			if batchErr != nil || singleErr != nil && len(got) != 0 || singleErr == nil && !reflect.DeepEqual(want, got[ref]) {
				t.Fatal("large route/TLS decoder changed single-read semantics", a.ArtifactKind, variant, singleErr, batchErr)
			}
			decode := cost.Artifacts.DecodeByKind[a.ArtifactKind]
			var rejected int64
			if singleErr != nil {
				rejected = 1
			}
			if decode == nil || decode.Attempts != 1 || decode.SourceBytes < 64<<10 || decode.TransportBytes < 64<<10 || decode.Rejected != rejected {
				t.Fatal("large source accounting or rejection changed", a.ArtifactKind, variant, cost.Artifacts)
			}
		}
	}
}

func TestHotInventoryPostgresDNSProjectionPreservesTypedDependenciesAndBounds(t *testing.T) {
	db, _, _ := hotInventoryTestDatabase(t)
	for _, transition := range []bool{false, true} {
		req, _, _ := celldns.SourceAuthorizedRequest(t, transition)
		artifact := celldns.Compile(t, req).DNSArtifact
		artifact.ID = fmt.Sprintf("neutral-projected-dns-%t", transition)
		artifact.Content["query_views"] = strings.Repeat("irrelevant-answer-view", 16384)
		artifact.Content["readiness_plan"] = strings.Repeat("irrelevant-probe-view", 8192)
		original := artifact.Content
		for _, mutation := range []string{"valid", "missing-lineage", "null-lineage", "missing-policy", "null-policy", "missing-cell_route_publications", "null-cell_route_publications", "missing-previous_traffic_publication", "null-previous_traffic_publication", "missing-cell_dns_source", "null-cell_dns_source"} {
			artifact.Content = map[string]any{}
			for key, value := range original {
				artifact.Content[key] = value
			}
			if key, ok := strings.CutPrefix(mutation, "missing-"); ok {
				delete(artifact.Content, key)
			}
			if key, ok := strings.CutPrefix(mutation, "null-"); ok {
				artifact.Content[key] = nil
			}
			content, err := json.Marshal(artifact.Content)
			if err != nil {
				t.Fatal(err)
			}
			metadata, err := json.Marshal(artifact.Metadata)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = db.Exec(`INSERT INTO fugue_platform_artifacts(id,artifact_kind,scope_key,generation,status,content_hash,content_json,metadata_json,created_at,updated_at)
 VALUES($1,'dns_answer_bundle','scope-neutral',$1,'validated',$2,$3,$4,now(),now())
 ON CONFLICT(id) DO UPDATE SET content_json=EXCLUDED.content_json`, artifact.ID, artifact.ContentHash, content, metadata); err != nil {
				t.Fatal(err)
			}
			tx, err := db.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
			if err != nil {
				t.Fatal(err)
			}
			// Unrelated answer views must not be expanded into a tuplestore.
			// Exercise the production no-temporary-file guard with a small
			// local work_mem budget, including null lineage and transitions.
			if _, err = tx.Exec(`SET LOCAL work_mem='64kB'; SET LOCAL temp_file_limit=0`); err != nil {
				t.Fatal(err)
			}
			ref := inputarchive.HotReference{Kind: "artifact", ID: artifact.ID}
			want, singleErr := resolveCompilerInputHotNode(t.Context(), tx, ref)
			got, batchErr := resolveCompilerInputHotBatch(t.Context(), tx, []inputarchive.HotReference{ref})
			_ = tx.Rollback()
			if batchErr != nil || singleErr != nil && len(got) != 0 || singleErr == nil && !reflect.DeepEqual(want, got[ref]) {
				t.Fatal("DNS projection changed typed dependency behavior", transition, mutation, singleErr, batchErr)
			}
		}
	}
	// Omitting a large answer body cannot turn an oversized source into a
	// valid bounded artifact. Source size, not projected size, owns the cap.
	large := `{"query_views":"` + strings.Repeat("x", compilerInputHotArtifactBytes+1) + `"}`
	if _, err := db.Exec(`INSERT INTO fugue_platform_artifacts(id,artifact_kind,scope_key,generation,status,content_hash,content_json,metadata_json,created_at,updated_at)
 VALUES('neutral-oversized-dns','dns_answer_bundle','scope-neutral','neutral-oversized-dns','validated',$1,$2,'{}',now(),now())`, "sha256:"+strings.Repeat("a", 64), large); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	ref := inputarchive.HotReference{Kind: "artifact", ID: "neutral-oversized-dns"}
	got, err := resolveCompilerInputHotBatch(t.Context(), tx, []inputarchive.HotReference{ref})
	if err != nil || len(got) != 0 {
		t.Fatal("projected source bypassed original artifact size bound", got, err)
	}
}

func TestHotInventoryPostgresLargeDNSDependencyFieldsDoNotMaterialize(t *testing.T) {
	db, address, _ := hotInventoryTestDatabase(t)
	installHotIndexTest(t, db, address)
	req, _, _ := celldns.SourceAuthorizedRequest(t, false)
	artifact := celldns.Compile(t, req).DNSArtifact
	content, err := json.Marshal(artifact.Content)
	if err != nil {
		t.Fatal(err)
	}
	for _, variant := range []string{"large-policy", "large-lineage", "null-policy", "missing-policy"} {
		var value map[string]any
		if err = json.Unmarshal(content, &value); err != nil {
			t.Fatal(err)
		}
		switch variant {
		case "large-policy", "large-lineage":
			field := strings.TrimPrefix(variant, "large-")
			object, ok := value[field].(map[string]any)
			if !ok {
				t.Fatal("fixture dependency must be an object", field)
			}
			object["synthetic_padding"] = strings.Repeat("x", 2<<20)
		case "null-policy":
			value["policy"] = nil
		case "missing-policy":
			delete(value, "policy")
		}
		body, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		id := "neutral-large-dependency-" + variant
		if _, err = db.Exec(`INSERT INTO fugue_platform_artifacts(id,artifact_kind,scope_key,generation,status,content_hash,content_json,metadata_json,created_at,updated_at)
 VALUES($1,'dns_answer_bundle','neutral-scope',$1,'validated',$2,$3,'{}',now(),now())`, id, artifact.ContentHash, body); err != nil {
			t.Fatal(err)
		}
		for _, indexed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/indexed-%t", variant, indexed), func(t *testing.T) {
				tx, err := db.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				if _, err = tx.Exec(`SET LOCAL statement_timeout='5s'; SET LOCAL lock_timeout='250ms'; SET LOCAL work_mem='1MB'; SET LOCAL temp_file_limit=0; SET LOCAL max_parallel_workers_per_gather=0; SET LOCAL jit=off`); err != nil {
					t.Fatal(err)
				}
				ref := inputarchive.HotReference{Kind: "artifact", ID: id}
				want, singleErr := resolveCompilerInputHotNode(t.Context(), tx, ref)
				got, batchErr := resolveCompilerInputHotBatchWithIndex(t.Context(), tx, []inputarchive.HotReference{ref}, nil, indexed)
				if batchErr != nil || singleErr != nil && len(got) != 0 || singleErr == nil && !reflect.DeepEqual(want, got[ref]) {
					t.Fatal("large dependency transport changed bounded single-read semantics", singleErr, batchErr)
				}
			})
		}
	}
}

func TestHotInventoryPostgresWorksWithReadOnlyOrdinaryRole(t *testing.T) {
	db, address, schema := hotInventoryTestDatabase(t)
	seedHotInventoryHistory(t, db)
	var canCreateRole bool
	if err := db.QueryRow(`SELECT rolsuper OR rolcreaterole FROM pg_roles WHERE rolname=current_user`).Scan(&canCreateRole); err != nil {
		t.Fatal(err)
	}
	if !canCreateRole {
		t.Skip("limited-role creation requires disposable PostgreSQL role administration")
	}
	role := model.NewID("hot_inventory_reader_test")
	quotedRole := quotePostgresIntegrationIdentifier(t, role)
	quotedSchema := quotePostgresIntegrationIdentifier(t, schema)
	if _, err := db.Exec("CREATE ROLE " + quotedRole + " LOGIN NOSUPERUSER NOCREATEROLE NOCREATEDB"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, statement := range []string{"DROP OWNED BY " + quotedRole, "DROP ROLE " + quotedRole} {
			if _, err := db.Exec(statement); err != nil {
				t.Error(err)
			}
		}
	})
	if _, err := db.Exec("GRANT USAGE ON SCHEMA " + quotedSchema + " TO " + quotedRole + "; GRANT SELECT ON ALL TABLES IN SCHEMA " + quotedSchema + " TO " + quotedRole); err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(address)
	u.User = url.User(role)
	report, err := RunCompilerInputHotInventory(context.Background(), u.String())
	if err != nil || !report.CoherentMetadata || !report.Closure.MetadataComplete {
		t.Fatal("ordinary read-only role could not run metadata inventory", report, err)
	}
	if report.RetirementAuthorized || report.BodyVerification || report.ReactivationProtected || report.Closure.LocalCopiesVerified {
		t.Fatal("read-only metadata was promoted to destructive authority", report)
	}
	retention, err := RunCompilerInputRetentionInventory(t.Context(), u.String())
	if err != nil || !retention.UniverseComplete || retention.ClassificationComplete || retention.ExecutionAuthorized || retention.Totals["H"].Inputs != 1 || retention.Totals["D"].Inputs != 0 {
		t.Fatal("ordinary read-only role could not audit the universe without destructive authority", retention, err)
	}
}

func TestHotInventoryPostgresKeepsScopedShadowRootsAndUnresolvedNativeIdentities(t *testing.T) {
	db, address, _ := hotInventoryTestDatabase(t)
	seedHotInventoryHistory(t, db)
	for _, scope := range []string{"authority-cell:cell-neutral-a", "authority-cell:cell-neutral-b"} {
		if _, err := db.Exec(`INSERT INTO fugue_platform_shadow_consumers(consumer_id,artifact_kind,scope_key,body)
 VALUES('consumer-neutral','dns_answer_bundle',$1,jsonb_build_object('consumer_id','consumer-neutral','artifact_kind','dns_answer_bundle','scope_key',$1::text,'release_set_id','artifact-neutral-1','actual_generation','native-generation-neutral'))`, scope); err != nil {
			t.Fatal(err)
		}
	}
	report, err := RunCompilerInputHotInventory(t.Context(), address)
	if err != nil || report.Closure.MetadataComplete || report.Closure.LocalCopiesVerified || len(report.Closure.Blockers) != 2 {
		t.Fatal("native execution identities were guessed, dropped or rejected as duplicate shadow roots", report, err)
	}
	parentRoots := 0
	for _, root := range report.Closure.Roots {
		if root.Source == "shadow-consumer-parent" && root.Reference.ID == "artifact-neutral-1" {
			parentRoots++
		}
	}
	if parentRoots != 2 {
		t.Fatal("two scopes sharing one consumer and parent were merged", report)
	}
	for _, blocker := range report.Closure.Blockers {
		if blocker.Reference.Kind != "generation" || blocker.Reference.ID != "native-generation-neutral" || blocker.Reference.ArtifactKind != "dns_answer_bundle" || blocker.Reference.ScopeKey == "" {
			t.Fatal("unresolved native reference lost its exact scope or became an input", blocker)
		}
	}
}
