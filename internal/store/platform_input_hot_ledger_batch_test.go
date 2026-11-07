package store

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/inputarchive"
	"fugue/internal/model"
)

type hotInventoryQueryCounter struct {
	inputQueryer
	queries int
}

func TestHotInventoryPostgresReleasePolicyScopeSurvivesAllReadPaths(t *testing.T) {
	db, address, _ := hotInventoryTestDatabase(t)
	installHotIndexTest(t, db, address)
	s := &Store{db: db, databaseURL: address}
	configureTestPlatformArtifactSigning(s)
	refs := []inputarchive.HotReference{}
	for _, scope := range []string{"scope-neutral-a", "scope-neutral-b"} {
		id := "parent-" + scope
		a, err := s.CreatePlatformArtifact(model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindReleaseSet,
			Scope: model.PlatformArtifactScope{Key: scope}, Generation: id, Content: map[string]any{"artifact_ids": []string{"child-" + scope}},
			Metadata: map[string]string{"policy_generation": "shared-policy-generation"}})
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, inputarchive.HotReference{Kind: "artifact", ID: a.ID})
	}
	tx, err := db.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for _, indexed := range []bool{false, true} {
		batch, err := resolveCompilerInputHotBatchWithIndex(t.Context(), tx, refs, nil, indexed)
		if err != nil || len(batch) != 2 {
			t.Fatal("policy parent transport failed", indexed, batch, err)
		}
		for i, ref := range refs {
			single, err := resolveCompilerInputHotNode(t.Context(), tx, ref)
			if err != nil || !reflect.DeepEqual(single, batch[ref]) {
				t.Fatal("batch changed scoped promotion policy", indexed, single, batch[ref], err)
			}
			want := inputarchive.HotReference{Kind: "generation", ID: "shared-policy-generation", ArtifactKind: model.PlatformArtifactKindPolicySnapshot,
				ScopeKey: []string{"scope-neutral-a", "scope-neutral-b"}[i]}
			found := false
			for _, dependency := range single.References {
				found = found || dependency == want
			}
			if !found {
				t.Fatal("policy generation lost its exact original scope", indexed, want, single)
			}
		}
	}
}

func (q *hotInventoryQueryCounter) QueryContext(ctx context.Context, statement string, args ...any) (*sql.Rows, error) {
	q.queries++
	return q.inputQueryer.QueryContext(ctx, statement, args...)
}

func (q *hotInventoryQueryCounter) QueryRowContext(ctx context.Context, statement string, args ...any) *sql.Row {
	q.queries++
	return q.inputQueryer.QueryRowContext(ctx, statement, args...)
}

func TestHotInventoryPostgresLedgerBatchPreservesScopesRollbackAndSnapshot(t *testing.T) {
	db, _, _ := hotInventoryTestDatabase(t)
	refs := []inputarchive.HotReference{}
	for i := 0; i < 16; i++ {
		id, scope := fmt.Sprintf("artifact-ledger-%d", i), fmt.Sprintf("scope-ledger-%d", i)
		kind := "edge_route_bundle"
		if i%2 == 1 {
			kind = "caddy_route_config"
		}
		if _, err := db.Exec(`INSERT INTO fugue_platform_artifacts(id,artifact_kind,scope_key,generation,status,content_hash,created_at,updated_at)
 VALUES($1,$2,$3,'shared-generation','validated',$4,now(),now())`, id, kind, scope, "sha256:"+strings.Repeat("a", 64)); err != nil {
			t.Fatal(err)
		}
		// A repeated generation must retain both declared edges, and every
		// candidate/serving/LKG/pinned/rollback slot keeps its original scope.
		if _, err := db.Exec(`INSERT INTO fugue_platform_artifact_releases(id,artifact_id,artifact_kind,scope_key,generation,release_channel,status,
 candidate_generation,serving_unverified_generation,verified_lkg_generation,pinned_rollback_generation,rollback_target_generation,released_at,created_at,updated_at)
 VALUES($1,$2,$3,$4,'shared-generation','stable','superseded','shared-generation','serving','lkg','pinned','rollback',now(),now(),now())`, fmt.Sprintf("release-ledger-%d", i), id, kind, scope); err != nil {
			t.Fatal(err)
		}
		refs = append(refs, inputarchive.HotReference{Kind: "generation", ID: "shared-generation", ArtifactKind: kind, ScopeKey: scope},
			inputarchive.HotReference{Kind: "release", ID: fmt.Sprintf("release-ledger-%d", i)})
	}
	tx, err := db.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	batchQueries := &hotInventoryQueryCounter{inputQueryer: tx}
	start := time.Now()
	batch, err := resolveCompilerInputHotBatch(t.Context(), batchQueries, refs)
	batchDuration := time.Since(start)
	if err != nil || len(batch) != len(refs) || batchQueries.queries != 2 {
		t.Fatal("32 scoped identities must use two complete ledger reads", len(batch), batchQueries.queries, err)
	}
	// A concurrent ledger replacement must remain invisible to all subsequent
	// reads from this exact RR snapshot, including the batch/single comparison.
	if _, err = db.Exec(`UPDATE fugue_platform_artifacts SET content_hash=$1 WHERE id='artifact-ledger-0'`, "sha256:"+strings.Repeat("b", 64)); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`UPDATE fugue_platform_artifact_releases SET pinned_rollback_generation='replacement' WHERE id='release-ledger-0'`); err != nil {
		t.Fatal(err)
	}
	singleQueries := &hotInventoryQueryCounter{inputQueryer: tx}
	start = time.Now()
	for i, ref := range refs {
		single, err := resolveCompilerInputHotNode(t.Context(), singleQueries, ref)
		if err != nil || !reflect.DeepEqual(single, batch[ref]) {
			t.Fatal("ledger batch changed single-identity semantics", ref, err)
		}
		if ref.Kind == "generation" {
			if single.ContentHash != "sha256:"+strings.Repeat("a", 64) || len(single.References) != 1 || single.References[0].ID != fmt.Sprintf("artifact-ledger-%d", i/2) {
				t.Fatal("generation crossed a scope or RR snapshot", single)
			}
		} else {
			if len(single.References) != 7 {
				t.Fatal("release lost a declared dependency", single)
			}
			for slot, generation := range []string{"shared-generation", "shared-generation", "serving", "lkg", "pinned", "rollback"} {
				child := single.References[slot+1]
				if child.Kind != "generation" || child.ID != generation || child.ScopeKey != fmt.Sprintf("scope-ledger-%d", i/2) || child.ArtifactKind != refs[i-1].ArtifactKind {
					t.Fatal("release dependency slot or scope changed", slot, child)
				}
			}
		}
	}
	if singleQueries.queries != 32 {
		t.Fatal("single-identity measurement did not execute all original lookups", singleQueries.queries)
	}
	t.Logf("actual PostgreSQL metadata reads: single=%d/%s, batched=%d/%s", singleQueries.queries, time.Since(start), batchQueries.queries, batchDuration)
	missing := []inputarchive.HotReference{
		{Kind: "generation", ID: "shared-generation", ArtifactKind: "caddy_route_config", ScopeKey: "scope-ledger-0"},
		{Kind: "generation", ID: "shared-generation", ArtifactKind: "edge_route_bundle", ScopeKey: "scope-missing"},
		{Kind: "release", ID: "release-missing"},
	}
	if got, err := resolveCompilerInputHotBatch(t.Context(), tx, missing); err != nil || len(got) != 0 {
		t.Fatal("batch invented missing or cross-scope ledger identities", got, err)
	}
	if _, err = tx.Exec(`SET LOCAL statement_timeout='1ms'`); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(`SELECT pg_sleep(0.02)`); err == nil {
		t.Fatal("transaction abort was not exercised")
	}
	if _, err = resolveCompilerInputHotBatch(t.Context(), tx, refs); err == nil {
		t.Fatal("failed ledger batch was misreported as missing identities")
	}
}
