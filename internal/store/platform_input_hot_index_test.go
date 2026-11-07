package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/inputarchive"
	"fugue/internal/schemamigrate"
	"fugue/internal/testfixture/celldns"
)

func installHotIndexTest(t *testing.T, db *sql.DB, address string) {
	t.Helper()
	if err := schemamigrate.MigrateCompilerInputHotIndex(t.Context(), address); err != nil {
		t.Fatal(err)
	}
	if err := schemamigrate.MigrateCompilerInputHotIndex(t.Context(), address); err != nil {
		t.Fatal("idempotent migration", err)
	}
	if err := schemamigrate.VerifyCompilerInputHotIndex(t.Context(), db); err != nil {
		t.Fatal(err)
	}
}

func seedHotIndexDNS(t *testing.T, db *sql.DB, id string, bytes int) inputarchive.HotReference {
	t.Helper()
	req, _, _ := celldns.SourceAuthorizedRequest(t, false)
	a := celldns.Compile(t, req).DNSArtifact
	a.Content["query_views"] = strings.Repeat("x", bytes)
	content, err := json.Marshal(a.Content)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := json.Marshal(a.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`INSERT INTO fugue_platform_artifacts(id,artifact_kind,scope_key,generation,status,content_hash,content_json,metadata_json,created_at,updated_at)
 VALUES($1,'dns_answer_bundle','neutral-scope',$1,'validated',$2,$3,$4,now(),now())`, id, a.ContentHash, content, metadata); err != nil {
		t.Fatal(err)
	}
	return inputarchive.HotReference{Kind: "artifact", ID: id}
}

func readHotIndexTest(t *testing.T, tx inputQueryer, refs []inputarchive.HotReference, indexed bool) map[inputarchive.HotReference]inputarchive.HotNode {
	t.Helper()
	nodes, err := resolveCompilerInputHotBatchWithIndex(t.Context(), tx, refs, nil, indexed)
	if err != nil {
		t.Fatal(err)
	}
	return nodes
}

func TestHotIndexPostgresDerivesGuardsAndInvalidatesEverySourceUpdate(t *testing.T) {
	db, address, _ := hotInventoryTestDatabase(t)
	installHotIndexTest(t, db, address)
	ref := seedHotIndexDNS(t, db, "neutral-index-dns", 2<<20)
	var originalMetadata []byte
	if err := db.QueryRow(`SELECT metadata_json FROM fugue_platform_artifacts WHERE id=$1`, ref.ID).Scan(&originalMetadata); err != nil {
		t.Fatal(err)
	}
	want := readHotIndexTest(t, db, []inputarchive.HotReference{ref}, false)
	// Missing entries preserve the same bounded source path.
	if got := readHotIndexTest(t, db, []inputarchive.HotReference{ref}, true); !reflect.DeepEqual(got, want) {
		t.Fatal("missing cache changed dependencies")
	}
	preview, err := RunCompilerInputHotIndex(t.Context(), address, CompilerInputHotIndexOptions{Limit: 1})
	if err != nil || preview.Applied || preview.Selected != 1 || preview.SourceBytes < 2<<20 {
		t.Fatal(preview, err)
	}
	var count int
	if err = db.QueryRow(`SELECT count(*) FROM fugue_platform_input_hot_dependency_index_v1`).Scan(&count); err != nil || count != 0 {
		t.Fatal("preview persisted an entry", count, err)
	}
	applied, err := RunCompilerInputHotIndex(t.Context(), address, CompilerInputHotIndexOptions{Limit: 1, Apply: true})
	if err != nil || !applied.Applied || applied.RetirementAuthorized || applied.NextAfterID != ref.ID {
		t.Fatal(applied, err)
	}
	if got := readHotIndexTest(t, db, []inputarchive.HotReference{ref}, true); !reflect.DeepEqual(got, want) {
		t.Fatal("derived projection changed dependencies")
	}
	// A caller cannot inject a false projection, size, or version binding.
	if _, err = db.Exec(`UPDATE fugue_platform_input_hot_dependency_index_v1 SET content_text='{}',metadata_text='{}',source_bytes=1,built_xid=1`); err != nil {
		t.Fatal(err)
	}
	if got := readHotIndexTest(t, db, []inputarchive.HotReference{ref}, true); !reflect.DeepEqual(got, want) {
		t.Fatal("caller supplied projection was trusted")
	}
	snapshot, err := db.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Rollback()
	if got := readHotIndexTest(t, snapshot, []inputarchive.HotReference{ref}, true); !reflect.DeepEqual(got, want) {
		t.Fatal("snapshot baseline differs")
	}
	if _, err = db.Exec(`UPDATE fugue_platform_artifacts SET metadata_json=jsonb_build_object('input_snapshot_digest','sha256:'||repeat('b',64)) WHERE id=$1`, ref.ID); err != nil {
		t.Fatal(err)
	}
	if got := readHotIndexTest(t, snapshot, []inputarchive.HotReference{ref}, true); !reflect.DeepEqual(got, want) {
		t.Fatal("concurrent update crossed RR snapshot")
	}
	if got, source := readHotIndexTest(t, db, []inputarchive.HotReference{ref}, true), readHotIndexTest(t, db, []inputarchive.HotReference{ref}, false); !reflect.DeepEqual(got, source) || reflect.DeepEqual(got, want) {
		t.Fatal("stale projection hid source metadata update")
	}
	// Even two writes by the same transaction share xmin but not ctid.
	if _, err = db.Exec(`UPDATE fugue_platform_artifacts SET metadata_json=$2::jsonb WHERE id=$1`, ref.ID, originalMetadata); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.Exec(`UPDATE fugue_platform_artifacts SET content_json=content_json WHERE id=$1`, ref.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(`UPDATE fugue_platform_input_hot_dependency_index_v1 SET artifact_id=artifact_id`); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(`UPDATE fugue_platform_artifacts SET content_json=content_json-'lineage' WHERE id=$1`, ref.ID); err != nil {
		t.Fatal(err)
	}
	if got, source := readHotIndexTest(t, tx, []inputarchive.HotReference{ref}, true), readHotIndexTest(t, tx, []inputarchive.HotReference{ref}, false); !reflect.DeepEqual(got, source) {
		t.Fatal("same-transaction source update reused cache")
	}
	_ = tx.Rollback()
	// A changed/disabled derivation is refused rather than blessed by rerun.
	if _, err = db.Exec(`ALTER TABLE fugue_platform_input_hot_dependency_index_v1 DISABLE TRIGGER fugue_input_hot_dependency_index_v1_fill`); err != nil {
		t.Fatal(err)
	}
	if err = schemamigrate.VerifyCompilerInputHotIndex(t.Context(), db); err == nil {
		t.Fatal("disabled derivation accepted")
	}
	if err = schemamigrate.MigrateCompilerInputHotIndex(t.Context(), address); err == nil {
		t.Fatal("migration silently repaired an untrusted existing index")
	}
}

func TestHotIndexPostgresPreservesNullMissingAndSourceByteBounds(t *testing.T) {
	db, address, _ := hotInventoryTestDatabase(t)
	installHotIndexTest(t, db, address)
	ref := seedHotIndexDNS(t, db, "neutral-index-variants", 1024)
	for _, field := range []string{"lineage", "policy", "cell_route_publications", "previous_traffic_publication", "cell_dns_source"} {
		for _, null := range []bool{false, true} {
			tx, err := db.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			statement := `UPDATE fugue_platform_artifacts SET content_json=content_json-$1 WHERE id=$2`
			if null {
				statement = `UPDATE fugue_platform_artifacts SET content_json=jsonb_set(content_json,ARRAY[$1::text],'null') WHERE id=$2`
			}
			if _, err = tx.Exec(statement, field, ref.ID); err != nil {
				t.Fatal(err)
			}
			if _, err = tx.Exec(`INSERT INTO fugue_platform_input_hot_dependency_index_v1(artifact_id) VALUES($1)`, ref.ID); err != nil {
				t.Fatal(err)
			}
			got, source := readHotIndexTest(t, tx, []inputarchive.HotReference{ref}, true), readHotIndexTest(t, tx, []inputarchive.HotReference{ref}, false)
			if !reflect.DeepEqual(got, source) {
				t.Fatal("missing/null behavior changed", field, null)
			}
			_ = tx.Rollback()
		}
	}
	// The original source limit still owns admission even if DNS projection is tiny.
	seedHotIndexDNS(t, db, "neutral-oversized", compilerInputHotArtifactBytes+1)
	if _, err := RunCompilerInputHotIndex(t.Context(), address, CompilerInputHotIndexOptions{AfterID: ref.ID, Limit: 1, Apply: true}); err == nil {
		t.Fatal("oversized source became a small valid cached artifact")
	}
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM fugue_platform_input_hot_dependency_index_v1`).Scan(&count); err != nil || count != 0 {
		t.Fatal("refused page left partial entries", count, err)
	}
}

func TestHotIndexPostgresMeasuresProjectionReadsAndBatchRefusal(t *testing.T) {
	db, address, _ := hotInventoryTestDatabase(t)
	installHotIndexTest(t, db, address)
	refs := []inputarchive.HotReference{}
	for i := 0; i < 8; i++ {
		refs = append(refs, seedHotIndexDNS(t, db, fmt.Sprintf("neutral-index-measure-%02d", i), 2<<20))
	}
	if _, err := RunCompilerInputHotIndex(t.Context(), address, CompilerInputHotIndexOptions{Limit: 8, Apply: true}); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.Exec(`SET LOCAL work_mem='1MB'; SET LOCAL temp_file_limit=0; SET LOCAL jit=off`); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	want := readHotIndexTest(t, tx, refs, false)
	originalDuration := time.Since(start)
	start = time.Now()
	got := readHotIndexTest(t, tx, refs, true)
	indexedDuration := time.Since(start)
	if !reflect.DeepEqual(got, want) || len(got) != 8 {
		t.Fatal("indexed read changed measured graph")
	}
	t.Logf("8x2MiB DNS sources: original=%s indexed=%s; same dependencies, original size cap, temp0", originalDuration, indexedDuration)
	_ = tx.Rollback()
	// Above 32MiB, a whole page refuses; it cannot commit its earlier entries.
	for i := 8; i < 17; i++ {
		seedHotIndexDNS(t, db, fmt.Sprintf("neutral-index-measure-%02d", i), 2<<20)
	}
	if _, err := RunCompilerInputHotIndex(context.Background(), address, CompilerInputHotIndexOptions{Limit: 17, Apply: true}); err == nil {
		t.Fatal("oversized batch accepted")
	}
	var count int
	if err = db.QueryRow(`SELECT count(*) FROM fugue_platform_input_hot_dependency_index_v1`).Scan(&count); err != nil || count != 8 {
		t.Fatal("refused page partially committed", count, err)
	}
}
