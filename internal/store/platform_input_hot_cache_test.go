package store

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/inputarchive"
	"github.com/jackc/pgx/v5/pgconn"
)

func hotCacheSnapshot(t *testing.T, conn *sql.Conn) *sql.Tx {
	t.Helper()
	tx, err := conn.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	if err := compilerInputHotInventoryGuards(t.Context(), tx); err != nil {
		t.Fatal(err)
	}
	return tx
}

func hotCacheWarm(t *testing.T, conn *sql.Conn, refs []inputarchive.HotReference) *compilerInputHotCache {
	t.Helper()
	tx := hotCacheSnapshot(t, conn)
	defer tx.Rollback()
	c := &compilerInputHotCache{entries: map[string]compilerInputHotCacheEntry{}, capture: true, stopBefore: time.Now().Add(time.Minute)}
	if err := c.bind(t.Context(), tx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.resolve(t.Context(), tx, refs, nil, false); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	c.capture = false
	return c
}

func hotCacheRead(t *testing.T, tx *sql.Tx, c *compilerInputHotCache, refs []inputarchive.HotReference) map[inputarchive.HotReference]inputarchive.HotNode {
	t.Helper()
	if err := c.bind(t.Context(), tx); err != nil || !c.report.SessionValidated {
		t.Fatal("unsafe cache session", err)
	}
	got, err := c.resolve(t.Context(), tx, refs, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	want, err := resolveCompilerInputHotBatch(t.Context(), tx, refs)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatal("cache differs from fresh same-snapshot decoding", err)
	}
	return got
}

func TestHotCachePostgresRevalidatesConcurrentUpdatesDeletesAndInsertions(t *testing.T) {
	db, _, _ := hotInventoryTestDatabase(t)
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	for _, change := range []string{"unchanged", "metadata-hot-update", "content-update", "delete", "delete-reinsert", "insert"} {
		t.Run(change, func(t *testing.T) {
			ref := inputarchive.HotReference{Kind: "artifact", ID: "neutral-cache-" + change}
			if change != "insert" {
				seedHotIndexDNS(t, db, ref.ID, 128<<10)
			}
			c := hotCacheWarm(t, conn, []inputarchive.HotReference{ref})
			had := len(c.entries)
			switch change {
			case "metadata-hot-update":
				writer, e := db.BeginTx(t.Context(), nil)
				if e != nil {
					t.Fatal(e)
				}
				defer writer.Rollback()
				if _, err = writer.Exec(`UPDATE fugue_platform_artifacts SET metadata_json=metadata_json||jsonb_build_object('cache_probe','changed') WHERE id=$1`, ref.ID); err != nil {
					t.Fatal(err)
				}
				var hot int64
				if err := writer.QueryRow(`SELECT pg_stat_get_xact_tuples_hot_updated('fugue_platform_artifacts'::regclass)`).Scan(&hot); err != nil || hot < 1 {
					t.Fatal("fixture did not exercise a HOT update", hot, err)
				}
				err = writer.Commit()
			case "content-update":
				_, err = db.Exec(`UPDATE fugue_platform_artifacts SET content_json=content_json-'cell_route_publications' WHERE id=$1`, ref.ID)
			case "delete", "delete-reinsert":
				_, err = db.Exec(`DELETE FROM fugue_platform_artifacts WHERE id=$1`, ref.ID)
				if err == nil && change == "delete-reinsert" {
					seedHotIndexDNS(t, db, ref.ID, 64<<10)
				}
			case "insert":
				seedHotIndexDNS(t, db, ref.ID, 32<<10)
			}
			if err != nil {
				t.Fatal(err)
			}
			tx := hotCacheSnapshot(t, conn)
			hotCacheRead(t, tx, c, []inputarchive.HotReference{ref})
			if change == "unchanged" && (c.report.Hits != 1 || had != 1) || change != "unchanged" && c.report.Hits != 0 {
				t.Fatal("stale/missing row reused", c.report)
			}
			_ = tx.Rollback()
		})
	}
}

func TestHotCachePostgresFinalSnapshotCannotMixLaterSourceOrRoots(t *testing.T) {
	db, _, _ := hotInventoryTestDatabase(t)
	seedHotInventoryHistory(t, db)
	ref := inputarchive.HotReference{Kind: "artifact", ID: "artifact-neutral-1"}
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	c := hotCacheWarm(t, conn, []inputarchive.HotReference{ref})
	tx := hotCacheSnapshot(t, conn)
	if err := c.bind(t.Context(), tx); err != nil {
		t.Fatal(err)
	}
	before, err := compilerInputRetentionInventorySnapshotCached(t.Context(), tx, c)
	if err != nil || !before.UniverseComplete || c.report.Hits == 0 {
		t.Fatal(before, err)
	}
	if _, err := db.Exec(`UPDATE fugue_platform_artifacts SET metadata_json='{}' WHERE id=$1`, ref.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE fugue_platform_lkg_snapshots SET artifact_id='artifact-neutral-4',generation='artifact-neutral-4'`); err != nil {
		t.Fatal(err)
	}
	after, err := compilerInputRetentionInventorySnapshotCached(t.Context(), tx, c)
	if err != nil || before.AuditSHA256 != after.AuditSHA256 {
		t.Fatal("concurrent roots/source crossed RR", err)
	}
	_ = tx.Rollback()
	tx = hotCacheSnapshot(t, conn)
	defer tx.Rollback()
	if err := c.bind(t.Context(), tx); err != nil {
		t.Fatal(err)
	}
	fresh, err := compilerInputRetentionInventorySnapshotCached(t.Context(), tx, c)
	plain, plainErr := compilerInputRetentionInventorySnapshot(t.Context(), tx)
	if err != nil || plainErr != nil || fresh.AuditSHA256 != plain.AuditSHA256 || !reflect.DeepEqual(fresh.Entries, plain.Entries) || !reflect.DeepEqual(fresh.Native.Closure, plain.Native.Closure) || fresh.AuditSHA256 == before.AuditSHA256 {
		t.Fatal("new snapshot used old authority or graph", err, plainErr)
	}
}

func TestHotCachePostgresRelationRewriteAndDifferentConnectionInvalidate(t *testing.T) {
	db, _, _ := hotInventoryTestDatabase(t)
	ref := seedHotIndexDNS(t, db, "neutral-cache-rewrite", 32<<10)
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	c := hotCacheWarm(t, conn, []inputarchive.HotReference{ref})
	other, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = other.Close() })
	tx := hotCacheSnapshot(t, other)
	stamp, err := compilerInputHotCacheSessionRead(t.Context(), tx)
	if err != nil || c.session.accepts(stamp) {
		t.Fatal("other physical connection accepted", err)
	}
	_ = tx.Rollback()
	_ = other.Close()
	if _, err := db.Exec(`VACUUM FULL fugue_platform_artifacts`); err != nil {
		t.Fatal(err)
	}
	tx = hotCacheSnapshot(t, conn)
	defer tx.Rollback()
	stamp, err = compilerInputHotCacheSessionRead(t.Context(), tx)
	if err != nil || c.session.accepts(stamp) {
		t.Fatal("rewritten relation accepted", err)
	}
	// The production path discards all old entries on this mismatch.
	if err := c.bind(t.Context(), tx); err != nil || c.report.SessionValidated {
		t.Fatal("relation rewrite did not discard cache", err)
	}
	got, err := c.resolve(t.Context(), tx, []inputarchive.HotReference{ref}, nil, false)
	want, wantErr := resolveCompilerInputHotBatch(t.Context(), tx, []inputarchive.HotReference{ref})
	if err != nil || wantErr != nil || c.report.Hits != 0 || !reflect.DeepEqual(got, want) {
		t.Fatal("relation mismatch bypassed fresh decoding", err, wantErr)
	}
}

func TestHotCacheRejectsEpochRegressionWrapAndCustodyChanges(t *testing.T) {
	s := compilerInputHotCacheSession{Backend: 7, Database: 8, Relation: 9, Filenode: 10, Schema: "neutral", Role: "fixture", SearchPath: "neutral", Started: time.Now(), Horizon: 100}
	for _, mutate := range []func(*compilerInputHotCacheSession){
		func(v *compilerInputHotCacheSession) { v.Horizon-- },
		func(v *compilerInputHotCacheSession) { v.Horizon += 1 << 31 },
		func(v *compilerInputHotCacheSession) { v.Backend++ },
		func(v *compilerInputHotCacheSession) { v.Database++ },
		func(v *compilerInputHotCacheSession) { v.Relation++ },
		func(v *compilerInputHotCacheSession) { v.Filenode++ },
		func(v *compilerInputHotCacheSession) { v.Schema += "_other" },
		func(v *compilerInputHotCacheSession) { v.Role += "_other" },
		func(v *compilerInputHotCacheSession) { v.SearchPath += "_other" },
		func(v *compilerInputHotCacheSession) { v.Started = v.Started.Add(time.Second) },
	} {
		current := s
		mutate(&current)
		if s.accepts(current) {
			t.Fatal("invalid cache custody/horizon accepted")
		}
	}
	current := s
	current.Horizon += 1<<31 - 1
	if !s.accepts(current) || !s.accepts(s) {
		t.Fatal("safe horizon rejected")
	}
}

func TestHotCacheRejectsAmbiguousFrozenXIDsBeforeWrap(t *testing.T) {
	for _, test := range []struct {
		xmin    string
		horizon int64
		want    bool
	}{
		{"90", 100, true}, {"100", 100, false}, {"101", 100, false},
		{"100", 1<<32 - 100, false}, // old frozen row could collide after 200 XIDs
		{"4294967196", 1<<32 + 100, true},
		{"100", 100 + 1<<31, false}, {"100", 99 + 1<<31, true},
		{"2", 100, false}, {"0", 100, false}, {"-1", 100, false},
		{"4294967296", 100, false}, {"bad", 100, false}, {"90", -1, false},
	} {
		if got := compilerInputHotCacheXIDSafe(test.xmin, test.horizon); got != test.want {
			t.Fatal(test, got)
		}
	}
}

func TestHotCachePostgresOwnsResultsAndChargesCombinedBatch(t *testing.T) {
	db, _, _ := hotInventoryTestDatabase(t)
	refs := []inputarchive.HotReference{}
	for i := range 5 {
		refs = append(refs, seedHotIndexDNS(t, db, "neutral-cache-batch-"+strings.Repeat("x", i+1), 7<<20))
	}
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	c := hotCacheWarm(t, conn, refs[:4])
	tx := hotCacheSnapshot(t, conn)
	got := hotCacheRead(t, tx, c, refs[:4])
	node := got[refs[0]]
	if len(node.References) == 0 {
		t.Fatal("fixture has no dependencies")
	}
	node.References[0].ID = "mutated"
	hotCacheRead(t, tx, c, refs[:4])
	if _, err := c.resolve(t.Context(), tx, refs, nil, false); err == nil {
		t.Fatal("cache hits concealed a combined source batch over 32MiB")
	}
}

func TestPreparedRetentionInventoryPostgresMatchesFinalProofAndRefusesCancellation(t *testing.T) {
	db, address, _ := hotInventoryTestDatabase(t)
	seedHotInventoryHistory(t, db)
	report, err := RunCompilerInputPreparedRetentionInventory(t.Context(), address)
	if err != nil || !report.UniverseComplete || report.Preparation == nil || !report.Preparation.SessionValidated || report.Preparation.Hits < 1 || report.ClassificationComplete || report.ExecutionAuthorized || report.Native.BodyVerification || report.Native.RetirementAuthorized {
		t.Fatal("prepared inventory lost proof/refusal scope", report, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	refused, err := RunCompilerInputPreparedRetentionInventory(ctx, address)
	if err == nil || refused.UniverseComplete || refused.AuditSHA256 != "" || len(refused.Entries) != 0 {
		t.Fatal("canceled preparation returned a proof", refused, err)
	}
}

func TestHotCachePostgresFinalSnapshotLocksRelationAndBoundsPreparation(t *testing.T) {
	db, _, _ := hotInventoryTestDatabase(t)
	ref := seedHotIndexDNS(t, db, "neutral-cache-locked", 64<<10)
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	c := hotCacheWarm(t, conn, []inputarchive.HotReference{ref})
	tx := hotCacheSnapshot(t, conn)
	hotCacheRead(t, tx, c, []inputarchive.HotReference{ref})
	writer, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Rollback()
	if _, err := writer.Exec(`SET LOCAL lock_timeout='250ms'`); err != nil {
		t.Fatal(err)
	}
	_, err = writer.Exec(`TRUNCATE fugue_platform_artifacts`)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "55P03" {
		t.Fatal("final proof did not hold relation lock", err)
	}
	_ = writer.Rollback()
	_ = tx.Rollback()
	for _, bound := range []string{"time", "encoded-bytes", "references"} {
		t.Run(bound, func(t *testing.T) {
			tx := hotCacheSnapshot(t, conn)
			defer tx.Rollback()
			limited := &compilerInputHotCache{entries: map[string]compilerInputHotCacheEntry{}, capture: true, stopBefore: time.Now().Add(time.Minute)}
			if err := limited.bind(t.Context(), tx); err != nil {
				t.Fatal(err)
			}
			switch bound {
			case "time":
				limited.stopBefore = time.Now().Add(-time.Second)
			case "encoded-bytes":
				limited.report.EncodedBytes = compilerInputHotCacheBytes
			case "references":
				limited.report.References = 262144
			}
			if _, err := limited.resolve(t.Context(), tx, []inputarchive.HotReference{ref}, nil, false); !errors.Is(err, errCompilerInputHotPreparationStop) || len(limited.entries) != 0 {
				t.Fatal("preparation exceeded its bound", bound, err)
			}
		})
	}
}

func TestHotCachePostgresMeasuresColdAndRevalidatedWork(t *testing.T) {
	db, _, _ := hotInventoryTestDatabase(t)
	refs := []inputarchive.HotReference{}
	for i := range 8 {
		refs = append(refs, seedHotIndexDNS(t, db, "neutral-cache-measure-"+strings.Repeat("x", i+1), 2<<20))
	}
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	started := time.Now()
	c := hotCacheWarm(t, conn, refs)
	warmDuration := time.Since(started)
	tx := hotCacheSnapshot(t, conn)
	if err := c.bind(t.Context(), tx); err != nil {
		t.Fatal(err)
	}
	var coldCost, preparedCost CompilerInputHotCost
	started = time.Now()
	want, err := resolveCompilerInputHotBatchObserved(t.Context(), tx, refs, &coldCost)
	coldDuration := time.Since(started)
	if err != nil {
		t.Fatal(err)
	}
	started = time.Now()
	got, err := c.resolve(t.Context(), tx, refs, &preparedCost, false)
	preparedDuration := time.Since(started)
	if err != nil || !reflect.DeepEqual(got, want) || c.report.Hits != 8 || c.report.VersionQueries != 1 || preparedCost.Artifacts.Queries != 0 || coldCost.Artifacts.SourceBytes < 16<<20 {
		t.Fatal("cache did not avoid body transport/decoding or changed nodes", err, c.report)
	}
	t.Logf("preparation=%s cold=%s revalidated=%s source=%d transport=%d retained_encoded=%d hits=%d version_queries=%d", warmDuration, coldDuration, preparedDuration, coldCost.Artifacts.SourceBytes, coldCost.Artifacts.TransportBytes, c.report.EncodedBytes, c.report.Hits, c.report.VersionQueries)
}

func TestHotCachePostgresConnectionLossRefusesSameConnectionWork(t *testing.T) {
	db, _, _ := hotInventoryTestDatabase(t)
	ref := seedHotIndexDNS(t, db, "neutral-cache-disconnected", 64<<10)
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	c := hotCacheWarm(t, conn, []inputarchive.HotReference{ref})
	if _, err := db.Exec(`SELECT pg_terminate_backend($1)`, c.session.Backend); err != nil {
		t.Fatal(err)
	}
	// database/sql must not replace the pinned Conn with a pooled session.
	tx, err := conn.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err == nil {
		_ = tx.Rollback()
		t.Fatal("lost pinned connection was silently replaced")
	}
	if c.report.Hits != 0 {
		t.Fatal("connection loss consumed cache proof")
	}
}
