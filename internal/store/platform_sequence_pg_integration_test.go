package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"fugue/internal/model"
)

func platformSequenceTestDB(t *testing.T) *sql.DB {
	t.Helper()
	base := metricsProjectionTestDB(t) // Enforces a disposable loopback database.
	schema := model.NewID("sequence_test")
	quoted := quotePostgresIntegrationIdentifier(t, schema)
	if _, err := base.db.Exec("CREATE SCHEMA " + quoted); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := base.db.Exec("DROP SCHEMA " + quoted + " CASCADE"); err != nil {
			t.Error(err)
		}
	})
	db, err := sql.Open("pgx", postgresIntegrationURLWithSearchPath(t, base.databaseURL, schema))
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(8)
	t.Cleanup(func() { _ = db.Close() })
	for _, ddl := range []string{
		`CREATE TABLE fugue_platform_artifacts (id text PRIMARY KEY, artifact_kind text NOT NULL, scope_key text NOT NULL, generation_sequence bigint NOT NULL DEFAULT 0, padding text DEFAULT '')`,
		`CREATE UNIQUE INDEX idx_fugue_platform_artifacts_sequence ON fugue_platform_artifacts (artifact_kind, scope_key, generation_sequence) WHERE generation_sequence > 0`,
		`CREATE TABLE fugue_platform_artifact_generation_sequences (artifact_kind text NOT NULL, scope_key text NOT NULL, last_sequence bigint NOT NULL DEFAULT 0, updated_at timestamptz NOT NULL, PRIMARY KEY(artifact_kind, scope_key))`,
	} {
		if _, err := db.Exec(ddl); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func TestPlatformSequencePostgresPreservesRecoveryAndRollback(t *testing.T) {
	db := platformSequenceTestDB(t)
	for _, tc := range []struct {
		name    string
		counter int64
		history []int64
		want    int64
	}{
		{"empty", 0, nil, 1},
		{"legacy_unallocated", 0, []int64{0, 0, -1}, 1},
		{"restored_history", 0, []int64{0, 5, 12}, 13},
		{"counter_behind_restore", 4, []int64{5, 12}, 13},
		{"counter_ahead_of_retained_history", 42, []int64{5, 12}, 43},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for i, sequence := range tc.history {
				if _, err := db.Exec(`INSERT INTO fugue_platform_artifacts(id,artifact_kind,scope_key,generation_sequence) VALUES($1,'route',$2,$3)`, fmt.Sprintf("%s-%d", tc.name, i), tc.name, sequence); err != nil {
					t.Fatal(err)
				}
			}
			// Higher sequences in another kind or scope cannot advance this one.
			if _, err := db.Exec(`INSERT INTO fugue_platform_artifacts(id,artifact_kind,scope_key,generation_sequence) VALUES($1,'dns',$2,9000),($3,'route',$4,9000)`, tc.name+"-other-kind", tc.name, tc.name+"-other-scope", tc.name+"-neighbor"); err != nil {
				t.Fatal(err)
			}
			if tc.counter != 0 {
				if _, err := db.Exec(`INSERT INTO fugue_platform_artifact_generation_sequences VALUES('route',$1,$2,now())`, tc.name, tc.counter); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			got, err := pgNextPlatformArtifactGenerationSequence(ctx, tx, "route", tc.name, time.Now())
			_ = tx.Rollback()
			if err != nil || got != tc.want {
				t.Fatalf("reservation: got=%d want=%d err=%v", got, tc.want, err)
			}
			got, err = pgNextPlatformArtifactGenerationSequence(ctx, db, "route", tc.name, time.Now())
			if err != nil || got != tc.want {
				t.Fatalf("rolled-back reservation persisted: got=%d want=%d err=%v", got, tc.want, err)
			}
			cancelled, stop := context.WithCancel(ctx)
			stop()
			if _, err := pgNextPlatformArtifactGenerationSequence(cancelled, db, "route", tc.name, time.Now()); err == nil {
				t.Fatal("cancelled allocation succeeded")
			}
			got, err = pgNextPlatformArtifactGenerationSequence(ctx, db, "route", tc.name, time.Now())
			if err != nil || got != tc.want+1 {
				t.Fatalf("next allocation: got=%d want=%d err=%v", got, tc.want+1, err)
			}
		})
	}
}

type platformSequenceQueryCapture struct {
	platformStateDB
	query string
	args  []any
}

func (db *platformSequenceQueryCapture) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	db.query, db.args = query, args
	return db.platformStateDB.QueryRowContext(ctx, query, args...)
}

func TestPlatformSequencePostgresBoundsHistoryAndSerializesWriters(t *testing.T) {
	db := platformSequenceTestDB(t)
	if _, err := db.Exec(`INSERT INTO fugue_platform_artifacts(id,artifact_kind,scope_key,generation_sequence,padding) SELECT 'history-'||n,'route','scope',n,repeat(md5(n::text),8) FROM generate_series(1,100000) n; ANALYZE fugue_platform_artifacts`); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	capture := &platformSequenceQueryCapture{platformStateDB: db}
	got, err := pgNextPlatformArtifactGenerationSequence(ctx, capture, "route", "scope", time.Now())
	if err != nil || got != 100001 {
		t.Fatalf("restored high-water mark: got=%d err=%v", got, err)
	}
	// Explain the actual allocator UPDATE, then roll back its reservation.
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	var raw []byte
	err = tx.QueryRowContext(ctx, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "+capture.query, capture.args...).Scan(&raw)
	_ = tx.Rollback()
	if err != nil {
		t.Fatal(err)
	}
	var plans []map[string]any
	if err := json.Unmarshal(raw, &plans); err != nil {
		t.Fatal(err)
	}
	indexed, examined := false, float64(0)
	var inspect func(map[string]any)
	inspect = func(plan map[string]any) {
		if plan["Relation Name"] == "fugue_platform_artifacts" {
			if plan["Index Name"] != "idx_fugue_platform_artifacts_sequence" || plan["Scan Direction"] != "Backward" {
				t.Fatalf("history scan is not using the sequence index: %s", raw)
			}
			indexed = true
			examined += plan["Actual Rows"].(float64) * plan["Actual Loops"].(float64)
		}
		if children, ok := plan["Plans"].([]any); ok {
			for _, child := range children {
				inspect(child.(map[string]any))
			}
		}
	}
	inspect(plans[0]["Plan"].(map[string]any))
	if !indexed || examined != 1 {
		t.Fatalf("100000 history rows required %v reads (indexed=%t): %s", examined, indexed, raw)
	}
	t.Logf("100000 history rows: allocator index rows=%v, execution_ms=%v", examined, plans[0]["Execution Time"])

	type allocation struct {
		sequence int64
		backend  int
		err      error
	}
	ready, release := make(chan struct{}, 8), make(chan struct{})
	results := make(chan allocation, 8)
	var workers sync.WaitGroup
	for i := 0; i < 8; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				ready <- struct{}{}
				results <- allocation{err: err}
				return
			}
			defer tx.Rollback()
			var backend int
			err = tx.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&backend)
			ready <- struct{}{}
			select {
			case <-release:
			case <-ctx.Done():
				results <- allocation{err: ctx.Err()}
				return
			}
			var sequence int64
			if err == nil {
				sequence, err = pgNextPlatformArtifactGenerationSequence(ctx, tx, "route", "scope", time.Now())
			}
			if err == nil {
				err = tx.Commit()
			}
			results <- allocation{sequence, backend, err}
		}()
	}
	for i := 0; i < 8; i++ {
		select {
		case <-ready:
		case <-ctx.Done():
			t.Fatal("writers did not acquire separate connections")
		}
	}
	close(release)
	workers.Wait()
	close(results)
	var sequences []int64
	backends := map[int]bool{}
	for result := range results {
		if result.err != nil {
			t.Fatal(result.err)
		}
		sequences = append(sequences, result.sequence)
		backends[result.backend] = true
	}
	sort.Slice(sequences, func(i, j int) bool { return sequences[i] < sequences[j] })
	if len(backends) != 8 {
		t.Fatalf("wanted eight actual concurrent backends, got %d", len(backends))
	}
	for i, sequence := range sequences {
		if sequence != int64(100002+i) {
			t.Fatalf("concurrent allocations lost order or uniqueness: %v", sequences)
		}
	}
}
