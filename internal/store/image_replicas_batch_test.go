package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"github.com/DATA-DOG/go-sqlmock"
)

func replicaBatchRows() []model.ImageReplica {
	now := time.Now().UTC().Truncate(time.Microsecond)
	verified, expired := now.Add(-time.Minute), now.Add(-time.Hour)
	var out []model.ImageReplica
	for i := 0; i < 9; i++ {
		x := model.ImageReplica{ID: fmt.Sprintf("replica-%d", i), ImageID: fmt.Sprintf("image-%d", i%3), TenantID: fmt.Sprintf("tenant-%d", i%2), AppID: fmt.Sprintf("app-%d", i%2), Digest: "sha256:" + strings.Repeat("a", 64), NodeID: fmt.Sprintf("node-%d", i%2), RuntimeID: "runtime", ClusterNodeName: "worker", Status: model.ImageReplicaStatusPresent, CreatedAt: now.Add(-24 * time.Hour), UpdatedAt: now.Add(-time.Duration(i) * time.Second), SizeBytes: int64(i + 1), LastError: "retained diagnostic"}
		if i%2 == 0 {
			x.LastVerifiedAt = &verified
		}
		if i == 4 {
			x.LeaseExpiresAt = &expired
		}
		if i == 7 {
			x.Status = model.ImageReplicaStatusStale
		}
		out = append(out, x)
	}
	return out
}

func checkReplicaBatchMatchesIndividualReads(t *testing.T, s *Store) {
	t.Helper()
	for _, filter := range []model.ImageReplicaFilter{
		{PlatformAdmin: true}, {TenantID: "tenant-1"}, {PlatformAdmin: true, TenantID: "ignored-by-existing-admin-filter"},
		{PlatformAdmin: true, Status: model.ImageReplicaStatusPresent}, {PlatformAdmin: true, Status: model.ImageReplicaStatusStale},
		{PlatformAdmin: true, NodeID: "node-0", RuntimeID: "runtime", ClusterNodeName: "worker"},
		{TenantID: "tenant-1", AppID: "app-1"}, {PlatformAdmin: true, ImageID: "image-1"},
		{PlatformAdmin: true, Digest: "sha256:" + strings.Repeat("a", 64)},
	} {
		got, err := s.ListImageReplicasByImageIDs(filter, []string{" image-2 ", "image-0", "image-2", "", "missing"})
		if err != nil {
			t.Fatal(err)
		}
		for _, id := range []string{"image-0", "image-2", "missing"} {
			individual := filter
			var want []model.ImageReplica
			if filter.ImageID == "" || filter.ImageID == id {
				individual.ImageID = id
				want, err = s.ListImageReplicas(individual)
				if err != nil {
					t.Fatal(err)
				}
			}
			if len(got[id]) != len(want) || len(want) > 0 && !reflect.DeepEqual(got[id], want) {
				t.Fatalf("scope/order/fields changed for %s filter=%+v: got %+v want %+v", id, filter, got[id], want)
			}
		}
		if _, exists := got["image-1"]; exists {
			t.Fatal("unselected image leaked into batch")
		}
	}
}

func TestImageReplicaBatchMatchesFileReads(t *testing.T) {
	p := filepath.Join(t.TempDir(), "state.json")
	raw, err := json.Marshal(model.State{ImageReplicas: replicaBatchRows()})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, raw, 0600); err != nil {
		t.Fatal(err)
	}
	s := New(p)
	checkReplicaBatchMatchesIndividualReads(t, s)
	bad := New(t.TempDir())
	if _, err := bad.ListImageReplicasByImageIDs(model.ImageReplicaFilter{PlatformAdmin: true}, []string{"selected"}); err == nil {
		t.Fatal("storage failure became empty success")
	}
	if got, err := bad.ListImageReplicasByImageIDs(model.ImageReplicaFilter{PlatformAdmin: true}, []string{" ", ""}); err != nil || len(got) != 0 {
		t.Fatal("empty batch scanned storage", err)
	}
}

func TestImageReplicaBatchUsesOneScopedQueryAndPropagatesFailure(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual), sqlmock.ValueConverterOption(performanceArrayConverter{}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := &Store{db: db, databaseURL: "postgres://example", dbReady: true}
	query := "SELECT " + imageReplicaColumns() + " FROM fugue_image_replicas WHERE tenant_id = $1 AND status = $2 AND image_id = ANY($3::text[]) ORDER BY COALESCE(last_verified_at, updated_at) DESC, updated_at DESC"
	columns := strings.Split(strings.ReplaceAll(imageReplicaColumns(), " ", ""), ",")
	mock.ExpectQuery(query).WithArgs("tenant", model.ImageReplicaStatusPresent, `["one","two"]`).WillReturnRows(sqlmock.NewRows(columns))
	if _, err := s.ListImageReplicasByImageIDs(model.ImageReplicaFilter{TenantID: "tenant", Status: model.ImageReplicaStatusPresent}, []string{"one", "two", "two"}); err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("replica query unavailable")
	mock.ExpectQuery(query).WithArgs("tenant", model.ImageReplicaStatusPresent, `["one","two"]`).WillReturnError(wantErr)
	if _, err := s.ListImageReplicasByImageIDs(model.ImageReplicaFilter{TenantID: "tenant", Status: model.ImageReplicaStatusPresent}, []string{"one", "two"}); !errors.Is(err, wantErr) {
		t.Fatalf("query error changed: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func replicaBatchPostgresStore(tb testing.TB) *Store {
	tb.Helper()
	address := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if address == "" {
		tb.Skip("requires disposable local PostgreSQL")
	}
	u, err := url.Parse(address)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.Contains(u.Path, "fugue_test") {
		tb.Fatal("requires disposable loopback fugue_test database")
	}
	db, err := sql.Open("pgx", address)
	if err != nil {
		tb.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	tb.Cleanup(func() { db.Close() })
	_, err = db.Exec(`CREATE TEMPORARY TABLE fugue_image_replicas(id text,image_id text,tenant_id text,app_id text,digest text,node_id text,runtime_id text,cluster_node_name text,cache_endpoint text,failure_domain text,status text,source_replica_id text,last_verified_at timestamptz,lease_expires_at timestamptz,size_bytes bigint,last_error text,created_at timestamptz,updated_at timestamptz)`)
	if err != nil {
		tb.Fatal(err)
	}
	if _, err := db.Exec(`CREATE INDEX ON fugue_image_replicas(image_id)`); err != nil {
		tb.Fatal(err)
	}
	return &Store{db: db, databaseURL: address, dbReady: true}
}

func TestImageReplicaBatchMatchesPostgresReads(t *testing.T) {
	s := replicaBatchPostgresStore(t)
	for _, x := range replicaBatchRows() {
		_, err := s.db.Exec(`INSERT INTO fugue_image_replicas VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)`, x.ID, x.ImageID, nullIfEmpty(x.TenantID), x.AppID, x.Digest, x.NodeID, x.RuntimeID, x.ClusterNodeName, x.CacheEndpoint, x.FailureDomain, x.Status, x.SourceReplicaID, x.LastVerifiedAt, x.LeaseExpiresAt, x.SizeBytes, x.LastError, x.CreatedAt, x.UpdatedAt)
		if err != nil {
			t.Fatal(err)
		}
	}
	checkReplicaBatchMatchesIndividualReads(t, s)
	// A single array also works beyond the SQL bind-parameter count limit.
	ids := make([]string, 70000)
	for i := range ids {
		ids[i] = fmt.Sprintf("image-%d", i)
	}
	got, err := s.ListImageReplicasByImageIDs(model.ImageReplicaFilter{PlatformAdmin: true}, ids)
	if err != nil || len(got) != 3 {
		t.Fatalf("large batch failed: groups=%d err=%v", len(got), err)
	}
	// Keep the temporary table shadowing any public table in the disposable
	// database; dropping it would accidentally expose that unrelated table.
	if _, err := s.db.Exec(`ALTER TABLE fugue_image_replicas RENAME COLUMN id TO unavailable_id`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ListImageReplicasByImageIDs(model.ImageReplicaFilter{PlatformAdmin: true}, []string{"image-0"}); err == nil {
		t.Fatal("query failure became empty success")
	}
	if got, err := s.ListImageReplicasByImageIDs(model.ImageReplicaFilter{PlatformAdmin: true}, nil); err != nil || len(got) != 0 {
		t.Fatal("empty batch queried invalid table")
	}
}

func BenchmarkImageReplicaBatchPostgres(b *testing.B) {
	s := replicaBatchPostgresStore(b)
	_, err := s.db.Exec(`INSERT INTO fugue_image_replicas SELECT 'replica-'||i,'image-'||(i/2),'tenant','app','sha256:'||repeat('a',64),'node','runtime','worker','','','present','',NULL,NULL,128,'',now(),now() FROM generate_series(0,2047) i`)
	if err != nil {
		b.Fatal(err)
	}
	ids := make([]string, 1024)
	for i := range ids {
		ids[i] = fmt.Sprintf("image-%d", i)
	}
	for _, batch := range []bool{false, true} {
		name := "individual"
		if batch {
			name = "batch"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				count := 0
				if batch {
					rows, err := s.ListImageReplicasByImageIDs(model.ImageReplicaFilter{PlatformAdmin: true}, ids)
					if err != nil {
						b.Fatal(err)
					}
					for _, v := range rows {
						count += len(v)
					}
				} else {
					for _, id := range ids {
						rows, err := s.ListImageReplicas(model.ImageReplicaFilter{PlatformAdmin: true, ImageID: id})
						if err != nil {
							b.Fatal(err)
						}
						count += len(rows)
					}
				}
				if count != 2048 {
					b.Fatalf("row set changed: %d", count)
				}
			}
		})
	}
}
