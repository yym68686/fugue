package store

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"github.com/DATA-DOG/go-sqlmock"
)

func TestNetworkSamplesImmutableDeduplicatedBoundedAndScoped(t *testing.T) {
	state := New(filepath.Join(t.TempDir(), "state.json"))
	if err := state.Init(); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	value := 7.25
	sample := model.EdgeNetworkSample{ID: "sample-a", EdgeID: "edge-a", EdgeGroupID: "shared", Hostname: "app.example.test",
		PathPrefix: "/", TrafficClass: "streaming", RouteDigest: "sha256:" + strings.Repeat("a", 64), BundleVersion: "bundle-one",
		ServiceTarget: "app.tenant.svc.cluster.local:3000", Source: "service_endpoint_tcp_info_v1", ServiceRTTMS: &value, ObservedAt: now}
	ctx := context.Background()
	if err := state.RecordEdgeNetworkSamples(ctx, []model.EdgeNetworkSample{sample}, now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	changed := sample
	changed.ServiceRTTMS = nil
	if err := state.RecordEdgeNetworkSamples(ctx, []model.EdgeNetworkSample{changed}, now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	samples, err := state.ListEdgeNetworkSamples(ctx, sample.Hostname, now.Add(-time.Minute), 1)
	if err != nil || len(samples) != 1 || samples[0].ServiceRTTMS == nil || *samples[0].ServiceRTTMS != value {
		t.Fatal("observation changed on retry", samples, err)
	}
	sample.EdgeID = "edge-b"
	if err := state.RecordEdgeNetworkSamples(ctx, []model.EdgeNetworkSample{sample}, now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	samples, err = state.ListEdgeNetworkSamples(ctx, sample.Hostname, now.Add(-time.Minute), 10)
	if err != nil || len(samples) != 2 || samples[0].EdgeID == samples[1].EdgeID {
		t.Fatal("sibling physical edges merged", samples, err)
	}
	if samples, err := state.ListEdgeNetworkSamples(ctx, "other.example.test", now.Add(-time.Minute), 10); err != nil || len(samples) != 0 {
		t.Fatal("cross-host evidence", samples, err)
	}
	if _, err := state.ListEdgeNetworkSamples(ctx, "", now, 10); err == nil {
		t.Fatal("unbounded query allowed")
	}
	if err := state.RecordEdgeNetworkSamples(ctx, nil, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if samples, err := state.ListEdgeNetworkSamples(ctx, sample.Hostname, now.Add(-time.Minute), 10); err != nil || len(samples) != 0 {
		t.Fatal("retention failed", samples, err)
	}
}

func TestNetworkWitnessCollectionDoesNotBreakPreviousReaders(t *testing.T) {
	state := New(filepath.Join(t.TempDir(), "state.json"))
	if err := state.Init(); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	witness := model.EdgeNetworkSample{ID: "witness-a", EdgeID: "edge-a", EdgeGroupID: "shared", Hostname: "app.example.test", PathPrefix: "/",
		TrafficClass: "streaming", RouteDigest: "sha256:" + strings.Repeat("a", 64), BundleVersion: "bundle-one", Source: "route_tls_witness_v1", ObservedAt: now,
		RouteWitness: &model.EdgeNetworkRouteWitness{Address: "203.0.113.5", ValidUntil: now.Add(time.Minute)}}
	ctx := context.Background()
	if err := state.RecordEdgeNetworkSamples(ctx, []model.EdgeNetworkSample{witness}, time.Time{}); err == nil {
		t.Fatal("new evidence polluted previous-reader collection")
	}
	if err := state.RecordEdgeNetworkRouteWitnesses(ctx, []model.EdgeNetworkSample{witness}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if samples, err := state.ListEdgeNetworkSamples(ctx, witness.Hostname, now.Add(-time.Minute), 10); err != nil || len(samples) != 0 {
		t.Fatal("previous reader sees unsupported source", samples, err)
	}
	if samples, err := state.ListEdgeNetworkRouteWitnesses(ctx, witness.Hostname, now.Add(-time.Minute), 10); err != nil || len(samples) != 1 || samples[0].Hostname != witness.Hostname {
		t.Fatal("witness identity lost", samples, err)
	}
	database, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	state = &Store{databaseURL: "postgres://example", db: database, dbReady: true}
	raw, _ := json.Marshal(witness)
	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO fugue_edge_network_samples (edge_id, id, hostname, observed_at, sample_json) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (edge_id, id) DO NOTHING`).
		WithArgs(witness.EdgeID, witness.ID, "route-witness:"+witness.Hostname, witness.ObservedAt, raw).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	if err := state.RecordEdgeNetworkRouteWitnesses(ctx, []model.EdgeNetworkSample{witness}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	mock.ExpectQuery(`SELECT sample_json FROM fugue_edge_network_samples WHERE hostname = $1 AND observed_at >= $2 ORDER BY observed_at DESC, edge_id, id LIMIT $3`).
		WithArgs("route-witness:"+witness.Hostname, now, 10).WillReturnRows(sqlmock.NewRows([]string{"sample_json"}).AddRow(raw))
	if samples, err := state.ListEdgeNetworkRouteWitnesses(ctx, witness.Hostname, now, 10); err != nil || len(samples) != 1 || samples[0].Hostname != witness.Hostname {
		t.Fatal(samples, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestNetworkSamplesPostgresBatchAtomicityAndParity(t *testing.T) {
	s := metricsProjectionTestDB(t)
	_, err := s.db.Exec(`CREATE TEMP TABLE fugue_edge_network_samples (
 edge_id text NOT NULL, id text NOT NULL CHECK (id <> 'reject-batch'), hostname text NOT NULL,
 observed_at timestamptz NOT NULL, sample_json jsonb NOT NULL, PRIMARY KEY (edge_id,id))`)
	if err != nil {
		t.Fatal(err)
	}
	file := New(filepath.Join(t.TempDir(), "state.json"))
	if err := file.Init(); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	value := 7.25
	sample := model.EdgeNetworkSample{ID: "sample-a", EdgeID: "edge-a", EdgeGroupID: "shared", Hostname: "app.example.test",
		PathPrefix: "/", TrafficClass: "streaming", RouteDigest: "sha256:" + strings.Repeat("a", 64), BundleVersion: "bundle-one",
		ServiceTarget: "app.tenant.svc.cluster.local:3000", Source: "service_endpoint_tcp_info_v1", ServiceRTTMS: &value, ObservedAt: now}
	batch := make([]model.EdgeNetworkSample, 128)
	for i := range batch {
		batch[i] = sample
		batch[i].ID = fmt.Sprintf("sample-%03d", i)
	}
	// Duplicate keys in the same statement retain the first observation.
	batch[127] = batch[0]
	batch[127].ServiceRTTMS = nil
	ctx := context.Background()
	for _, state := range []*Store{s, file} {
		if err := state.RecordEdgeNetworkSamples(ctx, batch, now.Add(-time.Hour)); err != nil {
			t.Fatal(err)
		}
		if err := state.RecordEdgeNetworkSamples(ctx, batch, now.Add(-time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.ListEdgeNetworkSamples(ctx, sample.Hostname, now.Add(-time.Minute), 200)
	want, wantErr := file.ListEdgeNetworkSamples(ctx, sample.Hostname, now.Add(-time.Minute), 200)
	if err != nil || wantErr != nil || len(got) != 127 || !reflect.DeepEqual(got, want) {
		t.Fatalf("batch differs: pg=%d file=%d errors=%v/%v", len(got), len(want), err, wantErr)
	}
	first, blocked := sample, sample
	first.ID = "new-before-failure"
	blocked.ID = "reject-batch"
	if err := s.RecordEdgeNetworkSamples(ctx, []model.EdgeNetworkSample{first, blocked}, now.Add(time.Second)); err == nil {
		t.Fatal("constraint failure accepted")
	}
	// A failed insert must also undo retention, not leave a partially pruned batch.
	got, err = s.ListEdgeNetworkSamples(ctx, sample.Hostname, now.Add(-time.Minute), 200)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatal("failed batch changed retained evidence", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := s.RecordEdgeNetworkSamples(cancelled, []model.EdgeNetworkSample{first}, now.Add(time.Second)); err == nil {
		t.Fatal("cancelled batch accepted")
	}
	if err := s.RecordEdgeNetworkSamples(ctx, append(batch, sample), time.Time{}); err == nil {
		t.Fatal("oversize batch accepted")
	}
	if err := s.RecordEdgeNetworkSamples(ctx, nil, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	got, err = s.ListEdgeNetworkSamples(ctx, sample.Hostname, now.Add(-time.Minute), 200)
	if err != nil || len(got) != 0 {
		t.Fatal("empty-batch retention failed", err)
	}
	witness := sample
	witness.ID, witness.Source, witness.ServiceRTTMS = "witness-a", "route_tls_witness_v1", nil
	witness.ServiceTarget = ""
	witness.RouteWitness = &model.EdgeNetworkRouteWitness{Address: "203.0.113.5", ValidUntil: now.Add(time.Minute)}
	witnesses := make([]model.EdgeNetworkSample, 65)
	for i := range witnesses {
		witnesses[i] = witness
		witnesses[i].ID = fmt.Sprintf("witness-%03d", i)
	}
	if err := s.RecordEdgeNetworkRouteWitnesses(ctx, witnesses, now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	got, err = s.ListEdgeNetworkRouteWitnesses(ctx, sample.Hostname, now.Add(-time.Minute), 200)
	if err != nil || len(got) != 65 {
		t.Fatal("witness batch lost", err)
	}
	got, err = s.ListEdgeNetworkSamples(ctx, sample.Hostname, now.Add(-time.Minute), 200)
	if err != nil || len(got) != 0 {
		t.Fatal("witness polluted legacy samples", err)
	}
}

func TestNetworkSamplesUseOneBoundedInsert(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := &Store{databaseURL: "postgres://example", db: db, dbReady: true}
	now := time.Now().UTC()
	value := 7.25
	sample := model.EdgeNetworkSample{ID: "sample-a", EdgeID: "edge-a", EdgeGroupID: "shared", Hostname: "app.example.test", PathPrefix: "/", TrafficClass: "streaming", RouteDigest: "sha256:" + strings.Repeat("a", 64), BundleVersion: "bundle-one", ServiceTarget: "app.tenant.svc.cluster.local:3000", Source: "service_endpoint_tcp_info_v1", ServiceRTTMS: &value, ObservedAt: now}
	second := sample
	second.ID = "sample-b"
	mock.ExpectBegin()
	mock.ExpectExec(`DELETE FROM fugue_edge_network_samples WHERE observed_at < $1`).WithArgs(now.Add(-time.Hour)).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`INSERT INTO fugue_edge_network_samples (edge_id, id, hostname, observed_at, sample_json) VALUES ($1, $2, $3, $4, $5), ($6, $7, $8, $9, $10) ON CONFLICT (edge_id, id) DO NOTHING`).WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectCommit()
	if err := s.RecordEdgeNetworkSamples(context.Background(), []model.EdgeNetworkSample{sample, second}, now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
