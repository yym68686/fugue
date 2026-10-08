package store

import (
	"context"
	"encoding/json"
	"path/filepath"
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
