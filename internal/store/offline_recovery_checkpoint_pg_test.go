package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"fugue/internal/platformproducer"
)

func TestCapacityRecoveryCheckpointPostgresReplicaIsolation(t *testing.T) {
	database := disposablePhysicalDNSDatabase(t)
	first, second := New("", database), New("", database)
	for _, s := range []*Store{first, second} {
		if err := s.Init(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { s.db.Close() })
	}
	ctx := context.Background()
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		_, err := first.WithAdvisoryLock(ctx, platformproducer.Actor+"|offline-recovery", func() error { close(entered); <-release; return nil })
		done <- err
	}()
	select {
	case <-entered:
	case err := <-done:
		t.Fatal(err)
	case <-time.After(3 * time.Second):
		t.Fatal("recovery election not acquired")
	}
	defer func() {
		close(release)
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	if locked, err := second.WithAdvisoryLock(ctx, platformproducer.Actor+"|offline-recovery", func() error { t.Error("second recovery replica entered"); return nil }); err != nil || locked {
		t.Fatal("duplicate recovery election", locked, err)
	}
	if locked, err := second.WithAdvisoryLock(ctx, platformproducer.Actor, func() error { return nil }); err != nil || !locked {
		t.Fatal("stalled recovery owns producer lock", locked, err)
	}
	hold := platformproducer.OfflineEdge{EdgeID: "edge-a", EdgeGroupID: "edge-group-a", PublicIPv4: "8.8.8.8", Reason: "quota_exhausted", Recovery: &platformproducer.OfflineEdgeRecoveryPolicy{Hostname: "api.example.test", Path: "/", ProbeIntervalSeconds: 300, ProbeTimeoutSeconds: 5, SuccessThreshold: 3, FailureThreshold: 1, FreshnessSeconds: 1800, Capacity: &platformproducer.CapacityRecoveryPolicy{Version: platformproducer.CapacityRecoveryVersion, TrafficClass: "streaming", ReferenceEdgeIDs: []string{"edge-b"}, MinimumBytesPerSecond: 262144, MaxElapsedMilliseconds: 5000, MinimumStableSeconds: 600}}}
	code := platformproducer.CapacityCodeIdentity{NodeUID: "node", FrontPodUID: "front", WorkerPodUID: "worker", ServiceUID: "service", AuthorityUID: "authority", AuthorityRecordDigest: "sha256:" + strings.Repeat("a", 64), WorkerSourceSHA: strings.Repeat("a", 40), WorkerImageDigest: "sha256:" + strings.Repeat("b", 64), ProxyImageDigest: "sha256:" + strings.Repeat("f", 64), FrontImageDigest: "sha256:" + strings.Repeat("c", 64), Slot: "a", Fence: 1, Bundle: "bundle-a"}
	base := time.Now().UTC().Add(-10 * time.Minute)
	observation := platformproducer.OfflineRecoveryObservation{}
	for i := 0; i < 3; i++ {
		at := base.Add(time.Duration(i) * 5 * time.Minute)
		e := platformproducer.CapacityRecoveryEvidence{CheckedAt: at, ReportDigest: "sha256:" + strings.Repeat("d", 64), ReferenceEdgeID: "edge-b", Bytes: 1 << 20, ElapsedMilliseconds: 2000, BytesPerSecond: 524288, Code: code, RouteDigest: "sha256:" + strings.Repeat("e", 64)}
		observation = observation.RecordCapacity(hold, at, true, at.Add(time.Hour), &e, "capacity_verified")
	}
	key := platformproducer.OfflineRecoveryKey(hold)
	if locked, err := first.UpdateObservationCheckpoint(ctx, key, func([]byte) ([]byte, error) {
		if concurrent, e := second.UpdateObservationCheckpoint(ctx, key, func([]byte) ([]byte, error) { t.Error("overlapping checkpoint claim entered"); return nil, nil }); e != nil || concurrent {
			t.Fatal("checkpoint claim not exclusive", concurrent, e)
		}
		return json.Marshal(observation)
	}); err != nil || !locked {
		t.Fatal(err)
	}
	raw, err := second.ReadObservationCheckpoint(ctx, key)
	var loaded platformproducer.OfflineRecoveryObservation
	if err != nil || json.Unmarshal(raw, &loaded) != nil || !loaded.Ready(hold, time.Now().UTC()) {
		t.Fatal("JSON/replica roundtrip lost admitted evidence", loaded, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	if _, err := first.UpdateObservationCheckpoint(canceled, key, func([]byte) ([]byte, error) { cancel(); return []byte(`{"admitted":false}`), nil }); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled checkpoint committed", err)
	}
	retained, err := second.ReadObservationCheckpoint(ctx, key)
	if err != nil || string(retained) != string(raw) {
		t.Fatal("failed write destroyed positive observation", err)
	}
}
