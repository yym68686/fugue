package edge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/routeprobe"
)

func TestServingObservationBoundedPrivateAndIndependent(t *testing.T) {
	s := &Service{}
	for range servingObservationLimit + 5 {
		value := servingObservation{StartedAt: time.Now().UTC(), Stage: "route_probes", Applied: servingPublication(&model.TrafficReleaseBinding{ReleaseSetID: "private-parent", RouteArtifactID: "private-artifact", ReleaseID: "private-release", ReleaseChannel: "private-channel", FencingToken: 7})}
		for n := range servingProbeObservationLimit + 4 {
			value.Routes.failure(fmt.Sprintf("private-host-%d", n), "unclassified")
		}
		s.retainServingObservation(value, errors.New("private-token-and-error"))
		value.Routes.Failures[0].Identity = "mutated-input"
	}
	// A success must not erase the failure ring or reset its counters.
	s.retainServingObservation(servingObservation{Stage: "complete", Outcome: "ok"}, nil)
	read := func() map[string]any {
		value, err := s.ServingObservationSnapshot(context.Background())
		raw, marshalErr := json.Marshal(value)
		if err != nil || marshalErr != nil || len(raw) > 1<<20 {
			t.Fatal(err, marshalErr, len(raw))
		}
		for _, private := range []string{"private-", "mutated-input"} {
			if strings.Contains(string(raw), private) {
				t.Fatalf("private input retained: %s", private)
			}
		}
		var snapshot map[string]any
		if err := json.Unmarshal(raw, &snapshot); err != nil {
			t.Fatal(err)
		}
		return snapshot
	}
	value := read()
	if value["completed"] != float64(servingObservationLimit+6) || value["failed"] != float64(servingObservationLimit+5) || value["evicted"] != float64(5) || len(value["failures"].([]any)) != servingObservationLimit {
		t.Fatal("incorrect bounded coverage", value)
	}
	last := value["failures"].([]any)[servingObservationLimit-1].(map[string]any)
	routes := last["routes"].(map[string]any)
	if routes["failed"] != float64(servingProbeObservationLimit+4) || routes["failures_omitted"] != float64(4) || len(routes["failures"].([]any)) != servingProbeObservationLimit {
		t.Fatal(routes)
	}
	// The provider owns copies, not mutable aliases into the ring.
	snapshot, _ := s.ServingObservationSnapshot(context.Background())
	raw, _ := json.Marshal(snapshot)
	s.servingObservations.mu.Lock()
	s.servingObservations.failures[0].Routes.Failures[0].Reason = "changed-after-snapshot"
	s.servingObservations.mu.Unlock()
	after, _ := json.Marshal(snapshot)
	if string(after) != string(raw) {
		t.Fatal("snapshot aliases ring")
	}
	var workers sync.WaitGroup
	for range 4 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for range 100 {
				s.retainServingObservation(servingObservation{Stage: "complete", Outcome: "ok"}, nil)
				_, _ = s.ServingObservationSnapshot(context.Background())
			}
		}()
	}
	workers.Wait()
}

func TestServingTLSObservationUsesExistingReadinessSemantics(t *testing.T) {
	now := time.Now().UTC()
	valid := platformTLSFact{Hostname: "private-host", Ready: true, CertificateDigest: "certificate", NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), TrustValidUntil: now.Add(time.Hour), CheckedAt: now.Add(-time.Second), ValidUntil: now.Add(time.Minute)}
	for _, mode := range []string{"valid", "failed", "unknown-reason", "expired", "future", "version", "bundle", "certificate"} {
		t.Run(mode, func(t *testing.T) {
			r := &platformTLSReadinessReceipt{BundleVersion: "bundle", BundleValidUntil: now.Add(time.Hour), Facts: []platformTLSFact{valid}}
			switch mode {
			case "failed":
				r.Facts[0].Ready, r.Facts[0].Reason = false, "tls_handshake_failed"
			case "unknown-reason":
				r.Facts[0].Ready, r.Facts[0].Reason = false, "private-error"
			case "expired":
				r.Facts[0].ValidUntil = now
			case "future":
				r.Facts[0].CheckedAt = now.Add(time.Second)
			case "version":
				r.BundleVersion = "different"
			case "bundle":
				r.BundleValidUntil = now
			case "certificate":
				r.Facts[0].CertificateDigest = ""
			}
			got := servingTLSObservation(r, "bundle", now)
			want := summarizePlatformTLSReadiness(r, "bundle", now)
			if got.Total != want.Probes || got.Failed != want.FailedProbes {
				t.Fatal("diagnostic differs from actual readiness", got, want)
			}
			if mode == "failed" && got.Failures[0].Reason != "tls_handshake_failed" {
				t.Fatal(got)
			}
			raw, _ := json.Marshal(got)
			if strings.Contains(string(raw), "private-") {
				t.Fatal("raw hostname or reason escaped")
			}
		})
	}
}

func TestServingRouteObservationDoesNotChangeProbeResult(t *testing.T) {
	s := NewService(config.EdgeConfig{}, nil)
	bundle := model.EdgeRouteBundle{Version: "bundle"}
	for i := range 20 {
		bundle.Routes = append(bundle.Routes, model.EdgeRouteBinding{Hostname: fmt.Sprintf("private-host-%d", i), PathPrefix: "/private-path", RoutePolicy: model.EdgeRoutePolicyEnabled})
	}
	policy := &platformconfig.ReadinessProbePolicy{ProbeIntervalSeconds: 10, ProbeTimeoutSeconds: 1, FactFreshnessSeconds: 60, MaxConcurrency: 4, MaxProbes: 32}
	want := errors.New("private-probe-error")
	probe := func(context.Context, model.EdgeRouteBinding, time.Duration) (routeprobe.Proof, error) {
		return routeprobe.Proof{}, want
	}
	var observed servingProbeObservation
	_, err := s.observePlatformServingRoutesObserved(context.Background(), bundle, policy, probe, &observed)
	_, plain := s.observePlatformServingRoutes(context.Background(), bundle, policy, probe)
	if err != want || plain != want || observed.Total != 20 || observed.Failed != 20 || observed.Omitted != 12 || len(observed.Failures) != 8 {
		t.Fatal("probe behavior or bounds changed", observed, err, plain)
	}
	raw, _ := json.Marshal(observed)
	if strings.Contains(string(raw), "private-") {
		t.Fatal("raw probe input or error escaped")
	}
}
