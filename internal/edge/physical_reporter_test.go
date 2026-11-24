package edge

import (
	"context"
	"io"
	"log"
	"net/http"
	"strings"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/edgecontrol"
	"fugue/internal/edgegroupfront"
)

func TestOverlapWorkerOnlySelectedImageReportsThroughRollback(t *testing.T) {
	group := "edge-group-test"
	now := time.Now().UTC()
	src := strings.Repeat("d", 40)
	oldImage, nextImage := "sha256:"+strings.Repeat("b", 64), "sha256:"+strings.Repeat("c", 64)
	path := writeInventoryActivationFixture(t, now, group, "a", src)
	newWorker := func(slot, digest, source string) *Service {
		ref := "registry.example.test/worker@" + digest
		edge := config.EdgeConfig{EdgeID: "node-x", EdgeGroupID: group, EdgeSlot: slot, EdgeInstanceUID: slot + "-pod-" + digest, EdgeReleaseEpoch: source, APIURL: "http://control.example.test", EdgeToken: "local-fixture"}
		route := RouteBundleSourceConfig{URL: "http://control.example.test" + edgeControlBundlePath, CandidateURL: "http://control.example.test" + edgeControlCandidateBundlePath, TokenFile: "/test/token", VerifierKeyringFile: "/test/keyring", ActivationStateFile: path, ReporterImageRef: &ref}
		s := NewServiceWithRouteBundleSource(edge, route, log.New(io.Discard, "", 0))
		s.InventoryProducer.ActivationStateFile = path
		reject := &http.Client{Transport: inventoryRoundTripFunc(func(r *http.Request) (*http.Response, error) {
			t.Fatalf("inactive Worker attempted %s %s", r.Method, r.URL.Path)
			return nil, nil
		})}
		s.HTTPClient, s.InventoryProducerHTTPClient = reject, reject
		return s
	}
	a, b, replacement := newWorker("a", oldImage, src), newWorker("b", oldImage, src), newWorker("a", nextImage, src)
	wrongSource := newWorker("a", nextImage, strings.Repeat("a", 40))
	workers := []*Service{a, b, replacement, wrongSource}
	check := func(active *Service) {
		t.Helper()
		for _, s := range workers {
			v, err := s.selectRouteBundleSource()
			if err != nil || v.reporterInactive != (s != active) || v.candidate != (s.Config.EdgeSlot != active.Config.EdgeSlot) {
				t.Fatalf("read source and reporting identity disagree: %+v err=%v", v, err)
			}
			if yes, known := s.servingActiveForHeartbeat(); !known || yes != (s == active) {
				t.Fatal("wrong node heartbeat owner")
			}
			if s == active {
				continue
			}
			for _, write := range []func(context.Context) error{s.HeartbeatOnce, s.SyncPlatformShadowOnce, s.SyncPlatformTLSShadowOnce, s.SyncPlatformServingOnce, s.syncHostedRoutes} {
				if err := write(t.Context()); err != nil {
					t.Fatalf("inactive reporting: %v", err)
				}
			}
			if err := s.inventoryHeartbeatAttempt(t.Context(), s.Config, Status{}); err != nil {
				t.Fatalf("inactive inventory opened an identity or cursor: %v", err)
			}
		}
	}
	change := func(fence uint64, from, to, image, op string, rollback uint64) {
		t.Helper()
		_, err := edgegroupfront.ApplyActivationCAS(path, edgegroupfront.ActivationCASRequest{GroupID: group, ExpectedGeneration: fence, ExpectedSlot: from, TargetSlot: to, BundleGeneration: "bundle-generation-1", WorkerSourceCommit: src, WorkerImageDigest: image, Operation: op, RollbackOfGeneration: rollback, Reason: "node local overlap fixture"}, now.Add(time.Duration(fence)*time.Second))
		if err != nil {
			t.Fatal(err)
		}
	}
	check(a)
	change(1, "a", "b", oldImage, edgegroupfront.ActivationOperationPromote, 0)
	check(b)
	change(2, "b", "a", nextImage, edgegroupfront.ActivationOperationPromote, 0)
	check(replacement)

	// The replacement loses ownership while reading the producer cursor.
	// No late POST (including a failed observation) should represent it.
	replacement.InventoryProducer.IdentityKeyringFile, _ = writeInventoryProducerKeyringFixture(t, group)
	replacement.InventoryProducer.URL = "http://control.example.test"
	gets := 0
	replacement.InventoryProducerHTTPClient = &http.Client{Transport: inventoryRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodGet {
			t.Fatal("late inventory write after rollback")
		}
		gets++
		change(3, "a", "b", oldImage, edgegroupfront.ActivationOperationRollback, 3)
		return inventoryJSONResponse(http.StatusOK, edgecontrol.AuthorityGroupStatus{GroupID: group, Status: edgecontrol.GroupAuthorityHealthServingLKG, ServingHealthy: true, InventorySequence: 9, InventoryProducerGeneration: 7}), nil
	})}
	if err := replacement.inventoryHeartbeatAttempt(t.Context(), replacement.Config, Status{}); err == nil || !strings.Contains(err.Error(), "selection changed") || gets != 1 {
		t.Fatalf("rollback race: gets=%d err=%v", gets, err)
	}
	check(b)
}

func TestPhysicalReporterCannotFallBackToLegacyWhenProjectionMissing(t *testing.T) {
	for _, image := range []string{"", "registry.example.test/worker:latest", "wrong repo@sha256:" + strings.Repeat("b", 64)} {
		t.Setenv("FUGUE_EDGE_PHYSICAL_WORKER_IMAGE", image)
		s := NewServiceWithRouteBundleSource(config.EdgeConfig{}, RouteBundleSourceFromEnv(), log.New(io.Discard, "", 0))
		if yes, known := s.servingActiveForHeartbeat(); !known || yes {
			t.Fatal("missing immutable identity inherited the unbound default")
		}
	}
}
