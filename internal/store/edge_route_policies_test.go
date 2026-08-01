package store

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/model"
)

func testExclusionPolicy() model.EdgeRoutePolicy {
	now := time.Now().UTC()
	return model.EdgeRoutePolicy{
		Hostname: "api.example.test", AppID: "app-test", TenantID: "tenant-test",
		ExcludedEdgeIDs: []string{"edge-de-1"}, ExclusionReason: "tls failure",
		ExclusionOwnerDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", ExclusionCreatedAt: &now,
		MinHealthyEdgeNodes: 1, RoutePolicy: model.EdgeRoutePolicyEnabled,
	}
}

func TestEdgeRoutePolicyExclusionCASAndCrashReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.json")
	s := New(path)
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	created, err := s.PutEdgeRoutePolicyCAS(testExclusionPolicy(), 0, "")
	if err != nil {
		t.Fatal(err)
	}
	if created.ExclusionGeneration != 1 || created.ExclusionFence == "" || created.ExclusionScope != model.EdgeExclusionScopeEdge {
		t.Fatalf("unexpected created CAS record: %+v", created)
	}
	if _, err := s.DeleteEdgeRoutePolicy(created.Hostname); !errors.Is(err, ErrConflict) {
		t.Fatalf("generic delete bypassed exclusion fence: %v", err)
	}
	unsafeClear := created
	unsafeClear.EdgeGroupID = "edge-group-country-us"
	unsafeClear.ExcludedEdgeIDs = nil
	if _, err := s.PutEdgeRoutePolicyCAS(unsafeClear, created.ExclusionGeneration, created.ExclusionFence); !errors.Is(err, ErrConflict) {
		t.Fatalf("clear without enforced active/TLS evidence = %v, want conflict", err)
	}

	var wins atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			candidate := created
			candidate.ExclusionReason = "concurrent update"
			if _, err := s.PutEdgeRoutePolicyCAS(candidate, created.ExclusionGeneration, created.ExclusionFence); err == nil {
				wins.Add(1)
			} else if !errors.Is(err, ErrConflict) {
				t.Errorf("unexpected CAS error: %v", err)
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("CAS wins = %d, want 1", wins.Load())
	}

	reopened := New(path)
	if err := reopened.Init(); err != nil {
		t.Fatal(err)
	}
	stored, err := reopened.GetEdgeRoutePolicy(created.Hostname)
	if err != nil {
		t.Fatal(err)
	}
	if stored.ExclusionGeneration != 2 || stored.ExclusionFence == created.ExclusionFence || stored.ExclusionReason != "concurrent update" {
		t.Fatalf("reopened CAS record mismatch: %+v", stored)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "exclusion_lifecycle") || strings.Contains(string(raw), "exclusion_evidence_fresh") {
		t.Fatalf("derived lifecycle/evidence became authoritative file state: %s", raw)
	}
	if _, err := reopened.DeleteEdgeRoutePolicyCAS(stored.Hostname, created.ExclusionGeneration, created.ExclusionFence); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale delete = %v, want conflict", err)
	}
	if _, err := reopened.DeleteEdgeRoutePolicyCAS(stored.Hostname, stored.ExclusionGeneration, stored.ExclusionFence); !errors.Is(err, ErrConflict) {
		t.Fatalf("delete without enforced active/TLS evidence = %v, want conflict", err)
	}
}

func TestEdgeRoutePolicyExclusionCASRejectsUnsealedOwnerMaterial(t *testing.T) {
	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	invalidOwner := testExclusionPolicy()
	invalidOwner.ExclusionOwnerDigest = "platform-admin"
	if _, err := s.PutEdgeRoutePolicyCAS(invalidOwner, 0, ""); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("unsealed owner material = %v, want invalid input", err)
	}
	missingReason := testExclusionPolicy()
	missingReason.ExclusionReason = ""
	if _, err := s.PutEdgeRoutePolicyCAS(missingReason, 0, ""); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("missing exclusion reason = %v, want invalid input", err)
	}
}

func TestEdgeExclusionTLSClearPredicateRejectsStaleAndUnknown(t *testing.T) {
	now := time.Now().UTC()
	instance := model.EdgeNodeInstance{EffectiveHealthy: true, ConsecutiveHealthy: 2, LastHeartbeatAt: now, Node: model.EdgeNode{TLSStatus: model.EdgeTLSStatusReady}}
	if !edgeExclusionInstanceClearEligible(instance, now) {
		t.Fatal("fresh exact TLS-ready instance rejected")
	}
	instance.Node.TLSStatus = model.EdgeTLSStatusPending
	if edgeExclusionInstanceClearEligible(instance, now) {
		t.Fatal("TLS unknown/pending evidence cleared exclusion")
	}
	instance.Node.TLSStatus = model.EdgeTLSStatusReady
	instance.LastHeartbeatAt = now.Add(-time.Minute - time.Nanosecond)
	if edgeExclusionInstanceClearEligible(instance, now) {
		t.Fatal("stale TLS evidence cleared exclusion")
	}
}

func TestEdgeExclusionClearMaterialRequiresEnforcedAuthorityAndNoConflict(t *testing.T) {
	now := time.Now().UTC()
	instance := model.EdgeNodeInstance{EdgeID: "edge-de-1", EdgeGroupID: "edge-group-country-de", Slot: model.EdgeSlotB, InstanceUID: "pod-b", ReleaseEpoch: "release-b", EffectiveHealthy: true, ConsecutiveHealthy: 2, LastHeartbeatAt: now, Node: model.EdgeNode{TLSStatus: model.EdgeTLSStatusReady}}
	epoch := model.EdgeActiveEpoch{EdgeGroupID: instance.EdgeGroupID, Slot: instance.Slot, ReleaseEpoch: instance.ReleaseEpoch, FenceSequence: 1, MinHealthyInstances: 1}
	activation := model.EdgeActivationState{Phase: model.EdgeActivationPhaseActive, RouteAuthority: model.EdgeRouteAuthorityActiveEpoch}
	if err := validateEdgeExclusionClearMaterial(activation, []model.EdgeNodeInstance{instance}, []model.EdgeActiveEpoch{epoch}, []string{instance.EdgeID}, nil, now); !errors.Is(err, ErrConflict) {
		t.Fatalf("in-flight active phase clear = %v", err)
	}
	activation.Phase = model.EdgeActivationPhaseEnforced
	if err := validateEdgeExclusionClearMaterial(activation, []model.EdgeNodeInstance{instance}, []model.EdgeActiveEpoch{epoch}, []string{instance.EdgeID}, nil, now); err != nil {
		t.Fatalf("stable enforced evidence rejected: %v", err)
	}
	activation.Remediation = &model.EdgeRemediationAction{Phase: model.EdgeRemediationPhasePrepared}
	if err := validateEdgeExclusionClearMaterial(activation, []model.EdgeNodeInstance{instance}, []model.EdgeActiveEpoch{epoch}, []string{instance.EdgeID}, nil, now); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflicting remediation clear = %v", err)
	}
}

func TestLegacyEdgeRoutePolicyExclusionMigratesToHold(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.json")
	s := New(path)
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	legacy := testExclusionPolicy()
	legacy.ExclusionOwnerDigest = ""
	legacy.ExclusionCreatedAt = nil
	legacy.ExclusionGeneration = 0
	legacy.ExclusionFence = ""
	if _, err := s.PutEdgeRoutePolicy(legacy); err != nil {
		t.Fatal(err)
	}

	reopened := New(path)
	if err := reopened.Init(); err != nil {
		t.Fatal(err)
	}
	stored, err := reopened.GetEdgeRoutePolicy(legacy.Hostname)
	if err != nil {
		t.Fatal(err)
	}
	if stored.ExclusionLifecycle != model.EdgeExclusionLifecycleLegacyHold || stored.ExclusionGeneration != 1 || stored.ExclusionFence == "" || stored.ExclusionCreatedAt == nil {
		t.Fatalf("legacy exclusion did not migrate fail-closed: %+v", stored)
	}
	if !model.EdgeRoutePolicyHasExclusions(stored) {
		t.Fatal("legacy migration silently cleared exclusion")
	}
}
