package store

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestEdgeRoutePolicyExclusionFilePostgresParityAndCAS(t *testing.T) {
	databaseURL := requireEdgePostgresTestURL(t)
	pg := New("", databaseURL)
	if err := pg.Init(); err != nil {
		t.Fatal(err)
	}
	file := New(filepath.Join(t.TempDir(), "store.json"))
	if err := file.Init(); err != nil {
		t.Fatal(err)
	}

	suffix := model.NewID("edgeexclusionpg")
	tenant, err := pg.CreateTenant("edge exclusion " + suffix)
	if err != nil {
		t.Fatal(err)
	}
	project, err := pg.CreateProject(tenant.ID, "edge exclusion", "")
	if err != nil {
		t.Fatal(err)
	}
	app, err := pg.CreateAppWithRoute(tenant.ID, project.ID, "edge exclusion", "", model.AppSpec{Image: "example.invalid/test:latest", Ports: []int{8080}, Replicas: 1, RuntimeID: model.DefaultManagedRuntimeID}, model.AppRoute{Hostname: suffix + ".example.test", BaseDomain: "example.test", PublicURL: "https://" + suffix + ".example.test", ServicePort: 8080})
	if err != nil {
		t.Fatal(err)
	}
	edgeID, groupID := "edge-"+suffix, "edge-group-"+suffix
	if _, _, err := pg.UpdateEdgeHeartbeat(model.EdgeNode{ID: edgeID, EdgeGroupID: groupID, Status: model.EdgeHealthUnknown}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		resetPostgresEdgeActivationTestState(t, pg, groupID)
		_, _ = pg.db.Exec(`DELETE FROM fugue_tenants WHERE id = $1`, tenant.ID)
	})

	now, err := pg.EdgeRoutePolicyTime()
	if err != nil {
		t.Fatal(err)
	}
	expires := now.Add(30 * time.Minute)
	input := model.EdgeRoutePolicy{Hostname: app.Route.Hostname, AppID: app.ID, TenantID: tenant.ID, ExcludedEdgeGroupIDs: []string{groupID}, ExclusionReason: "signature failure", ExclusionOwnerDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", ExclusionExpiresAt: &expires, MinHealthyEdgeNodes: 1, RoutePolicy: model.EdgeRoutePolicyEnabled}
	pgCreated, err := pg.PutEdgeRoutePolicyCAS(input, 0, "")
	if err != nil {
		t.Fatal(err)
	}

	fileInput := input
	fileInput.Hostname, fileInput.AppID, fileInput.TenantID = "file.example.test", "file-app", "file-tenant"
	fileCreated, err := file.PutEdgeRoutePolicyCAS(fileInput, 0, "")
	if err != nil {
		t.Fatal(err)
	}
	assertExclusionSemanticParity(t, fileCreated, pgCreated)

	second := New("", databaseURL)
	if err := second.Init(); err != nil {
		t.Fatal(err)
	}
	clockA, err := pg.EdgeRoutePolicyTime()
	if err != nil {
		t.Fatal(err)
	}
	clockB, err := second.EdgeRoutePolicyTime()
	if err != nil {
		t.Fatal(err)
	}
	if delta := clockA.Sub(clockB); delta < -2*time.Second || delta > 2*time.Second {
		t.Fatalf("database lifecycle clocks diverged: %s vs %s", clockA, clockB)
	}

	var wins atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			candidate := pgCreated
			candidate.ExclusionReason = fmt.Sprintf("writer-%d", index)
			if _, err := second.PutEdgeRoutePolicyCAS(candidate, pgCreated.ExclusionGeneration, pgCreated.ExclusionFence); err == nil {
				wins.Add(1)
			} else if !errors.Is(err, ErrConflict) {
				t.Errorf("unexpected PG CAS error: %v", err)
			}
		}(i)
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("PG CAS wins = %d, want 1", wins.Load())
	}
	stored, err := pg.GetEdgeRoutePolicy(input.Hostname)
	if err != nil {
		t.Fatal(err)
	}
	if stored.ExclusionGeneration != 2 || stored.ExclusionFence == pgCreated.ExclusionFence {
		t.Fatalf("PG CAS did not advance: %+v", stored)
	}
	if _, err := pg.DeleteEdgeRoutePolicyCAS(input.Hostname, pgCreated.ExclusionGeneration, pgCreated.ExclusionFence); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale PG delete = %v", err)
	}
	active := healthyEdgeTestInstance(edgeID, groupID, model.EdgeSlotB, "pod-b", "release-b")
	state, err := pg.GetEdgeActivationState()
	if err != nil {
		t.Fatal(err)
	}
	state = advanceEdgeActivationTest(t, pg, state, model.EdgeActivationPhaseShadow, nil, nil, "")
	heartbeatEdgeInstanceTwice(t, pg, active)
	expected := []model.EdgeExpectedInstance{{EdgeID: edgeID, EdgeGroupID: groupID, Slot: active.Slot, InstanceUID: active.InstanceUID, ReleaseEpoch: active.ReleaseEpoch}}
	epochs := []model.EdgeActiveEpoch{{EdgeGroupID: groupID, Slot: active.Slot, ReleaseEpoch: active.ReleaseEpoch, FenceSequence: 1, MinHealthyInstances: 1}}
	state = advanceEdgeActivationTest(t, pg, state, model.EdgeActivationPhaseFenced, expected, epochs, "")
	state = advanceEdgeActivationTest(t, pg, state, model.EdgeActivationPhaseActive, expected, nil, "api-generation-edge-exclusion")
	_ = advanceEdgeActivationTest(t, pg, state, model.EdgeActivationPhaseEnforced, expected, nil, "api-generation-edge-exclusion")
	stored, err = pg.GetEdgeRoutePolicy(input.Hostname)
	if err != nil {
		t.Fatal(err)
	}
	cleared := stored
	cleared.EdgeGroupID = groupID
	cleared.ExcludedEdgeGroupIDs = nil
	cleared, err = pg.PutEdgeRoutePolicyCAS(cleared, stored.ExclusionGeneration, stored.ExclusionFence)
	if err != nil {
		t.Fatalf("atomic PG evidence-backed clear: %v", err)
	}
	if model.EdgeRoutePolicyHasExclusions(cleared) || cleared.ExclusionGeneration != stored.ExclusionGeneration+1 {
		t.Fatalf("PG clear did not preserve CAS semantics: %+v", cleared)
	}
}

func assertExclusionSemanticParity(t *testing.T, file, pg model.EdgeRoutePolicy) {
	t.Helper()
	if file.ExclusionScope != pg.ExclusionScope || file.ExclusionOwnerDigest != pg.ExclusionOwnerDigest || file.ExclusionGeneration != pg.ExclusionGeneration || file.ExclusionReason != pg.ExclusionReason || file.MinHealthyEdgeNodes != pg.MinHealthyEdgeNodes || model.EdgeRoutePolicyExclusionLifecycleAt(file, *file.ExclusionCreatedAt) != model.EdgeRoutePolicyExclusionLifecycleAt(pg, *pg.ExclusionCreatedAt) {
		t.Fatalf("file/PG exclusion semantics differ:\nfile=%+v\npg=%+v", file, pg)
	}
}
