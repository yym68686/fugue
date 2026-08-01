package api

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestAdminEdgeActivationIsPlatformOnlyCASAndHeartbeatCannotPromote(t *testing.T) {
	t.Parallel()
	storeState, server, _, adminKey, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	if _, _, err := storeState.UpdateEdgeHeartbeat(model.EdgeNode{ID: "edge-de-1", EdgeGroupID: "edge-group-country-de", Status: model.EdgeHealthHealthy, Healthy: true, TLSStatus: model.EdgeTLSStatusReady}); err != nil {
		t.Fatal(err)
	}
	instance := model.EdgeNodeInstance{EdgeID: "edge-de-1", EdgeGroupID: "edge-group-country-de", Slot: model.EdgeSlotB, InstanceUID: "pod-b", ReleaseEpoch: "release-b", Node: model.EdgeNode{ID: "edge-de-1", EdgeGroupID: "edge-group-country-de", Status: model.EdgeHealthHealthy, Healthy: true, TLSStatus: model.EdgeTLSStatusReady, RouteBundleVersion: "route-b", CaddyRouteCount: 2}}
	for index := 0; index < 2; index++ {
		if _, err := storeState.UpdateEdgeInstanceHeartbeat(instance); err != nil {
			t.Fatal(err)
		}
	}

	unauthorized := performJSONRequest(t, server, http.MethodGet, "/v1/admin/edge/activation", "", nil)
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("activation inventory must require auth: %d", unauthorized.Code)
	}
	state, _ := storeState.GetEdgeActivationState()
	shadow := activationAPIRequest(state.Generation, model.EdgeActivationPhaseShadow, nil, nil, "")
	denied := performJSONRequest(t, server, http.MethodPost, "/v1/admin/edge/activation", "edge-secret", shadow)
	if denied.Code != http.StatusUnauthorized && denied.Code != http.StatusForbidden {
		t.Fatalf("edge heartbeat credential must not promote: %d body=%s", denied.Code, denied.Body.String())
	}
	tampered := shadow
	tampered.ReleaseRecordVersion = "11"
	invalidPlan := performJSONRequest(t, server, http.MethodPost, "/v1/admin/edge/activation", adminKey, tampered)
	if invalidPlan.Code != http.StatusForbidden {
		t.Fatalf("signed candidate CM identity mix must fail: %d body=%s", invalidPlan.Code, invalidPlan.Body.String())
	}
	wrongGeneration := shadow
	wrongGeneration.Authorization.KeyGeneration = "generation-test-0002"
	invalidGeneration := performJSONRequest(t, server, http.MethodPost, "/v1/admin/edge/activation", adminKey, wrongGeneration)
	if invalidGeneration.Code != http.StatusForbidden {
		t.Fatalf("old key/new generation mix must fail: %d body=%s", invalidGeneration.Code, invalidGeneration.Body.String())
	}

	var wg sync.WaitGroup
	results := make(chan int, 2)
	for index := 0; index < 2; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- performJSONRequest(t, server, http.MethodPost, "/v1/admin/edge/activation", adminKey, shadow).Code
		}()
	}
	wg.Wait()
	close(results)
	counts := map[int]int{}
	for status := range results {
		counts[status]++
	}
	if counts[http.StatusOK] != 1 || counts[http.StatusConflict] != 1 {
		t.Fatalf("two replica CAS must have one commit and one conflict: %v", counts)
	}
	state, _ = storeState.GetEdgeActivationState()
	expected := []model.EdgeExpectedInstance{{EdgeID: instance.EdgeID, EdgeGroupID: instance.EdgeGroupID, Slot: instance.Slot, InstanceUID: instance.InstanceUID, ReleaseEpoch: instance.ReleaseEpoch}}
	epochs := []model.EdgeActiveEpoch{{EdgeGroupID: instance.EdgeGroupID, Slot: instance.Slot, ReleaseEpoch: instance.ReleaseEpoch, FenceSequence: 1, MinHealthyInstances: 1}}
	fenced := performJSONRequest(t, server, http.MethodPost, "/v1/admin/edge/activation", adminKey, activationAPIRequest(state.Generation, model.EdgeActivationPhaseFenced, expected, epochs, ""))
	if fenced.Code != http.StatusOK {
		t.Fatalf("phase2: %d body=%s", fenced.Code, fenced.Body.String())
	}
	state, _ = storeState.GetEdgeActivationState()
	active := performJSONRequest(t, server, http.MethodPost, "/v1/admin/edge/activation", adminKey, activationAPIRequest(state.Generation, model.EdgeActivationPhaseActive, expected, nil, "api-generation-1"))
	if active.Code != http.StatusOK {
		t.Fatalf("phase3: %d body=%s", active.Code, active.Body.String())
	}
	before, _ := storeState.GetEdgeActivationState()
	heartbeat := performJSONRequest(t, server, http.MethodPost, "/v1/edge/heartbeat?token=edge-secret", "", map[string]any{"edge_id": instance.EdgeID, "edge_group_id": instance.EdgeGroupID, "slot": instance.Slot, "instance_uid": instance.InstanceUID, "release_epoch": instance.ReleaseEpoch, "status": model.EdgeHealthHealthy, "healthy": true, "tls_status": model.EdgeTLSStatusReady})
	if heartbeat.Code != http.StatusOK {
		t.Fatalf("active heartbeat: %d body=%s", heartbeat.Code, heartbeat.Body.String())
	}
	after, _ := storeState.GetEdgeActivationState()
	if after.Generation != before.Generation || after.Phase != before.Phase {
		t.Fatalf("heartbeat promoted activation: before=%+v after=%+v", before, after)
	}
}

func activationAPIRequest(generation uint64, phase string, expected []model.EdgeExpectedInstance, epochs []model.EdgeActiveEpoch, apiGeneration string) model.EdgeActivationAdvance {
	advance := model.EdgeActivationAdvance{ExpectedGeneration: generation, ToPhase: phase, PlanDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", EvidenceDigest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", ReleaseID: "release-test", ReleaseRecordUID: "record-uid", ReleaseRecordVersion: "10", ReleaseRecordDigest: "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", ExpectedInstances: expected, ActiveEpochs: epochs, LegacySnapshotDigest: "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd", APIReplicaGeneration: apiGeneration}
	expectedDigest, epochsDigest, _ := edgeActivationMaterialDigests(expected, epochs)
	nonce := sha256.Sum256([]byte(fmt.Sprintf("%d/%s", generation, phase)))
	advance.Authorization = model.EdgeActivationAuthorization{ReleaseFence: "github:test/repo:1:1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", PhaseNonce: "sha256:" + hex.EncodeToString(nonce[:]), ValidUntil: time.Now().UTC().Add(5 * time.Minute).Truncate(time.Second).Format(time.RFC3339), ExpectedInstancesDigest: expectedDigest, ActiveEpochsDigest: epochsDigest}
	if err := SignEdgeActivationAdvance(&advance, "edge-activation-api-test-signing-key-material", "public-data-plane-release", "generation-test-0001", "signing-secret-uid", "17"); err != nil {
		panic(err)
	}
	return advance
}
