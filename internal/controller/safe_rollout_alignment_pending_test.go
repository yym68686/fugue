package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

func TestCanonicalSchedulingDeferralRequiresServingRevisionProof(t *testing.T) {
	for _, scenario := range []string{"ready", "wrong_marker", "wrong_template", "wrong_selector", "empty_endpoints", "unready_candidate", "unconfirmed_edge", "policy_changed"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			s, app, candidate, f := newDrainWorkloadFixtureWithSnapshot(t, false, true)
			app.Spec.RuntimeID, app.Spec.Replicas = candidate.RuntimeID, 2
			candidate.Role, candidate.Status = model.AppReleaseRoleStable, model.AppReleaseStatusServing
			candidate, err := s.Store.UpdateAppRelease(candidate)
			if err != nil {
				t.Fatal(err)
			}
			policy := model.AppTrafficPolicy{TenantID: app.TenantID, AppID: app.ID, Mode: model.AppTrafficModeSingle, StableReleaseID: candidate.ID, StableWeight: 100}
			if _, err := s.Store.UpsertAppTrafficPolicy(policy); err != nil {
				t.Fatal(err)
			}
			name, serviceName := runtime.RuntimeAppResourceName(app), runtime.RuntimeAppServiceName(app)
			canonical := cloneKubeMap(f.deployment)
			metadata := objectMapField(canonical, "metadata")
			metadata["name"], metadata["uid"] = name, "canonical-uid"
			marker := safeCanonicalHandoff{ReleaseID: candidate.ID, DeploymentUID: "canonical-uid", ReleaseKey: "previous-key", Image: "registry.example/previous:v1"}
			if scenario == "wrong_marker" {
				marker.DeploymentUID = "foreign-uid"
			}
			b, _ := json.Marshal(marker)
			objectMapField(metadata, "annotations")[safeCanonicalHandoffAnnotation] = string(b)
			key := s.Renderer.ManagedAppReleaseKey(s.Renderer.PrepareApp(app), runtime.SchedulingConstraints{})
			if scenario == "wrong_template" {
				key = "unrelated-key"
			}
			objectMapField(objectMapValue(nestedObjectValue(canonical, "spec", "template", "metadata")), "annotations")[runtime.FugueAnnotationReleaseKey] = key
			canonical["status"] = map[string]any{"readyReplicas": 0, "replicas": 1}
			service := cloneKubeMap(f.service)
			objectMapField(service, "metadata")["name"] = serviceName
			if scenario == "wrong_selector" {
				objectMapField(service, "spec")["selector"] = map[string]any{runtime.FugueLabelAppWorkload: name}
			}
			endpoints := []map[string]any{}
			for i, pod := range f.pods {
				ip := []string{"10.0.0.10", "10.0.0.11"}[i]
				objectMapField(pod, "status")["podIP"] = ip
				endpoints = append(endpoints, map[string]any{"addresses": []string{ip}, "conditions": map[string]any{"ready": true}})
			}
			if scenario == "empty_endpoints" {
				endpoints = nil
			}
			if scenario == "unready_candidate" {
				objectMapField(f.deployment, "status")["readyReplicas"] = 0
			}
			if scenario == "unconfirmed_edge" {
				s.safeRolloutEdgeBundleObserver = staticSafeRolloutEdgeObserver{}
			}
			if scenario == "policy_changed" {
				s.safeRolloutEdgeBundleObserver = retirementTestObserver(func() {
					policy.StableWeight, policy.CandidateWeight, policy.Mode, policy.CandidateReleaseID = 50, 50, model.AppTrafficModeCanary, "other"
					if _, err := s.Store.UpsertAppTrafficPolicy(policy); err != nil {
						t.Error(err)
					}
				})
			}
			writes := 0
			f.extra = func(w http.ResponseWriter, r *http.Request) bool {
				if r.Method != http.MethodGet {
					writes++
					http.Error(w, "read-only proof", 403)
					return true
				}
				switch {
				case strings.HasSuffix(r.URL.Path, "/deployments/"+name):
					_ = json.NewEncoder(w).Encode(canonical)
				case strings.HasSuffix(r.URL.Path, "/services/"+serviceName):
					_ = json.NewEncoder(w).Encode(service)
				case strings.HasSuffix(r.URL.Path, "/endpointslices"):
					_ = json.NewEncoder(w).Encode(map[string]any{"items": []map[string]any{{"endpoints": endpoints}}})
				default:
					return false
				}
				return true
			}
			state := &safeRolloutState{Enabled: true, StableAlignmentAllowed: true, Candidate: candidate, CandidateApp: app}
			err = s.verifyPromotedServingDuringCanonicalWait(context.Background(), app, state, runtime.SchedulingConstraints{})
			if (err == nil) != (scenario == "ready") {
				t.Fatalf("serving proof scenario=%s error=%v", scenario, err)
			}
			if writes != 0 || f.deleteCalls != 0 {
				t.Fatal("read-only serving proof mutated resources")
			}
		})
	}
}
