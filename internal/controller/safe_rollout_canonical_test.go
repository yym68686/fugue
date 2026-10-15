package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/drainprotocol"
	"fugue/internal/model"
	"fugue/internal/runtime"
)

func TestManagedAppLiveGuardResumesVerifiedCanonicalHandoffAfterPromotion(t *testing.T) {
	for _, scenario := range []string{"valid", "wrong_uid", "wrong_image", "wrong_key", "changed_desired", "missing_source"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			st, source, desired, op := newSafeRolloutTestState(t)
			svc := &Service{Store: st, Renderer: runtime.Renderer{}}
			promoted, err := st.CreateAppRelease(model.AppRelease{ID: "promoted", AppID: desired.ID, TenantID: desired.TenantID,
				Role: model.AppReleaseRoleCandidate, Status: model.AppReleaseStatusCreating, SpecSnapshot: cloneControllerAppSpec(&desired.Spec),
				DeploymentName: "independent-revision", ServiceName: "independent-revision", RuntimeID: desired.Spec.RuntimeID, ResolvedImageRef: desired.Spec.Image, RollbackTargetID: "previous-canonical"})
			if err != nil {
				t.Fatal(err)
			}
			if _, ok, err := st.TryClaimPendingOperation(op.ID); err != nil || !ok {
				t.Fatal("claim", err)
			}
			promoted, err = st.BindAppReleaseWorkload(context.Background(), promoted, model.AppReleaseWorkload{OperationID: op.ID,
				Namespace: runtime.NamespaceForTenant(desired.TenantID), DeploymentName: promoted.DeploymentName, DeploymentUID: "candidate-uid",
				DeploymentGeneration: 1, ServiceName: promoted.ServiceName, ServiceUID: "candidate-service-uid", ReleaseKey: "candidate-key",
				RuntimeID: desired.Spec.RuntimeID, ImageRef: desired.Spec.Image})
			if err != nil {
				t.Fatal(err)
			}
			promoted.Role, promoted.Status = model.AppReleaseRoleStable, model.AppReleaseStatusServing
			if _, err := st.UpdateAppRelease(promoted); err != nil {
				t.Fatal(err)
			}
			if _, err := st.UpsertAppTrafficPolicy(model.AppTrafficPolicy{AppID: desired.ID, TenantID: desired.TenantID,
				Mode: model.AppTrafficModeSingle, StableReleaseID: promoted.ID, StableWeight: 100}); err != nil {
				t.Fatal(err)
			}
			if scenario != "missing_source" {
				if _, err := st.CreateAppRelease(model.AppRelease{ID: "previous-canonical", AppID: source.ID, TenantID: source.TenantID,
					Role: model.AppReleaseRolePrevious, Status: model.AppReleaseStatusDraining, SpecSnapshot: cloneControllerAppSpec(&source.Spec)}); err != nil {
					t.Fatal(err)
				}
			}
			live, _ := svc.expectedManagedAppDeployment(svc.Renderer.PrepareApp(source), runtime.SchedulingConstraints{})
			managedAppLiveGuardMarkReady(&live, 1)
			live.Metadata.UID = "canonical-uid"
			marker := safeCanonicalHandoff{ReleaseID: promoted.ID, DeploymentUID: live.Metadata.UID,
				ReleaseKey: live.Metadata.Annotations[runtime.FugueAnnotationReleaseKey], Image: source.Spec.Image}
			switch scenario {
			case "wrong_uid":
				marker.DeploymentUID = "recreated-uid"
			case "wrong_image":
				marker.Image = "ghcr.io/example/api:unverified"
			case "wrong_key":
				marker.ReleaseKey = "unverified-key"
			case "changed_desired":
				desired.Spec.Image = "ghcr.io/example/api:later"
			}
			encoded, _ := json.Marshal(marker)
			live.Metadata.Annotations[safeCanonicalHandoffAnnotation] = string(encoded)
			// Promotion already advanced both config projections; the source Pod
			// remains alive until the separate canonical drain guard permits apply.
			managed := managedAppLiveGuardObject(t, desired, runtime.SchedulingConstraints{})
			managed.Status.CurrentReleaseKey = marker.ReleaseKey
			var writes int
			client := managedAppLiveGuardClient(t, managed, live, true, true, &writes)
			got, err := svc.prepareManagedAppReconcileRolloutWithEvidence(context.Background(), client, managed.Metadata.Namespace,
				managed, desired, "", runtime.SchedulingConstraints{})
			if (err == nil) != (scenario == "valid") {
				t.Fatalf("scenario %s: %v", scenario, err)
			}
			if err == nil && (got.Spec.Image != desired.Spec.Image || got.Spec.RolloutIntent != model.AppRolloutIntentOnlineImageUpdate) {
				t.Fatalf("lost guarded desired rollout: image=%s intent=%s", got.Spec.Image, got.Spec.RolloutIntent)
			}
			if writes != 0 {
				t.Fatal("source recovery mutated the workload before drain")
			}
		})
	}
}

func TestSafeCanonicalHandoffRetainsOldPodsUntilPositiveDrain(t *testing.T) {
	for _, scenario := range []string{"idle", "busy", "stale_endpoints", "observer_failure", "changed_original", "wrong_selector", "candidate_unhealthy", "resume", "restore_ready", "restore_unready", "restore_stale_endpoints", "restore_wrong_selector", "restore_patch_failure", "restore_wrong_image"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			s, app, candidate, f := newDrainWorkloadFixtureWithSnapshot(t, false, true)
			app.Spec.Replicas = 2
			app.Spec.RuntimeID = candidate.RuntimeID
			candidate.Role, candidate.Status = model.AppReleaseRoleStable, model.AppReleaseStatusServing
			var err error
			candidate, err = s.Store.UpdateAppRelease(candidate)
			if err != nil {
				t.Fatal(err)
			}
			_, err = s.Store.UpsertAppTrafficPolicy(model.AppTrafficPolicy{AppID: app.ID, TenantID: app.TenantID, Mode: model.AppTrafficModeSingle, StableReleaseID: candidate.ID, StableWeight: 100})
			if err != nil {
				t.Fatal(err)
			}
			name := runtime.RuntimeAppResourceName(app)
			serviceName := runtime.RuntimeAppServiceName(app)
			canonical := cloneKubeMap(f.deployment)
			canonical["kind"] = "Deployment"
			canonical["apiVersion"] = "apps/v1"
			cm := objectMapField(canonical, "metadata")
			cm["name"], cm["uid"] = name, "canonical-uid"
			cLabels := objectMapField(cm, "labels")
			delete(cLabels, runtime.FugueLabelAppReleaseID)
			cLabels[runtime.FugueLabelAppWorkload] = name
			objectMapField(cm, "annotations")[runtime.FugueAnnotationReleaseKey] = "old-key"
			tm := objectMapValue(nestedObjectValue(canonical, "spec", "template", "metadata"))
			tm["labels"] = cLabels
			objectMapField(tm, "annotations")[runtime.FugueAnnotationReleaseKey] = "old-key"
			oldImage := "ghcr.io/example/api:old"
			mapSlice(nestedObjectValue(canonical, "spec", "template", "spec", "containers"))[0]["image"] = oldImage
			desired := cloneKubeMap(canonical)
			objectMapField(objectMapField(desired, "metadata"), "annotations")[runtime.FugueAnnotationReleaseKey] = "release-key"
			objectMapField(objectMapValue(nestedObjectValue(desired, "spec", "template", "metadata")), "annotations")[runtime.FugueAnnotationReleaseKey] = "release-key"
			mapSlice(nestedObjectValue(desired, "spec", "template", "spec", "containers"))[0]["image"] = app.Spec.Image
			canonicalService := cloneKubeMap(f.service)
			canonicalService["kind"] = "Service"
			canonicalService["apiVersion"] = "v1"
			sm := objectMapField(canonicalService, "metadata")
			sm["name"], sm["uid"], sm["labels"] = serviceName, "canonical-service-uid", cLabels
			objectMapField(canonicalService, "spec")["selector"] = cLabels
			desiredService := cloneKubeMap(canonicalService)
			sourcePods := []map[string]any{}
			for i, p := range f.pods {
				objectMapField(p, "status")["podIP"] = fmt.Sprintf("10.0.0.%d", i+10)
				old := cloneKubeMap(p)
				m := objectMapField(old, "metadata")
				m["name"], m["uid"], m["labels"] = fmt.Sprintf("original-%d", i), fmt.Sprintf("original-uid-%d", i), cLabels
				objectMapField(m, "annotations")[runtime.FugueAnnotationReleaseKey] = "old-key"
				m["ownerReferences"] = []map[string]any{{"kind": "ReplicaSet", "name": "original-rs", "uid": "original-rs-uid", "controller": true}}
				mapSlice(nestedObjectValue(old, "spec", "containers"))[0]["image"] = oldImage
				sourcePods = append(sourcePods, old)
			}
			sourceReplica := cloneKubeMap(f.replicas[0])
			rm := objectMapField(sourceReplica, "metadata")
			rm["name"], rm["uid"], rm["labels"] = "original-rs", "original-rs-uid", cLabels
			rm["ownerReferences"] = []map[string]any{{"kind": "Deployment", "name": name, "uid": "canonical-uid", "controller": true}}
			targetSelector := objectStringMapValue(nestedObjectValue(f.service, "spec", "selector"))
			marked := scenario == "resume" || strings.HasPrefix(scenario, "restore_")
			if marked {
				b, _ := json.Marshal(safeCanonicalHandoff{ReleaseID: candidate.ID, DeploymentUID: "canonical-uid", ReleaseKey: "old-key", Image: oldImage})
				objectMapField(cm, "annotations")[safeCanonicalHandoffAnnotation] = string(b)
				objectMapField(canonicalService, "spec")["selector"] = targetSelector
			}
			if strings.HasPrefix(scenario, "restore_") {
				canonical["spec"] = cloneKubeMap(objectMapField(desired, "spec"))
				objectMapField(cm, "annotations")[runtime.FugueAnnotationReleaseKey] = "release-key"
				for i, pod := range sourcePods {
					objectMapField(objectMapField(pod, "metadata"), "annotations")[runtime.FugueAnnotationReleaseKey] = "release-key"
					mapSlice(nestedObjectValue(pod, "spec", "containers"))[0]["image"] = app.Spec.Image
					objectMapField(pod, "status")["podIP"] = fmt.Sprintf("10.0.1.%d", i+10)
				}
				if scenario == "restore_wrong_image" {
					mapSlice(nestedObjectValue(sourcePods[0], "spec", "containers"))[0]["image"] = oldImage
				}
				if scenario == "restore_unready" {
					objectMapField(canonical, "status")["updatedReplicas"] = 0
				}
			}
			if scenario == "wrong_selector" || scenario == "restore_wrong_selector" {
				objectMapField(canonicalService, "spec")["selector"] = map[string]string{"unexpected": "workload"}
			}
			if scenario == "candidate_unhealthy" {
				objectMapField(f.deployment, "status")["readyReplicas"] = 0
			}
			sourceReads, sourceProbes, servicePatches, templateWrites := 0, 0, 0, 0
			f.extra = func(w http.ResponseWriter, r *http.Request) bool {
				switch {
				case strings.HasSuffix(r.URL.Path, "/deployments/"+name):
					if r.Method == http.MethodGet {
						sourceReads++
						if scenario == "changed_original" && sourceProbes > 0 {
							cm["uid"] = "replaced-uid"
						}
						_ = json.NewEncoder(w).Encode(canonical)
						return true
					}
					var patch map[string]any
					_ = json.NewDecoder(r.Body).Decode(&patch)
					if patch["spec"] != nil {
						templateWrites++
						http.Error(w, "template mutation forbidden before drain", 500)
						return true
					}
					for k, v := range objectMapField(objectMapField(patch, "metadata"), "annotations") {
						if v == nil {
							delete(objectMapField(cm, "annotations"), k)
						} else {
							objectMapField(cm, "annotations")[k] = v
						}
					}
					_ = json.NewEncoder(w).Encode(canonical)
					return true
				case strings.HasSuffix(r.URL.Path, "/services/"+serviceName):
					if r.Method == http.MethodPatch {
						if scenario == "restore_patch_failure" {
							http.Error(w, "conflicting selector update", http.StatusConflict)
							return true
						}
						var patch []map[string]any
						if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
							t.Error(err)
						}
						if len(patch) != 4 || patch[0]["op"] != "test" || patch[1]["op"] != "test" || patch[2]["op"] != "test" {
							t.Error("selector write lacks identity fences")
						}
						servicePatches++
						objectMapField(canonicalService, "spec")["selector"] = patch[3]["value"]
					}
					_ = json.NewEncoder(w).Encode(canonicalService)
					return true
				case strings.HasSuffix(r.URL.Path, "/replicasets"):
					_ = json.NewEncoder(w).Encode(map[string]any{"items": append(append([]map[string]any{}, f.replicas...), sourceReplica)})
					return true
				case strings.HasSuffix(r.URL.Path, "/pods"):
					_ = json.NewEncoder(w).Encode(map[string]any{"items": append(append([]map[string]any{}, f.pods...), sourcePods...)})
					return true
				case strings.HasSuffix(r.URL.Path, "/endpointslices"):
					addresses := []string{"10.0.0.10", "10.0.0.11"}
					if strings.HasPrefix(scenario, "restore_") && scenario != "restore_stale_endpoints" && objectStringMapValue(nestedObjectValue(canonicalService, "spec", "selector"))[runtime.FugueLabelAppWorkload] == name {
						addresses = []string{"10.0.1.10", "10.0.1.11"}
					}
					if scenario == "stale_endpoints" {
						addresses = append(addresses, "10.0.0.99")
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{map[string]any{"endpoints": []any{map[string]any{"addresses": addresses, "conditions": map[string]bool{"ready": true}}}}}})
					return true
				case strings.Contains(r.URL.Path, "/pods/original-") && strings.HasSuffix(r.URL.Path, "/proxy/drain/observe"):
					sourceProbes++
					if scenario == "observer_failure" {
						http.Error(w, "unavailable", 503)
						return true
					}
					pod := strings.Split(strings.Split(r.URL.Path, "/pods/")[1], ":")[0]
					active := 0
					if scenario == "busy" {
						active = 1
					}
					_ = json.NewEncoder(w).Encode(drainprotocol.Snapshot{APIVersion: drainprotocol.Version, Nonce: r.URL.Query().Get("nonce"), Pod: pod, Namespace: runtime.NamespaceForTenant(app.TenantID), AppPorts: []int{8080}, ActiveConnections: &active})
					return true
				}
				return false
			}
			client, _ := s.kubeClient()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if scenario == "busy" || scenario == "stale_endpoints" || scenario == "restore_stale_endpoints" {
				ctx, cancel = context.WithTimeout(context.Background(), 150*time.Millisecond)
				defer cancel()
			}
			got, err := s.prepareSafeCanonicalHandoff(ctx, client, app, []map[string]any{desired, desiredService})
			success := scenario == "idle" || scenario == "resume" || scenario == "restore_ready"
			if success != (err == nil) {
				t.Fatalf("expected success=%v got %v (reads=%d probes=%d)", success, err, sourceReads, sourceProbes)
			}
			if templateWrites != 0 || f.deleteCalls != 0 {
				t.Fatal("handoff changed or deleted the original workload")
			}
			if success && scenario != "restore_ready" {
				if sourceProbes < 4 {
					t.Fatal("positive quiet interval was not observed")
				}
				if !reflect.DeepEqual(objectStringMapValue(nestedObjectValue(got[1], "spec", "selector")), targetSelector) {
					t.Fatal("apply would redirect traffic to old canonical Pods")
				}
			}
			if scenario == "restore_ready" {
				if servicePatches != 1 || !reflect.DeepEqual(objectStringMapValue(nestedObjectValue(canonicalService, "spec", "selector")), objectStringMapValue(nestedObjectValue(desiredService, "spec", "selector"))) {
					t.Fatal("canonical selector was not replaced exactly")
				}
				if objectStringMapValue(cm["annotations"])[safeCanonicalHandoffAnnotation] != "" {
					t.Fatal("handoff marker not cleared")
				}
				if objectStringMapValue(nestedObjectValue(got[1], "spec", "selector"))[runtime.FugueLabelAppWorkload] != name {
					t.Fatal("new canonical service not restored")
				}
			}
			if strings.HasPrefix(scenario, "restore_") && !success && objectStringMapValue(cm["annotations"])[safeCanonicalHandoffAnnotation] == "" {
				t.Fatal("failed restoration lost its resumable marker")
			}
			if scenario == "candidate_unhealthy" && servicePatches != 0 {
				t.Fatal("unhealthy candidate received canonical service")
			}
		})
	}
}
