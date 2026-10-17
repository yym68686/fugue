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
	for _, scenario := range []string{"valid", "next_desired", "wrong_uid", "wrong_image", "wrong_key", "changed_desired", "missing_source"} {
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
			if scenario == "next_desired" {
				desired.Spec = *cloneControllerAppSpec(&desired.Spec)
				desired.Spec.Image = "ghcr.io/example/api:v3"
			}
			var writes int
			client := managedAppLiveGuardClient(t, managed, live, true, true, &writes)
			got, err := svc.prepareManagedAppReconcileRolloutWithEvidence(context.Background(), client, managed.Metadata.Namespace,
				managed, desired, "", runtime.SchedulingConstraints{})
			if (err == nil) != (scenario == "valid" || scenario == "next_desired") {
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
	for _, scenario := range []string{"idle", "busy", "stale_endpoints", "observer_failure", "changed_original", "wrong_selector", "candidate_unhealthy", "resume", "restore_ready", "restore_unready", "restore_stale_endpoints", "restore_wrong_selector", "restore_patch_failure", "restore_wrong_image", "advance_busy", "advance_generation_lag_busy", "advance_interrupted_predecessor_busy", "advance_interrupted_wrong_service_uid", "advance_interrupted_wrong_lineage", "advance_generation_stuck", "advance_resume_busy", "advance_after_replacement_busy", "advance_marker_conflict", "advance_service_conflict", "advance_wrong_predecessor", "advance_wrong_service_uid", "advance_wrong_selector"} {
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
			advancing := strings.HasPrefix(scenario, "advance_")
			expectedSourceID, expectedSourceKey, expectedSourceImage := "original-source", "old-key", oldImage
			var previousService, olderService map[string]any
			var older model.AppRelease
			var predecessor model.AppRelease
			if advancing {
				predecessor = createCanonicalHandoffTestRevision(t, s, app, "previous-target", "original-source")
				if strings.HasPrefix(scenario, "advance_interrupted_") {
					older = createCanonicalHandoffTestRevision(t, s, app, "older-target", "original-source")
					older.Role, older.Status = model.AppReleaseRolePrevious, model.AppReleaseStatusDraining
					if _, err := s.Store.UpdateAppRelease(older); err != nil {
						t.Fatal(err)
					}
					predecessor.RollbackTargetID = older.ID
					if scenario == "advance_interrupted_wrong_lineage" {
						predecessor.RollbackTargetID = "unrelated-history"
					}
					olderService = cloneKubeMap(f.service)
					om := objectMapField(olderService, "metadata")
					om["name"], om["uid"] = older.ServiceName, older.RevisionWorkload.ServiceUID
					if scenario == "advance_interrupted_wrong_service_uid" {
						om["uid"] = "recreated-older-service"
					}
					ol := objectMapField(om, "labels")
					ol[runtime.FugueLabelAppReleaseID], ol[runtime.FugueLabelAppWorkload] = older.ID, older.DeploymentName
					objectMapField(olderService, "spec")["selector"] = ol
				}
				predecessor.Role, predecessor.Status = model.AppReleaseRolePrevious, model.AppReleaseStatusDraining
				if _, err := s.Store.UpdateAppRelease(predecessor); err != nil {
					t.Fatal(err)
				}
				candidate.RollbackTargetID = predecessor.ID
				if scenario == "advance_wrong_predecessor" {
					candidate.RollbackTargetID = "unrelated"
				}
				if _, err := s.Store.UpdateAppRelease(candidate); err != nil {
					t.Fatal(err)
				}
				previousService = cloneKubeMap(f.service)
				pm := objectMapField(previousService, "metadata")
				pm["name"], pm["uid"] = predecessor.ServiceName, predecessor.RevisionWorkload.ServiceUID
				pl := objectMapField(pm, "labels")
				pl[runtime.FugueLabelAppReleaseID], pl[runtime.FugueLabelAppWorkload] = predecessor.ID, predecessor.DeploymentName
				objectMapField(previousService, "spec")["selector"] = pl
				if scenario == "advance_wrong_service_uid" {
					pm["uid"] = "recreated-service"
				}
			}
			marked := scenario == "resume" || strings.HasPrefix(scenario, "restore_") || advancing
			if marked {
				handoff := safeCanonicalHandoff{ReleaseID: candidate.ID, DeploymentUID: "canonical-uid", ReleaseKey: "old-key", Image: oldImage}
				if advancing {
					handoff.ReleaseID = predecessor.ID
					if strings.HasPrefix(scenario, "advance_interrupted_") {
						handoff.SourceReleaseID, handoff.PreviousTargetReleaseID = "original-source", older.ID
					}
					if scenario == "advance_resume_busy" {
						handoff.ReleaseID, handoff.SourceReleaseID, handoff.PreviousTargetReleaseID = candidate.ID, "original-source", predecessor.ID
					}
				}
				b, _ := json.Marshal(handoff)
				objectMapField(cm, "annotations")[safeCanonicalHandoffAnnotation] = string(b)
				objectMapField(canonicalService, "spec")["selector"] = targetSelector
				if advancing {
					objectMapField(canonicalService, "spec")["selector"] = objectStringMapValue(nestedObjectValue(previousService, "spec", "selector"))
					if olderService != nil {
						objectMapField(canonicalService, "spec")["selector"] = objectStringMapValue(nestedObjectValue(olderService, "spec", "selector"))
					}
				}
			}
			if scenario == "advance_after_replacement_busy" {
				expectedSourceID, expectedSourceImage = predecessor.ID, app.Spec.Image
				expectedSourceKey = s.Renderer.ManagedAppReleaseKey(s.Renderer.PrepareApp(app), runtime.SchedulingConstraints{})
				objectMapField(cm, "annotations")[runtime.FugueAnnotationReleaseKey] = expectedSourceKey
				objectMapField(tm, "annotations")[runtime.FugueAnnotationReleaseKey] = expectedSourceKey
				mapSlice(nestedObjectValue(canonical, "spec", "template", "spec", "containers"))[0]["image"] = expectedSourceImage
				for _, pod := range sourcePods {
					objectMapField(objectMapField(pod, "metadata"), "annotations")[runtime.FugueAnnotationReleaseKey] = expectedSourceKey
					mapSlice(nestedObjectValue(pod, "spec", "containers"))[0]["image"] = expectedSourceImage
				}
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
			if scenario == "wrong_selector" || scenario == "restore_wrong_selector" || scenario == "advance_wrong_selector" {
				objectMapField(canonicalService, "spec")["selector"] = map[string]string{"unexpected": "workload"}
			}
			if scenario == "candidate_unhealthy" {
				objectMapField(f.deployment, "status")["readyReplicas"] = 0
			}
			sourceReads, sourceProbes, servicePatches, templateWrites := 0, 0, 0, 0
			busyObservation := make(chan struct{})
			waitForBusyObservation := scenario == "busy" || (advancing && strings.HasSuffix(scenario, "_busy"))
			markerReads := 0
			markerPatched := false
			f.extra = func(w http.ResponseWriter, r *http.Request) bool {
				if olderService != nil && strings.HasSuffix(r.URL.Path, "/services/"+older.ServiceName) {
					_ = json.NewEncoder(w).Encode(olderService)
					return true
				}
				if advancing && strings.HasSuffix(r.URL.Path, "/services/"+predecessor.ServiceName) {
					_ = json.NewEncoder(w).Encode(previousService)
					return true
				}
				switch {
				case strings.HasSuffix(r.URL.Path, "/deployments/"+name):
					if r.Method == http.MethodGet {
						sourceReads++
						if (scenario == "advance_generation_lag_busy" || scenario == "advance_generation_stuck") && markerPatched {
							markerReads++
							if markerReads >= 2 && scenario != "advance_generation_stuck" {
								objectMapField(canonical, "status")["observedGeneration"] = cm["generation"]
							}
						}
						if scenario == "changed_original" && sourceProbes > 0 {
							cm["uid"] = "replaced-uid"
						}
						_ = json.NewEncoder(w).Encode(canonical)
						return true
					}
					if scenario == "advance_marker_conflict" {
						http.Error(w, "marker version conflict", http.StatusConflict)
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
					if scenario == "advance_generation_lag_busy" || scenario == "advance_generation_stuck" {
						markerPatched = true
						cm["generation"] = float64(99)
					}
					_ = json.NewEncoder(w).Encode(canonical)
					return true
				case strings.HasSuffix(r.URL.Path, "/services/"+serviceName):
					if r.Method == http.MethodPatch {
						if scenario == "restore_patch_failure" || scenario == "advance_service_conflict" {
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
					if scenario == "busy" || strings.HasSuffix(scenario, "_busy") {
						active = 1
					}
					_ = json.NewEncoder(w).Encode(drainprotocol.Snapshot{APIVersion: drainprotocol.Version, Nonce: r.URL.Query().Get("nonce"), Pod: pod, Namespace: runtime.NamespaceForTenant(app.TenantID), AppPorts: []int{8080}, ActiveConnections: &active})
					if waitForBusyObservation && sourceProbes == len(sourcePods) {
						w.(http.Flusher).Flush()
						close(busyObservation)
					}
					return true
				}
				return false
			}
			client, _ := s.kubeClient()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if waitForBusyObservation {
				// Cancel only after the guarded handoff has observed every busy
				// source Pod. A 150ms wall budget could expire before those checks
				// under concurrent package load, never exercising drain retention.
				go func() {
					select {
					case <-busyObservation:
						cancel()
					case <-ctx.Done():
					}
				}()
			} else if scenario == "stale_endpoints" || scenario == "restore_stale_endpoints" || advancing {
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
			if waitForBusyObservation && sourceProbes < len(sourcePods) {
				t.Fatal("handoff stopped before observing the busy source cohort")
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
			if advancing && strings.HasSuffix(scenario, "_busy") {
				var checkpoint safeCanonicalHandoff
				_ = json.Unmarshal([]byte(objectStringMapValue(cm["annotations"])[safeCanonicalHandoffAnnotation]), &checkpoint)
				if servicePatches != 1 || sourceProbes == 0 || !reflect.DeepEqual(objectStringMapValue(nestedObjectValue(canonicalService, "spec", "selector")), targetSelector) ||
					checkpoint.ReleaseID != candidate.ID || checkpoint.SourceReleaseID != expectedSourceID || checkpoint.DeploymentUID != "canonical-uid" || checkpoint.ReleaseKey != expectedSourceKey || checkpoint.Image != expectedSourceImage {
					t.Fatal("successor did not retain original drain cohort while moving new connections", checkpoint, servicePatches, sourceProbes)
				}
			}
			if (strings.HasPrefix(scenario, "advance_wrong_") || strings.HasPrefix(scenario, "advance_interrupted_wrong_") || scenario == "advance_generation_stuck") && servicePatches != 0 {
				t.Fatal("unverified promotion changed canonical traffic")
			}
			if (scenario == "advance_marker_conflict" || scenario == "advance_service_conflict") && servicePatches != 0 {
				t.Fatal("failed checkpoint CAS moved traffic")
			}
		})
	}
}

func createCanonicalHandoffTestRevision(t *testing.T, s *Service, app model.App, id, predecessorID string) model.AppRelease {
	t.Helper()
	operationSpec := cloneControllerAppSpec(&app.Spec)
	operationSpec.RuntimeID = model.DefaultManagedRuntimeID
	op, err := s.Store.CreateOperation(model.Operation{TenantID: app.TenantID, AppID: app.ID, Type: model.OperationTypeDeploy, DesiredSpec: operationSpec, ExecutionMode: model.ExecutionModeManaged})
	if err != nil {
		t.Fatal("create predecessor operation", err)
	}
	if _, claimed, err := s.Store.TryClaimPendingOperation(op.ID); err != nil || !claimed {
		t.Fatal("claim successor fixture", err)
	}
	r, err := s.Store.CreateAppRelease(model.AppRelease{ID: id, AppID: app.ID, TenantID: app.TenantID, RuntimeID: app.Spec.RuntimeID,
		Role: model.AppReleaseRoleCandidate, Status: model.AppReleaseStatusCreating, SpecSnapshot: cloneControllerAppSpec(&app.Spec),
		DeploymentName: id + "-deployment", ServiceName: id + "-service", ResolvedImageRef: app.Spec.Image, RollbackTargetID: predecessorID})
	if err != nil {
		t.Fatal("create predecessor release", err)
	}
	r, err = s.Store.BindAppReleaseWorkload(context.Background(), r, model.AppReleaseWorkload{OperationID: op.ID, Namespace: runtime.NamespaceForTenant(app.TenantID),
		DeploymentName: r.DeploymentName, DeploymentUID: id + "-deployment-uid", DeploymentGeneration: 1, ServiceName: r.ServiceName, ServiceUID: id + "-service-uid",
		ReleaseKey: id + "-key", RuntimeID: app.Spec.RuntimeID, ImageRef: app.Spec.Image})
	if err != nil {
		t.Fatal("bind predecessor", err)
	}
	if _, err := s.Store.CompleteManagedOperation(op.ID, "", "fixture complete"); err != nil {
		t.Fatal("complete predecessor operation", err)
	}
	return r
}

func TestCanonicalSourceSurvivesSuccessivePromotions(t *testing.T) {
	for _, scenario := range []string{"legacy_successor", "retained_source", "completed_replacement", "unrelated_successor", "missing_source", "retired_predecessor"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			st, original, desired, _ := newSafeRolloutTestState(t)
			s := &Service{Store: st, Renderer: runtime.Renderer{}}
			if _, err := st.CreateAppRelease(model.AppRelease{ID: "original", AppID: original.ID, TenantID: original.TenantID,
				Role: model.AppReleaseRolePrevious, Status: model.AppReleaseStatusDraining, SpecSnapshot: cloneControllerAppSpec(&original.Spec)}); err != nil {
				t.Fatal(err)
			}
			previous := createCanonicalHandoffTestRevision(t, s, desired, "first-promoted", "original")
			previous.Role, previous.Status = model.AppReleaseRolePrevious, model.AppReleaseStatusDraining
			if scenario == "retired_predecessor" {
				previous.Role, previous.Status = model.AppReleaseRoleRetired, model.AppReleaseStatusRetired
			}
			if _, err := st.UpdateAppRelease(previous); err != nil {
				t.Fatal(err)
			}
			desired.Spec = *cloneControllerAppSpec(&desired.Spec)
			desired.Spec.Env = map[string]string{"FEATURE_ENABLED": "true"}
			promoted := createCanonicalHandoffTestRevision(t, s, desired, "next-promoted", previous.ID)
			promoted.Role, promoted.Status = model.AppReleaseRoleStable, model.AppReleaseStatusServing
			if scenario == "unrelated_successor" {
				promoted.RollbackTargetID = "unrelated"
			}
			if _, err := st.UpdateAppRelease(promoted); err != nil {
				t.Fatal(err)
			}
			if _, err := st.UpsertAppTrafficPolicy(model.AppTrafficPolicy{AppID: desired.ID, TenantID: desired.TenantID,
				Mode: model.AppTrafficModeSingle, StableReleaseID: promoted.ID, StableWeight: 100}); err != nil {
				t.Fatal(err)
			}
			live, _ := s.expectedManagedAppDeployment(s.Renderer.PrepareApp(original), runtime.SchedulingConstraints{})
			managedAppLiveGuardMarkReady(&live, 1)
			live.Metadata.UID = "original-uid"
			handoff := safeCanonicalHandoff{ReleaseID: previous.ID, DeploymentUID: live.Metadata.UID,
				ReleaseKey: live.Metadata.Annotations[runtime.FugueAnnotationReleaseKey], Image: original.Spec.Image}
			if scenario == "retained_source" {
				handoff.ReleaseID, handoff.SourceReleaseID, handoff.PreviousTargetReleaseID = promoted.ID, "original", previous.ID
			}
			if scenario == "missing_source" {
				handoff.SourceReleaseID = "missing"
			}
			expectedImage := original.Spec.Image
			if scenario == "completed_replacement" {
				replacement := original
				replacement.Spec = *cloneControllerAppSpec(previous.SpecSnapshot)
				live, _ = s.expectedManagedAppDeployment(s.Renderer.PrepareApp(replacement), runtime.SchedulingConstraints{})
				managedAppLiveGuardMarkReady(&live, 1)
				live.Metadata.UID = handoff.DeploymentUID
				expectedImage = replacement.Spec.Image
			}
			b, _ := json.Marshal(handoff)
			live.Metadata.Annotations[safeCanonicalHandoffAnnotation] = string(b)
			got, _, recovered, err := s.recoverSafeCanonicalHandoffSource(live, desired, desired)
			want := scenario == "legacy_successor" || scenario == "retained_source" || scenario == "completed_replacement"
			if (err == nil && recovered) != want || (want && got.Spec.Image != expectedImage) {
				t.Fatal("lost immutable original source across promotion", scenario, got.Spec.Image, recovered, err)
			}
		})
	}
}

func TestPolicyUpdateCompletionRetainsExecutableChangesOnPendingHandoff(t *testing.T) {
	for _, scenario := range []string{"policy", "wrapped_pending", "image", "environment", "database", "replicas", "source", "disable", "unchanged", "other_error", "no_error", "not_deploy", "missing_spec"} {
		t.Run(scenario, func(t *testing.T) {
			_, current, _, _ := newDrainWorkloadFixtureWithSnapshot(t, false, true)
			current.Spec.Continuity = &model.AppContinuityPolicy{ZeroDowntime: &model.AppZeroDowntimePolicy{
				Enabled: true, Mode: model.AppZeroDowntimeModeSafe, RetireGraceSeconds: 60,
			}}
			op := model.Operation{Type: model.OperationTypeDeploy, DesiredSpec: cloneControllerAppSpec(&current.Spec)}
			op.DesiredSpec.Continuity.ZeroDowntime.RetireGraceSeconds = 120
			var applyErr error = errSafeCanonicalHandoffPending
			switch scenario {
			case "wrapped_pending":
				applyErr = fmt.Errorf("reconcile: %w", applyErr)
			case "image":
				op.DesiredSpec.Image = "ghcr.io/example/api:other"
			case "environment":
				op.DesiredSpec.Env = map[string]string{"FEATURE": "enabled"}
			case "database":
				op.DesiredSpec.Postgres = &model.AppPostgresSpec{}
			case "replicas":
				op.DesiredSpec.Replicas++
			case "source":
				op.DesiredSource = &model.AppSource{}
			case "disable":
				op.DesiredSpec.Continuity = nil
			case "unchanged":
				op.DesiredSpec = cloneControllerAppSpec(&current.Spec)
			case "other_error":
				applyErr = fmt.Errorf("failed to persist desired state")
			case "no_error":
				applyErr = nil
			case "not_deploy":
				op.Type = model.OperationTypeMigrate
			case "missing_spec":
				op.DesiredSpec = nil
			}
			want := scenario == "policy" || scenario == "wrapped_pending"
			if got := safeRolloutPolicyUpdateCanComplete(op, current, applyErr); got != want {
				t.Fatalf("policy update completion = %v, want %v", got, want)
			}
			if current.Spec.Continuity.ZeroDowntime.RetireGraceSeconds != 60 {
				t.Fatal("classification mutated stored policy")
			}
		})
	}
}
