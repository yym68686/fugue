package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/drainprotocol"
	"fugue/internal/model"
	"fugue/internal/runtime"
)

func TestManagedAppLiveGuardResumesVerifiedCanonicalHandoffAfterPromotion(t *testing.T) {
	for _, scenario := range []string{"valid", "next_desired", "wrong_uid", "wrong_image", "wrong_key", "changed_desired", "missing_source", "stopping", "stopping_changed_desired"} {
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
			if strings.HasPrefix(scenario, "stopping") {
				zero := 0
				live.Spec.Replicas = &zero
				deadline := time.Now().UTC().Add(time.Minute)
				marker.DrainDeadline, marker.Stopping = &deadline, true
			}
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
			if scenario == "next_desired" || scenario == "stopping_changed_desired" {
				desired.Spec = *cloneControllerAppSpec(&desired.Spec)
				desired.Spec.Image = "ghcr.io/example/api:v3"
			}
			var writes int
			client := managedAppLiveGuardClient(t, managed, live, true, true, &writes)
			got, err := svc.prepareManagedAppReconcileRolloutWithEvidence(context.Background(), client, managed.Metadata.Namespace,
				managed, desired, "", runtime.SchedulingConstraints{})
			if (err == nil) != (scenario == "valid" || scenario == "next_desired" || scenario == "stopping") {
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

type canonicalHandoffTestEdgeObserver func(since time.Time) safeRolloutEdgeBundleObservation

func (observe canonicalHandoffTestEdgeObserver) WaitForSafeRolloutEdgeRouteBundle(_ context.Context, _ model.App, _ model.AppRelease, _ int, since time.Time) (safeRolloutEdgeBundleObservation, error) {
	return observe(since), nil
}

func TestSafeCanonicalHandoffStartsBoundedStopAfterTrafficProof(t *testing.T) {
	for _, scenario := range []string{"idle", "legacy_source_selector", "legacy_source_selector_busy", "legacy_source_selector_mismatch", "legacy_source_selector_broad", "retired_source", "retired_logical_source", "busy", "stale_endpoints", "observer_failure", "changed_original", "wrong_selector", "candidate_unhealthy", "resume", "restore_ready", "restore_unready", "restore_stale_endpoints", "restore_wrong_selector", "restore_patch_failure", "restore_wrong_image", "advance_busy", "advance_generation_lag_busy", "advance_interrupted_predecessor_busy", "advance_interrupted_wrong_service_uid", "advance_interrupted_wrong_lineage", "advance_generation_stuck", "advance_resume_busy", "advance_after_replacement_busy", "advance_marker_conflict", "advance_service_conflict", "advance_wrong_predecessor", "advance_wrong_service_uid", "advance_wrong_selector",
		"deadline", "deadline_terminating", "deadline_stale_endpoints", "deadline_unhealthy", "deadline_wrong_pod", "advance_deadline", "deadline_source_rerouted", "missing_hook",
		"deadline_worker_pending", "deadline_worker_slow_traffic", "deadline_worker_failed_probe", "deadline_worker_unconfirmed_edge", "deadline_worker_stale_endpoints",
		"deadline_worker_other_operation", "deadline_worker_source_rerouted", "deadline_slow_observer", "deadline_regular_slow_traffic"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			s, app, candidate, f := newDrainWorkloadFixtureWithSnapshot(t, false, true)
			app.Spec.Replicas = 2
			app.Spec.RuntimeID = candidate.RuntimeID
			candidate.Role, candidate.Status = model.AppReleaseRoleStable, model.AppReleaseStatusServing
			if scenario == "deadline_worker_slow_traffic" {
				promoted := time.Now().UTC().Add(-25 * time.Second)
				candidate.PromotedAt = &promoted
				heartbeat := promoted.Add(5 * time.Second)
				s.safeRolloutEdgeBundleObserver = canonicalHandoffTestEdgeObserver(func(since time.Time) safeRolloutEdgeBundleObservation {
					if !since.Equal(promoted) {
						t.Errorf("edge heartbeat floor = %s; want the later promotion time %s", since, promoted)
					}
					return safeRolloutEdgeBundleObservation{Ready: !heartbeat.Before(since)}
				})
			}
			switch scenario {
			case "deadline_worker_slow_traffic", "deadline_worker_failed_probe", "deadline_regular_slow_traffic":
				probe := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path != "/ready" {
						t.Errorf("unexpected probe path: %s", r.URL.Path)
					}
					if scenario == "deadline_worker_failed_probe" {
						w.WriteHeader(http.StatusServiceUnavailable)
						return
					}
					w.WriteHeader(http.StatusOK)
				}))
				t.Cleanup(probe.Close)
				candidate.UpstreamURL = probe.URL
				app.Spec.Continuity.ZeroDowntime.Gate = &model.AppReleaseGatePolicy{
					MaxP95TTFBMilliseconds: 60000,
					Probes:                 []model.AppReleaseProbe{{Path: "/ready"}},
				}
				s.releaseGateMetricsQuerier = releaseMetricsByIDQuerier{metrics: map[string]map[string]any{
					candidate.ID: {"request_count": 100, "p95_ttfb_ms": 350967},
				}}
			case "deadline_worker_unconfirmed_edge":
				s.safeRolloutEdgeBundleObserver = staticSafeRolloutEdgeObserver{}
			}
			var err error
			candidate, err = s.Store.UpdateAppRelease(candidate)
			if err != nil {
				t.Fatal(err)
			}
			_, err = s.Store.UpsertAppTrafficPolicy(model.AppTrafficPolicy{AppID: app.ID, TenantID: app.TenantID, Mode: model.AppTrafficModeSingle, StableReleaseID: candidate.ID, StableWeight: 100})
			if err != nil {
				t.Fatal(err)
			}
			oldImage := "ghcr.io/example/api:old"
			sourceApp := app
			sourceApp.Spec = *cloneControllerAppSpec(&app.Spec)
			sourceApp.Spec.Image = oldImage
			oldKey := s.Renderer.ManagedAppReleaseKey(s.Renderer.PrepareApp(sourceApp), runtime.SchedulingConstraints{})
			desiredKey := "release-key"
			if _, err := s.Store.CreateAppRelease(model.AppRelease{ID: "original-source", AppID: app.ID, TenantID: app.TenantID, RuntimeID: app.Spec.RuntimeID, Role: model.AppReleaseRolePrevious, Status: model.AppReleaseStatusDraining, ResolvedImageRef: oldImage, SpecSnapshot: cloneControllerAppSpec(&sourceApp.Spec)}); err != nil {
				t.Fatal(err)
			}
			candidate.RollbackTargetID = "original-source"
			if scenario == "retired_source" || scenario == "retired_logical_source" {
				source, _ := s.Store.GetAppRelease(app.TenantID, true, "original-source")
				archiveCanonicalTestRelease(&source)
				if _, err := s.Store.UpdateAppRelease(source); err != nil {
					t.Fatal(err)
				}
				if scenario == "retired_logical_source" {
					logical := createCanonicalHandoffTestRevision(t, s, app, "logical-source", source.ID)
					archiveCanonicalTestRelease(&logical)
					if _, err := s.Store.UpdateAppRelease(logical); err != nil {
						t.Fatal(err)
					}
					candidate.RollbackTargetID = logical.ID
				}
			}
			if _, err := s.Store.UpdateAppRelease(candidate); err != nil {
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
			objectMapField(cm, "annotations")[runtime.FugueAnnotationReleaseKey] = oldKey
			tm := objectMapValue(nestedObjectValue(canonical, "spec", "template", "metadata"))
			tm["labels"] = cLabels
			objectMapField(tm, "annotations")[runtime.FugueAnnotationReleaseKey] = oldKey
			mapSlice(nestedObjectValue(canonical, "spec", "template", "spec", "containers"))[0]["image"] = oldImage
			desired := cloneKubeMap(canonical)
			objectMapField(objectMapField(desired, "metadata"), "annotations")[runtime.FugueAnnotationReleaseKey] = desiredKey
			objectMapField(objectMapValue(nestedObjectValue(desired, "spec", "template", "metadata")), "annotations")[runtime.FugueAnnotationReleaseKey] = desiredKey
			mapSlice(nestedObjectValue(desired, "spec", "template", "spec", "containers"))[0]["image"] = app.Spec.Image
			canonicalService := cloneKubeMap(f.service)
			canonicalService["kind"] = "Service"
			canonicalService["apiVersion"] = "v1"
			sm := objectMapField(canonicalService, "metadata")
			sm["name"], sm["uid"], sm["labels"] = serviceName, "canonical-service-uid", cLabels
			objectMapField(canonicalService, "spec")["selector"] = cLabels
			desiredService := cloneKubeMap(canonicalService)
			if strings.HasPrefix(scenario, "legacy_source_selector") {
				// The old Service retains the full template selector while the
				// current renderer emits stable workload identity labels only.
				legacy := clonePlacementStringMap(objectStringMapValue(cLabels))
				legacy["app.kubernetes.io/name"] = "prior-name"
				cLabels["app.kubernetes.io/name"] = "prior-name"
				objectMapField(canonicalService, "spec")["selector"] = legacy
				objectMapField(desiredService, "spec")["selector"] = map[string]string{
					runtime.FugueLabelManagedBy: runtime.FugueLabelManagedByValue, runtime.FugueLabelAppID: app.ID,
					runtime.FugueLabelTenantID: app.TenantID, runtime.FugueLabelAppWorkload: name,
				}
				if scenario == "legacy_source_selector_mismatch" {
					legacy["app.kubernetes.io/name"] = "unrelated-name"
				}
				if scenario == "legacy_source_selector_broad" {
					delete(legacy, runtime.FugueLabelAppWorkload)
				}
			}
			sourcePods := []map[string]any{}
			for i, p := range f.pods {
				objectMapField(p, "status")["podIP"] = fmt.Sprintf("10.0.0.%d", i+10)
				old := cloneKubeMap(p)
				m := objectMapField(old, "metadata")
				m["name"], m["uid"], m["labels"] = fmt.Sprintf("original-%d", i), fmt.Sprintf("original-uid-%d", i), cLabels
				objectMapField(m, "annotations")[runtime.FugueAnnotationReleaseKey] = oldKey
				m["ownerReferences"] = []map[string]any{{"kind": "ReplicaSet", "name": "original-rs", "uid": "original-rs-uid", "controller": true}}
				container := mapSlice(nestedObjectValue(old, "spec", "containers"))[0]
				container["image"] = oldImage
				if scenario != "missing_hook" {
					container["lifecycle"] = map[string]any{"preStop": map[string]any{"httpGet": map[string]any{"path": "/drain/prestop", "port": float64(19090)}}}
				}
				for _, init := range mapSlice(nestedObjectValue(old, "spec", "initContainers")) {
					if init["name"] == "fugue-drain-agent" {
						init["restartPolicy"] = "Always"
					}
				}
				sourcePods = append(sourcePods, old)
			}
			sourceReplica := cloneKubeMap(f.replicas[0])
			rm := objectMapField(sourceReplica, "metadata")
			rm["name"], rm["uid"], rm["labels"] = "original-rs", "original-rs-uid", cLabels
			rm["ownerReferences"] = []map[string]any{{"kind": "Deployment", "name": name, "uid": "canonical-uid", "controller": true}}
			targetSelector := objectStringMapValue(nestedObjectValue(f.service, "spec", "selector"))
			advancing := strings.HasPrefix(scenario, "advance_")
			expectedSourceID, expectedSourceKey, expectedSourceImage := "original-source", oldKey, oldImage
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
			hasDeadline := strings.HasPrefix(scenario, "deadline") || scenario == "advance_deadline" || scenario == "missing_hook"
			deadlineWorker := strings.HasPrefix(scenario, "deadline_worker_")
			expires := time.Now().UTC().Add(20 * time.Second)
			if scenario == "deadline_slow_observer" {
				expires = time.Now().UTC().Add(canonicalDrainStopReserve + 1500*time.Millisecond)
			}
			marked := scenario == "resume" || strings.HasPrefix(scenario, "restore_") || advancing || hasDeadline
			if marked {
				handoff := safeCanonicalHandoff{ReleaseID: candidate.ID, DeploymentUID: "canonical-uid", ReleaseKey: oldKey, Image: oldImage}
				if hasDeadline {
					handoff.DrainDeadline = &expires
				}
				if deadlineWorker {
					handoff.SourceReleaseID = "original-source"
					cLabels[canonicalDrainLabel] = "true"
				}
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
				objectMapField(cm, "annotations")[runtime.FugueAnnotationReleaseKey] = desiredKey
				for i, pod := range sourcePods {
					objectMapField(objectMapField(pod, "metadata"), "annotations")[runtime.FugueAnnotationReleaseKey] = desiredKey
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
			if scenario == "candidate_unhealthy" || scenario == "deadline_unhealthy" {
				objectMapField(f.deployment, "status")["readyReplicas"] = 0
			}
			if scenario == "deadline_source_rerouted" || scenario == "deadline_worker_source_rerouted" {
				objectMapField(canonicalService, "spec")["selector"] = cLabels
			}
			managed := managedAppLiveGuardObject(t, app, runtime.SchedulingConstraints{})
			if deadlineWorker {
				operationSpec := cloneControllerAppSpec(&app.Spec)
				operationSpec.RuntimeID = model.DefaultManagedRuntimeID
				op, err := s.Store.CreateOperation(model.Operation{TenantID: app.TenantID, AppID: app.ID, Type: model.OperationTypeDeploy, ExecutionMode: model.ExecutionModeManaged, DesiredSpec: operationSpec})
				if err != nil {
					t.Fatal(err)
				}
				if scenario != "deadline_worker_other_operation" {
					if _, claimed, err := s.Store.TryClaimPendingOperation(op.ID); err != nil || !claimed {
						t.Fatalf("claim fixture: %v", err)
					}
					if _, err := s.Store.RequeueManagedOperation(op.ID, model.OperationResultCanonicalHandoffPending); err != nil {
						t.Fatal(err)
					}
				}
			}
			if scenario == "deadline_terminating" {
				for _, pod := range sourcePods {
					objectMapField(pod, "metadata")["deletionTimestamp"] = expires.Add(10 * time.Minute).Format(time.RFC3339)
				}
			}
			if scenario == "deadline_wrong_pod" {
				objectMapField(objectMapField(sourcePods[0], "metadata"), "annotations")[runtime.FugueAnnotationReleaseKey] = "unexpected-executable"
			}
			normalStop := scenario == "idle" || scenario == "legacy_source_selector" || scenario == "retired_source" || scenario == "retired_logical_source" || scenario == "resume"
			workerShouldStop := scenario == "deadline_worker_pending" || scenario == "deadline_worker_slow_traffic"
			expiredStop := scenario == "deadline" || scenario == "advance_deadline" || scenario == "deadline_terminating" || workerShouldStop || scenario == "deadline_slow_observer"
			waitForBusy := scenario == "busy" || strings.HasSuffix(scenario, "_busy") || scenario == "deadline_source_rerouted" || scenario == "observer_failure"
			busyObservation := make(chan struct{})
			sourceReads, sourceProbes, servicePatches, templateWrites := 0, 0, 0, 0
			scales, podStops := 0, 0
			podsGone := false
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
				case deadlineWorker && r.URL.Query().Get("labelSelector") == canonicalDrainLabel+"=true" && r.URL.Path == "/apis/apps/v1/deployments":
					_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{map[string]any{"metadata": cm}}})
					return true
				case deadlineWorker && strings.HasSuffix(r.URL.Path, "/managedapps/"+name):
					_ = json.NewEncoder(w).Encode(managed)
					return true
				case strings.HasSuffix(r.URL.Path, "/deployments/"+name):
					if r.Method == http.MethodGet {
						sourceReads++
						if (scenario == "advance_generation_lag_busy" || scenario == "advance_generation_stuck") && markerPatched {
							markerReads++
							if markerReads >= 2 && scenario != "advance_generation_stuck" {
								objectMapField(canonical, "status")["observedGeneration"] = cm["generation"]
							}
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
						if spec := objectMapField(patch, "spec"); len(spec) == 1 && spec["replicas"] == float64(0) && (normalStop || expiredStop) {
							scales++
							if patchMeta := objectMapField(patch, "metadata"); patchMeta["uid"] != cm["uid"] || patchMeta["resourceVersion"] != cm["resourceVersion"] {
								t.Error("stop scale omitted source UID/resourceVersion")
							}
							objectMapField(canonical, "spec")["replicas"] = float64(0)
						} else {
							templateWrites++
							http.Error(w, "unexpected canonical Deployment mutation", 500)
							return true
						}
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
					if podsGone {
						_ = json.NewEncoder(w).Encode(map[string]any{"items": f.pods})
						return true
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"items": append(append([]map[string]any{}, f.pods...), sourcePods...)})
					return true
				case r.Method == http.MethodDelete && strings.Contains(r.URL.Path, "/pods/original-"):
					podStops++
					var options struct {
						Grace         int64                `json:"gracePeriodSeconds"`
						Preconditions struct{ UID string } `json:"preconditions"`
					}
					_ = json.NewDecoder(r.Body).Decode(&options)
					if scales != 1 || options.Grace < 1 || options.Grace >= 600 || expiredStop && scenario != "deadline_slow_observer" && options.Grace > 20 || options.Preconditions.UID != fmt.Sprintf("original-uid-%d", podStops-1) {
						t.Errorf("unbounded or unfenced Pod termination: %+v, scale=%d", options, scales)
					}
					if scenario == "deadline_slow_observer" && (options.Grace < 40 || options.Grace > 60) {
						t.Errorf("slow observer consumed the stop reserve: grace=%d", options.Grace)
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"kind": "Status", "status": "Success"})
					return true
				case strings.HasSuffix(r.URL.Path, "/endpointslices"):
					if scenario == "changed_original" {
						cm["uid"] = "replaced-uid"
					}
					addresses := []string{"10.0.0.10", "10.0.0.11"}
					if strings.HasPrefix(scenario, "restore_") && scenario != "restore_stale_endpoints" && objectStringMapValue(nestedObjectValue(canonicalService, "spec", "selector"))[runtime.FugueLabelAppWorkload] == name {
						addresses = []string{"10.0.1.10", "10.0.1.11"}
					}
					if scenario == "stale_endpoints" || scenario == "deadline_stale_endpoints" || scenario == "deadline_worker_stale_endpoints" {
						addresses = append(addresses, "10.0.0.99")
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{map[string]any{"endpoints": []any{map[string]any{"addresses": addresses, "conditions": map[string]bool{"ready": true}}}}}})
					return true
				case strings.Contains(r.URL.Path, "/pods/original-") && strings.HasSuffix(r.URL.Path, "/proxy/drain/observe"):
					sourceProbes++
					if scenario == "deadline_slow_observer" {
						<-r.Context().Done()
						return true
					}
					if scenario == "observer_failure" {
						http.Error(w, "unavailable", 503)
					} else {
						pod := strings.Split(strings.Split(r.URL.Path, "/pods/")[1], ":")[0]
						active := 0
						if waitForBusy {
							active = 1
						}
						_ = json.NewEncoder(w).Encode(drainprotocol.Snapshot{APIVersion: drainprotocol.Version, Nonce: r.URL.Query().Get("nonce"), Pod: pod,
							Namespace: runtime.NamespaceForTenant(app.TenantID), AppPorts: []int{8080}, ActiveConnections: &active})
					}
					if waitForBusy && sourceProbes == len(sourcePods) {
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
			if waitForBusy {
				go func() {
					select {
					case <-busyObservation:
						cancel()
					case <-ctx.Done():
					}
				}()
			} else if scenario == "stale_endpoints" || scenario == "restore_stale_endpoints" || scenario == "deadline_stale_endpoints" || scenario == "advance_generation_stuck" {
				ctx, cancel = context.WithTimeout(context.Background(), 150*time.Millisecond)
				defer cancel()
			}
			call := func(ctx context.Context) ([]map[string]any, error) {
				return s.prepareSafeCanonicalHandoff(ctx, client, app, []map[string]any{cloneKubeMap(desired), cloneKubeMap(desiredService)})
			}
			if deadlineWorker {
				if err := s.reconcileCanonicalDrainDeadlines(ctx); err != nil {
					t.Fatal(err)
				}
				if workerShouldStop {
					if scales != 1 || podStops != len(sourcePods) {
						t.Fatalf("verified deadline worker did not stop old Pods: scales=%d stops=%d", scales, podStops)
					}
				} else if scales != 0 || podStops != 0 {
					t.Fatalf("deadline worker bypassed an unsafe state: scales=%d stops=%d", scales, podStops)
				}
				if servicePatches != 0 || templateWrites != 0 || sourceProbes != 0 {
					t.Fatal("deadline worker changed traffic or depended on the old socket observer")
				}
				return
			}
			got, err := call(ctx)
			if scenario == "deadline_regular_slow_traffic" && (err == nil || !strings.Contains(err.Error(), "promoted release health gate failed")) {
				t.Fatalf("ordinary alignment bypassed release performance gate: %v", err)
			}
			if normalStop || expiredStop {
				if !errors.Is(err, errSafeCanonicalHandoffPending) || !strings.Contains(err.Error(), "canonical_drain_terminating") || scales != 1 || podStops != len(sourcePods) {
					t.Fatalf("expected the old cohort to enter bounded termination: err=%v scales=%d stops=%d", err, scales, podStops)
				}
				var saved safeCanonicalHandoff
				_ = json.Unmarshal([]byte(objectStringMapValue(cm["annotations"])[safeCanonicalHandoffAnnotation]), &saved)
				if !saved.Stopping || saved.DrainDeadline == nil || (expiredStop && !saved.DrainDeadline.Equal(expires)) {
					t.Fatalf("stop did not retain its original deadline: %+v", saved)
				}
				for _, pod := range sourcePods {
					objectMapField(pod, "metadata")["deletionTimestamp"] = saved.DrainDeadline.Add(-canonicalDrainKubeletBuffer).Format(time.RFC3339)
				}
				if _, retryErr := call(ctx); !errors.Is(retryErr, errSafeCanonicalHandoffPending) || podStops != len(sourcePods) || scales != 1 {
					t.Fatalf("retry replaced source early or reset termination: %v, stops=%d", retryErr, podStops)
				}
				podsGone = true
				objectMapField(canonical, "status")["replicas"] = float64(0)
				got, err = call(ctx)
			}
			success := normalStop || expiredStop || scenario == "restore_ready"
			if success != (err == nil) {
				t.Fatalf("expected success=%v got %v (reads=%d probes=%d)", success, err, sourceReads, sourceProbes)
			}
			if templateWrites != 0 || f.deleteCalls != 0 {
				t.Fatal("handoff changed or deleted the original workload")
			}
			if !normalStop && !expiredStop && (scales != 0 || podStops != 0) {
				t.Fatal("handoff stopped Pods without sufficient proof")
			}
			if expiredStop && scenario != "deadline_slow_observer" && sourceProbes != 0 {
				t.Fatal("handoff near deadline still depended on the old socket observer")
			}
			if scenario == "deadline_slow_observer" && sourceProbes != 1 {
				t.Fatalf("expected one deliberately stalled source observer, got %d", sourceProbes)
			}
			if waitForBusy {
				var saved safeCanonicalHandoff
				_ = json.Unmarshal([]byte(objectStringMapValue(cm["annotations"])[safeCanonicalHandoffAnnotation]), &saved)
				if sourceProbes < len(sourcePods) || saved.DrainDeadline == nil || saved.Stopping ||
					(scenario == "deadline_source_rerouted" && !saved.DrainDeadline.After(expires)) {
					t.Fatalf("busy/unknown drain terminated early or lost its deadline: %+v probes=%d", saved, sourceProbes)
				}
			}
			if success && scenario != "restore_ready" {
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
			if (scenario == "legacy_source_selector_mismatch" || scenario == "legacy_source_selector_broad") && servicePatches != 0 {
				t.Fatal("unverified legacy selector redirected traffic")
			}
			if scenario == "legacy_source_selector_busy" && servicePatches != 1 {
				t.Fatal("verified legacy selector did not withdraw new source connections")
			}
			if advancing && strings.HasSuffix(scenario, "_busy") {
				var checkpoint safeCanonicalHandoff
				_ = json.Unmarshal([]byte(objectStringMapValue(cm["annotations"])[safeCanonicalHandoffAnnotation]), &checkpoint)
				if servicePatches != 1 || !reflect.DeepEqual(objectStringMapValue(nestedObjectValue(canonicalService, "spec", "selector")), targetSelector) ||
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

func TestManagedAppStatusSeparatesCanonicalDrainProgressFromFailure(t *testing.T) {
	for _, tc := range []struct {
		cause error
		phase string
	}{
		{&canonicalDrainInProgress{deadline: time.Now().Add(time.Minute), detail: "draining"}, runtime.ManagedAppPhaseProgressing},
		{&canonicalDrainInProgress{deadline: time.Now().Add(-time.Minute), detail: "overdue"}, runtime.ManagedAppPhaseError},
		{fmt.Errorf("%w: endpoint proof missing", errSafeCanonicalHandoffPending), runtime.ManagedAppPhaseError},
	} {
		got := managedAppPreApplyErrorStatus(context.Background(), nil, "test", runtime.ManagedAppObject{}, model.App{}, tc.cause)
		if got.Phase != tc.phase || got.Message != tc.cause.Error() {
			t.Fatalf("phase=%q message=%q, want %q / %q", got.Phase, got.Message, tc.phase, tc.cause.Error())
		}
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
	for _, scenario := range []string{"legacy_successor", "retained_source", "completed_replacement", "unrelated_successor", "missing_source", "retired_predecessor", "multiple_successors", "logical_source_differs", "logical_source_wrong_snapshot", "retired_executable", "retired_logical_source", "retired_unpromoted_source"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			st, original, desired, _ := newSafeRolloutTestState(t)
			s := &Service{Store: st, Renderer: runtime.Renderer{}}
			archiveSource := scenario == "retired_executable" || scenario == "retired_logical_source" || scenario == "retired_unpromoted_source"
			if _, err := st.CreateAppRelease(model.AppRelease{ID: "original", AppID: original.ID, TenantID: original.TenantID,
				Role: model.AppReleaseRolePrevious, Status: model.AppReleaseStatusDraining, ResolvedImageRef: original.Spec.Image, SpecSnapshot: cloneControllerAppSpec(&original.Spec)}); err != nil {
				t.Fatal(err)
			}
			if archiveSource {
				source, _ := st.GetAppRelease(original.TenantID, true, "original")
				archiveCanonicalTestRelease(&source)
				if scenario == "retired_unpromoted_source" {
					source.PromotedAt = nil
				}
				if _, err := st.UpdateAppRelease(source); err != nil {
					t.Fatal(err)
				}
			}
			previous := createCanonicalHandoffTestRevision(t, s, desired, "first-promoted", "original")
			previous.Role, previous.Status = model.AppReleaseRolePrevious, model.AppReleaseStatusDraining
			if scenario == "retired_predecessor" {
				previous.Role, previous.Status = model.AppReleaseRoleRetired, model.AppReleaseStatusRetired
			}
			if scenario == "retired_logical_source" {
				archiveCanonicalTestRelease(&previous)
			}
			if _, err := st.UpdateAppRelease(previous); err != nil {
				t.Fatal(err)
			}
			predecessorID := previous.ID
			if scenario == "multiple_successors" {
				middle := createCanonicalHandoffTestRevision(t, s, desired, "intermediate-promoted", previous.ID)
				middle.Role, middle.Status = model.AppReleaseRolePrevious, model.AppReleaseStatusDraining
				if _, err := st.UpdateAppRelease(middle); err != nil {
					t.Fatal(err)
				}
				predecessorID = middle.ID
			}
			desired.Spec = *cloneControllerAppSpec(&desired.Spec)
			desired.Spec.Env = map[string]string{"FEATURE_ENABLED": "true"}
			promoted := createCanonicalHandoffTestRevision(t, s, desired, "next-promoted", predecessorID)
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
			if scenario == "logical_source_differs" || scenario == "logical_source_wrong_snapshot" || archiveSource {
				handoff.SourceReleaseID = previous.ID
				if scenario == "retired_logical_source" {
					handoff.ReleaseID = promoted.ID
				}
				if scenario == "logical_source_wrong_snapshot" {
					bad, _ := st.GetAppRelease(original.TenantID, true, "original")
					bad.SpecSnapshot = cloneControllerAppSpec(bad.SpecSnapshot)
					bad.SpecSnapshot.Env = map[string]string{"UNVERIFIED": "change"}
					if _, err := st.UpdateAppRelease(bad); err != nil {
						t.Fatal(err)
					}
				}
			}
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
			want := scenario == "legacy_successor" || scenario == "retained_source" || scenario == "completed_replacement" || scenario == "multiple_successors" || scenario == "logical_source_differs" || scenario == "retired_executable" || scenario == "retired_logical_source"
			if (err == nil && recovered) != want || (want && got.Spec.Image != expectedImage) {
				t.Fatal("lost immutable original source across promotion", scenario, got.Spec.Image, recovered, err)
			}
			if archiveSource {
				row, err := st.GetAppRelease(original.TenantID, true, "original")
				if err != nil || row.Status != model.AppReleaseStatusRetired || row.Role != model.AppReleaseRoleRetired {
					t.Fatal("canonical handoff reactivated historical release", err)
				}
			}
		})
	}
}

func archiveCanonicalTestRelease(release *model.AppRelease) {
	release.Role, release.Status = model.AppReleaseRoleRetired, model.AppReleaseStatusRetired
	release.CreatedAt = time.Now().Add(-2 * time.Hour)
	ready, retired := time.Now().Add(-time.Hour), time.Now().Add(-30*time.Minute)
	release.ReadyAt, release.PromotedAt, release.RetiredAt = &ready, &ready, &retired
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
