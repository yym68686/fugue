package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	retirement "fugue/internal/releaseretirement/contract"
	"fugue/internal/runtime"
)

func newAlignedRevisionFixture(t *testing.T) (*Service, model.App, model.AppRelease, *drainWorkloadFixture, map[string]any, *int) {
	t.Helper()
	s, app, old, f := newDrainWorkloadFixture(t)
	app, err := s.Store.GetApp(app.ID)
	if err != nil {
		t.Fatal(err)
	}
	op, err := s.Store.CreateOperation(model.Operation{TenantID: app.TenantID, AppID: app.ID, Type: model.OperationTypeDeploy, DesiredSpec: &app.Spec})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := s.Store.TryClaimPendingOperation(op.ID); err != nil || !ok {
		t.Fatal("claim aligned source operation", err)
	}
	r, err := s.Store.CreateAppRelease(model.AppRelease{ID: "aligned-release", TenantID: app.TenantID, AppID: app.ID,
		Role: model.AppReleaseRoleCandidate, Status: model.AppReleaseStatusCreating, RuntimeID: app.Spec.RuntimeID,
		ResolvedImageRef: app.Spec.Image, DeploymentName: old.RevisionWorkload.DeploymentName, ServiceName: old.RevisionWorkload.ServiceName})
	if err != nil {
		t.Fatal(err)
	}
	b := *old.RevisionWorkload
	b.BoundAt, b.RuntimeID, b.ImageRef, b.OperationID = time.Time{}, app.Spec.RuntimeID, app.Spec.Image, op.ID
	r, err = s.Store.BindAppReleaseWorkload(context.Background(), r, b)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Store.CompleteManagedOperation(op.ID, "", "source release completed"); err != nil {
		t.Fatal(err)
	}
	app, _ = s.Store.GetApp(app.ID)
	r, _ = s.Store.GetAppRelease(app.TenantID, true, r.ID)
	r.Role, r.Status = model.AppReleaseRoleStable, model.AppReleaseStatusServing
	r.DeploymentName, r.ServiceName = runtime.RuntimeAppResourceName(app), runtime.RuntimeAppServiceName(app)
	r, err = s.Store.UpdateAppRelease(r)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Store.UpsertAppTrafficPolicy(model.AppTrafficPolicy{AppID: app.ID, TenantID: app.TenantID,
		Mode: model.AppTrafficModeSingle, StableReleaseID: r.ID, StableWeight: 100}); err != nil {
		t.Fatal(err)
	}
	objectMapField(f.deployment, "metadata")["labels"].(map[string]any)[runtime.FugueLabelAppReleaseID] = r.ID
	mapSlice(nestedObjectValue(f.deployment, "spec", "template", "spec", "containers"))[0]["image"] = app.Spec.Image
	canonical := map[string]any{
		"metadata": map[string]any{"name": r.DeploymentName, "uid": "canonical-uid", "generation": 1,
			"labels": map[string]any{runtime.FugueLabelAppID: app.ID, runtime.FugueLabelTenantID: app.TenantID, runtime.FugueLabelManagedBy: runtime.FugueLabelManagedByValue}},
		"spec": map[string]any{"replicas": app.Spec.Replicas, "template": map[string]any{
			"metadata": map[string]any{"annotations": map[string]any{runtime.FugueAnnotationReleaseKey: "canonical-key"}},
			"spec":     nestedObjectValue(f.deployment, "spec", "template", "spec")}},
		"status": map[string]any{"observedGeneration": 1, "replicas": 1, "updatedReplicas": 1, "readyReplicas": 1, "availableReplicas": 1},
	}
	labels := map[string]any{runtime.FugueLabelAppID: app.ID, runtime.FugueLabelTenantID: app.TenantID,
		runtime.FugueLabelManagedBy: runtime.FugueLabelManagedByValue, runtime.FugueLabelAppWorkload: r.DeploymentName}
	f.replicas = append(f.replicas, map[string]any{"metadata": map[string]any{"name": "canonical-rs", "uid": "canonical-rs-uid", "labels": labels,
		"ownerReferences": []map[string]any{{"kind": "Deployment", "name": r.DeploymentName, "uid": "canonical-uid", "controller": true}}}})
	var canonicalPod map[string]any
	rawPod, _ := json.Marshal(f.pods[0])
	_ = json.Unmarshal(rawPod, &canonicalPod)
	canonicalPod["metadata"] = map[string]any{"name": "canonical-pod", "uid": "canonical-pod-uid", "resourceVersion": "1", "labels": labels,
		"annotations":     map[string]any{runtime.FugueAnnotationReleaseKey: "canonical-key"},
		"ownerReferences": []map[string]any{{"kind": "ReplicaSet", "name": "canonical-rs", "uid": "canonical-rs-uid", "controller": true}}}
	objectMapField(canonicalPod, "status")["podIP"] = "192.0.2.1"
	f.pods = append(f.pods, canonicalPod)
	patches := new(int)
	f.extra = func(w http.ResponseWriter, req *http.Request) bool {
		switch {
		case req.Method == http.MethodPatch:
			*patches++
			if !strings.HasSuffix(req.URL.Path, "/deployments/"+b.DeploymentName) {
				t.Errorf("changed the serving workload: %s", req.URL.Path)
			}
			var patch map[string]any
			if err := json.NewDecoder(req.Body).Decode(&patch); err != nil {
				t.Fatal(err)
			}
			metadata := objectMapField(patch, "metadata")
			if metadata["uid"] != b.DeploymentUID || metadata["resourceVersion"] != "1" || nestedObjectValue(patch, "spec", "replicas") != float64(0) {
				t.Errorf("missing identity/version/zero precondition: %+v", patch)
			}
			if f.conflictDelete {
				http.Error(w, "resourceVersion conflict", http.StatusConflict)
			} else {
				objectMapField(f.deployment, "spec")["replicas"] = 0
				objectMapField(f.deployment, "metadata")["annotations"].(map[string]any)[alignedRevisionQuiescedAnnotation] = r.ID
				_, _ = w.Write([]byte(`{}`))
			}
			return true
		case strings.HasSuffix(req.URL.Path, "/deployments/"+r.DeploymentName):
			_ = json.NewEncoder(w).Encode(canonical)
			return true
		case strings.HasSuffix(req.URL.Path, "/services/"+r.ServiceName):
			_ = json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"uid": "canonical-service-uid", "labels": labels},
				"spec": map[string]any{"selector": map[string]any{runtime.FugueLabelAppWorkload: r.DeploymentName}}})
			return true
		case strings.HasSuffix(req.URL.Path, "/endpointslices"):
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []map[string]any{{"endpoints": []map[string]any{
				{"addresses": []string{"192.0.2.1"}, "conditions": map[string]any{"ready": true}},
			}}}})
			return true
		}
		return false
	}
	return s, app, r, f, canonical, patches
}

func TestAlignedRevisionQuiescesOnlyAfterCanonicalPublicationAndDrain(t *testing.T) {
	for _, scenario := range []string{"idle", "busy", "unconfirmed_traffic", "canonical_unready", "canonical_handoff", "changed_pod", "changed_policy", "changed_app", "changed_canonical", "original_uid", "active_operation", "retention", "scale_conflict"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			s, app, release, f, canonical, patches := newAlignedRevisionFixture(t)
			switch scenario {
			case "busy":
				f.active = 1
			case "unconfirmed_traffic":
				s.safeRolloutEdgeBundleObserver = staticSafeRolloutEdgeObserver{}
			case "canonical_unready":
				objectMapField(canonical, "status")["readyReplicas"] = 0
			case "canonical_handoff":
				objectMapField(canonical, "metadata")["annotations"] = map[string]any{safeCanonicalHandoffAnnotation: "pending"}
			case "changed_pod":
				f.change = func(f *drainWorkloadFixture) { objectMapField(f.pods[0], "metadata")["uid"] = "replacement-pod" }
			case "changed_policy":
				f.change = func(*drainWorkloadFixture) {
					p, _ := s.Store.GetAppTrafficPolicy(app.TenantID, true, app.ID)
					p.StickyCookie = "changed-during-drain"
					if _, err := s.Store.UpsertAppTrafficPolicy(p); err != nil {
						t.Error(err)
					}
				}
			case "changed_app":
				f.change = func(*drainWorkloadFixture) {
					if _, err := s.Store.UpdateAppRightSizing(app.ID, model.AppRightSizingSpec{Mode: "recommend"}); err != nil {
						t.Error(err)
					}
				}
			case "changed_canonical":
				f.change = func(*drainWorkloadFixture) { objectMapField(canonical, "metadata")["uid"] = "replacement-canonical" }
			case "original_uid":
				objectMapField(f.deployment, "metadata")["uid"] = "replacement-original"
			case "active_operation":
				if _, err := s.Store.CreateOperation(model.Operation{TenantID: app.TenantID, AppID: app.ID, Type: model.OperationTypeDeploy, DesiredSpec: &app.Spec}); err != nil {
					t.Fatal(err)
				}
			case "retention":
				future := time.Now().Add(time.Hour)
				release.RetentionUntil = &future
				var err error
				release, err = s.Store.UpdateAppRelease(release)
				if err != nil {
					t.Fatal(err)
				}
			case "scale_conflict":
				f.conflictDelete = true
			}
			policy, _ := s.Store.GetAppTrafficPolicy(app.TenantID, true, app.ID)
			release, _ = s.Store.GetAppRelease(app.TenantID, true, release.ID)
			ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
			defer cancel()
			err := s.quiesceAlignedReleaseRevision(ctx, app, release, policy)
			wantPatches := 0
			if scenario == "idle" || scenario == "scale_conflict" {
				wantPatches = 1
			}
			if *patches != wantPatches || (err == nil) != (scenario == "idle") {
				t.Fatalf("patches=%d want=%d err=%v", *patches, wantPatches, err)
			}
			current, _ := s.Store.GetAppRelease(app.TenantID, true, release.ID)
			if !reflect.DeepEqual(current, release) || f.deleteCalls != 0 {
				t.Fatal("retired or rewrote the serving release, provenance, or resource objects")
			}
			if scenario == "idle" {
				// A new leader uses only persisted zero replicas, never an in-memory
				// drain receipt. It must not restart or re-drain the old candidate.
				proxyCalls := f.proxyCalls
				restarted := &Service{Store: s.Store, Config: s.Config, newKubeClient: s.newKubeClient}
				if err := restarted.quiesceAlignedReleaseRevision(context.Background(), app, release, policy); err != nil || *patches != 1 || f.proxyCalls != proxyCalls {
					t.Fatal("non-idempotent recovery", err)
				}
				events, _ := s.Store.ListAuditEvents(app.TenantID, true, 100)
				if !auditEventsContainAction(events, "app.release.aligned_revision.quiesced") {
					t.Fatal("missing durable cleanup audit")
				}
			}
		})
	}
}

func TestAlignedRevisionIsQueuedAndQuiescedByBackgroundSweep(t *testing.T) {
	s, app, release, _, _, patches := newAlignedRevisionFixture(t)
	targets, err := s.Store.ListAppReleaseRetirementTargets(context.Background())
	if err != nil || len(targets) != 1 || targets[0].AppID != app.ID {
		t.Fatal("aligned revision omitted by background discovery", targets, err)
	}
	projection, err := s.Store.AppReleaseRetirement(app.TenantID, app.ID).List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	queued := false
	for _, item := range projection {
		queued = queued || item.ID == release.ID && retirement.HasAlignedRevision(item)
	}
	if !queued {
		t.Fatal("serving release's immutable workload omitted by retirement queue")
	}
	if err := s.retryDrainingAppReleaseRetirement(context.Background(), app); err != nil || *patches != 1 {
		t.Fatal("background sweep did not quiesce aligned revision", *patches, err)
	}
}

func TestQuiescedAlignedRevisionCanLaterBeRetired(t *testing.T) {
	s, app, release, f, _, _ := newAlignedRevisionFixture(t)
	client, _ := s.kubeClient()
	release.Role, release.Status = model.AppReleaseRolePrevious, model.AppReleaseStatusDraining
	release.DeploymentName, release.ServiceName = release.RevisionWorkload.DeploymentName, release.RevisionWorkload.ServiceName
	if _, err := s.captureUnstartedReleaseWorkload(context.Background(), client, app, release); err == nil {
		t.Fatal("unmarked running revision accepted as quiesced")
	}
	objectMapField(f.deployment, "metadata")["annotations"].(map[string]any)[alignedRevisionQuiescedAnnotation] = release.ID
	if _, err := s.captureUnstartedReleaseWorkload(context.Background(), client, app, release); err == nil {
		t.Fatal("marker allowed nonzero replicas")
	}
	objectMapField(f.deployment, "spec")["replicas"] = 0
	f.deployment["status"] = map[string]any{"observedGeneration": 1, "replicas": 0, "updatedReplicas": 0, "readyReplicas": 0, "availableReplicas": 0}
	f.pods = f.pods[2:]
	// A fully removed owned ReplicaSet is also a valid zero-workload state.
	f.replicas = f.replicas[1:]
	before, err := s.captureUnstartedReleaseWorkload(context.Background(), client, app, release)
	if err != nil || !before.Quiesced || len(before.Pods) != 0 {
		t.Fatal("previous aligned revision blocked from normal retirement", before, err)
	}
	after, err := s.recaptureReleaseDrainWorkload(context.Background(), client, app, release, before)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("zero workload could not be reverified", err)
	}
	delete(objectMapField(f.deployment, "metadata")["annotations"].(map[string]any), alignedRevisionQuiescedAnnotation)
	if _, err := s.recaptureReleaseDrainWorkload(context.Background(), client, app, release, before); err == nil {
		t.Fatal("quiescence identity change ignored")
	}
}
