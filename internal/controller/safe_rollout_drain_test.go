package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/drainprotocol"
	"fugue/internal/model"
	"fugue/internal/runtime"
)

type drainWorkloadFixture struct {
	deployment     map[string]any
	service        map[string]any
	replicas       []map[string]any
	pods           []map[string]any
	proxyCalls     int
	active         int
	change         func(*drainWorkloadFixture)
	deleteCalls    int
	failDelete     bool
	conflictDelete bool
	extra          func(http.ResponseWriter, *http.Request) bool
}

func newDrainWorkloadFixture(t *testing.T) (*Service, model.App, model.AppRelease, *drainWorkloadFixture) {
	t.Helper()
	return newDrainWorkloadFixtureWithOutcome(t, false)
}

func newDrainWorkloadFixtureWithOutcome(t *testing.T, failed bool, historical ...bool) (*Service, model.App, model.AppRelease, *drainWorkloadFixture) {
	t.Helper()
	st, app, _, op := newSafeRolloutTestState(t)
	if _, ok, err := st.TryClaimPendingOperation(op.ID); err != nil || !ok {
		t.Fatal("claim", err)
	}
	old, err := st.CreateAppRelease(model.AppRelease{ID: "release-old", TenantID: app.TenantID, AppID: app.ID,
		RuntimeID: "runtime-old", ResolvedImageRef: app.Spec.Image, DeploymentName: "workload-old", ServiceName: "service-old",
		Role: model.AppReleaseRoleCandidate, Status: model.AppReleaseStatusCreating})
	if err != nil {
		t.Fatal(err)
	}
	unbound := len(historical) > 0 && historical[0]
	if !unbound {
		old, err = st.BindAppReleaseWorkload(context.Background(), old, model.AppReleaseWorkload{OperationID: op.ID, Namespace: runtime.NamespaceForTenant(app.TenantID), DeploymentName: old.DeploymentName, DeploymentUID: "deployment-uid", DeploymentGeneration: 1, ServiceName: old.ServiceName, ServiceUID: "service-uid", ReleaseKey: "release-key", RuntimeID: old.RuntimeID, ImageRef: old.ResolvedImageRef})
	}
	if err != nil {
		t.Fatal(err)
	}
	old.Role, old.Status = model.AppReleaseRolePrevious, model.AppReleaseStatusDraining
	if unbound {
		options := runtime.RenderOptions{Revision: safeRolloutCandidateRevision(old.ID)}
		old.DeploymentName = runtime.RuntimeAppResourceNameWithOptions(app, options)
		old.ServiceName = runtime.RuntimeAppServiceNameWithOptions(app, options)
		spec := *cloneControllerAppSpec(&app.Spec)
		spec.Env = map[string]string{"HISTORICAL": "lost-snapshot"}
		old.SpecSnapshot = &spec
		now := time.Now().UTC()
		old.PromotedAt = &now
	}
	if failed {
		old.Role, old.Status = model.AppReleaseRoleCandidate, model.AppReleaseStatusFailed
		old.StatusReason = "canary latency gate failed"
	}
	old, err = st.UpdateAppRelease(old)
	if err != nil {
		t.Fatal(err)
	}
	if unbound {
		if err := st.AppendAuditEvent(model.AuditEvent{TenantID: app.TenantID, ActorType: model.ActorTypeSystem, ActorID: "safe-rollout-controller", Action: "app.release.promote", TargetType: "app_release", TargetID: old.ID, Metadata: map[string]string{"app_id": app.ID, "app_release_id": old.ID, "operation_id": op.ID, "mode": "safe_zero_downtime"}}); err != nil {
			t.Fatal(err)
		}
	}
	if failed {
		_, err = st.FailOperation(op.ID, old.StatusReason)
	} else {
		_, err = st.CompleteManagedOperation(op.ID, "", "done")
	}
	if err != nil {
		t.Fatal(err)
	}
	old, err = st.GetAppRelease(app.TenantID, false, old.ID)
	if err != nil {
		t.Fatal(err)
	}
	stable, err := st.CreateAppRelease(model.AppRelease{ID: "release-current", TenantID: app.TenantID, AppID: app.ID,
		RuntimeID: "runtime-current", ResolvedImageRef: "ghcr.io/example/api:v2", DeploymentName: "workload-current",
		Role: model.AppReleaseRoleStable, Status: model.AppReleaseStatusServing})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.UpsertAppTrafficPolicy(model.AppTrafficPolicy{AppID: app.ID, TenantID: app.TenantID,
		Mode: model.AppTrafficModeSingle, StableReleaseID: stable.ID, StableWeight: 100}); err != nil {
		t.Fatal(err)
	}
	labels := map[string]any{runtime.FugueLabelAppID: app.ID, runtime.FugueLabelTenantID: app.TenantID,
		runtime.FugueLabelManagedBy: runtime.FugueLabelManagedByValue, runtime.FugueLabelAppReleaseID: old.ID, runtime.FugueLabelAppWorkload: old.DeploymentName}
	metadata := func(name, uid, ownerKind, ownerName, ownerUID string) map[string]any {
		return map[string]any{"name": name, "namespace": runtime.NamespaceForTenant(app.TenantID), "creationTimestamp": op.CreatedAt.Truncate(time.Second).Format(time.RFC3339), "uid": uid, "resourceVersion": "1", "labels": labels, "generation": 1, "annotations": map[string]any{runtime.FugueAnnotationReleaseKey: "release-key"},
			"ownerReferences": []map[string]any{{"kind": ownerKind, "name": ownerName, "uid": ownerUID, "controller": true}}}
	}
	application := map[string]any{"name": "application", "image": app.Spec.Image, "ports": []map[string]any{{"containerPort": 8080}}}
	agent := map[string]any{"name": "fugue-drain-agent", "image": "ghcr.io/example/drain@sha256:" + strings.Repeat("a", 64),
		"ports": []map[string]any{{"name": "drain-agent", "containerPort": 19090}},
		"env":   []map[string]any{{"name": "FUGUE_DRAIN_FAIL_CLOSED", "value": "true"}, {"name": "FUGUE_DRAIN_QUIET_PERIOD_SECONDS", "value": "1"}}}
	spec := map[string]any{"containers": []map[string]any{application}, "initContainers": []map[string]any{agent}}
	f := &drainWorkloadFixture{deployment: map[string]any{"metadata": metadata(old.DeploymentName, "deployment-uid", "", "", ""),
		"spec":   map[string]any{"replicas": 2, "template": map[string]any{"metadata": metadata("", "", "", "", ""), "spec": spec}},
		"status": map[string]any{"observedGeneration": 1, "replicas": 2, "updatedReplicas": 2, "readyReplicas": 2, "availableReplicas": 2}},
		replicas: []map[string]any{{"metadata": metadata("revision", "replica-uid", "Deployment", old.DeploymentName, "deployment-uid")}}}
	f.service = map[string]any{"metadata": metadata(old.ServiceName, "service-uid", "", "", ""), "spec": map[string]any{"selector": labels}}
	for i := 0; i < 2; i++ {
		status := func(name string) map[string]any {
			return map[string]any{"name": name, "ready": true, "imageID": "sha256:" + strings.Repeat("a", 64),
				"containerID": fmt.Sprintf("containerd://%s-%d", name, i), "restartCount": 0, "state": map[string]any{"running": map[string]any{"startedAt": "2026-01-01T00:00:00Z"}}}
		}
		f.pods = append(f.pods, map[string]any{"metadata": metadata(fmt.Sprintf("pod-%d", i), fmt.Sprintf("pod-uid-%d", i), "ReplicaSet", "revision", "replica-uid"),
			"spec": spec, "status": map[string]any{"containerStatuses": []map[string]any{status("application")}, "initContainerStatuses": []map[string]any{status("fugue-drain-agent")}}})
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if f.extra != nil && f.extra(w, r) {
			return
		}
		if r.Method == http.MethodDelete {
			f.deleteCalls++
			var body struct {
				Preconditions map[string]string `json:"preconditions"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			uid := "deployment-uid"
			if strings.Contains(r.URL.Path, "/services/") {
				uid = "service-uid"
			}
			if body.Preconditions["uid"] != uid {
				t.Errorf("unsafe deletion: %+v", body)
			}
			if body.Preconditions["resourceVersion"] != "1" {
				t.Error("missing resource version")
			}
			if f.failDelete {
				f.failDelete = false
				http.Error(w, "unavailable", 503)
				return
			}
			if f.conflictDelete {
				http.Error(w, "UID precondition", 409)
				return
			}
			w.WriteHeader(http.StatusOK)
			return
		}
		if strings.Contains(r.URL.Path, "/proxy/") {
			f.proxyCalls++
			if f.change != nil {
				f.change(f)
			}
			active := f.active
			parts := strings.Split(r.URL.Path, "/")
			pod := strings.Split(parts[len(parts)-4], ":")[0]
			_ = json.NewEncoder(w).Encode(drainprotocol.Snapshot{APIVersion: drainprotocol.Version, Nonce: r.URL.Query().Get("nonce"), Pod: pod,
				Namespace: runtime.NamespaceForTenant(app.TenantID), AppPorts: []int{8080}, ActiveConnections: &active})
			return
		}
		switch {
		case strings.HasSuffix(r.URL.Path, "/deployments/"+old.DeploymentName):
			_ = json.NewEncoder(w).Encode(f.deployment)
		case strings.HasSuffix(r.URL.Path, "/services/"+old.ServiceName):
			_ = json.NewEncoder(w).Encode(f.service)
		case strings.HasSuffix(r.URL.Path, "/replicasets"):
			if r.Header.Get("Accept") != metadataListAccept || r.URL.RawQuery != "" {
				t.Error("drain ownership read must retain the full unfiltered metadata set")
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"items": f.replicas})
		case strings.HasSuffix(r.URL.Path, "/pods"):
			_ = json.NewEncoder(w).Encode(map[string]any{"items": f.pods})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	svc := &Service{Store: st, Logger: log.New(io.Discard, "", 0), Config: config.ControllerConfig{KubectlApply: true}, newKubeClient: func(string) (*kubeClient, error) { return &kubeClient{baseURL: srv.URL, client: srv.Client()}, nil },
		safeRolloutEdgeBundleObserver: staticSafeRolloutEdgeObserver{observation: safeRolloutEdgeBundleObservation{Ready: true}}}
	return svc, app, old, f
}

func TestFailedCandidateRetiresOnlyAfterVerifiedRollbackAndDrain(t *testing.T) {
	for _, scenario := range []string{"idle", "busy", "unconfirmed_traffic", "still_referenced", "retention", "retire_grace", "wrong_operation_outcome", "changed_after_drain"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			s, app, failed, f := newDrainWorkloadFixtureWithOutcome(t, scenario != "wrong_operation_outcome")
			if scenario == "wrong_operation_outcome" {
				failed.Role, failed.Status = model.AppReleaseRoleCandidate, model.AppReleaseStatusFailed
				var err error
				failed, err = s.Store.UpdateAppRelease(failed)
				if err != nil {
					t.Fatal(err)
				}
			}
			policy, err := s.Store.GetAppTrafficPolicy(app.TenantID, false, app.ID)
			if err != nil {
				t.Fatal(err)
			}
			s.safeRolloutDrainMetricsQuerier = kubeSafeRolloutDrainObserver{service: s}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			switch scenario {
			case "busy":
				f.active = 1
			case "unconfirmed_traffic":
				s.safeRolloutEdgeBundleObserver = staticSafeRolloutEdgeObserver{}
			case "still_referenced":
				policy.Mode, policy.CandidateReleaseID = model.AppTrafficModeCanary, failed.ID
				policy.StableWeight, policy.CandidateWeight = 99, 1
				if _, err = s.Store.UpsertAppTrafficPolicy(policy); err != nil {
					t.Fatal(err)
				}
			case "retention":
				future := time.Now().Add(time.Hour)
				failed.RetentionUntil = &future
				if _, err = s.Store.UpdateAppRelease(failed); err != nil {
					t.Fatal(err)
				}
			case "retire_grace":
				app.Spec.Continuity.ZeroDowntime.RetireGraceSeconds = 60
			case "changed_after_drain":
				calls := 0
				s.safeRolloutEdgeBundleObserver = retirementTestObserver(func() {
					calls++
					if calls == 2 {
						failed.Status = model.AppReleaseStatusReady
						if _, err = s.Store.UpdateAppRelease(failed); err != nil {
							t.Fatal(err)
						}
					}
				})
			}
			if err := s.retryDrainingAppReleaseRetirement(ctx, app); err != nil && !(scenario == "busy" && errors.Is(err, context.DeadlineExceeded)) {
				t.Fatal(err)
			}
			got, err := s.Store.GetAppRelease(app.TenantID, false, failed.ID)
			if err != nil {
				t.Fatal(err)
			}
			if scenario != "idle" {
				if got.Status == model.AppReleaseStatusRetired || f.deleteCalls != 0 {
					t.Fatalf("failed release lost without proof: %s, deletes=%d", got.Status, f.deleteCalls)
				}
				return
			}
			if got.Status != model.AppReleaseStatusRetired || f.deleteCalls != 2 || f.proxyCalls < 4 {
				t.Fatalf("failed release did not drain and retire: %s, deletes=%d, observations=%d", got.Status, f.deleteCalls, f.proxyCalls)
			}
			op, err := s.Store.GetOperation(failed.RevisionWorkload.OperationID)
			if err != nil || op.Status != model.OperationStatusFailed {
				t.Fatal("cleanup rewrote the failed operation", err)
			}
			current, err := s.Store.GetAppTrafficPolicy(app.TenantID, false, app.ID)
			if err != nil || current != policy {
				t.Fatal("cleanup changed traffic", err)
			}
		})
	}
}

func TestBoundRevisionRetiresAfterCanonicalAlignmentAndRetriesFailedDeletion(t *testing.T) {
	s, app, old, f := newDrainWorkloadFixture(t)
	old.DeploymentName, old.ServiceName = runtime.RuntimeAppResourceName(app), runtime.RuntimeAppServiceName(app)
	var err error
	old, err = s.Store.UpdateAppRelease(old)
	if err != nil {
		t.Fatal(err)
	}
	s.safeRolloutDrainMetricsQuerier = kubeSafeRolloutDrainObserver{service: s}
	f.failDelete = true
	if err = s.retryDrainingAppReleaseRetirement(context.Background(), app); err != nil {
		t.Fatal(err)
	}
	retired, err := s.Store.GetAppRelease(app.TenantID, false, old.ID)
	if err != nil || retired.Status != model.AppReleaseStatusRetired {
		t.Fatal("bound workload did not retire", err, retired.Status)
	}
	if f.deleteCalls != 1 {
		t.Fatalf("unexpected first cleanup %d", f.deleteCalls)
	}
	protected := map[string]map[string]struct{}{}
	if err = s.preserveActiveAppReleaseResources(app, protected); err != nil {
		t.Fatal(err)
	}
	for kind, name := range map[string]string{"Deployment": old.RevisionWorkload.DeploymentName, "Service": old.RevisionWorkload.ServiceName} {
		if _, ok := protected[kind][name]; !ok {
			t.Fatal("ordinary prune bypasses retired UID cleanup", kind)
		}
	}
	if err = s.retryDrainingAppReleaseRetirement(context.Background(), app); err != nil {
		t.Fatal(err)
	}
	if f.deleteCalls != 3 {
		t.Fatalf("retired entry filtered from retry: %d calls", f.deleteCalls)
	}
}

func TestBoundRetirementRetainsResourcesWhenPostDrainStateChanges(t *testing.T) {
	for _, change := range []string{"policy", "stable", "workload", "cancel", "retention"} {
		t.Run(change, func(t *testing.T) {
			s, app, old, f := newDrainWorkloadFixture(t)
			p, _ := s.Store.GetAppTrafficPolicy(app.TenantID, false, app.ID)
			stable, _ := s.Store.GetAppRelease(app.TenantID, false, p.StableReleaseID)
			op, _ := s.Store.GetOperation(old.RevisionWorkload.OperationID)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s.safeRolloutDrainMetricsQuerier = kubeSafeRolloutDrainObserver{service: s}
			calls := 0
			s.safeRolloutEdgeBundleObserver = retirementTestObserver(func() {
				calls++
				if calls != 2 {
					return
				}
				switch change {
				case "policy":
					p.StickyCookie = "changed"
					if _, err := s.Store.UpsertAppTrafficPolicy(p); err != nil {
						t.Fatal(err)
					}
				case "stable":
					stable.ServiceName = "different"
					if _, err := s.Store.UpdateAppRelease(stable); err != nil {
						t.Fatal(err)
					}
				case "workload":
					objectMapField(f.deployment, "metadata")["uid"] = "replacement"
				case "cancel":
					cancel()
				case "retention":
					future := time.Now().Add(time.Hour)
					old.RetentionUntil = &future
					if _, err := s.Store.UpdateAppRelease(old); err != nil {
						t.Fatal(err)
					}
				}
			})
			state := &safeRolloutState{Enabled: true, StableAlignmentAllowed: true, CandidateApp: app, Candidate: stable, StableRelease: old}
			s.finalizeSafeZeroDowntimePreviousRetire(ctx, op, state)
			kept, err := s.Store.GetAppRelease(app.TenantID, false, old.ID)
			if err != nil || kept.Status != model.AppReleaseStatusDraining || f.deleteCalls != 0 {
				t.Fatal("post-drain change authorized deletion", change, err, kept.Status, f.deleteCalls)
			}
		})
	}
}

type retirementTestObserver func()

func (f retirementTestObserver) WaitForSafeRolloutEdgeRouteBundle(context.Context, model.App, model.AppRelease, int, time.Time) (safeRolloutEdgeBundleObservation, error) {
	f()
	return safeRolloutEdgeBundleObservation{Ready: true}, nil
}

func TestBoundCleanupRejectsReplacementOrChangedIdentity(t *testing.T) {
	for _, change := range []string{"uid", "owner", "key", "conflict", "namespace", "not_retired"} {
		t.Run(change, func(t *testing.T) {
			s, app, r, f := newDrainWorkloadFixture(t)
			r.Role, r.Status = model.AppReleaseRoleRetired, model.AppReleaseStatusRetired
			switch change {
			case "uid":
				objectMapField(f.deployment, "metadata")["uid"] = "new-uid"
			case "owner":
				objectMapField(objectMapField(f.deployment, "metadata"), "labels")[runtime.FugueLabelAppID] = "other"
			case "key":
				objectMapField(objectMapValue(nestedObjectValue(f.deployment, "spec", "template", "metadata")), "annotations")[runtime.FugueAnnotationReleaseKey] = "other"
			case "conflict":
				f.conflictDelete = true
			case "namespace":
				r.RevisionWorkload.Namespace = "foreign"
			case "not_retired":
				r.Status = model.AppReleaseStatusServing
			}
			if err := s.cleanupSafeRolloutRetiredResources(context.Background(), app, r); err == nil {
				t.Fatal("unsafe cleanup accepted")
			}
			want := 0
			if change == "conflict" {
				want = 1
			}
			if f.deleteCalls != want {
				t.Fatalf("delete count %d want %d", f.deleteCalls, want)
			}
		})
	}
}

func TestReleaseDrainRequiresCompleteStablePodSet(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Service, model.App, *model.AppRelease, *drainWorkloadFixture)
		ready  bool
	}{
		{name: "complete", ready: true},
		{name: "canonical_binding", ready: true, mutate: func(s *Service, a model.App, r *model.AppRelease, _ *drainWorkloadFixture) {
			r.DeploymentName, r.ServiceName = runtime.RuntimeAppResourceName(a), runtime.RuntimeAppServiceName(a)
			var err error
			*r, err = s.Store.UpdateAppRelease(*r)
			if err != nil {
				t.Fatal(err)
			}
		}},
		{name: "unbound", mutate: func(_ *Service, _ model.App, r *model.AppRelease, _ *drainWorkloadFixture) { r.RevisionWorkload = nil }},
		{name: "replaced_service", mutate: func(_ *Service, _ model.App, _ *model.AppRelease, f *drainWorkloadFixture) {
			objectMapField(f.service, "metadata")["uid"] = "other"
		}},
		{name: "old_pod_release_key", mutate: func(_ *Service, _ model.App, _ *model.AppRelease, f *drainWorkloadFixture) {
			objectMapField(objectMapField(f.pods[0], "metadata"), "annotations")[runtime.FugueAnnotationReleaseKey] = "old"
		}},
		{name: "canonical", mutate: func(_ *Service, a model.App, r *model.AppRelease, _ *drainWorkloadFixture) {
			r.DeploymentName = runtime.RuntimeAppResourceName(a)
		}},
		{name: "traffic_unconfirmed", mutate: func(s *Service, _ model.App, _ *model.AppRelease, _ *drainWorkloadFixture) {
			s.safeRolloutEdgeBundleObserver = staticSafeRolloutEdgeObserver{}
		}},
		{name: "missing_pod", mutate: func(_ *Service, _ model.App, _ *model.AppRelease, f *drainWorkloadFixture) { f.pods = f.pods[:1] }},
		{name: "extra_pod", mutate: func(_ *Service, _ model.App, _ *model.AppRelease, f *drainWorkloadFixture) {
			f.pods = append(f.pods, f.pods[0])
		}},
		{name: "replaced_deployment", mutate: func(_ *Service, _ model.App, _ *model.AppRelease, f *drainWorkloadFixture) {
			objectMapField(f.deployment, "metadata")["uid"] = "replacement"
		}},
		{name: "invalid_agent_port", mutate: func(_ *Service, _ model.App, _ *model.AppRelease, f *drainWorkloadFixture) {
			mapSlice(mapSlice(nestedObjectValue(f.pods[0], "spec", "initContainers"))[0]["ports"])[0]["containerPort"] = "invalid"
		}},
		{name: "pod_deleted_during_observation", mutate: func(_ *Service, _ model.App, _ *model.AppRelease, f *drainWorkloadFixture) {
			f.change = func(f *drainWorkloadFixture) {
				objectMapField(f.pods[0], "metadata")["deletionTimestamp"] = "2026-01-01T00:00:00Z"
			}
		}},
		{name: "pod_replaced_during_observation", mutate: func(_ *Service, _ model.App, _ *model.AppRelease, f *drainWorkloadFixture) {
			f.change = func(f *drainWorkloadFixture) { objectMapField(f.pods[0], "metadata")["uid"] = "replacement" }
		}},
		{name: "agent_restarted_during_observation", mutate: func(_ *Service, _ model.App, _ *model.AppRelease, f *drainWorkloadFixture) {
			f.change = func(f *drainWorkloadFixture) {
				mapSlice(nestedObjectValue(f.pods[0], "status", "initContainerStatuses"))[0]["containerID"] = "containerd://new"
			}
		}},
		{name: "traffic_changed", mutate: func(s *Service, a model.App, r *model.AppRelease, f *drainWorkloadFixture) {
			f.change = func(f *drainWorkloadFixture) {
				f.change = nil
				p, _ := s.Store.GetAppTrafficPolicy(a.TenantID, true, a.ID)
				p.StableReleaseID = r.ID
				_, _ = s.Store.UpsertAppTrafficPolicy(p)
			}
		}},
		{name: "release_changed", mutate: func(s *Service, _ model.App, r *model.AppRelease, f *drainWorkloadFixture) {
			f.change = func(f *drainWorkloadFixture) {
				f.change = nil
				changed := *r
				// Executable intent is immutable after binding. Change a
				// mutable target to exercise the observation's version fence.
				changed.ServiceName = "replacement"
				if _, err := s.Store.UpdateAppRelease(changed); err != nil {
					t.Error(err)
				}
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s, app, previous, f := newDrainWorkloadFixture(t)
			if tc.mutate != nil {
				tc.mutate(s, app, &previous, f)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer cancel()
			metrics, err := (kubeSafeRolloutDrainObserver{service: s}).QuerySafeRolloutDrainMetrics(ctx, app, previous, time.Now())
			if tc.ready {
				if err != nil || !metrics.Ready || metrics.FinalCount != 2 || metrics.SampleCount < 12 {
					t.Fatalf("incomplete positive observation: %+v, %v", metrics, err)
				}
			} else if err == nil || metrics.Ready {
				t.Fatalf("unsafe drain accepted: %+v", metrics)
			}
		})
	}
}

func TestReleaseDrainSnapshotRejectsMissingOrWrongIdentity(t *testing.T) {
	for _, name := range []string{"valid", "wrong_nonce", "wrong_pod", "wrong_ports", "missing_active", "negative_active", "old_agent", "oversized", "trailing_json", "unknown_field"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body := map[string]any{"api_version": drainprotocol.Version, "nonce": r.URL.Query().Get("nonce"), "pod": "pod", "namespace": "tenant", "app_ports": []int{8080}, "active_connections": 0}
				switch name {
				case "wrong_nonce":
					body["nonce"] = "stale"
				case "wrong_pod":
					body["pod"] = "other"
				case "wrong_ports":
					body["app_ports"] = []int{9090}
				case "missing_active":
					delete(body, "active_connections")
				case "negative_active":
					body["active_connections"] = -1
				case "old_agent":
					http.NotFound(w, r)
					return
				case "oversized":
					body["nonce"] = strings.Repeat("a", 5000)
				case "unknown_field":
					body["untrusted"] = true
				}
				_ = json.NewEncoder(w).Encode(body)
				if name == "trailing_json" {
					_, _ = w.Write([]byte("{}"))
				}
			}))
			defer srv.Close()
			c := &kubeClient{baseURL: srv.URL, client: srv.Client()}
			_, err := c.observeReleasePodDrain(context.Background(), "tenant", releaseDrainPod{Name: "pod", Port: 19090, AppPorts: []int{8080}})
			if (err == nil) != (name == "valid") {
				t.Fatalf("unexpected result: %v", err)
			}
		})
	}
}

func TestReleaseDrainBusyPodsRemainRetainedAtDeadline(t *testing.T) {
	s, app, previous, f := newDrainWorkloadFixture(t)
	f.active = 1
	ctx, cancel := context.WithTimeout(context.Background(), 350*time.Millisecond)
	defer cancel()
	metrics, err := (kubeSafeRolloutDrainObserver{service: s}).QuerySafeRolloutDrainMetrics(ctx, app, previous, time.Now())
	if err == nil || metrics.Ready || metrics.MaxActiveConnections != 2 || metrics.FinalCount != 0 {
		t.Fatalf("busy Pod set was not retained: %+v %v", metrics, err)
	}
	current, err := s.Store.GetAppRelease(app.TenantID, true, previous.ID)
	if err != nil || current.Status != model.AppReleaseStatusDraining {
		t.Fatalf("observation changed release lifecycle: %+v %v", current, err)
	}
}
