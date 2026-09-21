package controller

import (
	"context"
	"encoding/json"
	"fmt"
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
	deployment map[string]any
	replicas   []map[string]any
	pods       []map[string]any
	proxyCalls int
	active     int
	change     func(*drainWorkloadFixture)
}

func newDrainWorkloadFixture(t *testing.T) (*Service, model.App, model.AppRelease, *drainWorkloadFixture) {
	t.Helper()
	st, app, _, _ := newSafeRolloutTestState(t)
	old, err := st.CreateAppRelease(model.AppRelease{ID: "release-old", TenantID: app.TenantID, AppID: app.ID,
		RuntimeID: "runtime-old", ResolvedImageRef: app.Spec.Image, DeploymentName: "workload-old",
		Role: model.AppReleaseRolePrevious, Status: model.AppReleaseStatusDraining})
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
		runtime.FugueLabelManagedBy: runtime.FugueLabelManagedByValue, runtime.FugueLabelAppReleaseID: old.ID}
	metadata := func(name, uid, ownerKind, ownerName, ownerUID string) map[string]any {
		return map[string]any{"name": name, "uid": uid, "labels": labels, "generation": 1,
			"ownerReferences": []map[string]any{{"kind": ownerKind, "name": ownerName, "uid": ownerUID, "controller": true}}}
	}
	application := map[string]any{"name": "application", "image": app.Spec.Image, "ports": []map[string]any{{"containerPort": 8080}}}
	agent := map[string]any{"name": "fugue-drain-agent", "image": "ghcr.io/example/drain@sha256:" + strings.Repeat("a", 64),
		"ports": []map[string]any{{"name": "drain-agent", "containerPort": 19090}},
		"env":   []map[string]any{{"name": "FUGUE_DRAIN_FAIL_CLOSED", "value": "true"}, {"name": "FUGUE_DRAIN_QUIET_PERIOD_SECONDS", "value": "1"}}}
	spec := map[string]any{"containers": []map[string]any{application}, "initContainers": []map[string]any{agent}}
	f := &drainWorkloadFixture{deployment: map[string]any{"metadata": metadata(old.DeploymentName, "deployment-uid", "", "", ""),
		"spec":   map[string]any{"replicas": 2, "template": map[string]any{"spec": spec}},
		"status": map[string]any{"observedGeneration": 1, "replicas": 2, "updatedReplicas": 2, "readyReplicas": 2, "availableReplicas": 2}},
		replicas: []map[string]any{{"metadata": metadata("revision", "replica-uid", "Deployment", old.DeploymentName, "deployment-uid")}}}
	for i := 0; i < 2; i++ {
		status := func(name string) map[string]any {
			return map[string]any{"name": name, "ready": true, "imageID": "sha256:" + strings.Repeat("a", 64),
				"containerID": fmt.Sprintf("containerd://%s-%d", name, i), "restartCount": 0, "state": map[string]any{"running": map[string]any{"startedAt": "2026-01-01T00:00:00Z"}}}
		}
		f.pods = append(f.pods, map[string]any{"metadata": metadata(fmt.Sprintf("pod-%d", i), fmt.Sprintf("pod-uid-%d", i), "ReplicaSet", "revision", "replica-uid"),
			"spec": spec, "status": map[string]any{"containerStatuses": []map[string]any{status("application")}, "initContainerStatuses": []map[string]any{status("fugue-drain-agent")}}})
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	svc := &Service{Store: st, Config: config.ControllerConfig{KubectlApply: true}, newKubeClient: func(string) (*kubeClient, error) { return &kubeClient{baseURL: srv.URL, client: srv.Client()}, nil },
		safeRolloutEdgeBundleObserver: staticSafeRolloutEdgeObserver{observation: safeRolloutEdgeBundleObservation{Ready: true}}}
	return svc, app, old, f
}

func TestReleaseDrainRequiresCompleteStablePodSet(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Service, model.App, *model.AppRelease, *drainWorkloadFixture)
		ready  bool
	}{
		{name: "complete", ready: true},
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
				changed.RuntimeID = "replacement"
				_, _ = s.Store.UpdateAppRelease(changed)
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
