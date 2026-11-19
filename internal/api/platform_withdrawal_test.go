package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

func TestPublicationWithdrawalReadsCurrentTargetBeforeReplacingHealthyRoute(t *testing.T) {
	for _, scenario := range []string{"recovered", "absent", "forbidden", "replaced", "different stable", "disabled"} {
		t.Run(scenario, func(t *testing.T) {
			app, release, snapshot := releaseReadinessFixture(t, false)
			ns := runtime.NamespaceForTenant(app.TenantID)
			d := snapshot.deployments[kubeNamespacedKey(ns, release.DeploymentName)]
			svc := snapshot.serviceDetails[kubeNamespacedKey(ns, release.ServiceName)]
			// A singleton has one ready successor and one withdrawn endpoint from
			// the preceding ReplicaSet. The production store projection used to
			// lose the singleton evidence before the publisher observed either.
			one, yes, no := 1, true, false
			app.Spec.Replicas, release.SpecSnapshot.Replicas = one, one
			d.Spec.Replicas = &one
			d.Status.Replicas, d.Status.UpdatedReplicas, d.Status.ReadyReplicas, d.Status.AvailableReplicas = one, one, one, one
			svc.Spec.ClusterIP = "192.0.2.200"
			ep := &snapshot.releaseEndpointSlices[kubeNamespacedKey(ns, release.ServiceName)][0].Endpoints[1]
			ep.Conditions.Ready, ep.Conditions.Terminating, ep.Conditions.Serving = &no, &yes, &yes
			key := kubeNamespacedKey(ns, ep.TargetRef.Name)
			pod := snapshot.releasePods[key]
			rs := snapshot.releaseReplicaSets[kubeNamespacedKey(ns, pod.Metadata.OwnerReferences[0].Name)]
			rs.Metadata.Name, rs.Metadata.UID, rs.Spec.Template.Metadata.Annotations.ReleaseKey = "previous-rs", "previous-rs-uid", "previous"
			pod.Metadata.OwnerReferences = []kubeOwnerReferenceEvidence{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: rs.Metadata.Name, UID: rs.Metadata.UID, Controller: &yes}}
			pod.Metadata.DeletionTimestamp, pod.Metadata.Annotations.ReleaseKey = "2026-01-01T00:00:00Z", "previous"
			snapshot.releasePods[key], snapshot.releaseReplicaSets[kubeNamespacedKey(ns, rs.Metadata.Name)] = pod, rs
			d.Metadata.ResourceVersion, svc.Metadata.ResourceVersion = "1", "1"
			dp := "/apis/apps/v1/namespaces/" + ns + "/deployments/" + d.Metadata.Name
			objects := map[string]any{
				"/api/v1/namespaces/kube-system": map[string]any{"metadata": map[string]string{"uid": "cluster"}},
				dp:                               d, "/api/v1/namespaces/" + ns + "/services/" + svc.Metadata.Name: svc,
				"/apis/discovery.k8s.io/v1/namespaces/" + ns + "/endpointslices": map[string]any{"items": snapshot.releaseEndpointSlices[kubeNamespacedKey(ns, release.ServiceName)]},
			}
			for _, p := range snapshot.releasePods {
				objects["/api/v1/namespaces/"+ns+"/pods/"+p.Metadata.Name] = p
			}
			for _, rs := range snapshot.releaseReplicaSets {
				objects["/apis/apps/v1/namespaces/"+ns+"/replicasets/"+rs.Metadata.Name] = rs
			}
			depReads := 0
			k := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					t.Errorf("unexpected write %s", r.Method)
				}
				if r.URL.Path == dp {
					depReads++
					switch scenario {
					case "absent":
						http.NotFound(w, r)
						return
					case "forbidden":
						w.WriteHeader(http.StatusForbidden)
						return
					case "replaced":
						if depReads > 1 {
							d.Metadata.ResourceVersion = "2"
							objects[dp] = d
						}
					}
				}
				v, ok := objects[r.URL.Path]
				if !ok {
					t.Errorf("unexpected read %s", r.URL.Path)
					http.NotFound(w, r)
					return
				}
				json.NewEncoder(w).Encode(v)
			}))
			defer k.Close()
			p := model.AppTrafficPolicy{ID: "selection", TenantID: app.TenantID, AppID: app.ID, Mode: model.AppTrafficModeSingle, StableReleaseID: release.ID, StableWeight: 100}
			rule := platformconfig.TrafficPolicyConstraint{ID: p.ID, TenantID: p.TenantID, AppID: p.AppID, Mode: p.Mode, StableReleaseID: p.StableReleaseID, StableWeight: 100}
			if scenario == "different stable" {
				rule.StableReleaseID = "old"
			}
			bundle := func(id string, positive bool) model.PlatformArtifact {
				r := map[string]any{"enabled": true, "tenant_id": app.TenantID, "app_id": app.ID, "hostname": "sample.example.test", "path_prefix": "/", "status": model.EdgeRouteStatusUnavailable, "status_reason": platformconfig.StableNotReadyRouteReason}
				if positive {
					r["status"] = model.EdgeRouteStatusActive
					r["upstreams"] = []any{map[string]any{"weight": 100, "release_id": "previous"}}
				} else if scenario == "disabled" {
					r["enabled"] = false
				}
				return model.PlatformArtifact{ID: id, ArtifactKind: model.PlatformArtifactKindEdgeRouteBundle, ScopeKey: "global", Content: map[string]any{"routes": []any{r}}}
			}
			parent := func(id string) model.PlatformArtifact {
				return model.PlatformArtifact{ScopeKey: "global", Content: map[string]any{"artifact_ids": []any{id}, "artifact_kinds": []any{model.PlatformArtifactKindEdgeRouteBundle}}, Metadata: map[string]string{"policy_generation": "p"}}
			}
			path := t.TempDir() + "/state.json"
			st := store.New(path)
			if err := st.Init(); err != nil {
				t.Fatal(err)
			}
			state := model.State{Apps: []model.App{app}, AppReleases: []model.AppRelease{release}, AppTrafficPolicies: []model.AppTrafficPolicy{p}, PlatformArtifacts: []model.PlatformArtifact{bundle("healthy", true), bundle("candidate", false), {ID: "policy", ScopeKey: "global", ArtifactKind: model.PlatformArtifactKindPolicySnapshot, Generation: "p", Content: map[string]any{"traffic_constraints": []any{rule}}}}}
			raw, err := json.Marshal(state)
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			s := &Server{store: st, newManagedAppStatusClient: func() (*managedAppStatusClient, error) {
				return &managedAppStatusClient{client: k.Client(), baseURL: k.URL}, nil
			}}
			err = s.recheckTrafficWithdrawal(t.Context(), parent("candidate"), parent("healthy"))
			switch scenario {
			case "recovered", "different stable":
				if !errors.Is(err, errObsoleteWithdrawal) {
					t.Fatalf("stale negative admitted or unclassified: %v", err)
				}
			case "absent", "disabled":
				if err != nil {
					t.Fatal(err)
				}
			default:
				if err == nil || errors.Is(err, errObsoleteWithdrawal) {
					t.Fatalf("invalid observation accepted: %v", err)
				}
			}
			if scenario == "recovered" && depReads != 2 || scenario == "disabled" && depReads != 0 {
				t.Fatal("wrong scope for live recheck", depReads)
			}
		})
	}
}
