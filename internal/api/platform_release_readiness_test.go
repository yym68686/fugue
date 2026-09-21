package api

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

func releaseReadinessFixture(t *testing.T, candidate bool) (model.App, model.AppRelease, managedAppKubeSnapshot) {
	t.Helper()
	app := model.App{ID: "app-a", TenantID: "tenant-a", Name: "sample", Spec: model.AppSpec{Image: "registry.example/app@sha256:" + strings.Repeat("a", 64), Replicas: 2, RuntimeID: model.DefaultManagedRuntimeID, Ports: []int{8080}}}
	app.Route = &model.AppRoute{Hostname: "app.example.test", ServicePort: 8080}
	target := app
	role := model.AppReleaseRoleStable
	revision := runtime.AppRevisionRenderOptions{}
	id := "stable-release"
	if candidate {
		id = "candidate-release"
		role = model.AppReleaseRoleCandidate
		target.Spec.Image = "registry.example/app@sha256:" + strings.Repeat("b", 64)
		revision = runtime.AppRevisionRenderOptions{Role: runtime.AppRevisionRoleCandidate, ReleaseID: id}
	}
	options := runtime.RenderOptions{Revision: revision}
	release := model.AppRelease{ID: id, AppID: app.ID, TenantID: app.TenantID, Role: role, Status: model.AppReleaseStatusReady, RuntimeID: app.Spec.RuntimeID, ResolvedImageRef: target.Spec.Image, SpecSnapshot: &target.Spec, DeploymentName: runtime.RuntimeAppResourceNameWithOptions(target, options), ServiceName: runtime.RuntimeAppServiceNameWithOptions(target, options), UpstreamURL: runtime.AppRevisionServiceURL(target, options)}
	ns := runtime.NamespaceForTenant(app.TenantID)
	snapshot := managedAppKubeSnapshot{namespaces: map[string]struct{}{ns: {}}, deployments: map[string]kubeDeploymentRuntimeEvidence{}, services: map[string]struct{}{}, serviceDetails: map[string]kubeServiceRuntimeEvidence{}, endpointSlicesAvailable: true}
	renderer := runtime.Renderer{}
	for _, object := range renderer.BuildManagedAppRevisionChildObjects(target, runtime.SchedulingConstraints{}, nil, nil, revision) {
		metadata := object["metadata"].(map[string]any)
		metadata["uid"] = id + "-" + object["kind"].(string)
		switch object["kind"] {
		case "Deployment":
			metadata["generation"] = int64(3)
			object["status"] = map[string]any{"observedGeneration": 3, "replicas": 2, "updatedReplicas": 2, "readyReplicas": 2, "availableReplicas": 2}
			raw, _ := json.Marshal(object)
			var d kubeDeploymentRuntimeEvidence
			if err := json.Unmarshal(raw, &d); err != nil {
				t.Fatal(err)
			}
			snapshot.deployments[kubeNamespacedKey(ns, d.Metadata.Name)] = d
		case "Service":
			raw, _ := json.Marshal(object)
			var service kubeServiceRuntimeEvidence
			if err := json.Unmarshal(raw, &service); err != nil {
				t.Fatal(err)
			}
			key := kubeNamespacedKey(ns, service.Metadata.Name)
			snapshot.serviceDetails[key] = service
			snapshot.services[key] = struct{}{}
		}
	}
	populateReleaseEndpointFixture(t, &snapshot, release, 2)
	return app, release, snapshot
}

func populateReleaseEndpointFixture(t *testing.T, snapshot *managedAppKubeSnapshot, release model.AppRelease, replicas int) {
	t.Helper()
	ns := runtime.NamespaceForTenant(release.TenantID)
	d := snapshot.deployments[kubeNamespacedKey(ns, release.DeploymentName)]
	svc := snapshot.serviceDetails[kubeNamespacedKey(ns, release.ServiceName)]
	controller := true
	rs := kubeReleaseReplicaSetEvidence{}
	rs.Metadata.Namespace, rs.Metadata.Name, rs.Metadata.UID = ns, d.Metadata.Name+"-rs", d.Metadata.UID+"-rs"
	rs.Metadata.Labels = maps.Clone(d.Spec.Template.Metadata.Labels)
	rs.Metadata.OwnerReferences = []kubeOwnerReferenceEvidence{{APIVersion: "apps/v1", Kind: "Deployment", Name: d.Metadata.Name, UID: d.Metadata.UID, Controller: &controller}}
	rs.Spec.Template.Metadata.Labels = maps.Clone(d.Spec.Template.Metadata.Labels)
	rs.Spec.Template.Metadata.Annotations = d.Spec.Template.Metadata.Annotations
	snapshot.releaseReplicaSets = map[string]kubeReleaseReplicaSetEvidence{kubeNamespacedKey(ns, rs.Metadata.Name): rs}
	snapshot.releasePods = map[string]kubeReleasePodEvidence{}
	slice := kubeEndpointSliceEvidence{}
	slice.Metadata.Name, slice.Metadata.Namespace, slice.Metadata.UID = svc.Metadata.Name+"-slice", ns, svc.Metadata.UID+"-slice"
	slice.Metadata.Labels = map[string]string{"kubernetes.io/service-name": svc.Metadata.Name}
	slice.Metadata.OwnerReferences = []kubeOwnerReferenceEvidence{{APIVersion: "v1", Kind: "Service", Name: svc.Metadata.Name, UID: svc.Metadata.UID, Controller: &controller}}
	for i := 0; i < replicas; i++ {
		pod := kubeReleasePodEvidence{}
		pod.Metadata.Name, pod.Metadata.Namespace, pod.Metadata.UID = fmt.Sprintf("%s-pod-%d", release.ID, i), ns, fmt.Sprintf("%s-pod-uid-%d", release.ID, i)
		pod.Metadata.Labels = maps.Clone(d.Spec.Template.Metadata.Labels)
		pod.Metadata.Annotations = d.Spec.Template.Metadata.Annotations
		pod.Metadata.OwnerReferences = []kubeOwnerReferenceEvidence{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: rs.Metadata.Name, UID: rs.Metadata.UID, Controller: &controller}}
		pod.Spec.Containers = d.Spec.Template.Spec.Containers
		pod.Status.Phase = "Running"
		pod.Status.PodIP = fmt.Sprintf("192.0.2.%d", i+10)
		pod.Status.Conditions = []struct {
			Type   string `json:"type"`
			Status string `json:"status"`
		}{{Type: "Ready", Status: "True"}}
		snapshot.releasePods[kubeNamespacedKey(ns, pod.Metadata.Name)] = pod
		ep := kubeReleaseEndpointEvidence{Addresses: []string{pod.Status.PodIP}}
		ep.TargetRef.Kind, ep.TargetRef.Name, ep.TargetRef.Namespace, ep.TargetRef.UID = "Pod", pod.Metadata.Name, ns, pod.Metadata.UID
		ep.Conditions.Ready = &controller
		slice.Endpoints = append(slice.Endpoints, ep)
	}
	snapshot.releaseEndpointSlices = map[string][]kubeEndpointSliceEvidence{kubeNamespacedKey(ns, svc.Metadata.Name): {slice}}
}

func TestReleaseRuntimeReadinessUsesExactIndependentWorkload(t *testing.T) {
	for _, candidate := range []bool{false, true} {
		for _, name := range []string{"ready", "scaled up", "scaled down", "other app", "other tenant", "snapshot runtime", "snapshot image", "old generation", "mixed replicas", "wrong image", "missing deployment", "missing UID", "foreign deployment", "foreign template", "foreign release", "missing service", "wrong selector", "wrong port", "foreign upstream", "stale endpoint owner", "no endpoints", "missing endpoint API", "no replicas", "retired"} {
			t.Run(map[bool]string{false: "stable", true: "candidate"}[candidate]+"/"+name, func(t *testing.T) {
				app, r, snap := releaseReadinessFixture(t, candidate)
				ns := runtime.NamespaceForTenant(app.TenantID)
				dk := kubeNamespacedKey(ns, r.DeploymentName)
				sk := kubeNamespacedKey(ns, r.ServiceName)
				d := snap.deployments[dk]
				svc := snap.serviceDetails[sk]
				switch name {
				case "scaled up", "scaled down":
					n := 3
					if name == "scaled down" {
						n = 1
					}
					d.Spec.Replicas = &n
					d.Status.Replicas = n
					d.Status.UpdatedReplicas = n
					d.Status.ReadyReplicas = n
					d.Status.AvailableReplicas = n
					snap.deployments[dk] = d
					populateReleaseEndpointFixture(t, &snap, r, n)
				case "other app":
					app.ID = "app-b"
				case "other tenant":
					app.TenantID = "tenant-b"
				case "snapshot runtime":
					r.SpecSnapshot.RuntimeID = "runtime-other"
				case "snapshot image":
					r.SpecSnapshot.Image = "registry.example/other:tag"
				case "old generation":
					d.Status.ObservedGeneration = 2
				case "mixed replicas":
					d.Status.Replicas = 3
				case "wrong image":
					d.Spec.Template.Spec.Containers[0].Image = "registry.example/other:tag"
				case "missing deployment":
					delete(snap.deployments, dk)
				case "missing UID":
					d.Metadata.UID = ""
				case "foreign deployment":
					d.Metadata.Labels[runtime.FugueLabelTenantID] = "tenant-other"
				case "foreign template":
					d.Spec.Template.Metadata.Labels[runtime.FugueLabelAppID] = "app-other"
				case "foreign release":
					d.Spec.Template.Metadata.Labels[runtime.FugueLabelAppReleaseID] = "release-other"
				case "missing service":
					delete(snap.serviceDetails, sk)
				case "wrong selector":
					svc.Spec.Selector["unmatched"] = "another-workload"
				case "wrong port":
					r.UpstreamURL = strings.TrimSuffix(r.UpstreamURL, ":8080") + ":9999"
				case "foreign upstream":
					r.UpstreamURL = "http://other.example:8080"
				case "stale endpoint owner":
					snap.releaseEndpointSlices[sk][0].Metadata.OwnerReferences[0].UID = "obsolete-service-uid"
				case "no endpoints":
					snap.releaseEndpointSlices[sk] = nil
				case "missing endpoint API":
					snap.endpointSlicesAvailable = false
				case "no replicas":
					n := 0
					d.Spec.Replicas = &n
					d.Status.Replicas = 0
					d.Status.UpdatedReplicas = 0
					d.Status.ReadyReplicas = 0
					d.Status.AvailableReplicas = 0
				case "retired":
					r.Status = model.AppReleaseStatusRetired
				}
				if name != "missing deployment" {
					snap.deployments[dk] = d
				}
				if name != "missing service" {
					snap.serviceDetails[sk] = svc
				}
				observed := time.Now().UTC()
				fact, proof := (&Server{}).observeReleaseRuntimeReadiness(app, r, !candidate, snap, observed)
				if !fact.ObservedAt.Equal(observed) || fact.ID != r.ID {
					t.Fatal("evidence identity/time changed")
				}
				if name == "ready" || name == "scaled up" || name == "scaled down" {
					if fact.Status != model.EdgeRouteStatusActive || !proof.Ready || app.ObservedStatus != nil {
						t.Fatal("ready workload required serving evidence", fact, proof)
					}
				} else if fact.Status == model.EdgeRouteStatusActive || proof.Ready || proof.Reason == "" {
					t.Fatal("invalid workload declared ready", name, fact, proof)
				}
			})
		}
	}
}

func TestCaptureReleaseRuntimeReadinessAllowsCanaryBeforeServing(t *testing.T) {
	app, stable, stableSnap := releaseReadinessFixture(t, false)
	_, candidate, candidateSnap := releaseReadinessFixture(t, true)
	deployments := []any{}
	services := []any{}
	slices := []any{}
	pods := []any{}
	replicas := []any{}
	for _, snapshot := range []managedAppKubeSnapshot{stableSnap, candidateSnap} {
		for _, d := range snapshot.deployments {
			deployments = append(deployments, d)
		}
		for _, s := range snapshot.serviceDetails {
			services = append(services, s)
		}
		for _, items := range snapshot.releaseEndpointSlices {
			for _, slice := range items {
				slices = append(slices, slice)
			}
		}
		for _, pod := range snapshot.releasePods {
			pods = append(pods, pod)
		}
		for _, rs := range snapshot.releaseReplicaSets {
			replicas = append(replicas, rs)
		}
	}
	for _, scenario := range []string{"ready", "cluster changed", "snapshot denied", "pods denied", "replicas denied"} {
		t.Run(scenario, func(t *testing.T) {
			clusterReads := 0
			kube := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var data any
				switch r.URL.Path {
				case "/api/v1/namespaces/kube-system":
					clusterReads++
					id := "cluster-a"
					if scenario == "cluster changed" && clusterReads > 1 {
						id = "cluster-b"
					}
					data = map[string]any{"metadata": map[string]string{"uid": id}}
				case "/apis/" + runtime.ManagedAppAPIGroup + "/v1alpha1/" + runtime.ManagedAppPlural:
					managed := runtime.BuildManagedAppObject(app, runtime.SchedulingConstraints{})
					managed["metadata"].(map[string]any)["generation"] = int64(1)
					managed["status"] = map[string]any{"phase": runtime.ManagedAppPhaseReady, "desiredReplicas": 2, "readyReplicas": 2, "observedGeneration": 1}
					data = map[string]any{"items": []any{managed}}
				case "/api/v1/namespaces":
					data = map[string]any{"items": []any{map[string]any{"metadata": map[string]string{"name": runtime.NamespaceForTenant(app.TenantID)}}}}
				case "/apis/apps/v1/deployments":
					if scenario == "snapshot denied" {
						http.Error(w, "denied", http.StatusForbidden)
						return
					}
					data = map[string]any{"items": deployments}
				case "/api/v1/services":
					data = map[string]any{"items": services}
				case "/api/v1/pods":
					if scenario == "pods denied" {
						http.Error(w, "denied", http.StatusForbidden)
						return
					}
					data = map[string]any{"items": pods}
				case "/apis/apps/v1/replicasets":
					if scenario == "replicas denied" {
						http.Error(w, "denied", http.StatusForbidden)
						return
					}
					data = map[string]any{"items": replicas}
				case "/api/v1/endpoints":
					data = map[string]any{"items": []any{}}
				case "/apis/discovery.k8s.io/v1/endpointslices":
					data = map[string]any{"items": slices}
				default:
					http.NotFound(w, r)
					return
				}
				json.NewEncoder(w).Encode(data)
			}))
			defer kube.Close()
			server := &Server{newManagedAppStatusClient: func() (*managedAppStatusClient, error) {
				return &managedAppStatusClient{client: kube.Client(), baseURL: kube.URL}, nil
			}}
			traffic := model.AppTrafficPolicy{ID: "traffic", AppID: app.ID, TenantID: app.TenantID, Mode: model.AppTrafficModeCanary, StableReleaseID: stable.ID, CandidateReleaseID: candidate.ID, StableWeight: 80, CandidateWeight: 20, StickyCookie: "Fugue-Release-Stickiness"}
			projection := platformIntentProjectionResponse{Intent: platformconfig.PlatformIntent{Generation: "intent", Scope: "global", Routes: []platformconfig.RouteIntent{{Hostname: "app.example.test", AppID: app.ID, TenantID: app.TenantID, Enabled: true, UpstreamURL: stable.UpstreamURL}}}, RuntimeSnapshot: platformconfig.RuntimeSnapshot{Releases: []platformconfig.ReleaseObservation{{ID: stable.ID}, {ID: candidate.ID}}}}
			policy, err := platformconfig.ProjectPolicySnapshot(platformconfig.PolicySnapshot{Scope: "global", MinimumHealthyEdges: 1, MaxStaleSeconds: 120}, nil, []model.AppTrafficPolicy{traffic}, "policy")
			if err != nil {
				t.Fatal(err)
			}
			projection.Policy = policy
			before, _ := json.Marshal(projection)
			err = server.captureReleaseRuntimeReadiness(context.Background(), &projection, store.RouteBusinessSnapshot{Apps: []model.App{app}, Releases: []model.AppRelease{stable, candidate}, TrafficPolicies: []model.AppTrafficPolicy{traffic}})
			if scenario != "ready" {
				after, _ := json.Marshal(projection)
				if err == nil || !reflect.DeepEqual(before, after) {
					t.Fatal("failed observation changed compiler input", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, r := range projection.RuntimeSnapshot.Releases {
				if r.Status != model.EdgeRouteStatusActive || r.ObservedAt.IsZero() {
					t.Fatal("independent release not ready", r)
				}
			}
			beforeInput, _ := json.Marshal(projection)
			compiled, err := platformconfig.Compile(platformconfig.CompileRequest{Intent: projection.Intent, Policy: projection.Policy, RuntimeSnapshot: projection.RuntimeSnapshot})
			if err != nil {
				t.Fatal(err)
			}
			raw, _ := json.Marshal(compiled.RouteArtifact.Content)
			var payload struct {
				Routes []platformconfig.CompiledRoute `json:"routes"`
			}
			json.Unmarshal(raw, &payload)
			if len(payload.Routes) != 1 || len(payload.Routes[0].Upstreams) != 2 || payload.Routes[0].Upstreams[0].Weight != 80 || payload.Routes[0].Upstreams[1].Weight != 20 {
				t.Fatal("compiler lost canary weights", string(raw))
			}
			afterInput, _ := json.Marshal(projection)
			if !reflect.DeepEqual(beforeInput, afterInput) {
				t.Fatal("compile mutated captured facts")
			}
			// Exercise the full business capture with a healthy stable workload
			// while desired code already points to the independent candidate.
			for _, advanceDesired := range []bool{false, true} {
				desired := app
				desired.Status = model.AppStatus{Phase: "deployed", CurrentReplicas: 2}
				if advanceDesired {
					desired.Spec.Image = candidate.ResolvedImageRef
				}
				path := t.TempDir() + "/state.json"
				state := store.New(path)
				if err := state.Init(); err != nil {
					t.Fatal(err)
				}
				raw, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				var persistent model.State
				if err = json.Unmarshal(raw, &persistent); err != nil {
					t.Fatal(err)
				}
				persistent.Apps = []model.App{desired}
				persistent.AppReleases = []model.AppRelease{stable, candidate}
				persistent.AppTrafficPolicies = []model.AppTrafficPolicy{traffic}
				persistent.Runtimes = []model.Runtime{{ID: model.DefaultManagedRuntimeID, Type: model.RuntimeTypeManagedShared}}
				raw, err = json.Marshal(persistent)
				if err != nil {
					t.Fatal(err)
				}
				if err = os.WriteFile(path, raw, 0600); err != nil {
					t.Fatal(err)
				}
				fullServer := NewServer(state, auth.New(state, ""), nil, ServerConfig{AppBaseDomain: "example.test"})
				fullServer.newManagedAppStatusClient = server.newManagedAppStatusClient
				observed := fullServer.overlayManagedAppStatuses(context.Background(), []model.App{desired})
				if observed[0].ObservedStatus == nil || observed[0].ObservedStatus.ServingReleaseID != "" {
					t.Fatal("canary must not claim a single serving release")
				}

				dnsPolicy, consumers, nodes := pinnedDNSFixture()
				dnsPolicy.DNSPlacementMode = platformconfig.DNSPlacementConsumerReadiness
				dnsPolicy.DNSQueryPolicy = &platformconfig.DNSQueryPolicy{RankingMode: "disabled", PreferenceMode: "runtime_locality", MinimumTTLSeconds: 60, MaximumTTLSeconds: 120}
				if _, _, err := state.CreateEdgeNodeToken(model.EdgeNode{ID: "edge-a", EdgeGroupID: "edge-group-a", PublicIPv4: "8.8.8.8"}); err != nil {
					t.Fatal(err)
				}
				if _, err := state.UpdateDNSHeartbeat(nodes[0]); err != nil {
					t.Fatal(err)
				}
				fullServer.clusterNodeInventoryCache = newExpiringResponseCache[[]clusterNodeSnapshot](time.Hour)
				fullServer.clusterNodeInventoryCache.set(clusterNodeInventoryCacheKey, []clusterNodeSnapshot{
					{node: model.ClusterNode{Name: "edge-a"}, labels: map[string]string{runtime.EdgeRoleLabelKey: runtime.NodeRoleLabelValue}},
					{node: model.ClusterNode{Name: "dns-a"}, labels: map[string]string{runtime.DNSRoleLabelKey: runtime.NodeRoleLabelValue}},
				})
				static := platformproducer.StaticIntentInput{Consumers: consumers, ApplicationDomains: &platformconfig.ApplicationDomainsIntent{AppBaseDomain: "example.test", DefaultDNSTTL: 60, ReservedHostnames: []string{}}}
				draft, err := fullServer.capturePlatformIntentWithInputs(context.Background(), platformProducerPrincipal(), static, &dnsPolicy, nil)
				if err != nil {
					t.Fatal("full capture", err)
				}
				if len(draft.Intent.Routes) != 1 {
					t.Fatalf("expected one route, got %d", len(draft.Intent.Routes))
				}
				result, err := platformconfig.Compile(platformconfig.CompileRequest{Intent: draft.Intent, Policy: draft.Policy, RuntimeSnapshot: draft.RuntimeSnapshot})
				if err != nil {
					t.Fatal("captured canary", advanceDesired, err)
				}
				raw, _ = json.Marshal(result.RouteArtifact.Content)
				var routes struct {
					Routes []platformconfig.CompiledRoute `json:"routes"`
				}
				json.Unmarshal(raw, &routes)
				if len(routes.Routes[0].Upstreams) != 2 || !routes.Routes[0].Enabled || (routes.Routes[0].Status != "" && routes.Routes[0].Status != model.EdgeRouteStatusActive) {
					t.Fatal("app-level observation blocked independently ready releases", advanceDesired, string(raw))
				}
			}
			proof, ok := projection.RuntimeSnapshot.Facts["release_readiness"].(map[string]any)
			if !ok || proof["cluster_id"] != "cluster-a" {
				t.Fatal("source proof absent")
			}
		})
	}
}
