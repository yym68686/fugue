package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

func TestReleaseEndpointReadinessValidatesPodOwnershipAndCoverage(t *testing.T) {
	cases := []string{"ready", "dual stack", "duplicate slices", "missing workload selector", "missing target UID", "replaced Pod", "foreign namespace", "non Pod target", "foreign Pod labels", "foreign workload label", "non controller Pod owner", "ambiguous Pod owner", "replaced ReplicaSet", "foreign Deployment", "non controller Deployment owner", "old Pod release key", "old ReplicaSet release key", "wrong Pod image", "wrong IP", "invalid IP", "duplicate address owners", "Pod unready", "Pod terminated", "Pod deleting", "endpoint terminating", "endpoint not serving", "endpoint unready", "endpoint unknown", "stale slice owner", "extra foreign slice", "slice deleting", "one dual stack Pod", "duplicate single Pod", "bound Deployment replaced", "bound Service replaced", "bound generation regressed", "bound newer generation"}
	for _, candidate := range []bool{false, true} {
		for _, name := range cases {
			t.Run(map[bool]string{false: "stable", true: "candidate"}[candidate]+"/"+name, func(t *testing.T) {
				app, release, snapshot := releaseReadinessFixture(t, candidate)
				ns := runtime.NamespaceForTenant(app.TenantID)
				sk := kubeNamespacedKey(ns, release.ServiceName)
				slice := snapshot.releaseEndpointSlices[sk][0]
				ep := slice.Endpoints[0]
				pk := kubeNamespacedKey(ns, ep.TargetRef.Name)
				pod := snapshot.releasePods[pk]
				pod.Spec.Containers = slices.Clone(pod.Spec.Containers)
				rk := kubeNamespacedKey(ns, pod.Metadata.OwnerReferences[0].Name)
				rs := snapshot.releaseReplicaSets[rk]
				yes, no := true, false
				switch name {
				case "dual stack", "one dual stack Pod":
					pod.Status.PodIPs = []struct {
						IP string `json:"ip"`
					}{{IP: pod.Status.PodIP}, {IP: "2001:db8::10"}}
					ep.Addresses = append(ep.Addresses, "2001:db8::10")
					if name == "one dual stack Pod" {
						slice.Endpoints = slice.Endpoints[:1]
					}
				case "duplicate slices", "duplicate single Pod":
					if name == "duplicate single Pod" {
						slice.Endpoints = slice.Endpoints[:1]
					}
					other := slice
					other.Metadata.Name += "-copy"
					other.Metadata.UID += "-copy"
					snapshot.releaseEndpointSlices[sk] = append(snapshot.releaseEndpointSlices[sk], other)
				case "missing workload selector":
					svc := snapshot.serviceDetails[sk]
					delete(svc.Spec.Selector, runtime.FugueLabelAppWorkload)
					snapshot.serviceDetails[sk] = svc
				case "missing target UID":
					ep.TargetRef.UID = ""
				case "replaced Pod":
					pod.Metadata.UID = "new-pod-uid"
				case "foreign namespace":
					ep.TargetRef.Namespace = "other-tenant"
				case "non Pod target":
					ep.TargetRef.Kind = "Node"
				case "foreign Pod labels":
					pod.Metadata.Labels[runtime.FugueLabelAppReleaseID] = "another-release"
				case "foreign workload label":
					pod.Metadata.Labels[runtime.FugueLabelAppWorkload] = "other-workload"
				case "non controller Pod owner":
					pod.Metadata.OwnerReferences[0].Controller = &no
				case "ambiguous Pod owner":
					pod.Metadata.OwnerReferences = append(pod.Metadata.OwnerReferences, pod.Metadata.OwnerReferences[0])
				case "replaced ReplicaSet":
					rs.Metadata.UID = "new-replica-set-uid"
				case "foreign Deployment":
					rs.Metadata.OwnerReferences[0].UID = "different-deployment-uid"
				case "non controller Deployment owner":
					rs.Metadata.OwnerReferences[0].Controller = &no
				case "old Pod release key":
					pod.Metadata.Annotations.ReleaseKey = "old"
				case "old ReplicaSet release key":
					rs.Spec.Template.Metadata.Annotations.ReleaseKey = "old"
				case "wrong Pod image":
					pod.Spec.Containers[0].Image = "registry.example/other:old"
				case "wrong IP":
					ep.Addresses = []string{"192.0.2.99"}
				case "invalid IP":
					ep.Addresses = []string{"not-an-address"}
				case "duplicate address owners":
					secondKey := kubeNamespacedKey(ns, slice.Endpoints[1].TargetRef.Name)
					second := snapshot.releasePods[secondKey]
					second.Status.PodIP = pod.Status.PodIP
					snapshot.releasePods[secondKey] = second
					slice.Endpoints[1].Addresses = slices.Clone(ep.Addresses)
				case "Pod unready":
					pod.Status.Conditions[0].Status = "False"
				case "Pod terminated":
					pod.Status.Phase = "Succeeded"
				case "Pod deleting":
					pod.Metadata.DeletionTimestamp = time.Now().UTC().Format(time.RFC3339)
				case "endpoint terminating":
					ep.Conditions.Terminating = &yes
				case "endpoint not serving":
					ep.Conditions.Serving = &no
				case "endpoint unready":
					ep.Conditions.Ready = &no
				case "endpoint unknown":
					ep.Conditions.Ready = nil
				case "stale slice owner":
					slice.Metadata.OwnerReferences[0].UID = "old-service-uid"
				case "extra foreign slice":
					other := kubeEndpointSliceEvidence{}
					other.Metadata.Name, other.Metadata.Namespace, other.Metadata.UID = "foreign", ns, "foreign"
					other.Endpoints = slices.Clone(slice.Endpoints)
					snapshot.releaseEndpointSlices[sk] = append(snapshot.releaseEndpointSlices[sk], other)
				case "slice deleting":
					slice.Metadata.DeletionTimestamp = time.Now().UTC().Format(time.RFC3339)
				case "bound Deployment replaced", "bound Service replaced", "bound generation regressed", "bound newer generation":
					d := snapshot.deployments[kubeNamespacedKey(ns, release.DeploymentName)]
					svc := snapshot.serviceDetails[sk]
					release.RevisionWorkload = &model.AppReleaseWorkload{Namespace: ns, DeploymentName: release.DeploymentName, DeploymentUID: d.Metadata.UID, DeploymentGeneration: d.Metadata.Generation, ServiceName: release.ServiceName, ServiceUID: svc.Metadata.UID, ReleaseKey: d.Spec.Template.Metadata.Annotations.ReleaseKey, RuntimeID: release.RuntimeID, ImageRef: release.ResolvedImageRef}
					if name == "bound Deployment replaced" {
						release.RevisionWorkload.DeploymentUID = "old-deployment"
					}
					if name == "bound Service replaced" {
						release.RevisionWorkload.ServiceUID = "old-service"
					}
					if name == "bound generation regressed" {
						release.RevisionWorkload.DeploymentGeneration++
					}
					if name == "bound newer generation" {
						release.RevisionWorkload.DeploymentGeneration--
					}
				}
				slice.Endpoints[0] = ep
				snapshot.releaseEndpointSlices[sk][0] = slice
				snapshot.releasePods[pk] = pod
				snapshot.releaseReplicaSets[rk] = rs
				fact, proof := (&Server{}).observeReleaseRuntimeReadiness(app, release, !candidate, snapshot, time.Now().UTC())
				valid := name == "ready" || name == "dual stack" || name == "duplicate slices" || name == "bound newer generation"
				if !valid {
					if proof.Ready || fact.Status == model.EdgeRouteStatusActive || proof.Reason == "" {
						t.Fatalf("unsafe endpoints accepted: %+v", proof)
					}
					return
				}
				if !proof.Ready || fact.Status != model.EdgeRouteStatusActive || proof.ReadyEndpoints != 2 || len(proof.EndpointPods) != 2 {
					t.Fatalf("valid endpoint owners rejected: %+v", proof)
				}
				if proof.EndpointPods[0].PodUID >= proof.EndpointPods[1].PodUID {
					t.Fatal("unstable identity ordering")
				}
				if name == "dual stack" && len(proof.EndpointPods[0].Addresses) != 2 {
					t.Fatal("dual-stack addresses lost")
				}
			})
		}
	}
}

func TestReleaseEndpointReadinessCapturedKubernetesObjects(t *testing.T) {
	path := os.Getenv("FUGUE_RELEASE_ENDPOINT_INPUT")
	if path == "" {
		t.Skip("set FUGUE_RELEASE_ENDPOINT_INPUT to a sanitized read-only capture")
	}
	var input struct {
		App       model.App                  `json:"app"`
		Releases  []model.AppRelease         `json:"releases"`
		ClusterID string                     `json:"cluster_id"`
		Resources map[string]json.RawMessage `json:"resources"`
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &input); err != nil {
		t.Fatal(err)
	}
	kube := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/namespaces/kube-system" {
			_ = json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]string{"uid": input.ClusterID}})
			return
		}
		if data, found := input.Resources[r.URL.Path]; found {
			_, _ = w.Write(data)
			return
		}
		http.NotFound(w, r)
	}))
	defer kube.Close()
	s := &Server{newManagedAppStatusClient: func() (*managedAppStatusClient, error) {
		return &managedAppStatusClient{client: kube.Client(), baseURL: kube.URL}, nil
	}}
	for _, release := range input.Releases {
		projection := platformIntentProjectionResponse{RuntimeSnapshot: platformconfig.RuntimeSnapshot{Releases: []platformconfig.ReleaseObservation{{ID: release.ID}}}}
		business := store.RouteBusinessSnapshot{Apps: []model.App{input.App}, Releases: []model.AppRelease{release}}
		if release.DeploymentName == runtime.RuntimeAppResourceName(input.App) {
			business.TrafficPolicies = []model.AppTrafficPolicy{{AppID: input.App.ID, StableReleaseID: release.ID}}
		}
		if err := s.captureReleaseRuntimeReadiness(context.Background(), &projection, business); err != nil {
			t.Fatal(err)
		}
		if projection.RuntimeSnapshot.Releases[0].Status != model.EdgeRouteStatusActive {
			t.Fatalf("captured workload rejected: %+v", projection.RuntimeSnapshot.Releases[0])
		}
		encoded, _ := json.Marshal(projection.RuntimeSnapshot.Facts["release_readiness"])
		t.Log(string(encoded))
	}
}

func TestStableReadinessRejectsCandidateEndpointsEvenWithForgedLabels(t *testing.T) {
	app, stable, snapshot := releaseReadinessFixture(t, false)
	_, candidate, candidateSnapshot := releaseReadinessFixture(t, true)
	ns := runtime.NamespaceForTenant(app.TenantID)
	stableKey := kubeNamespacedKey(ns, stable.ServiceName)
	candidateSlice := candidateSnapshot.releaseEndpointSlices[kubeNamespacedKey(ns, candidate.ServiceName)][0]
	snapshot.releaseEndpointSlices[stableKey][0].Endpoints = candidateSlice.Endpoints
	stableDeployment := snapshot.deployments[kubeNamespacedKey(ns, stable.DeploymentName)]
	for key, pod := range candidateSnapshot.releasePods {
		// Matching labels cannot substitute for the actual controller UID chain.
		pod.Metadata.Labels = stableDeployment.Spec.Template.Metadata.Labels
		pod.Metadata.Annotations = stableDeployment.Spec.Template.Metadata.Annotations
		snapshot.releasePods[key] = pod
	}
	for key, rs := range candidateSnapshot.releaseReplicaSets {
		rs.Metadata.Labels = stableDeployment.Spec.Template.Metadata.Labels
		rs.Spec.Template.Metadata.Labels = stableDeployment.Spec.Template.Metadata.Labels
		rs.Spec.Template.Metadata.Annotations = stableDeployment.Spec.Template.Metadata.Annotations
		snapshot.releaseReplicaSets[key] = rs
	}
	_, proof := (&Server{}).observeReleaseRuntimeReadiness(app, stable, true, snapshot, time.Now().UTC())
	if proof.Ready || proof.Reason != "release endpoint belongs to another workload or executable revision" {
		t.Fatalf("candidate origin accepted for stable: %+v", proof)
	}
}
