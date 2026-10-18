package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/runtime"
)

func TestReleaseOwnerReadsOnlyReferencedReplicaSets(t *testing.T) {
	for _, scenario := range []string{"ready", "replaced UID", "missing owner", "negative endpoint", "invalid Pod UID", "foreign owner kind", "conflicting owner UIDs"} {
		t.Run(scenario, func(t *testing.T) {
			app, release, original := releaseReadinessFixture(t, false)
			ns := runtime.NamespaceForTenant(app.TenantID)
			key := kubeNamespacedKey(ns, release.ServiceName)
			pods := make([]kubeReleasePodEvidence, 0, len(original.releasePods))
			for _, pod := range original.releasePods {
				pods = append(pods, pod)
			}
			sort.Slice(pods, func(i, j int) bool { return pods[i].Metadata.Name < pods[j].Metadata.Name })
			var rs kubeReleaseReplicaSetEvidence
			for _, value := range original.releaseReplicaSets {
				rs = value
			}
			wantReads := 1
			switch scenario {
			case "replaced UID":
				rs.Metadata.UID = "replacement-uid"
			case "missing owner":
				original.releaseReplicaSets = map[string]kubeReleaseReplicaSetEvidence{}
			case "negative endpoint":
				negative := false
				original.releaseEndpointSlices[key][0].Endpoints[0].Conditions.Ready = &negative
			case "invalid Pod UID":
				for i := range original.releaseEndpointSlices[key][0].Endpoints {
					original.releaseEndpointSlices[key][0].Endpoints[i].TargetRef.UID = "replaced-pod"
				}
				wantReads = 0
			case "foreign owner kind":
				for i := range pods {
					pods[i].Metadata.OwnerReferences = append([]kubeOwnerReferenceEvidence(nil), pods[i].Metadata.OwnerReferences...)
					pods[i].Metadata.OwnerReferences[0].Kind = "StatefulSet"
				}
				wantReads = 0
			case "conflicting owner UIDs":
				pods[1].Metadata.OwnerReferences = append([]kubeOwnerReferenceEvidence(nil), pods[1].Metadata.OwnerReferences...)
				pods[1].Metadata.OwnerReferences[0].UID = "different-owner-uid"
			}
			if scenario != "missing owner" {
				original.releaseReplicaSets[kubeNamespacedKey(ns, rs.Metadata.Name)] = rs
			}
			for _, pod := range pods {
				original.releasePods[kubeNamespacedKey(ns, pod.Metadata.Name)] = pod
			}
			// Unreferenced Pods and historical ReplicaSets must not create reads.
			unrelated := pods[0]
			unrelated.Metadata.Name, unrelated.Metadata.UID = "unrelated-pod", "unrelated-uid"
			unrelated.Metadata.OwnerReferences = append([]kubeOwnerReferenceEvidence(nil), unrelated.Metadata.OwnerReferences...)
			unrelated.Metadata.OwnerReferences[0].Name = "historical-unreferenced"
			pods = append(pods, unrelated)
			var ownerReads, podPages atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer synthetic-token" {
					t.Error("credentials changed")
				}
				if r.URL.Path == "/api/v1/pods" {
					podPages.Add(1)
					if r.URL.Query().Get("continue") == "" {
						json.NewEncoder(w).Encode(map[string]any{"items": pods[:1], "metadata": map[string]string{"continue": "page-two"}})
					} else {
						if r.URL.Query().Get("continue") != "page-two" {
							t.Error("pagination token changed")
						}
						json.NewEncoder(w).Encode(map[string]any{"items": pods[1:]})
					}
					return
				}
				if r.URL.Path != "/apis/apps/v1/namespaces/"+ns+"/replicasets/"+rs.Metadata.Name {
					t.Errorf("unrelated inventory read: %s", r.URL.Path)
					http.NotFound(w, r)
					return
				}
				ownerReads.Add(1)
				if scenario == "missing owner" {
					http.NotFound(w, r)
					return
				}
				json.NewEncoder(w).Encode(rs)
			}))
			defer server.Close()
			client := &managedAppStatusClient{baseURL: server.URL, client: server.Client(), bearerToken: "synthetic-token"}
			got := original
			got.releasePods, got.releaseReplicaSets = nil, nil
			if err := client.captureReleaseEndpointOwners(context.Background(), &got); err != nil {
				t.Fatal(err)
			}
			if ownerReads.Load() != int64(wantReads) || podPages.Load() != 2 {
				t.Fatal("ownership reads not deduplicated/paginated", ownerReads.Load(), podPages.Load())
			}
			now := time.Now().UTC()
			wantFact, wantProof := (&Server{}).observeReleaseRuntimeReadiness(app, release, true, original, now)
			gotFact, gotProof := (&Server{}).observeReleaseRuntimeReadiness(app, release, true, got, now)
			if !reflect.DeepEqual(wantFact, gotFact) || !reflect.DeepEqual(wantProof, gotProof) {
				t.Fatalf("targeted reads changed readiness: want=%+v got=%+v", wantProof, gotProof)
			}
			if scenario == "ready" && !gotProof.Ready || scenario != "ready" && gotProof.Ready {
				t.Fatal("fixture readiness incorrect", gotProof)
			}
		})
	}
}

func TestReleaseOwnerReadFailuresDoNotPublishPartialSnapshot(t *testing.T) {
	for _, scenario := range []string{"forbidden", "server failure", "method unsupported", "wrong namespace", "wrong name", "malformed", "trailing", "cancelled"} {
		t.Run(scenario, func(t *testing.T) {
			_, _, snapshot := releaseReadinessFixture(t, false)
			beforePods, _ := json.Marshal(snapshot.releasePods)
			beforeOwners, _ := json.Marshal(snapshot.releaseReplicaSets)
			var pods []kubeReleasePodEvidence
			for _, pod := range snapshot.releasePods {
				pods = append(pods, pod)
			}
			var rs kubeReleaseReplicaSetEvidence
			for _, value := range snapshot.releaseReplicaSets {
				rs = value
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/api/v1/pods" {
					json.NewEncoder(w).Encode(map[string]any{"items": pods})
					return
				}
				switch scenario {
				case "forbidden":
					http.Error(w, "denied", 403)
					return
				case "server failure":
					http.Error(w, "failed", 500)
					return
				case "method unsupported":
					http.Error(w, "unsupported", 405)
					return
				case "wrong namespace":
					rs.Metadata.Namespace = "foreign-namespace"
				case "wrong name":
					rs.Metadata.Name = "another-owner"
				case "malformed":
					fmt.Fprint(w, `{"metadata":`)
					return
				case "cancelled":
					cancel()
					<-r.Context().Done()
					return
				}
				json.NewEncoder(w).Encode(rs)
				if scenario == "trailing" {
					fmt.Fprint(w, "{}")
				}
			}))
			defer server.Close()
			client := &managedAppStatusClient{baseURL: server.URL, client: server.Client()}
			if err := client.captureReleaseEndpointOwners(ctx, &snapshot); err == nil {
				t.Fatal("incomplete owner read accepted")
			}
			afterPods, _ := json.Marshal(snapshot.releasePods)
			afterOwners, _ := json.Marshal(snapshot.releaseReplicaSets)
			if string(beforePods) != string(afterPods) || string(beforeOwners) != string(afterOwners) {
				t.Fatal("failed read changed prior snapshot")
			}
		})
	}
}

func TestReleaseOwnerReadsBoundConcurrentRequests(t *testing.T) {
	_, _, snapshot := releaseReadinessFixture(t, false)
	var seed kubeReleasePodEvidence
	for _, pod := range snapshot.releasePods {
		seed = pod
		break
	}
	var pods []kubeReleasePodEvidence
	slice := kubeEndpointSliceEvidence{}
	slice.Metadata.Namespace = seed.Metadata.Namespace
	for i := range 12 {
		pod := seed
		pod.Metadata.Name, pod.Metadata.UID = fmt.Sprintf("pod-%d", i), fmt.Sprintf("uid-%d", i)
		pod.Metadata.OwnerReferences = append([]kubeOwnerReferenceEvidence(nil), seed.Metadata.OwnerReferences...)
		pod.Metadata.OwnerReferences[0].Name = fmt.Sprintf("owner-%d", i)
		pods = append(pods, pod)
		endpoint := kubeReleaseEndpointEvidence{}
		endpoint.TargetRef.Kind, endpoint.TargetRef.Namespace, endpoint.TargetRef.Name, endpoint.TargetRef.UID = "Pod", pod.Metadata.Namespace, pod.Metadata.Name, pod.Metadata.UID
		slice.Endpoints = append(slice.Endpoints, endpoint)
	}
	snapshot.releaseEndpointSlices = map[string][]kubeEndpointSliceEvidence{"service": {slice}}
	var active, maximum, count atomic.Int64
	gate := make(chan struct{})
	var open sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/pods" {
			json.NewEncoder(w).Encode(map[string]any{"items": pods})
			return
		}
		n := active.Add(1)
		defer active.Add(-1)
		for old := maximum.Load(); n > old && !maximum.CompareAndSwap(old, n); old = maximum.Load() {
		}
		if count.Add(1) == 4 {
			open.Do(func() { close(gate) })
		}
		select {
		case <-gate:
		case <-r.Context().Done():
			return
		}
		var rs kubeReleaseReplicaSetEvidence
		rs.Metadata.Namespace = seed.Metadata.Namespace
		rs.Metadata.Name = r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
		json.NewEncoder(w).Encode(rs)
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client := &managedAppStatusClient{baseURL: server.URL, client: server.Client()}
	if err := client.captureReleaseEndpointOwners(ctx, &snapshot); err != nil {
		t.Fatal(err)
	}
	if count.Load() != 12 || maximum.Load() != 4 || len(snapshot.releaseReplicaSets) != 12 {
		t.Fatal("owner request concurrency or coverage changed", count.Load(), maximum.Load())
	}
}

func BenchmarkReleaseOwnerCapture(b *testing.B) {
	const total, referenced = 1024, 64
	owners := make(map[string]string, total)
	var allOwners, pods []json.RawMessage
	slice := kubeEndpointSliceEvidence{}
	slice.Metadata.Namespace = "tenant-example"
	for i := range total {
		name := fmt.Sprintf("revision-%d", i)
		raw, _ := json.Marshal(map[string]any{
			"metadata": map[string]any{"namespace": "tenant-example", "name": name, "uid": name + "-uid"},
			"spec": map[string]any{"template": map[string]any{
				"metadata": map[string]any{"labels": map[string]string{"app": "example"}},
				"spec":     map[string]any{"syntheticIgnoredContent": strings.Repeat("x", 8192)},
			}},
		})
		owners["/apis/apps/v1/namespaces/tenant-example/replicasets/"+name] = string(raw)
		allOwners = append(allOwners, raw)
		if i >= referenced {
			continue
		}
		podName, uid := fmt.Sprintf("pod-%d", i), fmt.Sprintf("pod-uid-%d", i)
		pod, _ := json.Marshal(map[string]any{"metadata": map[string]any{
			"namespace": "tenant-example", "name": podName, "uid": uid,
			"ownerReferences": []map[string]any{{"apiVersion": "apps/v1", "kind": "ReplicaSet", "name": name, "uid": name + "-uid", "controller": true}},
		}})
		pods = append(pods, pod)
		endpoint := kubeReleaseEndpointEvidence{}
		endpoint.TargetRef.Kind, endpoint.TargetRef.Namespace, endpoint.TargetRef.Name, endpoint.TargetRef.UID = "Pod", "tenant-example", podName, uid
		slice.Endpoints = append(slice.Endpoints, endpoint)
	}
	ownerList, _ := json.Marshal(map[string]any{"items": allOwners})
	podList, _ := json.Marshal(map[string]any{"items": pods})
	var bytesRead, requests atomic.Int64
	client := &managedAppStatusClient{baseURL: "https://cluster.example.test", client: &http.Client{Transport: observationRoundTripper(func(r *http.Request) (*http.Response, error) {
		var raw string
		switch r.URL.Path {
		case "/api/v1/pods":
			raw = string(podList)
		case "/apis/apps/v1/replicasets":
			raw = string(ownerList)
		default:
			raw = owners[r.URL.Path]
		}
		if raw == "" {
			return nil, fmt.Errorf("unexpected request %s", r.URL.Path)
		}
		requests.Add(1)
		bytesRead.Add(int64(len(raw)))
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(raw))}, nil
	})}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		snapshot := managedAppKubeSnapshot{releaseEndpointSlices: map[string][]kubeEndpointSliceEvidence{"service": {slice}}}
		if err := client.captureReleaseEndpointOwners(context.Background(), &snapshot); err != nil {
			b.Fatal(err)
		}
		for j := range referenced {
			if _, found := snapshot.releaseReplicaSets[fmt.Sprintf("tenant-example/revision-%d", j)]; !found {
				b.Fatal("referenced owner missing")
			}
		}
	}
	b.ReportMetric(float64(bytesRead.Load())/float64(b.N), "decoded-B/op")
	b.ReportMetric(float64(requests.Load())/float64(b.N), "requests/op")
}
