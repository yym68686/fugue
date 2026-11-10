package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/store"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func pendingResourceObservationFixture(t *testing.T) (map[string]any, store.PendingRootDatabaseResources) {
	t.Helper()
	meta := func(name, uid string) metav1.ObjectMeta {
		return metav1.ObjectMeta{Name: name, Namespace: "control-neutral", UID: types.UID(uid), ResourceVersion: "1"}
	}
	cluster := pendingResourceCluster{Metadata: meta("cluster-neutral", "cluster-uid")}
	cluster.Status.CurrentPrimary = "primary-neutral"
	cluster.Status.TargetPrimary = "primary-neutral"
	cluster.Status.Instances = 1
	cluster.Status.ReadyInstances = 1
	cluster.Status.Phase = "Cluster in healthy state"
	controller := true
	pod := corev1.Pod{ObjectMeta: meta("primary-neutral", "pod-uid")}
	pod.OwnerReferences = []metav1.OwnerReference{{APIVersion: "postgresql.cnpg.io/v1", Kind: "Cluster", Name: "cluster-neutral", UID: "cluster-uid", Controller: &controller}}
	pod.Spec.NodeName = "node-neutral"
	pod.Spec.Containers = []corev1.Container{{Name: "postgres", Env: []corev1.EnvVar{{Name: "PGDATA", Value: "/data/pgdata"}}, VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: "/data"}}}}
	pod.Spec.Volumes = []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "claim-neutral"}}}}
	pod.Status.Phase = corev1.PodRunning
	pod.Status.PodIP = "192.0.2.23"
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "postgres", Ready: true, ContainerID: "container-neutral", ImageID: "image-neutral", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(time.Now().Add(-time.Hour))}}}}
	pvc := corev1.PersistentVolumeClaim{ObjectMeta: meta("claim-neutral", "pvc-uid")}
	pvc.Spec.VolumeName = "pv-neutral"
	pvc.Spec.Resources.Requests = corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")}
	pvc.Status.Phase = corev1.ClaimBound
	pvc.Status.Capacity = corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")}
	pv := corev1.PersistentVolume{ObjectMeta: meta("pv-neutral", "pv-uid")}
	pv.Spec.Capacity = pvc.Status.Capacity
	pv.Spec.ClaimRef = &corev1.ObjectReference{Name: pvc.Name, Namespace: pvc.Namespace, UID: pvc.UID}
	pv.Spec.CSI = &corev1.CSIPersistentVolumeSource{Driver: "driver.longhorn.io", VolumeHandle: "volume-neutral"}
	pv.Status.Phase = corev1.VolumeBound
	lh := pendingResourceLonghornVolume{Metadata: meta("volume-neutral", "volume-uid")}
	lh.Metadata.Namespace = "storage-neutral"
	lh.Spec.Size = "10737418240"
	lh.Spec.NodeID = "node-neutral"
	lh.Status.CurrentNodeID = "node-neutral"
	lh.Status.State = "attached"
	lh.Status.Robustness = "healthy"
	node := corev1.Node{ObjectMeta: meta("node-neutral", "node-uid")}
	node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}
	stats := map[string]any{"pods": []any{map[string]any{"podRef": map[string]any{"name": pod.Name, "namespace": pod.Namespace, "uid": string(pod.UID)}, "volume": []any{map[string]any{"time": time.Now().UTC(), "capacityBytes": uint64(10 << 30), "availableBytes": uint64(5 << 30), "usedBytes": uint64(5 << 30), "pvcRef": map[string]any{"name": pvc.Name, "namespace": pvc.Namespace}}}}}}
	objects := map[string]any{
		"/apis/postgresql.cnpg.io/v1/namespaces/control-neutral/clusters/cluster-neutral": cluster,
		"/api/v1/namespaces/control-neutral/pods/primary-neutral":                         pod,
		"/api/v1/namespaces/control-neutral/persistentvolumeclaims/claim-neutral":         pvc,
		"/api/v1/persistentvolumes/pv-neutral":                                            pv,
		"/apis/longhorn.io/v1beta2/namespaces/storage-neutral/volumes/volume-neutral":     lh,
		"/api/v1/nodes/node-neutral":                                                      node,
		"/api/v1/nodes/node-neutral/proxy/stats/summary":                                  stats,
	}
	db := store.PendingRootDatabaseResources{ObservedAt: time.Now().UTC(), Database: "database-neutral", DatabaseOID: 42, Schema: "public", ServerAddress: pod.Status.PodIP, ServerPort: 5432, PostmasterStarted: time.Now().Add(-time.Hour), DataDirectory: "/data/pgdata", ReadOnly: true, TempFilesForbidden: true, LedgerRelations: []store.PendingRootResourceRelation{}}
	return objects, db
}

func TestPendingResourceObservationRejectsSourceDriftAndNeverGrantsBudget(t *testing.T) {
	for _, variant := range []string{"stable", "ordinary-role", "wrong-db-endpoint", "pod-replaced", "pv-rebound", "node-replaced", "stale-stats", "wrong-stats-uid", "db-restarted", "ledger-rewritten", "oversized-response", "duplicate-stats"} {
		t.Run(variant, func(t *testing.T) {
			objects, db := pendingResourceObservationFixture(t)
			reads := map[string]int{}
			var mutations atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.Header.Get("Authorization") != "Bearer private-neutral" {
					mutations.Add(1)
					http.Error(w, "denied", 403)
					return
				}
				reads[r.URL.Path]++
				value, ok := objects[r.URL.Path]
				if !ok {
					http.NotFound(w, r)
					return
				}
				raw, _ := json.Marshal(value)
				var object map[string]any
				_ = json.Unmarshal(raw, &object)
				if reads[r.URL.Path] == 2 {
					if (variant == "pod-replaced" && strings.Contains(r.URL.Path, "/pods/")) || (variant == "node-replaced" && r.URL.Path == "/api/v1/nodes/node-neutral") {
						object["metadata"].(map[string]any)["uid"] = "replacement-uid"
					}
					if variant == "pv-rebound" && r.URL.Path == "/api/v1/persistentvolumes/pv-neutral" {
						object["spec"].(map[string]any)["claimRef"].(map[string]any)["uid"] = "other-claim-uid"
					}
				}
				if strings.HasSuffix(r.URL.Path, "/stats/summary") {
					pod := object["pods"].([]any)[0].(map[string]any)
					volumes := pod["volume"].([]any)
					if variant == "stale-stats" {
						volumes[0].(map[string]any)["time"] = time.Now().Add(-time.Minute)
					}
					if variant == "wrong-stats-uid" {
						pod["podRef"].(map[string]any)["uid"] = "old-pod-uid"
					}
					if variant == "duplicate-stats" {
						pod["volume"] = append(volumes, volumes[0])
					}
					if variant == "oversized-response" {
						_, _ = w.Write([]byte(strings.Repeat("x", (2<<20)+1)))
						return
					}
				}
				_ = json.NewEncoder(w).Encode(object)
			}))
			defer server.Close()
			client := &kubeLogsClient{client: server.Client(), baseURL: server.URL, bearerToken: "private-neutral"}
			calls := 0
			reader := func(context.Context) (store.PendingRootDatabaseResources, error) {
				calls++
				copy := db
				if variant == "ordinary-role" {
					copy.DataDirectory = ""
				}
				if variant == "wrong-db-endpoint" {
					copy.ServerAddress = "192.0.2.99"
				}
				if calls == 2 && variant == "db-restarted" {
					copy.PostmasterStarted = time.Now()
				}
				if calls == 2 && variant == "ledger-rewritten" {
					copy.LedgerRelations = []store.PendingRootResourceRelation{{Name: "ledger-neutral", OID: 7, Filenode: 8}}
				}
				return copy, nil
			}
			result, err := observePendingResources(t.Context(), client, "control-neutral", "cluster-neutral", "storage-neutral", reader)
			if mutations.Load() != 0 {
				t.Fatal("non-read request sent")
			}
			if variant != "stable" && variant != "ordinary-role" {
				if err == nil || result != nil {
					t.Fatal("drift or untrusted sample accepted", result, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(result)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(raw), "/data/pgdata") || strings.Contains(string(raw), "private-neutral") {
				t.Fatal("private source details leaked")
			}
			v := result.(map[string]any)
			if v["admission_allowed"] != false || v["whole_operation_budget_proven"] != false || v["whole_operation_bounds"] != nil || v["observation_only"] != true {
				t.Fatal("sample became budget", v)
			}
			storage := v["storage"].(pendingResourceStorage)
			if variant == "ordinary-role" && storage.DataDirectorySource != "cnpg-pod-pgdata" {
				t.Fatal("configuration provenance omitted")
			}
			if storage.Identity.DatabaseOID != 42 || storage.Identity.PVCUID != "pvc-uid" || storage.Identity.PGPodUID != "pod-uid" {
				t.Fatal("identity omitted", storage)
			}
			if len(reads) != 7 || calls != 2 {
				t.Fatal("unexpected source coverage", reads, calls)
			}
		})
	}
}

func TestPendingResourceObservationHonorsDeadlineAndSingleFlight(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer server.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	err := pendingResourceGet(ctx, &kubeLogsClient{client: server.Client(), baseURL: server.URL}, "/exact-object", &map[string]any{})
	if err == nil || ctx.Err() != context.DeadlineExceeded {
		t.Fatal("read escaped deadline", err, ctx.Err())
	}
	s := &Server{}
	s.pendingResourceObservationMu.Lock()
	defer s.pendingResourceObservationMu.Unlock()
	if v, err := s.PendingResourceObservation(t.Context()); v != nil || err == nil {
		t.Fatal("concurrent observation admitted")
	}
}
