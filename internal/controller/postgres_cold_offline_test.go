package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	runtimepkg "fugue/internal/runtime"
)

// Fixture mutation must retain references; normalizeKubeMap intentionally copies.
func coldTestMap(value any) map[string]any {
	result, _ := value.(map[string]any)
	return result
}

func coldTestClusterPod(name, node string) map[string]any {
	return map[string]any{"metadata": map[string]any{"name": name, "uid": name + "-uid", "labels": map[string]any{"cnpg.io/cluster": "source"}, "ownerReferences": []map[string]any{{"apiVersion": runtimepkg.CloudNativePGAPIVersion, "kind": "Cluster", "name": "source", "uid": "source-uid", "controller": true}}}, "spec": map[string]any{"nodeName": node, "securityContext": map[string]any{"runAsUser": 26, "runAsGroup": 26}, "containers": []map[string]any{{"name": "postgres", "image": "example.test/pg:18", "env": []map[string]any{{"name": "PGDATA", "value": "/data/pgdata"}}, "volumeMounts": []map[string]any{{"name": "data", "mountPath": "/data"}}}}, "volumes": []map[string]any{{"name": "data", "persistentVolumeClaim": map[string]any{"claimName": name}}}}, "status": map[string]any{"phase": "Pending"}}
}

func coldTestDecodePod(t *testing.T, raw map[string]any) kubePod {
	t.Helper()
	b, _ := json.Marshal(raw)
	var pod kubePod
	if err := json.Unmarshal(b, &pod); err != nil {
		t.Fatal(err)
	}
	return pod
}

func TestColdUnstartedPrimaryAndVolumeLayout(t *testing.T) {
	raw := coldTestClusterPod("primary", "")
	pod := coldTestDecodePod(t, raw)
	if !coldPrimaryUnstarted(pod) {
		t.Fatal("unscheduled primary was not eligible for offline copy")
	}
	for _, mutation := range []func(*kubePod){func(p *kubePod) { p.Spec.NodeName = "node" }, func(p *kubePod) { p.Status.Phase = "Running" }, func(p *kubePod) { p.ObservedUID = "" }, func(p *kubePod) { p.Status.ContainerStatuses = []kubeContainerStatus{{Name: "postgres"}} }, func(p *kubePod) { p.Status.InitContainerStatuses = []kubeContainerStatus{{Name: "bootstrap"}} }} {
		changed := pod
		mutation(&changed)
		if coldPrimaryUnstarted(changed) {
			t.Fatal("started/unknown source accepted")
		}
	}
	st := &coldPostgresState{SourcePVC: "primary"}
	if err := coldRecordVolumeLayout(raw, st); err != nil {
		t.Fatal(err)
	}
	if st.SourceDataPath != "/data/pgdata" || st.SourceMountPath != "/data" || st.SourceRunAsUser != 26 || st.SourceRunAsGroup != 26 {
		t.Fatal("layout did not come from original Pod", st)
	}
	for _, mutation := range []func(map[string]any){
		func(spec map[string]any) {
			mapSlice(mapSlice(spec["containers"])[0]["env"])[0]["value"] = "/another/pgdata"
		},
		func(spec map[string]any) {
			mapSlice(mapSlice(spec["containers"])[0]["env"])[0]["value"] = "/data/../pgdata"
		},
		func(spec map[string]any) {
			mapSlice(mapSlice(spec["containers"])[0]["volumeMounts"])[0]["subPath"] = "part"
		},
		func(spec map[string]any) { coldTestMap(spec["securityContext"])["runAsUser"] = 0 },
		func(spec map[string]any) { spec["volumes"] = []any{} },
	} {
		changed := cloneKubeMap(raw)
		mutation(coldTestMap(changed["spec"]))
		if err := coldRecordVolumeLayout(changed, &coldPostgresState{SourcePVC: "primary"}); err == nil {
			t.Fatal("unproven source layout accepted")
		}
	}
}

func TestColdOfflineImageRequiresExactClusterAndBinary(t *testing.T) {
	primary := coldTestDecodePod(t, coldTestClusterPod("primary", ""))
	cluster := kubeCloudNativePGCluster{}
	cluster.Metadata.Name = "source"
	cluster.Metadata.UID = "source-uid"
	for _, scenario := range []string{"matching", "foreign-owner", "different-tag", "no-digest", "conflicting-digests"} {
		t.Run(scenario, func(t *testing.T) {
			peer := coldTestClusterPod("replica", "replica-node")
			peer["status"] = map[string]any{"phase": "Running", "containerStatuses": []map[string]any{{"name": "postgres", "ready": true, "imageID": "example.test/pg@sha256:" + strings.Repeat("a", 64)}}}
			switch scenario {
			case "foreign-owner":
				mapSlice(coldTestMap(peer["metadata"])["ownerReferences"])[0]["uid"] = "unrelated"
			case "different-tag":
				mapSlice(coldTestMap(peer["spec"])["containers"])[0]["image"] = "example.test/pg:19"
			case "no-digest":
				mapSlice(coldTestMap(peer["status"])["containerStatuses"])[0]["imageID"] = "example.test/pg:18"
			}
			items := []map[string]any{peer}
			if scenario == "conflicting-digests" {
				other := cloneKubeMap(peer)
				mapSlice(coldTestMap(other["status"])["containerStatuses"])[0]["imageID"] = "example.test/pg@sha256:" + strings.Repeat("b", 64)
				items = append(items, other)
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "GET" {
					t.Fatal("image witness mutated source")
				}
				json.NewEncoder(w).Encode(map[string]any{"items": items})
			}))
			defer server.Close()
			image, err := coldPeerImageID(context.Background(), &kubeClient{baseURL: server.URL, client: server.Client()}, "ns", cluster, primary)
			if (err == nil) != (scenario == "matching") || (err == nil && !strings.HasSuffix(image, strings.Repeat("a", 64))) {
				t.Fatalf("image=%s err=%v", image, err)
			}
		})
	}
}

func coldTestOfflineState() *coldPostgresState {
	return &coldPostgresState{ServiceID: "service", SourceName: "source", SourceUID: "source-uid", SourcePVC: "primary", SourceNode: "volume-node", SourceNodes: []string{"volume-node", "replica-node"}, SourceReader: "source-reader", SourceReaderUID: "reader-uid", SourceDataPath: "/data/pgdata", SourceMountPath: "/data", SourceRunAsUser: 26, SourceRunAsGroup: 26, Image: "example.test/pg@sha256:" + strings.Repeat("a", 64)}
}

func TestColdOfflineRequiresPausedReconciliationAndHealthyNodes(t *testing.T) {
	for _, scenario := range []string{"ready", "annotation-only", "not-fenced", "wrong-uid", "pod-remains", "node-offline"} {
		t.Run(scenario, func(t *testing.T) {
			st := coldTestOfflineState()
			cluster := map[string]any{"metadata": map[string]any{"uid": st.SourceUID, "annotations": map[string]any{coldMigrationAnnotation: coldStateName(st.ServiceID), "cnpg.io/fencedInstances": `["*"]`, runtimepkg.CloudNativePGHibernationAnno: runtimepkg.CloudNativePGHibernationOn, coldReconciliationAnnotation: coldReconciliationDisabled}}}
			switch scenario {
			case "annotation-only":
				delete(coldTestMap(coldTestMap(cluster["metadata"])["annotations"]), coldReconciliationAnnotation)
			case "not-fenced":
				coldTestMap(coldTestMap(cluster["metadata"])["annotations"])["cnpg.io/fencedInstances"] = "[]"
			case "wrong-uid":
				coldTestMap(cluster["metadata"])["uid"] = "replaced"
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "GET" {
					t.Fatal("quiescence check wrote a resource")
				}
				switch {
				case strings.HasSuffix(r.URL.Path, "/clusters/source"):
					json.NewEncoder(w).Encode(cluster)
				case strings.HasSuffix(r.URL.Path, "/pods"):
					items := []any{}
					if scenario == "pod-remains" {
						items = append(items, coldTestClusterPod("primary", ""))
					}
					json.NewEncoder(w).Encode(map[string]any{"items": items})
				case strings.Contains(r.URL.Path, "/nodes/"):
					status := "True"
					if scenario == "node-offline" {
						status = "Unknown"
					}
					json.NewEncoder(w).Encode(map[string]any{"status": map[string]any{"conditions": []map[string]string{{"type": "Ready", "status": status}}}})
				default:
					t.Error("unexpected resource", r.URL.Path)
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			err := verifyColdOfflineQuiesced(context.Background(), &kubeClient{baseURL: server.URL, client: server.Client()}, "ns", st)
			if (err == nil) != (scenario == "ready") {
				t.Fatal(scenario, err)
			}
		})
	}
}

func TestColdOfflineReaderCannotWriteOrStartDatabase(t *testing.T) {
	st := coldTestOfflineState()
	for _, scenario := range []string{"ready", "replaced", "writable-claim", "writable-mount", "database-command", "extra-container", "wrong-node", "privileged"} {
		t.Run(scenario, func(t *testing.T) {
			raw := coldJSONValue(coldOfflineReaderPod("ns", st)).(map[string]any)
			m := coldTestMap(raw["metadata"])
			m["uid"] = "reader-uid"
			spec := coldTestMap(raw["spec"])
			spec["nodeName"] = st.SourceNode
			container := mapSlice(spec["containers"])[0]
			switch scenario {
			case "replaced":
				m["uid"] = "foreign"
			case "writable-claim":
				coldTestMap(mapSlice(spec["volumes"])[0]["persistentVolumeClaim"])["readOnly"] = false
			case "writable-mount":
				mapSlice(container["volumeMounts"])[0]["readOnly"] = false
			case "database-command":
				container["command"] = []string{"postgres"}
			case "extra-container":
				spec["containers"] = append(mapSlice(spec["containers"]), map[string]any{"name": "other"})
			case "wrong-node":
				spec["nodeName"] = "other"
			case "privileged":
				coldTestMap(container["securityContext"])["privileged"] = true
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "GET" {
					t.Fatal("reader validation mutated source")
				}
				json.NewEncoder(w).Encode(raw)
			}))
			defer server.Close()
			err := verifyColdOfflineReader(context.Background(), &kubeClient{baseURL: server.URL, client: server.Client()}, "ns", st)
			if (err == nil) != (scenario == "ready") {
				t.Fatal(scenario, err)
			}
			if scenario == "ready" {
				requests := coldTestMap(coldTestMap(container["resources"])["requests"])
				if requests["cpu"] != "5m" || spec["automountServiceAccountToken"] != false || coldTestMap(spec["securityContext"])["fsGroup"] != nil {
					t.Fatal("reader needs database capacity or can chown source", spec)
				}
			}
		})
	}
}

func TestColdOfflineStopUsesNormalUIDBoundDeletionAndRetainsClaims(t *testing.T) {
	for _, scenario := range []string{"owned", "foreign", "not-paused", "node-offline"} {
		t.Run(scenario, func(t *testing.T) {
			st := coldTestOfflineState()
			deletes := 0
			pod := coldTestClusterPod("primary", "")
			if scenario == "foreign" {
				mapSlice(coldTestMap(pod["metadata"])["ownerReferences"])[0]["uid"] = "foreign"
			}
			annotations := map[string]string{coldMigrationAnnotation: coldStateName(st.ServiceID), "cnpg.io/fencedInstances": `["*"]`, coldReconciliationAnnotation: coldReconciliationDisabled}
			if scenario == "not-paused" {
				delete(annotations, coldReconciliationAnnotation)
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/clusters/source"):
					json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"name": "source", "uid": st.SourceUID, "annotations": annotations}})
				case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/nodes/"):
					status := "True"
					if scenario == "node-offline" {
						status = "Unknown"
					}
					json.NewEncoder(w).Encode(map[string]any{"status": map[string]any{"conditions": []map[string]string{{"type": "Ready", "status": status}}}})
				case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/pods"):
					json.NewEncoder(w).Encode(map[string]any{"items": []any{pod}})
				case r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, "/pods/primary"):
					deletes++
					var body map[string]any
					json.NewDecoder(r.Body).Decode(&body)
					if coldTestMap(body["preconditions"])["uid"] != "primary-uid" || body["gracePeriodSeconds"] != nil {
						t.Fatal("source force deletion or missing UID", body)
					}
					w.Write([]byte(`{}`))
				default:
					t.Error("unexpected source mutation/resource", r.Method, r.URL.Path)
					http.Error(w, "refused", 400)
				}
			}))
			defer server.Close()
			err := stopColdOfflineInstances(context.Background(), &kubeClient{baseURL: server.URL, client: server.Client()}, "ns", st)
			if (err == nil) != (scenario == "owned") || deletes != map[string]int{"owned": 1}[scenario] {
				t.Fatal(scenario, err, deletes)
			}
		})
	}
}

func TestColdHAExpansionOnlyMutatesOwnedTargetPlacement(t *testing.T) {
	for _, scenario := range []string{"owned", "replaced"} {
		t.Run(scenario, func(t *testing.T) {
			st := &coldPostgresState{ServiceID: "service", TargetName: "restored", TargetUID: "target-uid"}
			writes := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !strings.HasSuffix(r.URL.Path, "/clusters/restored") {
					t.Fatal("HA restoration touched source/app/endpoint", r.URL.Path)
				}
				if r.Method == "GET" {
					uid := st.TargetUID
					if scenario == "replaced" {
						uid = "foreign"
					}
					json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"uid": uid, "resourceVersion": "7", "annotations": map[string]string{coldMigrationAnnotation: coldStateName(st.ServiceID)}}})
					return
				}
				writes++
				if r.Method != "PATCH" {
					t.Fatal(r.Method)
				}
				var patch map[string]any
				json.NewDecoder(r.Body).Decode(&patch)
				m := coldTestMap(patch["metadata"])
				spec := coldTestMap(patch["spec"])
				if m["uid"] != st.TargetUID || m["resourceVersion"] != "7" || len(spec) != 4 || coldTestMap(m["annotations"])[runtimepkg.CloudNativePGReconcilePodSpecAnno] != runtimepkg.CloudNativePGReconcilePodSpecHold || spec["instances"] != float64(2) {
					t.Fatal("HA patch lost CAS/hold or changed unrelated fields", patch)
				}
				w.Write([]byte(`{}`))
			}))
			defer server.Close()
			desired := map[string]any{"spec": map[string]any{"instances": 2, "minSyncReplicas": 0, "maxSyncReplicas": 1, "affinity": map[string]any{"nodeSelector": map[string]string{"test": "yes"}}, "bootstrap": "do-not-apply", "imageName": "do-not-apply", "resources": "do-not-apply"}}
			err := patchColdTargetHA(context.Background(), &kubeClient{baseURL: server.URL, client: server.Client()}, "ns", st, desired)
			if (err == nil) != (scenario == "owned") || writes != map[string]int{"owned": 1, "replaced": 0}[scenario] {
				t.Fatal(scenario, err, writes)
			}
		})
	}
}

func TestColdHAReadinessRequiresDistinctOwnedReplicasAndDestinationPrimary(t *testing.T) {
	cluster := kubeCloudNativePGCluster{}
	cluster.Metadata.Name = "source"
	cluster.Metadata.UID = "source-uid"
	cluster.Status.CurrentPrimary = "primary"
	cluster.Status.ReadyInstances = 2
	makeReady := func(name, node string) kubePod {
		raw := coldTestClusterPod(name, node)
		raw["status"] = map[string]any{"phase": "Running", "conditions": []map[string]string{{"type": "Ready", "status": "True"}}}
		return coldTestDecodePod(t, raw)
	}
	pods := []kubePod{makeReady("primary", "target"), makeReady("replica", "replica-node")}
	primary, replicas, err := coldReadyHAInstances(cluster, pods, "target", 2)
	if err != nil || primary != "primary" || !reflect.DeepEqual(replicas, []string{"replica"}) {
		t.Fatal(primary, replicas, err)
	}
	for _, scenario := range []string{"same-node", "wrong-primary-node", "foreign-owner", "not-ready"} {
		t.Run(scenario, func(t *testing.T) {
			changed := []kubePod{makeReady("primary", "target"), makeReady("replica", "replica-node")}
			switch scenario {
			case "same-node":
				changed[1].Spec.NodeName = "target"
			case "wrong-primary-node":
				changed[0].Spec.NodeName = "other"
			case "foreign-owner":
				changed[1].ObservedOwnerReferences[0].UID = "foreign"
			case "not-ready":
				changed[1].Status.Conditions = nil
			}
			primary, _, err := coldReadyHAInstances(cluster, changed, "target", 2)
			if primary != "" || (scenario != "not-ready" && err == nil) {
				t.Fatal("invalid HA passed", scenario, primary, err)
			}
		})
	}
}
