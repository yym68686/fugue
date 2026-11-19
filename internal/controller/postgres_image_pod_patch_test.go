package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"fugue/internal/pgimage"
	"fugue/internal/runtime"
)

const migrationTestBootstrapPodPatch = `[{"op":"test","path":"/spec/initContainers/0/name","value":"bootstrap-controller"},{"op":"replace","path":"/spec/initContainers/0/resources/requests/cpu","value":"5m"}]`

func TestImageMigrationRecognizesOnlyDedicatedBootstrapCPUPodPatch(t *testing.T) {
	for _, value := range []string{
		"[]", "null", "", strings.Replace(migrationTestBootstrapPodPatch, "5m", "0m", 1),
		strings.Replace(migrationTestBootstrapPodPatch, "bootstrap-controller", "unrelated", 1),
		strings.Replace(migrationTestBootstrapPodPatch, "requests/cpu", "requests/memory", 1),
		strings.Replace(migrationTestBootstrapPodPatch, "requests/cpu", "limits/cpu", 1),
		strings.ReplaceAll(migrationTestBootstrapPodPatch, "initContainers/0", "containers/0"),
		strings.Replace(migrationTestBootstrapPodPatch, `"op":"replace"`, `"op":"replace","op":"replace"`, 1),
		strings.TrimSuffix(migrationTestBootstrapPodPatch, "]") + `,{"op":"remove","path":"/spec/containers/0/resources"}]`,
	} {
		if isLegacyBootstrapCPUPodPatch(value) {
			t.Errorf("accepted foreign podPatch %q", value)
		}
	}
	if !isLegacyBootstrapCPUPodPatch(migrationTestBootstrapPodPatch) {
		t.Fatal("obsolete bootstrap-only patch was not accepted")
	}
	expected := migrationTestBootstrapPodPatch
	w := postgresImageWitness{BootstrapPodPatch: &expected}
	if validateImageMigrationPodPatch(w, expected, false) != nil || validateImageMigrationPodPatch(w, "", true) != nil {
		t.Fatal("failed to validate the before and after annotation states")
	}
	if validateImageMigrationPodPatch(w, "", false) == nil || validateImageMigrationPodPatch(w, strings.Replace(expected, "5m", "6m", 1), false) == nil ||
		validateImageMigrationPodPatch(w, expected, true) == nil {
		t.Fatal("accepted drift or inconsistent cutover")
	}
}

func TestLegacyImageMigrationPodPatchRecovery(t *testing.T) {
	for _, scenario := range []string{"interrupted", "image-not-applied", "wrong-uid", "spec-drift", "fenced", "wrong-pgdata", "old-pod-present", "another-pod-present", "ambiguous-node", "unknown-patch", "write-conflict"} {
		t.Run(scenario, func(t *testing.T) {
			const namespace, name, primary, node, runtimeID = "tenant-ns", "database", "database-1", "node-a", "runtime-a"
			claimSpec := map[string]any{"volumeName": "pgdata-volume"}
			pvSpec := map[string]any{"claimRef": map[string]any{"name": primary, "namespace": namespace, "uid": "claim-uid"}}
			identity := pgimage.Hash([]any{primary, "claim-uid", claimSpec, "pgdata-volume", "volume-uid", pvSpec})
			pvc := map[string]any{"metadata": map[string]any{"uid": "claim-uid"}, "spec": claimSpec, "status": map[string]any{"phase": "Bound"}}
			pv := map[string]any{"metadata": map[string]any{"uid": "volume-uid"}, "spec": pvSpec}
			annotations := map[string]any{imageMigrationPodPatchAnnotation: migrationTestBootstrapPodPatch, "example.org/keep": "same"}
			cluster := map[string]any{
				"metadata": map[string]any{"uid": "cluster-uid", "resourceVersion": "85", "name": name, "namespace": namespace, "annotations": annotations},
				"spec": map[string]any{"imageName": "baked", "storage": map[string]any{"size": "1Gi"}, "instances": float64(1),
					"affinity": map[string]any{"nodeSelector": map[string]any{runtime.RuntimeIDLabelKey: runtimeID}}},
				"status": map[string]any{"phase": "Primary instance is being restarted without a switchover",
					"currentPrimary": primary, "targetPrimary": primary, "danglingPVC": []any{primary}},
			}
			w := postgresImageWitness{
				Namespace: namespace, Cluster: name, ClusterUID: "cluster-uid", Pod: primary, PodUID: "old-pod-uid", Node: node,
				SourceImage: "legacy", TargetImage: "baked", SpecHash: imagePreservedSpecHash(cluster), SystemID: "123",
				Volumes: map[string]string{"pgdata": identity},
			}
			nodeObject := func(n string) map[string]any {
				return map[string]any{"metadata": map[string]any{"name": n, "labels": map[string]string{runtime.RuntimeIDLabelKey: runtimeID}},
					"status": map[string]any{"conditions": []any{map[string]string{"type": "Ready", "status": "True"}}}}
			}
			nodes := []any{nodeObject(node)}
			switch scenario {
			case "image-not-applied":
				imageObject(cluster["spec"])["imageName"] = "legacy"
			case "wrong-uid":
				imageObject(cluster["metadata"])["uid"] = "replacement"
			case "spec-drift":
				imageObject(imageObject(cluster["spec"])["storage"])["size"] = "2Gi"
			case "fenced":
				annotations["cnpg.io/fencedInstances"] = `["*"]`
			case "wrong-pgdata":
				imageObject(pv["metadata"])["uid"] = "other-volume"
			case "ambiguous-node":
				nodes = append(nodes, nodeObject("node-b"))
			case "unknown-patch":
				annotations[imageMigrationPodPatchAnnotation] = "[]"
			}
			writes := 0
			server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPatch {
					writes++
					var patch []map[string]any
					want := append([]map[string]any{
						{"op": "test", "path": "/metadata/uid", "value": w.ClusterUID},
						{"op": "test", "path": "/metadata/resourceVersion", "value": "85"},
						{"op": "test", "path": "/spec", "value": cluster["spec"]},
					}, removeImageMigrationPodPatch(migrationTestBootstrapPodPatch)...)
					if err := json.NewDecoder(r.Body).Decode(&patch); err != nil || !reflect.DeepEqual(patch, want) ||
						r.URL.Path != cloudNativePGClusterAPIPath(namespace, name) || r.Header.Get("Content-Type") != "application/json-patch+json" {
						t.Errorf("unexpected guarded update: %v %v", patch, err)
					}
					if scenario == "write-conflict" {
						http.Error(rw, "resource version changed", http.StatusConflict)
						return
					}
					delete(annotations, imageMigrationPodPatchAnnotation)
				} else if r.Method != http.MethodGet {
					t.Error("unexpected mutation:", r.Method, r.URL.Path)
				}
				var response any
				switch r.URL.Path {
				case cloudNativePGClusterAPIPath(namespace, name):
					response = cluster
				case imageDrainPodPath(w):
					if scenario != "old-pod-present" {
						http.NotFound(rw, r)
						return
					}
					response = map[string]any{"metadata": map[string]string{"uid": w.PodUID}}
				case "/api/v1/namespaces/" + namespace + "/pods":
					var pods []any
					if scenario == "another-pod-present" {
						pods = append(pods, map[string]any{"metadata": map[string]string{"name": "database-2"}})
					}
					response = map[string]any{"items": pods}
				case "/api/v1/nodes":
					response = map[string]any{"items": nodes}
				case "/api/v1/nodes/" + node:
					response = nodeObject(node)
				case "/api/v1/namespaces/" + namespace + "/persistentvolumeclaims/" + primary:
					response = pvc
				case "/api/v1/persistentvolumes/pgdata-volume":
					response = pv
				default:
					t.Error("unexpected request path:", r.URL.Path)
					http.NotFound(rw, r)
					return
				}
				_ = json.NewEncoder(rw).Encode(response)
			}))
			defer server.Close()
			client := &kubeClient{client: server.Client(), baseURL: server.URL, namespace: namespace}
			recovered, err := client.recoverLegacyImageMigrationPodPatch(context.Background(), w, runtimeID)
			wantWrite := scenario == "interrupted" || scenario == "write-conflict"
			wantErr := scenario != "interrupted" && scenario != "image-not-applied" && scenario != "old-pod-present" && scenario != "another-pod-present"
			if (writes == 1) != wantWrite || (err != nil) != wantErr || recovered != (scenario == "interrupted") {
				t.Fatalf("scenario %s: writes=%d recovered=%v err=%v", scenario, writes, recovered, err)
			}
			if recovered {
				if again, err := client.recoverLegacyImageMigrationPodPatch(context.Background(), w, runtimeID); again || err != nil || writes != 1 {
					t.Fatalf("repeat repair after annotation removal: %v %v", again, err)
				}
			}
			if (annotations[imageMigrationPodPatchAnnotation] == nil) != recovered || annotations["example.org/keep"] != "same" {
				t.Fatal("annotation absent unexpectedly or unrelated annotation changed")
			}
		})
	}
}
