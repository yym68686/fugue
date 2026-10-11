package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
)

func readyEnrollmentPods() []model.ClusterPod {
	pods := []model.ClusterPod{}
	for _, slot := range []string{"a", "b"} {
		pods = append(pods, model.ClusterPod{Name: "worker-" + slot, NodeName: "node-new", Phase: "Running", Ready: true, Labels: map[string]string{runtimepkg.EdgeGroupIDLabelKey: "edge-group-fixture", "fugue.io/edge-slot": slot}, Containers: []model.ClusterPodContainer{{Name: "edge", Ready: true, State: "running"}}})
	}
	return append(pods, model.ClusterPod{Name: "front", NodeName: "node-new", Phase: "Running", Ready: true, Labels: map[string]string{runtimepkg.EdgeGroupIDLabelKey: "edge-group-fixture"}, Containers: []model.ClusterPodContainer{{Name: "edge-front", Ready: true, State: "running"}}})
}

func TestEdgeEnrollmentRejectsUnreadyOrForeignPods(t *testing.T) {
	for _, scenario := range []string{"missing-front", "unready-worker", "foreign-node", "foreign-group"} {
		t.Run(scenario, func(t *testing.T) {
			pods := readyEnrollmentPods()
			switch scenario {
			case "missing-front":
				pods = pods[:2]
			case "unready-worker":
				pods[0].Ready = false
			case "foreign-node":
				pods[0].NodeName = "node-other"
			case "foreign-group":
				pods[0].Labels[runtimepkg.EdgeGroupIDLabelKey] = "edge-group-other"
			}
			if edgeEnrollmentPodReason(pods, "node-new", "edge-group-fixture") == "" {
				t.Fatal("incomplete or foreign workload accepted")
			}
		})
	}
}

func TestEdgeEnrollmentCommandWaitsForRuntimeAndRetriesWithoutTokenRotation(t *testing.T) {
	now := time.Now().UTC()
	patches, tasks := 0, 0
	stale := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		var value any
		switch r.Method + " " + r.URL.Path {
		case "GET /v1/cluster/node-policies/node-new":
			value = map[string]any{"node_policy": model.ClusterNodePolicyStatus{NodeName: "node-new", Ready: true, NodeSchedulable: true, Reconciled: true, Policy: &model.ClusterNodePolicy{AllowEdge: true, EffectiveEdge: true}, Labels: map[string]string{runtimepkg.EdgeGroupIDLabelKey: "edge-group-fixture"}}}
		case "GET /v1/edge/nodes/node-new":
			seen := now
			if stale {
				seen = now.Add(-10 * time.Minute)
			}
			value = edgeNodeResponse{Node: model.EdgeNode{ID: "node-new", EdgeGroupID: "edge-group-fixture", WorkloadMode: "dynamic", Healthy: true, LastHeartbeatAt: &seen}}
		case "PATCH /v1/cluster/nodes/node-new/policy":
			var body setClusterNodePolicyRequest
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			if body.EdgeGroupID == nil || *body.EdgeGroupID != "edge-group-fixture" || body.AllowEdge == nil || !*body.AllowEdge || body.AllowSharedPool != nil {
				t.Errorf("unexpected intent: %+v", body)
			}
			patches++
			value = setClusterNodePolicyResponse{ClusterNode: model.ClusterNode{Name: "node-new"}, NodeReconciled: true}
		case "POST /v1/node-update-tasks":
			tasks++
			value = nodeUpdateTaskCreateResponse{Task: model.NodeUpdateTask{ID: "task-fixture", Type: model.NodeUpdateTaskTypeRefreshDesiredState}}
		case "GET /v1/cluster/pods":
			if r.URL.Query().Get("node") != "node-new" {
				t.Error("pod observation not scoped to the target")
			}
			value = map[string]any{"cluster_pods": readyEnrollmentPods()}
		case "GET /v1/admin/edge/nodes/node-new/desired-state":
			value = edgeNodeDesiredStateResponse{DesiredState: edgeNodeDesiredState{EdgeID: "node-new", EdgeGroupID: "edge-group-fixture", RouteReady: true, TLSReady: true, PublicProbeStatus: model.EdgePublicProbeStatusPassing}}
		default:
			t.Errorf("unexpected request, including any token rotation: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			value = map[string]string{"error": "unexpected endpoint"}
		}
		_ = json.NewEncoder(w).Encode(value)
	}))
	defer server.Close()
	for range 2 {
		var stdout, stderr bytes.Buffer
		err := runWithStreams([]string{"--base-url", server.URL, "--token", "bootstrap-fixture", "admin", "cluster", "node-policy", "set", "node-new", "--edge", "on", "--edge-group", "edge-group-fixture", "--json"}, &stdout, &stderr)
		if err != nil {
			t.Fatalf("command failed: %v / %s", err, stderr.String())
		}
		var result map[string]any
		if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		if result["edge_ready"] != true {
			t.Fatalf("runtime verification missing: %s", stdout.String())
		}
	}
	if patches != 2 || tasks != 2 {
		t.Fatalf("retry did not reuse enrollment: patches=%d tasks=%d", patches, tasks)
	}
	stale = true
	client, _ := NewClient(server.URL, "bootstrap-fixture")
	reason, probe, err := observeEdgeEnrollment(client, "node-new", "edge-group-fixture", now)
	if err != nil || probe || !strings.Contains(reason, "fresh") {
		t.Fatalf("stale runtime accepted: %q %v %v", reason, probe, err)
	}
}

func TestEdgeEnrollmentMissingAuthorityDoesNotMutatePolicy(t *testing.T) {
	mutated := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			mutated = true
			t.Error("mutation before preflight")
		}
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "node-policies") {
			_ = json.NewEncoder(w).Encode(map[string]any{"node_policy": model.ClusterNodePolicyStatus{NodeName: "node-new", Ready: true, NodeSchedulable: true}})
		} else {
			w.WriteHeader(404)
			_, _ = w.Write([]byte(`{"error":"not found"}`))
		}
	}))
	defer server.Close()
	client, _ := NewClient(server.URL, "fixture")
	on := true
	_, _, err := prepareEdgePolicyEnrollment(client, "node-new", "", setClusterNodePolicyRequest{AllowEdge: &on})
	if err == nil || !strings.Contains(err.Error(), "--edge-group") || mutated {
		t.Fatalf("missing explicit authority not rejected safely: %v", err)
	}
}

func TestEdgeEnrollmentActivationRequiresProofsAndPreservesExistingState(t *testing.T) {
	for _, scenario := range []string{"verified", "existing", "wrong-image", "wrong-identity", "stale-route", "missing-tls", "changed-authority", "cold-standby"} {
		t.Run(scenario, func(t *testing.T) {
			now := time.Now().UTC()
			intent := model.EdgeNodeEnrollment{ID: "fixture", NodeID: "node-new", EdgeGroupID: "edge-group-fixture", AuthorityRecordDigest: "sha256:" + strings.Repeat("a", 64), Slot: "a", FenceSequence: 7, BundleVersion: "fixture.p9.r0", WorkerSourceSHA: strings.Repeat("b", 40), WorkerImageDigest: "sha256:" + strings.Repeat("c", 64), IssuedAt: now, ExpiresAt: now.Add(time.Minute)}
			pods := readyEnrollmentPods()
			if scenario == "cold-standby" {
				pods[1].Ready = false
			}
			pods[0].Namespace = "fixture-system"
			pods[0].Containers[0].ImageID = "registry.example/edge@" + intent.WorkerImageDigest
			if scenario == "wrong-image" {
				pods[0].Containers[0].ImageID = "registry.example/edge@sha256:" + strings.Repeat("d", 64)
			}
			reads, activations, execs := 0, 0, 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.Method == "GET" && r.URL.Path == "/v1/admin/edge/nodes/node-new/desired-state" {
					reads++
					current := intent
					if scenario == "changed-authority" && reads > 1 {
						current.ID = "another-enrollment"
					}
					_ = json.NewEncoder(w).Encode(edgeNodeDesiredStateResponse{DesiredState: edgeNodeDesiredState{Enrollment: &current}})
					return
				}
				if r.Method != "POST" || r.URL.Path != "/v1/cluster/exec" {
					t.Errorf("unexpected endpoint: %s %s", r.Method, r.URL.Path)
					w.WriteHeader(404)
					return
				}
				var request clusterExecRequest
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Fatal(err)
				}
				execs++
				if request.Namespace != pods[0].Namespace || request.Pod != pods[0].Name || request.Container != "edge" {
					t.Errorf("exec escaped exact worker: %+v", request)
				}
				output := ""
				switch {
				case len(request.Command) == 3 && request.Command[0] == "sh" && strings.Contains(request.Command[2], "test -e"):
					output = "absent"
					if scenario == "existing" {
						output = "existing-state-even-if-invalid"
					}
				case len(request.Command) == 3 && request.Command[0] == "sh" && strings.Contains(request.Command[2], "for field"):
					output = intent.NodeID + "\n" + intent.EdgeGroupID + "\n" + intent.Slot + "\n" + intent.WorkerSourceSHA + "\n"
					if scenario == "wrong-identity" {
						output = strings.ReplaceAll(output, intent.NodeID, "node-other")
					}
				case request.Command[0] == "wget":
					seen := now
					if scenario == "stale-route" {
						seen = now.Add(-5 * time.Minute)
					}
					tlsProbes := 2
					if scenario == "missing-tls" {
						tlsProbes = 0
					}
					health := map[string]any{"healthy": true, "edge_id": intent.NodeID, "edge_group_id": intent.EdgeGroupID, "bundle_version": intent.BundleVersion, "route_bundle_source": "edge-control-group-authority/v1", "caddy_applied_version": intent.BundleVersion, "route_count": 2, "last_success_at": seen, "platform_serving": map[string]any{"state": "serving_verified", "bundle_version": intent.BundleVersion, "route_probes": 2, "tls_probes": tlsProbes, "verified_at": now}}
					data, _ := json.Marshal(health)
					output = string(data)
				case request.Command[0] == "/usr/local/bin/fugue-edge-front-cas":
					activations++
					flags := map[string]string{}
					for i := 1; i+1 < len(request.Command); i += 2 {
						flags[request.Command[i]] = request.Command[i+1]
					}
					if flags["--operation"] != "initialize" || flags["--expected-generation"] != "0" || flags["--initial-generation"] != "7" || flags["--worker-image-digest"] != intent.WorkerImageDigest || flags["--bundle-generation"] != intent.BundleVersion || reads != 2 {
						t.Errorf("unsafe activation: %+v; authority reads %d", flags, reads)
					}
				default:
					t.Errorf("unexpected exec: %v", request.Command)
				}
				_ = json.NewEncoder(w).Encode(clusterExecResponse{Output: output})
			}))
			defer server.Close()
			client, _ := NewClient(server.URL, "fixture")
			_, err := prepareEdgeEnrollmentActivation(client, pods, intent.NodeID, intent.EdgeGroupID, now)
			if scenario == "verified" {
				if err != nil || activations != 1 {
					t.Fatalf("verified enrollment failed: activations=%d error=%v", activations, err)
				}
			} else if activations != 0 {
				t.Fatalf("unsafe state created activation: %s", scenario)
			}
			if scenario == "existing" && execs != 1 {
				t.Fatalf("existing activation modified: %d exec calls", execs)
			}
		})
	}
}
