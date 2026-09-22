package controller

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/livediagnostics"
	"fugue/internal/model"
	"fugue/internal/runtime"
	batchv1 "k8s.io/api/batch/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func stoppedRuntimeTestReport(pod releaseDrainPod, namespace string, now time.Time) livediagnostics.ProbeReport {
	sandbox := strings.Repeat("a", 64)
	target := map[string]any{"namespace": namespace, "name": pod.Name, "uid": pod.UID}
	containers := []any{}
	for name, id := range pod.StoppedContainers {
		containers = append(containers, map[string]any{"id": id, "name": name, "sandbox_id": sandbox, "state": "CONTAINER_EXITED", "pid": 123, "process_present": false})
	}
	data, _ := json.Marshal(map[string]any{"node": pod.Node, "boot_id": pod.BootID, "runtime_peer": map[string]any{"pid": 42, "start_ticks": "5"}, "observed_from": now.Add(-2 * time.Second), "observed_until": now.Add(-time.Second), "facts": map[string]any{"target": target, "quiescent": true, "sandboxes": []any{map[string]any{"id": sandbox, "metadata": target, "state": "SANDBOX_NOTREADY", "pid": 0, "process_status": "deleted", "net_namespace_closed": true}}, "containers": containers}})
	return livediagnostics.ProbeReport{Schema: "fugue.diagnostic.probe_report.v1", ProbeID: "pod-runtime-state", StartedAt: now.Add(-3 * time.Second), FinishedAt: now, Quality: livediagnostics.EvidenceQuality{Status: "complete"}, Evidence: []livediagnostics.Evidence{{Source: "pod-runtime-state", Status: "complete", ObservedAt: now.Add(-2 * time.Second), Data: data}}}
}

func TestStoppedRuntimeReportRequiresFreshCompleteIdentity(t *testing.T) {
	pod := releaseDrainPod{Name: "pod", UID: "uid", Node: "node", NodeUID: "node-uid", BootID: "boot", StoppedContainers: map[string]string{"app": strings.Repeat("b", 64)}}
	for _, scenario := range []string{"valid", "stale", "future", "degraded", "empty", "wrong_uid", "wrong_boot", "wrong_container", "open_network", "running_sandbox", "pid_present", "missing_pid", "missing_network", "no_runtime_peer", "false_quiescent", "duplicate_container", "duplicate_sandbox"} {
		t.Run(scenario, func(t *testing.T) {
			now := time.Now().UTC()
			r := stoppedRuntimeTestReport(pod, "tenant", now)
			var d map[string]any
			_ = json.Unmarshal(r.Evidence[0].Data, &d)
			facts := objectMapField(d, "facts")
			sandboxes := mapSlice(facts["sandboxes"])
			containers := mapSlice(facts["containers"])
			switch scenario {
			case "stale":
				r.FinishedAt = now.Add(-time.Minute)
			case "future":
				r.FinishedAt = now.Add(time.Minute)
			case "degraded":
				r.Quality.Status = "degraded"
			case "empty":
				facts["sandboxes"] = []any{}
			case "wrong_uid":
				objectMapField(facts, "target")["uid"] = "foreign"
			case "wrong_boot":
				d["boot_id"] = "other"
			case "wrong_container":
				containers[0]["id"] = strings.Repeat("c", 64)
			case "open_network":
				sandboxes[0]["net_namespace_closed"] = false
			case "running_sandbox":
				sandboxes[0]["state"] = "SANDBOX_READY"
			case "pid_present":
				containers[0]["process_present"] = true
			case "missing_pid":
				delete(containers[0], "pid")
			case "missing_network":
				delete(sandboxes[0], "net_namespace_closed")
			case "no_runtime_peer":
				delete(d, "runtime_peer")
			case "false_quiescent":
				facts["quiescent"] = false
			case "duplicate_container":
				facts["containers"] = append(containers, containers[0])
			case "duplicate_sandbox":
				facts["sandboxes"] = append(sandboxes, sandboxes[0])
			}
			r.Evidence[0].Data, _ = json.Marshal(d)
			err := validateStoppedRuntimeReport(r, pod, "tenant", now)
			if (err == nil) != (scenario == "valid") {
				t.Fatalf("%s accepted=%t err=%v", scenario, err == nil, err)
			}
		})
	}
}

func TestStoppedWorkloadUsesSignedProbeBeforeRetirement(t *testing.T) {
	testStoppedWorkloadUsesSignedProbeBeforeRetirement(t, false)
}

func TestUnboundHistoricalRuntimeRetiresAtomicallyWithoutRepairingSnapshot(t *testing.T) {
	testStoppedWorkloadUsesSignedProbeBeforeRetirement(t, true)
}

func testStoppedWorkloadUsesSignedProbeBeforeRetirement(t *testing.T, historical bool) {
	for _, scenario := range []string{"valid", "old_status_fresh_lease", "stale_lease", "wrong_lease_owner", "wrong_lease_holder", "lease_missing", "revoked", "wrong_job_image", "wrong_job_owner", "stale_report", "wrong_report_container", "wrong_report_boot", "pod_restart", "node_reboot", "policy_changed", "unready_node", "running_container", "missing_container"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			s, app, old, f := newDrainWorkloadFixtureWithOutcome(t, false, historical)
			if historical {
				delete(objectMapValue(nestedObjectValue(f.service, "spec", "selector")), runtime.FugueLabelAppWorkload)
				for _, p := range f.pods {
					delete(objectMapValue(nestedObjectValue(p, "metadata", "labels")), runtime.FugueLabelAppWorkload)
				}
			}
			s.Config.KubectlNamespace = "system-test"
			f.pods = f.pods[:1]
			objectMapField(f.deployment, "spec")["replicas"] = float64(1)
			objectMapField(f.deployment, "status")["readyReplicas"] = float64(0)
			pod := f.pods[0]
			objectMapField(pod, "spec")["nodeName"] = "node-test"
			for i, field := range []string{"containerStatuses", "initContainerStatuses"} {
				status := mapSlice(nestedObjectValue(pod, "status", field))[0]
				status["ready"] = false
				status["containerID"] = "containerd://" + strings.Repeat(string(rune('b'+i)), 64)
				status["state"] = map[string]any{"terminated": map[string]any{"finishedAt": time.Now().Add(-time.Hour).UTC().Format(time.RFC3339), "exitCode": 255, "reason": "Unknown"}}
			}
			if scenario == "running_container" {
				mapSlice(nestedObjectValue(pod, "status", "containerStatuses"))[0]["state"] = map[string]any{"running": map[string]any{}}
			}
			if scenario == "missing_container" {
				objectMapField(pod, "status")["containerStatuses"] = []any{}
			}
			image := "registry.example/probe@sha256:" + strings.Repeat("a", 64)
			catalog := livediagnostics.Catalog{Protocol: livediagnostics.CatalogProtocol, Generation: 1, RunnerImage: image, Policy: livediagnostics.CatalogPolicy{Namespaces: []string{"*"}, Profiles: []string{"process-profile"}, ServiceAccount: "reader"}, Probes: []livediagnostics.Probe{{ID: "pod-runtime-state", Image: image, Profile: "process-profile", TargetTypes: []livediagnostics.TargetType{livediagnostics.TargetNode}, MaxDurationSeconds: 60, Parameters: map[string]livediagnostics.Parameter{"namespace": {Required: true}, "pod": {Required: true}, "pod_uid": {Required: true}}, Config: json.RawMessage(`{"collectors":[]}`)}}}
			pub, key, _ := ed25519.GenerateKey(rand.Reader)
			signed, err := livediagnostics.SignCatalog(catalog, "publisher", key)
			if err != nil {
				t.Fatal(err)
			}
			keys := map[string]string{"publisher": base64.StdEncoding.EncodeToString(pub)}
			var job *batchv1.Job
			var lease *coordinationv1.Lease
			logReads := 0
			nodeReads := 0
			f.extra = func(w http.ResponseWriter, r *http.Request) bool {
				encode := func(v any) {
					w.Header().Set("Content-Type", "application/json")
					if err := json.NewEncoder(w).Encode(v); err != nil {
						t.Error(err)
					}
				}
				switch {
				case r.URL.Path == "/api/v1/nodes/node-test":
					nodeReads++
					boot := "boot-test"
					ready := "True"
					if scenario == "unready_node" {
						ready = "False"
					}
					if scenario == "node_reboot" && logReads > 0 {
						boot = "changed"
					}
					at := time.Now().UTC()
					if scenario == "old_status_fresh_lease" {
						at = at.Add(-time.Hour)
					}
					encode(map[string]any{"metadata": map[string]any{"uid": "node-uid"}, "status": map[string]any{"nodeInfo": map[string]any{"bootID": boot}, "conditions": []any{map[string]any{"type": "Ready", "status": ready, "lastHeartbeatTime": at.Format(time.RFC3339)}}}})
				case strings.Contains(r.URL.Path, "/namespaces/kube-node-lease/"):
					if scenario == "lease_missing" {
						http.NotFound(w, r)
						break
					}
					at, uid, holder := time.Now().UTC(), "node-uid", "node-test"
					if scenario == "stale_lease" {
						at = at.Add(-time.Minute)
					}
					if scenario == "wrong_lease_owner" {
						uid = "foreign"
					}
					if scenario == "wrong_lease_holder" {
						holder = "foreign"
					}
					encode(map[string]any{"metadata": map[string]any{"ownerReferences": []any{map[string]any{"kind": "Node", "name": "node-test", "uid": uid}}}, "spec": map[string]any{"holderIdentity": holder, "renewTime": at.Format(time.RFC3339Nano), "leaseDurationSeconds": 40}})
				case strings.Contains(r.URL.Path, "/configmaps/"):
					data := map[string]string{"catalog.json": string(signed)}
					if strings.HasSuffix(r.URL.Path, "/"+livediagnostics.TrustConfigMap) {
						copy := keys
						if scenario == "revoked" {
							copy = map[string]string{}
						}
						raw, _ := json.Marshal(copy)
						data = map[string]string{"keys.json": string(raw)}
					}
					encode(corev1.ConfigMap{Data: data})
				case strings.Contains(r.URL.Path, "/leases"):
					if r.Method == "GET" && lease == nil {
						http.NotFound(w, r)
					} else if r.Method == "GET" {
						encode(lease)
					} else {
						if err := json.NewDecoder(r.Body).Decode(&lease); err != nil {
							t.Error(err)
						}
						lease.ResourceVersion = "1"
						encode(lease)
					}
				case strings.HasPrefix(r.URL.Path, "/apis/batch/v1/namespaces/system-test/jobs"):
					if r.Method == "POST" {
						if err := json.NewDecoder(r.Body).Decode(&job); err != nil {
							t.Error(err)
						}
						job.UID = "job-uid"
						job.Status.Succeeded = 1
						now := metav1.Now()
						job.Status.CompletionTime = &now
						if scenario == "wrong_job_image" {
							job.Spec.Template.Spec.Containers[0].Image = "registry.example/wrong:v1"
						}
						encode(job)
					} else if strings.HasSuffix(r.URL.Path, "/jobs") {
						encode(batchv1.JobList{})
					} else if job == nil {
						http.NotFound(w, r)
					} else {
						encode(job)
					}
				case r.URL.Path == "/api/v1/namespaces/system-test/pods":
					owner := types.UID("job-uid")
					if scenario == "wrong_job_owner" {
						owner = "foreign"
					}
					yes := true
					encode(corev1.PodList{Items: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "probe-pod", UID: "probe-uid", OwnerReferences: []metav1.OwnerReference{{Kind: "Job", Name: job.Name, UID: owner, Controller: &yes}}}, Spec: corev1.PodSpec{NodeName: "node-test", Containers: []corev1.Container{{Name: livediagnostics.DiagnosticAgentContainer, Image: image}}}, Status: corev1.PodStatus{Phase: corev1.PodSucceeded}}}})
				case r.URL.Path == "/api/v1/namespaces/system-test/pods/probe-pod/log":
					logReads++
					now := time.Now().UTC()
					target := releaseDrainPod{Name: "pod-0", UID: "pod-uid-0", Node: "node-test", BootID: "boot-test", StoppedContainers: map[string]string{"application": strings.Repeat("b", 64), "fugue-drain-agent": strings.Repeat("c", 64)}}
					if scenario == "wrong_report_container" {
						target.StoppedContainers["application"] = strings.Repeat("d", 64)
					}
					if scenario == "wrong_report_boot" {
						target.BootID = "other"
					}
					if scenario == "stale_report" {
						now = now.Add(-time.Minute)
					}
					report := stoppedRuntimeTestReport(target, runtime.NamespaceForTenant(app.TenantID), now)
					report.SessionID = job.Name
					report.ProbeImage = image
					report.ProbeDigest = job.Annotations[livediagnostics.ProbeDigestAnnotation]
					report.CatalogDigest = job.Annotations[livediagnostics.CatalogDigestAnnotation]
					report.Target = livediagnostics.Target{Type: livediagnostics.TargetNode, Node: "node-test"}
					if scenario == "pod_restart" {
						objectMapField(pod, "metadata")["resourceVersion"] = "2"
					}
					if scenario == "policy_changed" {
						policy, _ := s.Store.GetAppTrafficPolicy(app.TenantID, false, app.ID)
						policy.StickyCookie = "changed"
						if _, err := s.Store.UpsertAppTrafficPolicy(policy); err != nil {
							t.Error(err)
						}
					}
					encode(report)
				default:
					return false
				}
				return true
			}
			s.safeRolloutDrainMetricsQuerier = kubeSafeRolloutDrainObserver{service: s}
			if err := s.retryDrainingAppReleaseRetirement(context.Background(), app); err != nil {
				t.Fatal(err)
			}
			got, err := s.Store.GetAppRelease(app.TenantID, false, old.ID)
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "valid" || scenario == "old_status_fresh_lease" {
				if got.Status != model.AppReleaseStatusRetired || f.deleteCalls != 2 || logReads != 1 || nodeReads < 2 || !historical && nodeReads < 3 {

					t.Fatalf("not retired: status=%s deletes=%d logs=%d nodes=%d", got.Status, f.deleteCalls, logReads, nodeReads)
				}
			} else if got.Status == model.AppReleaseStatusRetired || f.deleteCalls != 0 {
				t.Fatal("unverified stopped runtime retired", scenario)
			}
			if historical && (scenario == "valid" || scenario == "old_status_fresh_lease") {
				if got.RevisionWorkload == nil || got.RevisionWorkload.BoundAt.IsZero() || !reflect.DeepEqual(got.SpecSnapshot, old.SpecSnapshot) {
					t.Fatal("atomic historical retirement rewrote intent or lost binding")
				}
			} else if !reflect.DeepEqual(got.RevisionWorkload, old.RevisionWorkload) {
				t.Fatal("runtime observation changed binding")
			}
		})
	}
}
