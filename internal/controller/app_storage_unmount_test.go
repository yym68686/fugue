package controller

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"fugue/internal/model"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestStorageUnmountTrafficProofRequiresExactReadyIdentities(t *testing.T) {
	const ns = "test-tenant"
	makePod := func(name, ip string) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, UID: types.UID(name + "-uid")},
			Status: corev1.PodStatus{PodIP: ip}}
	}
	old, candidate := makePod("old", "10.0.0.1"), makePod("candidate", "10.0.0.2")
	for _, scenario := range []string{"ready", "only-source", "stale-uid", "wrong-address", "unready", "terminating", "unexpected"} {
		t.Run(scenario, func(t *testing.T) {
			ready := true
			endpoint := func(p *corev1.Pod) discoveryv1.Endpoint {
				return discoveryv1.Endpoint{Addresses: []string{p.Status.PodIP}, Conditions: discoveryv1.EndpointConditions{Ready: &ready},
					TargetRef: &corev1.ObjectReference{Kind: "Pod", Namespace: ns, Name: p.Name, UID: p.UID}}
			}
			endpoints := []discoveryv1.Endpoint{endpoint(old), endpoint(candidate)}
			switch scenario {
			case "only-source":
				endpoints = endpoints[:1]
			case "stale-uid":
				endpoints[1].TargetRef.UID = "previous-pod-with-reused-ip"
			case "wrong-address":
				endpoints[1].Addresses = []string{old.Status.PodIP}
			case "unready":
				unready := false
				endpoints[1].Conditions.Ready = &unready
			case "terminating":
				endpoints[1].Conditions.Terminating = &ready
			case "unexpected":
				endpoints = append(endpoints, endpoint(makePod("unrelated", "10.0.0.3")))
			}
			err := matchStorageUnmountEndpoints([]discoveryv1.EndpointSlice{{Endpoints: endpoints}}, ns, []*corev1.Pod{old, candidate})
			if (err == nil) != (scenario == "ready") {
				t.Fatalf("unexpected endpoint proof: %v", err)
			}
		})
	}
}

func TestStorageUnmountCandidateMustActuallyExcludeTheRemovedMount(t *testing.T) {
	mount := model.AppPersistentStorageMount{Kind: "directory", Path: "/data"}
	spec := &model.AppPersistentStorageSpec{Mounts: []model.AppPersistentStorageMount{mount}}
	pod := &corev1.Pod{Spec: corev1.PodSpec{
		Volumes: []corev1.Volume{{Name: "persistent", VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "original"},
		}}},
		Containers: []corev1.Container{{Name: "app", VolumeMounts: []corev1.VolumeMount{{Name: "persistent",
			MountPath: mount.Path, SubPath: model.AppPersistentStorageMountSubPath(mount)}}}},
	}}
	if !storageUnmountPodMountsMatch(pod, "original", spec, "/config") {
		t.Fatal("correct mount layout was rejected")
	}
	wrongPath := pod.DeepCopy()
	wrongPath.Spec.Containers = append(wrongPath.Spec.Containers, corev1.Container{Name: "unexpected-sidecar",
		VolumeMounts: []corev1.VolumeMount{{Name: "different-volume", MountPath: "/config"}}})
	wrongVolume := pod.DeepCopy()
	wrongVolume.Spec.Volumes[0].PersistentVolumeClaim.ClaimName = "replacement"
	wrongSubpath := pod.DeepCopy()
	wrongSubpath.Spec.Containers[0].VolumeMounts[0].SubPath = "wrong"
	for _, p := range []*corev1.Pod{wrongPath, wrongVolume, wrongSubpath} {
		if storageUnmountPodMountsMatch(p, "original", spec, "/config") {
			t.Fatal("accepted an unverified live mount layout")
		}
	}
}

type storageUnmountProbeTransport func(*http.Request) (*http.Response, error)

func (f storageUnmountProbeTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestStorageUnmountProbeValidatesOnlyBusinessResponse(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
		pass       bool
	}{
		{"passed", `{"catalog_version":"1"}`, 200, true},
		{"wrong-response", `{"ready":true}`, 200, false},
		{"error", `{"catalog_version":"1"}`, 503, false},
		{"redirect", `{"catalog_version":"1"}`, 302, false},
		{"over-limit", strings.Repeat(" ", 64<<10) + `{"catalog_version":"1"}`, 200, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			c := &http.Client{Transport: storageUnmountProbeTransport(func(r *http.Request) (*http.Response, error) {
				calls++
				if r.URL.String() != "http://10.0.0.8:8080/v1/catalog" || r.Header.Get("Authorization") != "Bearer test-key" {
					t.Fatalf("probe was not targeted to the expected authenticated candidate")
				}
				return &http.Response{StatusCode: tc.status, Body: io.NopCloser(strings.NewReader(tc.body))}, nil
			})}
			err := runStorageUnmountProbeWithClient(context.Background(), c, "10.0.0.8", 8080,
				model.AppStorageUnmountProbe{Path: "/v1/catalog", BearerEnv: "PROBE_KEY", ExpectedBodyContains: "catalog_version"},
				map[string]string{"PROBE_KEY": "test-key"})
			if (err == nil) != tc.pass || calls != 1 {
				t.Fatalf("unexpected probe outcome: calls=%d err=%v", calls, err)
			}
		})
	}
}

func TestStorageUnmountHoldAndRetirementAreFencedWrites(t *testing.T) {
	patches := make(chan []map[string]any, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var patch []map[string]any
		if r.Method != http.MethodPatch || r.URL.Path != "/deployment" || json.NewDecoder(r.Body).Decode(&patch) != nil {
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		patches <- patch
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	c := &kubeClient{baseURL: srv.URL, client: srv.Client()}
	dep := appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{UID: "dep-uid", ResourceVersion: "7"}}
	for _, tc := range []struct {
		paused                       bool
		replicas, surge, unavailable float64
	}{{true, 2, 0, 1}, {false, 1, 1, 0}} {
		intent := storageUnmountTransition{OperationID: "exact-op", RetireAllowed: !tc.paused}
		if err := setStorageUnmountDeploymentPaused(context.Background(), c, "/deployment", dep, intent, tc.paused); err != nil {
			t.Fatal(err)
		}
		patch := <-patches
		if len(patch) != 6 || patch[0]["op"] != "test" || patch[0]["value"] != "dep-uid" ||
			patch[1]["op"] != "test" || patch[1]["value"] != "7" || patch[3]["path"] != "/spec/paused" || patch[3]["value"] != tc.paused ||
			patch[4]["path"] != "/spec/replicas" || patch[4]["value"] != tc.replicas || patch[5]["path"] != "/spec/strategy" {
			t.Fatalf("unexpected Deployment hold or resume patch: %+v", patch)
		}
		rolling := patch[5]["value"].(map[string]any)["rollingUpdate"].(map[string]any)
		if rolling["maxSurge"] != tc.surge || rolling["maxUnavailable"] != tc.unavailable {
			t.Fatalf("unsafe state-specific surge envelope: %+v", rolling)
		}
		var marker storageUnmountTransition
		if err := json.Unmarshal([]byte(patch[2]["value"].(string)), &marker); err != nil || marker.OperationID != "exact-op" || marker.RetireAllowed != intent.RetireAllowed {
			t.Fatalf("traffic state was not recorded with the same write: %v", err)
		}
	}
}
