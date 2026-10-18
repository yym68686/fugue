package api

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

type runtimeReadEvidence struct{ inspected map[string]bool }

func (e *runtimeReadEvidence) Run(_ context.Context, _, pod, _ string, _ []byte, command ...string) ([]byte, error) {
	if len(command) != 4 || command[0] != "head" || command[1] != "-c" || command[2] != "1048577" {
		return nil, fmt.Errorf("unbounded evidence command")
	}
	e.inspected[pod] = true
	switch command[3] {
	case "/proc/1/environ":
		return []byte("PROBE=release-value\x00"), nil
	case "/config":
		return []byte("private-config-marker"), nil
	default:
		return nil, fmt.Errorf("no evidence")
	}
}

func TestRuntimeStatePreservesReferencedSnapshotsAndMissingEvidence(t *testing.T) {
	spec := model.AppSpec{Image: "registry.example/app@sha256:" + strings.Repeat("a", 64), RuntimeID: model.DefaultManagedRuntimeID, Ports: []int{8080}, Replicas: 1}
	st, server, key, app := setupAppConfigTestServer(t, spec)
	create := func(appID, role, status string) model.AppRelease {
		r, err := st.CreateAppRelease(model.AppRelease{TenantID: app.TenantID, AppID: appID, Role: role, Status: status, SpecSnapshot: &model.AppSpec{Image: spec.Image, Replicas: 1, Env: map[string]string{"PROBE": "release-value"}, Files: []model.AppFile{{Path: "/config", Content: "private-config-marker", Secret: true}}}})
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	current := create(app.ID, model.AppReleaseRoleStable, model.AppReleaseStatusServing)
	retired := create(app.ID, model.AppReleaseRoleRetired, model.AppReleaseStatusRetired)
	foreign := create("other-app", model.AppReleaseRoleStable, model.AppReleaseStatusServing)
	create(app.ID, model.AppReleaseRolePrevious, model.AppReleaseStatusServing)
	selector, container, err := runtimeLogTarget(app, "app")
	if err != nil {
		t.Fatal(err)
	}
	pods := []kubePodInfo{}
	for i, id := range []string{current.ID, current.ID, retired.ID, foreign.ID, "missing-release", "", current.ID} {
		pod := fakePod(fmt.Sprintf("pod-%02d", i), "Running", time.Now().UTC(), container)
		pod.Metadata.UID = fmt.Sprintf("uid-%02d", i)
		pod.Metadata.Labels = map[string]string{runtime.FugueLabelAppReleaseID: id}
		pod.Status.ContainerStatuses = []kubeContainerStatus{{Name: container, Ready: i != 6, ImageID: "containerd://sha256:" + strings.Repeat("a", 64)}}
		pods = append(pods, pod)
	}
	client := newFakeAppLogsClient()
	client.setPods(selector, pods)
	server.newLogsClient = func(string) (appLogsClient, error) { return client, nil }
	evidence := &runtimeReadEvidence{inspected: map[string]bool{}}
	server.filesystemExecRunner = evidence
	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/runtime-state", key, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var result model.AppRuntimeState
	mustDecodeJSON(t, recorder, &result)
	if len(result.ReadyPods) != 6 || result.PendingOperations == nil || len(result.PendingOperations) != 0 {
		t.Fatalf("inventory or empty pending response changed: %+v", result)
	}
	if result.State != "inconclusive" {
		t.Fatalf("missing immutable snapshots must not pass: %s", result.State)
	}
	unknown := map[string]bool{}
	validFiles := 0
	for _, check := range result.Checks {
		if check.Kind == "release_spec" && check.State == "unknown" {
			unknown[check.Pod] = true
		}
		if check.Kind == "file" && check.State == "in_sync" {
			validFiles++
		}
	}
	if len(unknown) != 3 || !unknown["pod-02"] || !unknown["pod-03"] || !unknown["pod-04"] || validFiles != 2 {
		t.Fatalf("release eligibility/snapshot checks changed: unknown=%v valid_files=%d", unknown, validFiles)
	}
	if len(evidence.inspected) != 3 || !evidence.inspected["pod-00"] || !evidence.inspected["pod-01"] || !evidence.inspected["pod-05"] {
		t.Fatalf("unexpected runtime inspection membership: %v", evidence.inspected)
	}
	for _, secret := range []string{"release-value", "private-config-marker"} {
		if strings.Contains(recorder.Body.String(), secret) {
			t.Fatal("runtime response exposed snapshot values")
		}
	}

	// The complete ready inventory remains visible, but only the sorted first
	// sixteen Pods may consume runtime evidence or referenced snapshots.
	pods = nil
	for i := 17; i >= 0; i-- {
		pod := fakePod(fmt.Sprintf("pod-%02d", i), "Running", time.Now().UTC(), container)
		pod.Metadata.UID = fmt.Sprintf("uid-%02d", i)
		pod.Metadata.Labels = map[string]string{runtime.FugueLabelAppReleaseID: current.ID}
		pod.Status.ContainerStatuses = []kubeContainerStatus{{Name: container, Ready: true, ImageID: "containerd://sha256:" + strings.Repeat("a", 64)}}
		pods = append(pods, pod)
	}
	client.setPods(selector, pods)
	evidence.inspected = map[string]bool{}
	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/runtime-state", key, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d", recorder.Code)
	}
	mustDecodeJSON(t, recorder, &result)
	if len(result.ReadyPods) != 18 || !slices.Contains(result.MissingEvidence, "pod_inspection_limit") || len(evidence.inspected) != 16 || evidence.inspected["pod-16"] || evidence.inspected["pod-17"] {
		t.Fatalf("bounded evidence or complete inventory changed: ready=%d inspected=%v missing=%v", len(result.ReadyPods), evidence.inspected, result.MissingEvidence)
	}
}
