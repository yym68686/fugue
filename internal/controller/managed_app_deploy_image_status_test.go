package controller

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

func TestPatchManagedAppDeployImageBlockedStatusPreservesServingState(t *testing.T) {
	app := model.App{
		ID:       "app_image_guard",
		TenantID: "tenant_image_guard",
		Name:     "image-guard",
		Spec: model.AppSpec{
			Image:     "registry.example/image-guard:v1",
			Replicas:  1,
			RuntimeID: "runtime_managed_shared",
		},
	}
	managedMap := runtime.BuildManagedAppObject(app, runtime.SchedulingConstraints{})
	managed, err := runtime.ManagedAppObjectFromMap(managedMap)
	if err != nil {
		t.Fatalf("build managed app: %v", err)
	}
	managed.Status = runtime.ManagedAppStatus{
		Phase:                   runtime.ManagedAppPhaseReady,
		ReadyReplicas:           1,
		CurrentReleaseKey:       "release-current",
		CurrentReleaseStartedAt: "2026-07-29T03:00:00Z",
		CurrentReleaseReadyAt:   "2026-07-29T03:00:05Z",
		PendingReleaseKey:       "release-pending",
		PendingReleaseStartedAt: "2026-07-29T03:59:00Z",
		BackingServices: []runtime.ManagedBackingServiceStatus{{
			ServiceID:      "service-db",
			ReadyInstances: 1,
		}},
		Conditions: []runtime.ManagedAppCondition{{
			Type:   "Available",
			Status: "True",
		}},
	}
	cause := errors.New("deploy blocked because managed image registry.example/image-guard:v1 is missing and needs rebuild")

	var patched runtime.ManagedAppStatus
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.Method != http.MethodPatch || !strings.HasSuffix(req.URL.Path, "/status") {
			t.Fatalf("unexpected request %s %s", req.Method, req.URL.Path)
		}
		var body struct {
			Status runtime.ManagedAppStatus `json:"status"`
		}
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			t.Fatalf("decode blocked status: %v", err)
		}
		patched = body.Status
		return okJSONResponse(`{}`), nil
	})
	client := &kubeClient{
		client:    &http.Client{Transport: transport},
		baseURL:   "http://kube.test",
		namespace: managed.Metadata.Namespace,
	}

	if err := patchManagedAppDeployImageBlockedStatus(context.Background(), client, managed.Metadata.Namespace, managed, app, cause); !errors.Is(err, cause) {
		t.Fatalf("expected original image guard error, got %v", err)
	}
	if patched.ReadyReplicas != managed.Status.ReadyReplicas {
		t.Fatalf("image preflight failure cleared serving replicas: got %d want %d", patched.ReadyReplicas, managed.Status.ReadyReplicas)
	}
	if patched.CurrentReleaseKey != managed.Status.CurrentReleaseKey ||
		patched.CurrentReleaseStartedAt != managed.Status.CurrentReleaseStartedAt ||
		patched.CurrentReleaseReadyAt != managed.Status.CurrentReleaseReadyAt ||
		patched.PendingReleaseKey != managed.Status.PendingReleaseKey ||
		patched.PendingReleaseStartedAt != managed.Status.PendingReleaseStartedAt {
		t.Fatalf("image preflight failure lost release state: %+v", patched)
	}
	if len(patched.BackingServices) != 1 || len(patched.Conditions) != 2 {
		t.Fatalf("image preflight failure lost serving evidence: %+v", patched)
	}
	if !managedAppDeployImageBlockedStatusCurrent(patched, cause) {
		t.Fatalf("next reconcile must recognize the same image block without another status write: %+v", patched)
	}

	overlaid := runtime.OverlayAppStatusFromManagedApp(app, runtime.ManagedAppObject{Status: patched})
	if overlaid.Status.CurrentReplicas != 1 {
		t.Fatalf("API status overlay would take a serving app offline: %+v", overlaid.Status)
	}
}
