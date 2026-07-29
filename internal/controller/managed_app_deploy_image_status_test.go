package controller

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

func TestPatchManagedAppDeployImageBlockedStatusUsesLiveServingState(t *testing.T) {
	app := deployImageBlockedTestApp()
	managed := deployImageBlockedTestManagedApp(t, app)
	// Deliberately stale and over-reported. Only the live Deployment may
	// authorize a nonzero routing signal.
	managed.Status.ReadyReplicas = 7
	cause := deployImageBlockedTestCause()

	var patched runtime.ManagedAppStatus
	getCount := 0
	patchCount := 0
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch {
		case req.Method == http.MethodGet && req.URL.Path == deploymentAPIPath(managed.Metadata.Namespace, runtime.RuntimeAppResourceName(app)):
			getCount++
			return okJSONResponse(readyDeployImageBlockedTestDeploymentJSON()), nil
		case req.Method == http.MethodPatch && strings.HasSuffix(req.URL.Path, "/status"):
			patchCount++
			var body struct {
				Status runtime.ManagedAppStatus `json:"status"`
			}
			if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
				t.Fatalf("decode blocked status: %v", err)
			}
			patched = body.Status
			return okJSONResponse(`{}`), nil
		default:
			t.Fatalf("unexpected request %s %s", req.Method, req.URL.Path)
			return nil, nil
		}
	})
	client := &kubeClient{
		client:    &http.Client{Transport: transport},
		baseURL:   "http://kube.test",
		namespace: managed.Metadata.Namespace,
	}

	if err := patchManagedAppDeployImageBlockedStatus(context.Background(), client, managed.Metadata.Namespace, managed, app, cause); !errors.Is(err, cause) {
		t.Fatalf("expected original image guard error, got %v", err)
	}
	if patched.ReadyReplicas != app.Spec.Replicas {
		t.Fatalf("expected live verified replicas %d, got %d", app.Spec.Replicas, patched.ReadyReplicas)
	}
	if patched.CurrentReleaseKey != managed.Status.CurrentReleaseKey ||
		patched.CurrentReleaseStartedAt != managed.Status.CurrentReleaseStartedAt ||
		patched.CurrentReleaseReadyAt != managed.Status.CurrentReleaseReadyAt ||
		patched.PendingReleaseKey != managed.Status.PendingReleaseKey ||
		patched.PendingReleaseStartedAt != managed.Status.PendingReleaseStartedAt {
		t.Fatalf("image preflight failure lost release state: %+v", patched)
	}
	if len(patched.BackingServices) != 1 || !managedAppConditionTrue(patched.Conditions, managedAppDeployImageBlockedConditionType) {
		t.Fatalf("image preflight failure lost serving evidence: %+v", patched)
	}

	overlaid := runtime.OverlayAppStatusFromManagedApp(app, runtime.ManagedAppObject{Status: patched})
	if overlaid.Status.CurrentReplicas != 1 {
		t.Fatalf("API status overlay would take a verified serving app offline: %+v", overlaid.Status)
	}

	// An unchanged blocked status must still re-read the live Deployment before
	// suppressing a redundant status PATCH.
	managed.Status = patched
	if err := patchManagedAppDeployImageBlockedStatus(context.Background(), client, managed.Metadata.Namespace, managed, app, cause); err != nil {
		t.Fatalf("expected unchanged verified status to be idempotent, got %v", err)
	}
	if getCount != 2 || patchCount != 1 {
		t.Fatalf("expected two live reads and one status patch, got reads=%d patches=%d", getCount, patchCount)
	}
}

func TestPatchManagedAppDeployImageBlockedStatusFailsClosedWithoutLiveReadyDeployment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		response *http.Response
	}{
		{
			name: "deployment missing",
			response: deployImageBlockedTestResponse(http.StatusNotFound,
				`{"kind":"Status","status":"Failure","reason":"NotFound","code":404}`),
		},
		{
			name: "deployment unready",
			response: deployImageBlockedTestResponse(http.StatusOK, `{
				"metadata":{"name":"app-image-guard","generation":4},
				"status":{
					"observedGeneration":4,
					"replicas":1,
					"updatedReplicas":1,
					"readyReplicas":0,
					"availableReplicas":0,
					"unavailableReplicas":1
				}
			}`),
		},
		{
			name: "deployment read error",
			response: deployImageBlockedTestResponse(http.StatusInternalServerError,
				`{"kind":"Status","status":"Failure","reason":"InternalError","code":500}`),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			app := deployImageBlockedTestApp()
			managed := deployImageBlockedTestManagedApp(t, app)
			cause := deployImageBlockedTestCause()
			managed.Status.Phase = runtime.ManagedAppPhaseError
			managed.Status.Message = cause.Error()
			managed.Status.ReadyReplicas = 1
			managed.Status.Conditions = []runtime.ManagedAppCondition{{
				Type:    managedAppDeployImageBlockedConditionType,
				Status:  "True",
				Reason:  "PreflightFailed",
				Message: cause.Error(),
			}}

			patchCount := 0
			var patched runtime.ManagedAppStatus
			transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
				switch {
				case req.Method == http.MethodGet && req.URL.Path == deploymentAPIPath(managed.Metadata.Namespace, runtime.RuntimeAppResourceName(app)):
					return tt.response, nil
				case req.Method == http.MethodPatch && strings.HasSuffix(req.URL.Path, "/status"):
					patchCount++
					var body struct {
						Status runtime.ManagedAppStatus `json:"status"`
					}
					if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
						t.Fatalf("decode blocked status: %v", err)
					}
					patched = body.Status
					return okJSONResponse(`{}`), nil
				default:
					t.Fatalf("unexpected request %s %s", req.Method, req.URL.Path)
					return nil, nil
				}
			})
			client := &kubeClient{
				client:    &http.Client{Transport: transport},
				baseURL:   "http://kube.test",
				namespace: managed.Metadata.Namespace,
			}

			if err := patchManagedAppDeployImageBlockedStatus(context.Background(), client, managed.Metadata.Namespace, managed, app, cause); !errors.Is(err, cause) {
				t.Fatalf("expected original image guard error, got %v", err)
			}
			if patchCount != 1 || patched.ReadyReplicas != 0 {
				t.Fatalf("expected stale serving signal to be withdrawn, got patches=%d status=%+v", patchCount, patched)
			}
		})
	}
}

func TestReconcileManagedAppRepeatedImageBlockRevalidatesLiveDeployment(t *testing.T) {
	t.Parallel()

	app := deployImageBlockedTestApp()
	app.Source = &model.AppSource{
		Type:             model.AppSourceTypeGitHubPublic,
		RepoURL:          "https://github.com/example/image-guard",
		ResolvedImageRef: app.Spec.Image,
	}
	managed := deployImageBlockedTestManagedApp(t, app)
	cause := deployImageBlockedTestCause()
	managed.Status.Phase = runtime.ManagedAppPhaseError
	managed.Status.Message = cause.Error()
	managed.Status.ReadyReplicas = 1
	managed.Status.Conditions = []runtime.ManagedAppCondition{{
		Type:    managedAppDeployImageBlockedConditionType,
		Status:  "True",
		Reason:  "PreflightFailed",
		Message: cause.Error(),
	}}

	var patched runtime.ManagedAppStatus
	patchCount := 0
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch {
		case req.Method == http.MethodGet && req.URL.Path == deploymentAPIPath(managed.Metadata.Namespace, runtime.RuntimeAppResourceName(app)):
			return okJSONResponse(`{
				"metadata":{"name":"app-image-guard","generation":4},
				"status":{
					"observedGeneration":4,
					"replicas":1,
					"updatedReplicas":1,
					"readyReplicas":0,
					"availableReplicas":0,
					"unavailableReplicas":1
				}
			}`), nil
		case req.Method == http.MethodPatch && strings.HasSuffix(req.URL.Path, "/status"):
			patchCount++
			var body struct {
				Status runtime.ManagedAppStatus `json:"status"`
			}
			if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
				t.Fatalf("decode blocked status: %v", err)
			}
			patched = body.Status
			return okJSONResponse(`{}`), nil
		default:
			t.Fatalf("unexpected request %s %s", req.Method, req.URL.Path)
			return nil, nil
		}
	})
	client := &kubeClient{
		client:    &http.Client{Transport: transport},
		baseURL:   "http://kube.test",
		namespace: managed.Metadata.Namespace,
	}
	inspections := 0
	svc := &Service{
		Renderer:                      runtime.Renderer{},
		importImageInspectMaxAttempts: 1,
		inspectManagedImage: func(_ context.Context, imageRef string) (bool, map[string]int64, error) {
			inspections++
			if imageRef != app.Spec.Image {
				t.Fatalf("expected image inspection for %q, got %q", app.Spec.Image, imageRef)
			}
			return false, nil, nil
		},
	}

	err := svc.reconcileManagedAppResolvedObject(
		context.Background(),
		client,
		managed.Metadata.Namespace,
		managed,
		app,
		false,
		false,
		true,
	)
	if err == nil || !strings.Contains(err.Error(), "missing and needs rebuild") {
		t.Fatalf("expected repeated image preflight error, got %v", err)
	}
	if inspections != 1 || patchCount != 1 || patched.ReadyReplicas != 0 {
		t.Fatalf("expected repeated image block to revalidate and fail closed, inspections=%d patches=%d status=%+v", inspections, patchCount, patched)
	}
}

func deployImageBlockedTestApp() model.App {
	return model.App{
		ID:       "app_image_guard",
		TenantID: "tenant_image_guard",
		Name:     "image-guard",
		Spec: model.AppSpec{
			Image:     "registry.example/image-guard:v1",
			Replicas:  1,
			RuntimeID: "runtime_managed_shared",
		},
	}
}

func deployImageBlockedTestManagedApp(t *testing.T, app model.App) runtime.ManagedAppObject {
	t.Helper()
	managedMap := runtime.BuildManagedAppObject(app, runtime.SchedulingConstraints{})
	managed, err := runtime.ManagedAppObjectFromMap(managedMap)
	if err != nil {
		t.Fatalf("build managed app: %v", err)
	}
	managed.Metadata.Generation = 4
	managed.Status = runtime.ManagedAppStatus{
		Phase:                   runtime.ManagedAppPhaseReady,
		ReadyReplicas:           1,
		DesiredReplicas:         1,
		ObservedGeneration:      managed.Metadata.Generation,
		LastAppliedSpecHash:     runtime.ManagedAppSpecHash(managed.Spec),
		CurrentReleaseKey:       "release-current",
		CurrentReleaseStartedAt: "2026-07-29T03:00:00Z",
		CurrentReleaseReadyAt:   "2026-07-29T03:00:05Z",
		PendingReleaseKey:       "release-pending",
		PendingReleaseStartedAt: "2026-07-29T03:59:00Z",
		BackingServices: []runtime.ManagedBackingServiceStatus{{
			ServiceID:      "service-db",
			ReadyInstances: 1,
		}},
	}
	return managed
}

func deployImageBlockedTestCause() error {
	return errors.New("deploy blocked because managed image registry.example/image-guard:v1 is missing and needs rebuild")
}

func readyDeployImageBlockedTestDeploymentJSON() string {
	return `{
		"metadata":{"name":"app-image-guard","generation":4},
		"status":{
			"observedGeneration":4,
			"replicas":1,
			"updatedReplicas":1,
			"readyReplicas":1,
			"availableReplicas":1,
			"conditions":[{"type":"Available","status":"True","reason":"MinimumReplicasAvailable"}]
		}
	}`
}

func deployImageBlockedTestResponse(statusCode int, body string) *http.Response {
	return &http.Response{
		StatusCode: statusCode,
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     make(http.Header),
	}
}

func managedAppConditionTrue(conditions []runtime.ManagedAppCondition, conditionType string) bool {
	for _, condition := range conditions {
		if strings.EqualFold(strings.TrimSpace(condition.Type), strings.TrimSpace(conditionType)) &&
			strings.EqualFold(strings.TrimSpace(condition.Status), "True") {
			return true
		}
	}
	return false
}
