package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/livediagnostics"
)

type fakeRegisteredDiagnosticBackend struct {
	fakePlatformDiagnosticBackend
	catalog livediagnostics.VerifiedCatalog
}

func (f *fakeRegisteredDiagnosticBackend) DiagnosticCatalog(context.Context) (livediagnostics.VerifiedCatalog, error) {
	return f.catalog, nil
}

func registeredDiagnosticFixture(t *testing.T) (*Server, *fakeRegisteredDiagnosticBackend, string) {
	t.Helper()
	state, server, tenantKey, _ := setupAppConfigTestServer(t, appObservabilityTestSpec())
	server.auth = auth.New(state, "bootstrap-secret")
	server.controlPlaneNamespace = "system-test"
	server.controlPlaneReleaseInstance = "platform-test"
	image := "registry.example/observation@sha256:" + strings.Repeat("b", 64)
	catalog := livediagnostics.Catalog{Protocol: livediagnostics.CatalogProtocol, Generation: 1, RunnerImage: image, Policy: livediagnostics.CatalogPolicy{Namespaces: []string{"system-test"}, Profiles: []string{"cluster-read"}, ServiceAccount: "diagnostic-reader"}, Probes: []livediagnostics.Probe{{ID: "object-observer", Image: image, Profile: "cluster-read", TargetTypes: []livediagnostics.TargetType{livediagnostics.TargetPlatformComponent}, MaxDurationSeconds: 60, Parameters: map[string]livediagnostics.Parameter{}, Config: json.RawMessage(`{"collectors":[]}`)}}}
	backend := &fakeRegisteredDiagnosticBackend{fakePlatformDiagnosticBackend: fakePlatformDiagnosticBackend{namespace: "system-test"}, catalog: livediagnostics.VerifiedCatalog{Catalog: catalog, Digest: "catalog-test", Status: "current"}}
	var pod kubePodInfo
	pod.Metadata.Name = "operator-test"
	pod.Metadata.UID = "operator-uid"
	pod.Metadata.Labels = map[string]string{"app.kubernetes.io/instance": "independent-operator"}
	pod.Spec.NodeName = "node-test"
	pod.Status.Phase = "Running"
	pod.Spec.Containers = append(pod.Spec.Containers, struct {
		Name  string `json:"name"`
		Image string `json:"image,omitempty"`
	}{Name: "manager", Image: image})
	pod.Status.ContainerStatuses = []kubeContainerStatus{{Name: "manager", Ready: false, State: kubeRuntimeState{Running: &struct{}{}}, ContainerID: "containerd://0123456789abcdef", ImageID: image}}
	backend.pods = []kubePodInfo{pod}
	server.diagnosticSessionBackend = backend
	return server, backend, tenantKey
}
func TestRegisteredDiagnosticUsesApprovedImageAndExactRunningPod(t *testing.T) {
	server, backend, _ := registeredDiagnosticFixture(t)
	response := performJSONRequest(t, server, http.MethodPost, "/v1/admin/diagnostics/sessions", "bootstrap-secret", map[string]any{"probe_ref": "object-observer", "target": map[string]any{"type": "platform_component", "namespace": "system-test", "pod": "operator-test"}, "duration_seconds": 15})
	if response.Code != http.StatusAccepted {
		t.Fatalf("registered probe failed: %d %s", response.Code, response.Body.String())
	}
	if backend.created.Spec.Template.Spec.Containers[0].Image != backend.catalog.Probes[0].Image || backend.created.Annotations[livediagnostics.ProbeDigestAnnotation] != backend.catalog.Probes[0].Digest() {
		t.Fatal("probe was not pinned to the independent package")
	}
	if backend.created.Annotations[livediagnostics.TargetPodUIDAnnotation] != "operator-uid" {
		t.Fatal("third-party target identity was not frozen")
	}
	if backend.created.Spec.Template.Spec.HostPID {
		t.Fatal("object observer acquired host process access")
	}
}
func TestRegisteredDiagnosticCatalogRemainsAdminOnly(t *testing.T) {
	server, backend, tenantKey := registeredDiagnosticFixture(t)
	response := performJSONRequest(t, server, http.MethodGet, "/v1/admin/diagnostics/probes", tenantKey, nil)
	if response.Code != http.StatusForbidden {
		t.Fatalf("tenant catalog read: %d %s", response.Code, response.Body.String())
	}
	response = performJSONRequest(t, server, http.MethodGet, "/v1/admin/diagnostics/probes", "bootstrap-secret", nil)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), backend.catalog.Probes[0].Digest()) {
		t.Fatalf("catalog unavailable: %d %s", response.Code, response.Body.String())
	}
	response = performJSONRequest(t, server, http.MethodPost, "/v1/admin/diagnostics/sessions", "bootstrap-secret", map[string]any{"probe_ref": "object-observer", "kind": "cpu-profile", "target": map[string]any{"type": "platform_component", "pod": "operator-test"}})
	if response.Code != http.StatusBadRequest {
		t.Fatalf("ambiguous probe selector accepted: %d", response.Code)
	}
	response = performJSONRequest(t, server, http.MethodPost, "/v1/admin/diagnostics/sessions", "bootstrap-secret", map[string]any{"probe_ref": "object-observer", "target": map[string]any{"type": "platform_component", "namespace": "tenant-other", "pod": "operator-test"}})
	if response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), "not authorized") {
		t.Fatalf("namespace policy bypassed: %d %s", response.Code, response.Body.String())
	}
}

func TestDiagnosticEvidencePreservesLargeIntegerCounters(t *testing.T) {
	report, err := decodeDiagnosticReport(`{"counter":9007199254740993}`)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != `{"counter":9007199254740993}` {
		t.Fatalf("diagnostic counter was rounded: %s", encoded)
	}
}

func TestDiagnosticReportReadBoundsTheTransportBeforeDecode(t *testing.T) {
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("limitBytes") == "" {
			t.Error("missing source-side report bound")
		}
		_, _ = w.Write([]byte(strings.Repeat("x", diagnosticMaxReportBytes+1024)))
	}))
	defer source.Close()
	backend := kubeDiagnosticSessionBackend{cluster: &clusterNodeClient{baseURL: source.URL, client: source.Client()}}
	if _, err := backend.ReadPodLogs(context.Background(), "system-test", "diagnostic-test", "diagnostic-agent"); err == nil {
		t.Fatal("oversized source was read without a bound")
	}
}
