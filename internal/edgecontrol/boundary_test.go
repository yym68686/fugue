package edgecontrol

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestBoundaryStatusHasNoAuthorityOrMutationCapability(t *testing.T) {
	t.Parallel()

	boundary := NewBoundary(true)
	for _, path := range []string{"/healthz", "/readyz", "/v1/status"} {
		recorder := httptest.NewRecorder()
		boundary.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		if recorder.Code != http.StatusOK {
			t.Fatalf("GET %s status = %d body=%s", path, recorder.Code, recorder.Body.String())
		}
		var status BoundaryStatus
		if err := json.Unmarshal(recorder.Body.Bytes(), &status); err != nil {
			t.Fatalf("decode GET %s: %v", path, err)
		}
		if status.Schema != BoundarySchemaV1 || status.Mode != "boundary-only" || status.Authority != "none" ||
			!status.Enabled || status.PublicationEnabled || status.DataPlaneDependency || status.DatabaseCapability ||
			status.KubernetesCapability || status.BundleSignerCapability {
			t.Fatalf("unsafe boundary status for %s: %+v", path, status)
		}
	}
}

func TestBoundaryExposesNoMutationEndpoint(t *testing.T) {
	t.Parallel()

	handler := NewBoundary(false).Handler()
	for _, request := range []*http.Request{
		httptest.NewRequest(http.MethodPost, "/v1/status", strings.NewReader("{}")),
		httptest.NewRequest(http.MethodPut, "/v1/authority", strings.NewReader("{}")),
		httptest.NewRequest(http.MethodDelete, "/v1/bundles/current", nil),
	} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusMethodNotAllowed && recorder.Code != http.StatusNotFound {
			t.Fatalf("%s %s unexpectedly returned %d", request.Method, request.URL.Path, recorder.Code)
		}
	}
}

func TestBoundaryMetricsAreStaticAndNonAuthoritative(t *testing.T) {
	t.Parallel()

	recorder := httptest.NewRecorder()
	NewBoundary(true).Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), `authority="none",mode="boundary-only"`) {
		t.Fatalf("unexpected metrics response: status=%d body=%q", recorder.Code, recorder.Body.String())
	}
}

func TestShadowBoundaryRemainsNonAuthoritative(t *testing.T) {
	t.Parallel()

	boundary := NewShadowBoundary(true)
	status := boundary.Status("ready")
	if status.Mode != "shadow-only" || status.Authority != "none" || status.PublicationEnabled || status.DataPlaneDependency || status.DatabaseCapability || status.KubernetesCapability || status.BundleSignerCapability {
		t.Fatalf("shadow runtime boundary gained authority: %+v", status)
	}
	recorder := httptest.NewRecorder()
	boundary.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), `authority="none",mode="shadow-only"`) {
		t.Fatalf("unexpected shadow metrics: status=%d body=%q", recorder.Code, recorder.Body.String())
	}
}
