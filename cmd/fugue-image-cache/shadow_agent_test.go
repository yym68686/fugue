package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestImageCacheProcessModeIsExplicit(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		args []string
		want string
		ok   bool
	}{
		{name: "legacy default", want: "", ok: true},
		{name: "shadow agent", args: []string{"platform-plan-shadow"}, want: imageCacheProcessModePlatformPlanShadow, ok: true},
		{name: "unknown", args: []string{"serve"}},
		{name: "extra", args: []string{"platform-plan-shadow", "extra"}},
		{name: "whitespace", args: []string{" platform-plan-shadow "}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := imageCacheProcessMode(test.args)
			if (err == nil) != test.ok || got != test.want {
				t.Fatalf("imageCacheProcessMode(%q) = %q, %v; want %q ok=%t", test.args, got, err, test.want, test.ok)
			}
		})
	}
}

func TestImageCachePlatformPlanAgentListenAddressIsLoopbackOnly(t *testing.T) {
	t.Parallel()
	for _, valid := range []string{"127.0.0.1:5001", "127.0.0.2:65535", "[::1]:5001"} {
		if err := validateImageCachePlatformPlanAgentListenAddress(valid); err != nil {
			t.Fatalf("valid address %q rejected: %v", valid, err)
		}
	}
	for _, invalid := range []string{"", ":5001", "0.0.0.0:5001", "[::]:5001", "localhost:5001", "192.0.2.10:5001", "127.0.0.1", "127.0.0.1:0", "127.0.0.1:443", "127.0.0.1:65536"} {
		if err := validateImageCachePlatformPlanAgentListenAddress(invalid); err == nil {
			t.Fatalf("unsafe address %q accepted", invalid)
		}
	}
}

func TestImageCachePlatformPlanAgentExposesOnlyCredentialFreeHealth(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	consumer := &imageCachePlatformPlanConsumer{status: imageCachePlatformPlanStatus{
		Enabled:           true,
		ObservationOnly:   true,
		State:             "observed",
		Generation:        "generation-7",
		LastObservationAt: &now,
	}}
	handler := &imageCachePlatformPlanAgentHandler{owner: &imageCache{
		clusterNode:  "worker-a",
		platformPlan: consumer,
	}}

	for _, path := range []string{"/healthz", "/readyz", "/fugue/cache/v1/platform-plan/readyz"} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "http://image-plane.test"+path, nil))
		if recorder.Code != http.StatusOK || recorder.Header().Get("Cache-Control") != "private, no-store, max-age=0" {
			t.Fatalf("GET %s = %d headers=%v body=%s", path, recorder.Code, recorder.Header(), recorder.Body.String())
		}
	}

	health := httptest.NewRecorder()
	handler.ServeHTTP(health, httptest.NewRequest(http.MethodGet, "http://image-plane.test/fugue/cache/v1/health", nil))
	if health.Code != http.StatusOK {
		t.Fatalf("agent health=%d body=%s", health.Code, health.Body.String())
	}
	var payload struct {
		Status       string                       `json:"status"`
		Mode         string                       `json:"mode"`
		ClusterNode  string                       `json:"cluster_node"`
		PlatformPlan imageCachePlatformPlanStatus `json:"platform_plan_shadow"`
	}
	if err := json.Unmarshal(health.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode agent health: %v", err)
	}
	if payload.Status != "ok" || payload.Mode != imageCacheProcessModePlatformPlanShadow || payload.ClusterNode != "worker-a" ||
		!payload.PlatformPlan.Enabled || !payload.PlatformPlan.ObservationOnly || payload.PlatformPlan.State != "observed" ||
		payload.PlatformPlan.Generation != "generation-7" {
		t.Fatalf("unexpected agent health: %+v", payload)
	}
	if strings.Contains(health.Body.String(), "token") || strings.Contains(health.Body.String(), "credential_file") {
		t.Fatalf("agent health leaked credential material: %s", health.Body.String())
	}

	for _, path := range []string{"/v2/", "/fugue/cache/v1/inventory", "/fugue/cache/v1/prune", "/metrics", "/"} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "http://image-plane.test"+path, nil))
		if recorder.Code != http.StatusNotFound {
			t.Fatalf("agent exposed forbidden path %s with status %d", path, recorder.Code)
		}
	}
	method := httptest.NewRecorder()
	handler.ServeHTTP(method, httptest.NewRequest(http.MethodPost, "http://image-plane.test/healthz", nil))
	if method.Code != http.StatusMethodNotAllowed || method.Header().Get("Allow") != http.MethodGet {
		t.Fatalf("agent accepted non-GET health request: %d headers=%v", method.Code, method.Header())
	}
}

func TestImageCachePlatformPlanAgentHealthSeparatesLivenessFromReadiness(t *testing.T) {
	t.Parallel()
	handler := &imageCachePlatformPlanAgentHandler{owner: &imageCache{platformPlanErr: "invalid configuration"}}
	liveness := httptest.NewRecorder()
	handler.ServeHTTP(liveness, httptest.NewRequest(http.MethodGet, "http://image-plane.test/healthz", nil))
	if liveness.Code != http.StatusOK {
		t.Fatalf("configuration error failed agent liveness: %d", liveness.Code)
	}
	readiness := httptest.NewRecorder()
	handler.ServeHTTP(readiness, httptest.NewRequest(http.MethodGet, "http://image-plane.test/readyz", nil))
	if readiness.Code != http.StatusServiceUnavailable {
		t.Fatalf("configuration error readiness=%d, want 503", readiness.Code)
	}
	health := httptest.NewRecorder()
	handler.ServeHTTP(health, httptest.NewRequest(http.MethodGet, "http://image-plane.test/fugue/cache/v1/health", nil))
	if !strings.Contains(health.Body.String(), `"state":"configuration_error"`) || !strings.Contains(health.Body.String(), `"observation_only":true`) {
		t.Fatalf("configuration error was not explicit in agent health: %s", health.Body.String())
	}
}
