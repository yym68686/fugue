package api

import (
	"context"
	"fugue/internal/model"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHealthyRuntimeCannotHidePublic503(t *testing.T) {
	edge := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "HEAD" || r.URL.Path != "/console/" {
			t.Errorf("wrong probe %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("X-Fugue-Edge-Request-Id", "edge-probe")
		w.WriteHeader(503)
	}))
	defer edge.Close()
	server := &Server{appPublicRequestHTTPClient: edge.Client()}
	app := model.App{Spec: model.AppSpec{Replicas: 1}, Route: &model.AppRoute{PublicURL: edge.URL + "/console/"}}
	diagnosis := appDiagnosis{Category: "available", ReadyPods: 1, LivePods: 1}
	server.appendAppPublicRouteEvidence(context.Background(), app, &diagnosis)
	if diagnosis.Category != "public-route-unavailable" || !strings.Contains(diagnosis.Summary, "503") || !strings.Contains(strings.Join(diagnosis.Evidence, " "), "edge-probe") {
		t.Fatalf("false healthy result %+v", diagnosis)
	}
	diagnosis.Category = "pod-failure"
	server.appendAppPublicRouteEvidence(context.Background(), app, &diagnosis)
	if diagnosis.Category != "pod-failure" {
		t.Fatal("public route failure hid workload root cause")
	}
}
func TestPausedAndInternalAppsDoNotProbePublicRoutes(t *testing.T) {
	server := &Server{appPublicRequestHTTPClient: &http.Client{Transport: publicProbeForbidden{t}}}
	for _, spec := range []model.AppSpec{{Replicas: 0}, {Replicas: 1, NetworkMode: "internal"}, {Replicas: 1, NetworkMode: "background"}} {
		diagnosis := appDiagnosis{Category: "no-pods"}
		server.appendAppPublicRouteEvidence(context.Background(), model.App{Spec: spec, Route: &model.AppRoute{PublicURL: "https://example.test/"}}, &diagnosis)
	}
}

type publicProbeForbidden struct{ t *testing.T }

func (p publicProbeForbidden) RoundTrip(*http.Request) (*http.Response, error) {
	p.t.Fatal("unexpected public probe")
	return nil, nil
}
