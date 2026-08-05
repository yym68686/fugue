package main

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestEdgeControlRouteIntentTLSHandlerExposesOnlyExactGET(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	handler, err := edgeControlRouteIntentTLSHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("Authorization") != "Bearer exact-token" {
			t.Errorf("authorization was not preserved")
		}
		for _, name := range []string{"Forwarded", "X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto", "X-Real-Ip"} {
			if r.Header.Get(name) != "" {
				t.Errorf("untrusted forwarding header %s reached the API", name)
			}
		}
		w.WriteHeader(http.StatusNoContent)
	}), "fugue-api-tls.fugue-system.svc")
	if err != nil {
		t.Fatal(err)
	}

	request := httptest.NewRequest(http.MethodGet, "https://fugue-api-tls.fugue-system.svc:8443/v1/edge/route-intents", nil)
	request.Host = "fugue-api-tls.fugue-system.svc:8443"
	request.TLS = &tls.ConnectionState{ServerName: "fugue-api-tls.fugue-system.svc"}
	request.Header.Set("Authorization", "Bearer exact-token")
	request.Header.Set("Forwarded", "for=203.0.113.7")
	request.Header.Set("X-Forwarded-For", "203.0.113.7")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNoContent || calls.Load() != 1 {
		t.Fatalf("exact RouteIntent request was not served: status=%d calls=%d", recorder.Code, calls.Load())
	}

	for _, test := range []struct {
		method string
		target string
		host   string
		tls    bool
	}{
		{http.MethodPost, "https://fugue-api-tls.fugue-system.svc:8443/v1/edge/route-intents", "fugue-api-tls.fugue-system.svc:8443", true},
		{http.MethodGet, "https://fugue-api-tls.fugue-system.svc:8443/v1/edge/route-intents?scope=global", "fugue-api-tls.fugue-system.svc:8443", true},
		{http.MethodGet, "https://fugue-api-tls.fugue-system.svc:8443/v1/edge/routes", "fugue-api-tls.fugue-system.svc:8443", true},
		{http.MethodGet, "https://other.fugue-system.svc:8443/v1/edge/route-intents", "other.fugue-system.svc:8443", true},
		{http.MethodGet, "http://fugue-api-tls.fugue-system.svc:8443/v1/edge/route-intents", "fugue-api-tls.fugue-system.svc:8443", false},
	} {
		request := httptest.NewRequest(test.method, test.target, nil)
		request.Host = test.host
		if test.tls {
			request.TLS = &tls.ConnectionState{ServerName: "fugue-api-tls.fugue-system.svc"}
		}
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusNotFound {
			t.Errorf("%s %s host=%s tls=%t: got %d", test.method, test.target, test.host, test.tls, recorder.Code)
		}
	}
	wrongSNI := httptest.NewRequest(http.MethodGet, "https://fugue-api-tls.fugue-system.svc:8443/v1/edge/route-intents", nil)
	wrongSNI.Host = "fugue-api-tls.fugue-system.svc:8443"
	wrongSNI.TLS = &tls.ConnectionState{ServerName: "other.fugue-system.svc"}
	wrongSNIRecorder := httptest.NewRecorder()
	handler.ServeHTTP(wrongSNIRecorder, wrongSNI)
	if wrongSNIRecorder.Code != http.StatusNotFound {
		t.Fatalf("wrong SNI reached RouteIntent handler: status=%d", wrongSNIRecorder.Code)
	}
	if calls.Load() != 1 {
		t.Fatalf("rejected request reached API handler: calls=%d", calls.Load())
	}
}

func TestEdgeControlRouteIntentTLSConfigDefaultsOffAndRequiresExactContract(t *testing.T) {
	t.Parallel()
	from := func(values map[string]string) func(string) string {
		return func(key string) string { return values[key] }
	}
	defaults, err := edgeControlRouteIntentTLSConfigFromEnv(from(nil))
	if err != nil || defaults != (edgeControlRouteIntentTLSConfig{}) {
		t.Fatalf("RouteIntent TLS must default off: config=%+v err=%v", defaults, err)
	}
	configured, err := edgeControlRouteIntentTLSConfigFromEnv(from(map[string]string{
		"FUGUE_EDGE_CONTROL_ROUTE_INTENT_TLS_BIND_ADDR":      ":8443",
		"FUGUE_EDGE_CONTROL_ROUTE_INTENT_TLS_PROJECTION_DIR": "/var/run/secrets/fugue-api-tls",
		"FUGUE_EDGE_CONTROL_ROUTE_INTENT_TLS_SERVER_NAME":    "fugue-api-tls.fugue-system.svc",
	}))
	if err != nil || configured.BindAddr != ":8443" || configured.ProjectionDir != "/var/run/secrets/fugue-api-tls" || configured.ServerName != "fugue-api-tls.fugue-system.svc" {
		t.Fatalf("unexpected exact config: %+v err=%v", configured, err)
	}
	for _, values := range []map[string]string{
		{"FUGUE_EDGE_CONTROL_ROUTE_INTENT_TLS_BIND_ADDR": ":8443"},
		{"FUGUE_EDGE_CONTROL_ROUTE_INTENT_TLS_BIND_ADDR": ":9443", "FUGUE_EDGE_CONTROL_ROUTE_INTENT_TLS_PROJECTION_DIR": "/var/run/secrets/fugue-api-tls", "FUGUE_EDGE_CONTROL_ROUTE_INTENT_TLS_SERVER_NAME": "fugue-api-tls.fugue-system.svc"},
		{"FUGUE_EDGE_CONTROL_ROUTE_INTENT_TLS_BIND_ADDR": ":8443", "FUGUE_EDGE_CONTROL_ROUTE_INTENT_TLS_PROJECTION_DIR": "/tmp/tls", "FUGUE_EDGE_CONTROL_ROUTE_INTENT_TLS_SERVER_NAME": "fugue-api-tls.fugue-system.svc"},
		{"FUGUE_EDGE_CONTROL_ROUTE_INTENT_TLS_BIND_ADDR": ":8443", "FUGUE_EDGE_CONTROL_ROUTE_INTENT_TLS_PROJECTION_DIR": "/var/run/secrets/fugue-api-tls", "FUGUE_EDGE_CONTROL_ROUTE_INTENT_TLS_SERVER_NAME": "other.fugue-system.svc"},
	} {
		if _, err := edgeControlRouteIntentTLSConfigFromEnv(from(values)); err == nil {
			t.Fatalf("invalid partial or drifted contract was accepted: %+v", values)
		}
	}
}
