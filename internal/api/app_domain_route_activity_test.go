package api

import (
	"encoding/json"
	"net/http"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestDomainRouteActivityRequiresIndependentRuntimeEvidence(t *testing.T) {
	now := time.Now().UTC()
	yes := true
	one := 1
	base := model.App{Spec: model.AppSpec{Replicas: 1}, ObservedStatus: &model.AppObservedStatus{Phase: "deployed", Fresh: true, ObservedAt: now, ClusterID: "cluster", EvidenceSource: "runtime", Generation: 2, ObservedGeneration: 2, DesiredReplicas: 1, RuntimeObjectPresent: &yes, NamespacePresent: &yes, EndpointPresent: &yes, EndpointReady: &yes, ImagePresent: &yes, ReadyReplicas: &one, PhysicalReplicas: &one, PhysicalDesired: &one}}
	for _, tc := range []struct {
		name, want    string
		configuration bool
		change        func(*model.App)
	}{
		{"ready", "pass", true, func(*model.App) {}},
		{"DNS or TLS missing", "fail", false, func(*model.App) {}},
		{"stopped with historical ready", "fail", true, func(a *model.App) { a.Spec.Replicas = 0 }},
		{"missing observation", "unknown", true, func(a *model.App) { a.ObservedStatus = nil }},
		{"stale observation", "unknown", true, func(a *model.App) { a.ObservedStatus.ObservedAt = now.Add(-24 * time.Hour) }},
		{"missing freshness", "unknown", true, func(a *model.App) { a.ObservedStatus.Fresh = false }},
		{"old generation", "fail", true, func(a *model.App) { a.ObservedStatus.ObservedGeneration = 1 }},
		{"unknown cluster", "fail", true, func(a *model.App) { a.ObservedStatus.ClusterID = "" }},
		{"missing endpoint", "fail", true, func(a *model.App) { a.ObservedStatus.EndpointReady = nil }},
		{"unavailable", "fail", true, func(a *model.App) { a.ObservedStatus.Phase = "unavailable" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, _ := json.Marshal(base)
			var app model.App
			json.Unmarshal(raw, &app)
			tc.change(&app)
			before, _ := json.Marshal(app)
			check := appDomainRouteActivityCheck(app, tc.configuration, now)
			if check.Status != tc.want {
				t.Fatal(check)
			}
			after, _ := json.Marshal(app)
			if !reflect.DeepEqual(before, after) {
				t.Fatal("diagnosis changed runtime evidence")
			}
		})
	}
}

func TestStoppedDomainDiagnosisKeepsTLSReadyWithoutClaimingActiveRoute(t *testing.T) {
	state, server, key, _, app, resolver := setupAppDomainTestServerWithDomains(t, "example.test")
	host := "stopped.customer.test"
	resolver.cname[host] = server.primaryCustomDomainTarget(app) + "."
	r := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/domains", key, map[string]any{"hostname": host})
	if r.Code != 200 {
		t.Fatal(r.Body.String())
	}
	cert, private, metadata := generateTestTLSCertificateBundle(t, host)
	r = performJSONRequest(t, server, http.MethodPut, "/v1/edge/domains/"+host+"/tls-bundle?token=edge-secret", "", map[string]any{"certificate_pem": cert, "private_key_pem": private, "metadata_json": metadata, "issuer_storage": "issuer"})
	if r.Code != 200 {
		t.Fatal(r.Body.String())
	}
	// A pure diagnosis of stopped intent must not need a runtime refresh or
	// modify the existing valid domain/certificate to correct its status.
	domain, err := state.GetAppDomain(host)
	if err != nil {
		t.Fatal(err)
	}
	app.Spec.Replicas = 0
	before, _ := json.Marshal(domain)
	diagnosis := server.buildAppDomainDiagnosis(t.Context(), app, domain)
	checks := map[string]string{}
	for _, check := range diagnosis.Checks {
		checks[check.Name] = check.Status
	}
	if checks["tls_ready"] != "pass" || checks["shared_tls_certificate"] != "pass" || checks["route_active"] != "fail" {
		t.Fatal(checks)
	}
	after, err := state.GetAppDomain(host)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(after)
	if !reflect.DeepEqual(before, raw) {
		t.Fatal("route diagnosis modified TLS domain history")
	}
}
