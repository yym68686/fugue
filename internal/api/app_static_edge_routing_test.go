package api

import (
	"fugue/internal/model"
	"net/http"
	"strings"
	"testing"
)

func TestAppStaticDNSRequiresAdminRegistrationAndCAS(t *testing.T) {
	s, server, tenantKey, adminKey, app, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	if _, _, err := s.CreateEdgeNodeToken(model.EdgeNode{ID: "edge-a", EdgeGroupID: "edge-group-a", PublicIPv4: "8.8.8.8"}); err != nil {
		t.Fatal(err)
	}
	reg, err := s.CreateStaticEdgeRegistration(model.StaticEdgeRegistration{TenantID: app.TenantID, ProjectID: app.ProjectID, Name: "independent", EdgeID: "static-a", Transport: "mtls", ManagerURL: "https://1.1.1.1:9443", CertificateFingerprint: strings.Repeat("b", 64), SigningKeyID: "signer", PossessionProofDigest: "sha256:" + strings.Repeat("c", 64)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.UpdateStaticEdgeRegistrationProof(reg.ID, app.TenantID, "sha256:"+strings.Repeat("d", 64), "signer", true); err != nil {
		t.Fatal(err)
	}
	routing := &model.AppStaticEdgeRouting{Version: model.StaticEdgeDNSVersion, Candidates: []model.StaticEdgeDNSCandidate{{RegistrationID: reg.ID, EdgeID: "static-a", Addresses: []string{"1.1.1.1"}, ProbePath: "/health", RouteDigest: "sha256:" + strings.Repeat("e", 64), StatusCode: 200}}, Availability: model.DNSAvailabilityPolicy{Version: model.DNSAvailabilityVersion, OrderedEdgeIDs: []string{"static-a", "edge-a"}, RecoverySuccesses: 2, RecoveryMinSeconds: 10}}
	path := "/v1/apps/" + app.ID + "/route/static-edge"
	body := map[string]any{"expected_revision": model.AppRouteRevision(app.Route), "routing": routing}
	if response := performJSONRequest(t, server, http.MethodPut, path, tenantKey, body); response.Code != 403 {
		t.Fatal("non-admin changed platform DNS", response.Code, response.Body.String())
	}
	response := performJSONRequest(t, server, http.MethodPut, path, adminKey, body)
	if response.Code != 200 {
		t.Fatal(response.Code, response.Body.String())
	}
	if response := performJSONRequest(t, server, http.MethodPut, path, adminKey, body); response.Code != 409 {
		t.Fatal("stale CAS accepted", response.Code)
	}
	updated, err := s.GetApp(app.ID)
	if err != nil || updated.Route.DNSRouting == nil {
		t.Fatal("routing not persisted", err)
	}
	if updated.ProjectID != app.ProjectID || updated.Spec.Image != app.Spec.Image {
		t.Fatal("DNS write changed application")
	}
	response = performJSONRequest(t, server, http.MethodPut, path, adminKey, map[string]any{"expected_revision": model.AppRouteRevision(updated.Route)})
	if response.Code != 200 {
		t.Fatal("restore failed", response.Code, response.Body.String())
	}
	updated, _ = s.GetApp(app.ID)
	if updated.Route.DNSRouting != nil {
		t.Fatal("restore retained static authority")
	}
	routing.Candidates[0].RegistrationID = "foreign-registration"
	response = performJSONRequest(t, server, http.MethodPut, path, adminKey, map[string]any{"expected_revision": model.AppRouteRevision(updated.Route), "routing": routing})
	if response.Code != 400 {
		t.Fatal("foreign registration accepted", response.Code)
	}
	if _, err = s.UpdateAppRoute(app.ID, model.AppRoute{Hostname: app.Route.Hostname, DNSRouting: routing}); err == nil {
		t.Fatal("generic route update bypassed dedicated authorization")
	}
}
