package api

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformcontrol"
)

func TestCellCertificateReferenceAllowsSharedHostnamePlatformPaths(t *testing.T) {
	host := "shared.example.test"
	artifact := model.PlatformArtifact{Content: map[string]any{"certificates": []any{map[string]any{"hostname": host, "policy": "custom-domain", "app_id": "app-one", "tenant_id": "tenant-one"}}}}
	bundle := model.EdgeRouteBundle{Routes: []model.EdgeRouteBinding{
		{Hostname: host, PathPrefix: "/api", TLSPolicy: "platform", RoutePolicy: model.EdgeRoutePolicyEnabled},
		{Hostname: host, PathPrefix: "/", TLSPolicy: "custom-domain", AppID: "app-one", TenantID: "tenant-one", RoutePolicy: model.EdgeRoutePolicyEnabled},
	}, TLSAllowlist: []model.EdgeTLSAllowlistEntry{{Hostname: host, Status: model.AppDomainStatusVerified, AppID: "app-one", TenantID: "tenant-one"}}}
	if _, err := certificateReferenceForRoutes(artifact, bundle, host); err != nil {
		t.Fatal("unrelated platform path invalidated the signed custom-domain owner", err)
	}
	bundle.Routes[1].TenantID = "foreign"
	if _, err := certificateReferenceForRoutes(artifact, bundle, host); err == nil {
		t.Fatal("foreign custom-domain route owner accepted")
	}
}

func assertCellCertificateAccess(t *testing.T, s *Server, app model.App, parent model.PlatformArtifact, release model.PlatformArtifactRelease, allowed bool) {
	t.Helper()
	s.auth.PlatformComponentIdentityKeyring = edgeRouteIntentTestKeyring()
	sets, err := s.store.ListPlatformExpectedConsumerSets(model.PlatformExpectedConsumerSetFilter{ReleaseSetID: parent.ID, ArtifactReleaseID: release.ID, ArtifactKind: model.PlatformArtifactKindCaddyRouteConfig})
	if err != nil || len(sets) != 1 {
		t.Fatal("TLS expectation", err)
	}
	child, err := s.consumerAssignmentChild(parent, model.PlatformArtifactKindCaddyRouteConfig)
	if err != nil {
		t.Fatal(err)
	}
	base := "/v1/platform-state/consumers/artifacts/" + child.ID + "/tls/"
	path := base + "private.example.net?expected_consumer_set_id=" + sets[0].ID
	issue := func(component, node, cell string) string {
		t.Helper()
		token, err := platformcontrol.IssuePlatformComponentIdentity(edgeRouteIntentTestKeyring(), platformcontrol.PlatformComponentIdentityClaims{CredentialID: "cell-certificate-worker", Component: component, NodeID: node, AuthorityID: cell, ScopeKey: "authority-cell:" + cell, ArtifactKinds: []string{model.PlatformArtifactKindCaddyRouteConfig}}, time.Now().UTC(), time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	token := issue(model.PlatformConsumerComponentEdgeWorker, "edge-a", "cell-a")
	read := func(token, path string, status int) {
		t.Helper()
		response := performJSONRequest(t, s, http.MethodGet, path, token, nil)
		if response.Code != status {
			t.Fatalf("certificate response %d want %d: %s", response.Code, status, response.Body.String())
		}
		if response.Header().Get("Cache-Control") != "private, no-store" {
			t.Fatal("certificate response is cacheable")
		}
		if strings.Contains(response.Body.String(), "synthetic-private-key") != (status == http.StatusOK) {
			t.Fatal("certificate disclosure does not match authorization")
		}
	}
	if !allowed {
		read(token, path, http.StatusNotFound)
		return
	}
	read(token, path, http.StatusOK)
	read(token, path+"&expected_consumer_set_id="+sets[0].ID, http.StatusBadRequest)
	read(token, base+"foreign.example.net?expected_consumer_set_id="+sets[0].ID, http.StatusNotFound)
	read(token, base+"private.example.net?expected_consumer_set_id=foreign", http.StatusNotFound)
	read(issue(model.PlatformConsumerComponentEdgeWorker, "edge-other", "cell-a"), path, http.StatusNotFound)
	read(issue(model.PlatformConsumerComponentEdgeWorker, "edge-a", "cell-other"), path, http.StatusNotFound)
	domain, err := s.store.GetAppDomain("private.example.net")
	if err != nil {
		t.Fatal(err)
	}
	changed := domain
	changed.Status = model.AppDomainStatusPending
	if _, err := s.store.PutAppDomain(changed); err != nil {
		t.Fatal(err)
	}
	read(token, path, http.StatusNotFound)
	if _, err := s.store.PutAppDomain(domain); err != nil {
		t.Fatal(err)
	}
	cert, err := s.store.GetEdgeTLSCertificate(domain.Hostname)
	if err != nil {
		t.Fatal(err)
	}
	changedCert := cert
	changedCert.TenantID = "foreign-tenant"
	if _, err := s.store.PutEdgeTLSCertificate(changedCert); err != nil {
		t.Fatal(err)
	}
	read(token, path, http.StatusNotFound)
	if _, err := s.store.PutEdgeTLSCertificate(cert); err != nil {
		t.Fatal(err)
	}
	read(token, path, http.StatusOK)
}
