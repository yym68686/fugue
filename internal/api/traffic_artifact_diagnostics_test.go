package api

import (
	"context"
	"net/http/httptest"
	"testing"

	"fugue/internal/model"
)

func TestPublishedTrafficDiagnosticsRejectLegacySourcesAndMissingConsumers(t *testing.T) {
	_, s, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	s.platformRoutes = []model.PlatformRoute{{Hostname: "ambient.example.test", UpstreamURL: "http://ambient:8080"}}
	s.dnsStaticRecords = []model.EdgeDNSRecord{{Name: "ambient.example.test", Type: "A", Values: []string{"8.8.8.8"}, TTL: 60}}
	opts := dnsDelegationPreflightOptions{Zone: "example.test"}
	if c, _ := s.buildRouteDNSInvariantPreflightCheck(context.Background(), opts); c.Pass {
		t.Fatal("legacy configuration supplied passing invariant")
	}
	checks, err := s.robustnessGeneratedArtifactChecks(httptest.NewRequest("GET", "/", nil), opts)
	if err != nil || len(checks) != 2 {
		t.Fatal(checks, err)
	}
	for _, c := range checks {
		if c.Pass || c.Severity != model.RobustnessSeverityBlockPublish {
			t.Fatal("missing source became passing diagnostics", c)
		}
	}
}
