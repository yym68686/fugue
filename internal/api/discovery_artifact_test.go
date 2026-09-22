package api

import (
	"fugue/internal/model"
	"testing"
)

func TestDiscoveryPlatformRoutesFailClosedWithoutVerifiedTrafficArtifact(t *testing.T) {
	_, s, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	s.platformRoutes = []model.PlatformRoute{{Hostname: "ambient.example.test", UpstreamURL: "http://ambient:8080"}}
	routes, err := s.publishedDiscoveryPlatformRoutes()
	if err != nil || len(routes) != 0 {
		t.Fatal("discovery accepted ambient routes without verified artifact", routes, err)
	}
}
