package api

import "testing"

func TestDiscoveryEndpointsExposeTopologyAwareEndpointMode(t *testing.T) {
	server := &Server{
		clusterJoinServer:          "https://cp-1.example:6443",
		clusterJoinServerFallbacks: []string{"https://cp-2.example:6443", " "},
	}
	apiEndpoints := server.discoveryAPIEndpoints("https://api.fugue.pro")
	if len(apiEndpoints) != 2 {
		t.Fatalf("expected public and cluster-join endpoints, got %+v", apiEndpoints)
	}
	if apiEndpoints[0].EndpointMode != controlPlaneEndpointModeSingle {
		t.Fatalf("public endpoint should remain single until HA endpoint is configured, got %+v", apiEndpoints[0])
	}
	if apiEndpoints[1].EndpointMode != controlPlaneEndpointModeMultiAddress {
		t.Fatalf("cluster join endpoint should expose multi-address mode with fallbacks, got %+v", apiEndpoints[1])
	}
	kubernetes := server.discoveryKubernetesEndpoints()
	if len(kubernetes) != 1 || kubernetes[0].EndpointMode != controlPlaneEndpointModeMultiAddress {
		t.Fatalf("expected Kubernetes endpoint multi-address mode, got %+v", kubernetes)
	}
}
