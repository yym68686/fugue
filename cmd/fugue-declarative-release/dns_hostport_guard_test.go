package main

import "testing"

func TestLegacyDNSPortCannotCompeteWithIndependentPublicTransport(t *testing.T) {
	for _, address := range []string{"", "0.0.0.0", "::", "192.0.2.1", "192.0.2.2"} {
		for _, hostNetwork := range []bool{false, true} {
			port := map[string]any{"containerPort": 53, "hostIP": address}
			if !hostNetwork {
				port["hostPort"] = 53
			}
			template := map[string]any{"spec": map[string]any{"hostNetwork": hostNetwork, "containers": []any{map[string]any{"ports": []any{port}}}}}
			service := map[string]any{"spec": map[string]any{"externalIPs": []any{"192.0.2.1"}, "ports": []any{map[string]any{"port": 53}}}}
			if dnsHostClaimOverlapsService(dnsHostPortClaims(template), service) != (address != "192.0.2.2") {
				t.Fatal("legacy DNS overlap was not rejected", address, hostNetwork)
			}
			delete(mapField(service, "spec"), "externalIPs")
			if dnsHostClaimOverlapsService(dnsHostPortClaims(template), service) {
				t.Fatal("internal DNS Service was treated as a public claim")
			}
		}
	}
}
