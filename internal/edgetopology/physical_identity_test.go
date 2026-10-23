package edgetopology

import (
	"strings"
	"testing"
)

func TestPhysicalNodeIdentityMatchesRegisteredDNSSubdomains(t *testing.T) {
	for _, name := range []string{"42-worker", "7f2b681d-a25a-4123-a123-121234123412", "worker.example.test", "worker--standby"} {
		t.Run(name, func(t *testing.T) {
			intent := Intent{SchemaVersion: SchemaVersion, Cells: []AuthorityCell{{ID: "cell-test"}}, Pools: []ServingPool{{ID: "pool-test"}}, Edges: []Edge{{ID: name, AuthorityCellID: "cell-test", ServingPoolIDs: []string{"pool-test"}, Capabilities: []string{"http", "tls"}, FailureDomains: map[string]string{"host": "host-test"}}}}
			if err := intent.Validate(); err != nil {
				t.Fatal("registered physical identity rejected", err)
			}
			grant := RouteGrant{TenantID: "tenant-test", Hostname: "app.example.test", AllowedPoolIDs: []string{"pool-test"}, RequiredCapabilities: []string{"http"}, RequiredRouteDigestsByCell: map[string][]string{"cell-test": {"sha256:" + strings.Repeat("a", 64)}}, ExcludedEdgeIDs: []string{name}, MinCandidates: 1, FactMaxAgeSeconds: 30}
			if err := grant.Validate(intent); err != nil {
				t.Fatal("same physical identity cannot be excluded", err)
			}
		})
	}
}

func TestPhysicalIdentityAcceptanceDoesNotRelaxLogicalNamespaces(t *testing.T) {
	for _, name := range []string{"", "Upper", "bad_id", "-node", "node-", "node..test", "node/test", "node\n", strings.Repeat("a", 254)} {
		if validEdgeID(name) {
			t.Errorf("invalid physical identity accepted: %q", name)
		}
	}
	for _, name := range []string{"42-pool", "cell.example.test", "7f2b681d-a25a-4123-a123-121234123412"} {
		if validID(name) {
			t.Errorf("physical identity changed logical namespace grammar: %q", name)
		}
	}
}
