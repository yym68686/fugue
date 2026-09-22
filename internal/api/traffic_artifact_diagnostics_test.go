package api

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"reflect"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestPublishedTrafficDiagnosticsRejectLegacySourcesAndMissingConsumers(t *testing.T) {
	_, s, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	s.platformRoutes = []model.PlatformRoute{{Hostname: "ambient.example.test", UpstreamURL: "http://ambient:8080"}}
	s.dnsStaticRecords = []model.EdgeDNSRecord{{Name: "ambient.example.test", Type: "A", Values: []string{"8.8.8.8"}, TTL: 60}}
	opts := dnsDelegationPreflightOptions{Zone: "example.test"}
	if c := s.buildRouteDNSInvariantPreflightCheck(context.Background(), opts); c.Pass {
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

func TestPublishedTrafficDiagnosticsBindPublicationAndFreshFacts(t *testing.T) {
	path := t.TempDir() + "/state.json"
	state := store.New(path)
	if err := state.Init(); err != nil {
		t.Fatal(err)
	}
	s := NewServer(state, auth.New(state, ""), nil, ServerConfig{BundleSigningKey: "synthetic-diagnostic-key", BundleSigningKeyID: "diagnostic-key"})
	now := time.Now().UTC()
	if _, err := state.UpdateDNSHeartbeat(model.DNSNode{ID: "dns-a", EdgeGroupID: "edge-group-a", Zone: "example.test", PublicIPv4: "8.8.8.8", LastSeenAt: &now}); err != nil {
		t.Fatal(err)
	}
	seedVerifiedDNSDelegationFixture(t, s, "example.test")
	owners := map[string]string{"dns-a": "edge-group-a"}
	before, err := s.inspectPublishedTrafficArtifacts(context.Background(), "example.test", owners)
	if err != nil || before.Routes != 1 || before.RequiredConsumers != 3 || before.DNSRecords == 0 {
		t.Fatal(before, err)
	}
	artifactBefore, _ := state.ListPlatformArtifacts(model.PlatformArtifactFilter{})
	s.platformRoutes = []model.PlatformRoute{{Hostname: "changed.example.test", UpstreamURL: "http://wrong:8080"}}
	s.dnsStaticRecords = []model.EdgeDNSRecord{{Name: "changed.example.test", Type: "A", Values: []string{"9.9.9.9"}, TTL: 1}}
	// Even a new inconsistent business policy is not the published policy.
	if _, err := state.PutEdgeRoutePolicy(model.EdgeRoutePolicy{Hostname: "changed.example.test", EdgeGroupID: "edge-group-unapproved", AppID: "synthetic-app", TenantID: "synthetic-tenant", RoutePolicy: model.EdgeRoutePolicyEnabled}); err != nil {
		t.Fatal(err)
	}
	after, err := s.inspectPublishedTrafficArtifacts(context.Background(), "example.test", owners)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("ambient/business inputs changed artifact diagnostics", after, err)
	}
	artifactAfter, _ := state.ListPlatformArtifacts(model.PlatformArtifactFilter{})
	if !reflect.DeepEqual(artifactBefore, artifactAfter) {
		t.Fatal("diagnostics wrote artifacts")
	}
	if _, err := s.inspectPublishedTrafficArtifacts(context.Background(), "missing.example.test", owners); err == nil {
		t.Fatal("undeclared zone passed")
	}
	if _, err := s.inspectPublishedTrafficArtifacts(context.Background(), "example.test", map[string]string{"unknown": "edge-group-a"}); err == nil {
		t.Fatal("unknown consumer passed")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.inspectPublishedTrafficArtifacts(ctx, "example.test", owners); err != context.Canceled {
		t.Fatal("canceled diagnostics continued", err)
	}
	// Corruption and stale facts are confined to this disposable store. Each
	// scenario restores the same signed baseline before making one change.
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []string{"stale heartbeat", "foreign release", "member signature", "missing expectation"} {
		t.Run(scenario, func(t *testing.T) {
			var st model.State
			if err := json.Unmarshal(raw, &st); err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "stale heartbeat":
				for i := range st.PlatformConsumerInstances {
					st.PlatformConsumerInstances[i].LastHeartbeatAt = now.Add(-time.Hour)
				}
			case "foreign release":
				for i := range st.PlatformConsumerInstances {
					st.PlatformConsumerInstances[i].ReleaseSetID = "different-parent"
				}
			case "member signature":
				for i := range st.PlatformArtifacts {
					if st.PlatformArtifacts[i].ArtifactKind == model.PlatformArtifactKindDNSAnswerBundle {
						st.PlatformArtifacts[i].Provenance.Signature = "corrupt"
					}
				}
			case "missing expectation":
				st.ExpectedConsumerSets = nil
			}
			damaged, _ := json.Marshal(st)
			if err := os.WriteFile(path, damaged, 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := s.inspectPublishedTrafficArtifacts(context.Background(), "example.test", owners); err == nil {
				t.Fatal("invalid published state became passing diagnostics")
			}
		})
	}
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
}
