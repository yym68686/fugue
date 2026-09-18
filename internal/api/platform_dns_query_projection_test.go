package api

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func TestDNSQueryMigrationSplitsSignedConfigurationFromObservedRanking(t *testing.T) {
	state, server, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	now := time.Now().UTC()
	node, err := state.UpdateDNSHeartbeat(model.DNSNode{ID: "dns-a", PhysicalNodeID: "dns-a", EdgeGroupID: "edge-group-a", Zone: "example.test", PublicIPv4: "8.8.8.8", Status: "healthy", Healthy: true})
	if err != nil {
		t.Fatal(err)
	}
	options, ok := server.edgeDNSBundleOptionsForDNSNode(node)
	if !ok {
		t.Fatal("missing source identity")
	}
	candidate := model.EdgeDNSAnswerCandidate{IP: "93.184.216.34", EdgeID: "edge-a", EdgeGroupID: "edge-group-a", Country: "aa", Weight: 100, Healthy: true, RouteReady: true, TLSReady: true, DNSEligible: true, ServingGeneration: "legacy-serving", Score: 125}
	source := signEdgeDNSBundle(model.EdgeDNSBundle{Version: "source-dns-query", Generation: "source-dns-query", GeneratedAt: now, DNSNodeID: node.ID, EdgeGroupID: node.EdgeGroupID, Zone: node.Zone, Records: []model.EdgeDNSRecord{{Name: "app.example.test", Type: "A", Values: []string{candidate.IP}, TTL: 60, Status: "active", RecordKind: "platform", AppID: "app-a", TenantID: "tenant-a", AnswerPolicy: model.DNSAnswerPolicy{PolicyKind: "geo", ECSEnabled: true, ExplorationPercent: 5, SwitchCooldownSec: 1800, PreferredEdgeGroups: []string{"edge-group-a"}, SelectedEdgeGroupID: "edge-group-a"}, Candidates: []model.EdgeDNSAnswerCandidate{candidate}, ScopedCandidates: []model.EdgeDNSScopedAnswerCandidates{{ScopeKey: "country:aa", Country: "aa", PolicyKind: "latency_aware", SelectedEdgeGroupID: "edge-group-a", Candidates: []model.EdgeDNSAnswerCandidate{candidate}}}}}}, server.bundleKeyring(), 10*time.Minute)
	publishFullEdgeDNSArtifactForTest(t, state, server, newEdgeDNSBundleArtifact(options, source, now))
	result := platformIntentProjectionResponse{Intent: platformconfig.PlatformIntent{Generation: "intent", Scope: "global", Routes: []platformconfig.RouteIntent{{Hostname: "app.example.test", AppID: "app-a", TenantID: "tenant-a", UpstreamURL: "http://origin:8080", Enabled: true, RoutePolicy: model.EdgeRoutePolicyEnabled}}, DNS: []platformconfig.DNSIntent{{Hostname: "app.example.test", AppID: "app-a", TenantID: "tenant-a", Type: "FUGUE_APP", Values: []string{"app-a"}, TTL: 60, Application: &platformconfig.DNSApplicationIntent{IPv4Policy: "auto", IPv6Policy: "auto", TTLPolicy: "record", FallbackPolicy: "fail_closed"}}}, DNSConsumers: []platformconfig.DNSConsumerIntent{{NodeID: "dns-a", EdgeGroupID: "edge-group-a", Zones: []string{"example.test"}, ProbeLabel: "probe", ProbeTTL: 60}}}, Policy: platformconfig.NormalizePolicySnapshot(platformconfig.PolicySnapshot{Generation: "policy", Scope: "global"}), RuntimeSnapshot: platformconfig.RuntimeSnapshot{CapturedAt: &now}}
	intentBefore, _ := json.Marshal(result.Intent)
	if err := server.projectDNSQueryRules(&result, []model.DNSNode{node}); err != nil {
		t.Fatal(err)
	}
	if len(result.Policy.DNSAnswerRules) != 1 || len(result.RuntimeSnapshot.DNSSelections) != 1 {
		t.Fatal("missing query projection")
	}
	rule, fact := result.Policy.DNSAnswerRules[0], result.RuntimeSnapshot.DNSSelections[0]
	if rule.SelectionMode != "geo" || rule.ScopedSelectionMode != "latency_aware" || !rule.ECSEnabled || rule.ExplorationPercent != 5 || rule.TTLSeconds != 60 || fact.Candidates[0].Score != 125 || fact.SourceGeneration != source.Generation || fact.SourceDigest == "" || !fact.ObservedAt.Equal(source.GeneratedAt) {
		t.Fatal("configuration/fact partition lost source semantics")
	}
	raw, _ := json.Marshal(fact)
	for _, forbidden := range []string{"healthy", "route_ready", "tls_ready", "serving_generation", "dns_eligible"} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatal("legacy readiness copied into new authority", forbidden)
		}
	}
	intentAfter, _ := json.Marshal(result.Intent)
	if string(intentBefore) != string(intentAfter) || result.RuntimeSnapshot.PolicyGeneration != result.Policy.Generation {
		t.Fatal("migration mutated desired intent or lost policy binding")
	}
	before, _ := json.Marshal(result)
	alias := node
	alias.ID = "duplicate-alias"
	if err := server.projectDNSQueryRules(&result, []model.DNSNode{node, alias}); err == nil {
		t.Fatal("ambiguous published source accepted")
	}
	after, _ := json.Marshal(result)
	if string(before) != string(after) {
		t.Fatal("failed source capture partially modified draft")
	}
	server.bundleRevokedKeyIDs = append(server.bundleRevokedKeyIDs, source.KeyID)
	if err := server.projectDNSQueryRules(&result, []model.DNSNode{node}); err == nil {
		t.Fatal("untrusted selection source accepted")
	}
}
