package api

import (
	"encoding/json"
	"net/http"
	"reflect"
	"slices"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func TestDNSMigrationComparisonPreservesSelectionSemantics(t *testing.T) {
	now := time.Now().UTC()
	base := model.EdgeDNSRecord{Name: "app.example.test", Type: "A", Values: []string{"192.0.2.1", "192.0.2.2"}, TTL: 60, Status: "active", RecordKind: "platform",
		AnswerPolicy:     model.DNSAnswerPolicy{PolicyKind: "geo", ECSEnabled: true, PreferredEdgeGroups: []string{"group-a", "group-b"}, TTLSeconds: 60},
		Candidates:       []model.EdgeDNSAnswerCandidate{{IP: "192.0.2.1", EdgeID: "edge-a", EdgeGroupID: "group-a", Healthy: true, RouteReady: true, TLSReady: true}},
		ScopedCandidates: []model.EdgeDNSScopedAnswerCandidates{{ScopeKey: "country:ZZ", Country: "ZZ", SelectedEdgeGroupID: "group-a", Candidates: []model.EdgeDNSAnswerCandidate{{IP: "192.0.2.1", EdgeID: "edge-a", EdgeGroupID: "group-a", Weight: 100}}}},
	}
	clone := func() model.EdgeDNSRecord {
		raw, _ := json.Marshal(base)
		var r model.EdgeDNSRecord
		json.Unmarshal(raw, &r)
		return r
	}
	for name, tc := range map[string]struct {
		field  string
		change func(*model.EdgeDNSRecord)
	}{
		"policy":              {"answer_policy", func(r *model.EdgeDNSRecord) { r.AnswerPolicy.ECSEnabled = false }},
		"preference order":    {"answer_policy", func(r *model.EdgeDNSRecord) { slices.Reverse(r.AnswerPolicy.PreferredEdgeGroups) }},
		"candidate readiness": {"candidates", func(r *model.EdgeDNSRecord) { r.Candidates[0].TLSReady = false }},
		"scope selection":     {"scoped_candidates", func(r *model.EdgeDNSRecord) { r.ScopedCandidates[0].SelectedEdgeGroupID = "group-b" }},
		"ttl":                 {"ttl", func(r *model.EdgeDNSRecord) { r.TTL = 30 }},
		"ownership":           {"tenant_id", func(r *model.EdgeDNSRecord) { r.TenantID = "other" }},
	} {
		t.Run(name, func(t *testing.T) {
			changed := clone()
			tc.change(&changed)
			result, err := comparePlatformDNSRecords([]model.EdgeDNSRecord{base}, []model.EdgeDNSRecord{changed}, now)
			if err != nil {
				t.Fatal(err)
			}
			if result.Equivalent || len(result.Differences) != 1 || !slices.Contains(result.Differences[0].Fields, tc.field) {
				t.Fatalf("lost DNS semantics: %+v", result)
			}
		})
	}
	reordered := clone()
	slices.Reverse(reordered.Values)
	reordered.RecordGeneration = "other"
	result, err := comparePlatformDNSRecords([]model.EdgeDNSRecord{base}, []model.EdgeDNSRecord{reordered}, now)
	if err != nil || !result.Equivalent || result.MatchingRecordCount != 1 {
		t.Fatalf("representation drift reported as behavior: %+v %v", result, err)
	}
}

func TestDNSMigrationComparisonLeasesNamesAndTXTBytes(t *testing.T) {
	now := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	leased := model.EdgeDNSRecord{Name: "_acme-challenge.example.test", Type: "TXT", Values: []string{" old ", "new"}, TTL: 60, ValueExpirations: map[string]time.Time{" old ": now.Add(-time.Second), "new": now.Add(10 * time.Second)}}
	raw, _ := json.Marshal(leased)
	result, err := comparePlatformDNSRecords([]model.EdgeDNSRecord{leased}, []model.EdgeDNSRecord{leased}, now)
	if err != nil || !result.Equivalent || result.ExpiredCandidateValueCount != 1 {
		t.Fatalf("lease comparison invalid: %+v %v", result, err)
	}
	after, _ := json.Marshal(leased)
	if string(raw) != string(after) {
		t.Fatal("comparison mutated original leases")
	}
	index, _, err := dnsMigrationRecordIndex([]model.EdgeDNSRecord{leased}, now)
	if err != nil {
		t.Fatal(err)
	}
	if index[leased.Name+"\x00TXT"]["ttl"] != float64(10) {
		t.Fatal("remaining TTL was renewed")
	}
	expired, _, err := dnsMigrationRecordIndex([]model.EdgeDNSRecord{leased}, now.Add(time.Minute))
	if err != nil || len(expired) != 1 {
		t.Fatal("expired authoritative name was discarded", err)
	}
	txt := model.EdgeDNSRecord{Name: "text.example.test", Type: "TXT", Values: []string{" value "}, TTL: 60}
	changed := txt
	changed.Values = []string{"value"}
	result, err = comparePlatformDNSRecords([]model.EdgeDNSRecord{txt}, []model.EdgeDNSRecord{changed}, now)
	if err != nil || result.Equivalent {
		t.Fatal("TXT bytes were trimmed", err)
	}
	if _, err = comparePlatformDNSRecords([]model.EdgeDNSRecord{txt, txt}, []model.EdgeDNSRecord{txt}, now); err == nil {
		t.Fatal("duplicate RRsets accepted")
	}
	leased.Candidates = []model.EdgeDNSAnswerCandidate{{IP: "192.0.2.1"}}
	if _, err = comparePlatformDNSRecords([]model.EdgeDNSRecord{leased}, nil, now); err == nil {
		t.Fatal("unsupported leased dynamic candidate accepted")
	}
}

func TestDNSMigrationComparisonAPIRequiresExactTrustedPublicationAndIsReadOnly(t *testing.T) {
	state, server, tenant, admin, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	now := time.Now().UTC().Truncate(time.Microsecond)
	node, err := state.UpdateDNSHeartbeat(model.DNSNode{ID: "dns-a", PhysicalNodeID: "dns-a", EdgeGroupID: "group-a", Zone: "example.test", PublicIPv4: "8.8.8.8", Status: "healthy", Healthy: true})
	if err != nil {
		t.Fatal(err)
	}
	request := platformConfigCompileRequest{
		Intent:          platformconfig.PlatformIntent{Generation: "dns-compare-1", Scope: "global", DNS: []platformconfig.DNSIntent{{Hostname: "app.example.test", Type: "A", Values: []string{"192.0.2.1"}, TTL: 60, Status: "active"}}, DNSConsumers: []platformconfig.DNSConsumerIntent{{NodeID: "dns-a", EdgeGroupID: "group-a", Zones: []string{"example.test"}, ProbeLabel: "probe", ProbeTTL: 60}}},
		Policy:          platformconfig.PolicySnapshot{Generation: "policy-1", Scope: "global"},
		RuntimeSnapshot: platformconfig.RuntimeSnapshot{CapturedAt: &now, DNSConsumers: []platformconfig.DNSConsumerObservation{{NodeID: "dns-a", EdgeGroupID: "group-a", ObservedAt: now, A: []string{"8.8.8.8"}}}},
	}
	response := performJSONRequest(t, server, http.MethodPost, "/v1/admin/platform-config/compile", admin, request)
	if response.Code != http.StatusCreated {
		t.Fatal(response.Body.String())
	}
	var compiled platformConfigCompileResponse
	mustDecodeJSON(t, response, &compiled)
	path := "/v1/admin/platform-config/dns/compare?node_id=dns-a&zone=example.test&artifact_id="
	for _, tc := range []struct {
		name, token, id string
		status          int
	}{
		{"anonymous", "", compiled.DNSArtifact.ID, 401}, {"tenant", tenant, compiled.DNSArtifact.ID, 403}, {"missing", admin, "", 400}, {"unknown", admin, "unknown", 404}, {"generation alias", admin, compiled.DNSArtifact.Generation, 409}, {"wrong kind", admin, compiled.RouteArtifact.ID, 409}, {"missing reference", admin, compiled.DNSArtifact.ID, 503},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := performJSONRequest(t, server, http.MethodGet, path+tc.id, tc.token, nil)
			if r.Code != tc.status {
				t.Fatalf("%d: %s", r.Code, r.Body.String())
			}
		})
	}
	options, ok := server.edgeDNSBundleOptionsForDNSNode(node)
	if !ok {
		t.Fatal("missing test DNS options")
	}
	bundle := signEdgeDNSBundle(model.EdgeDNSBundle{Version: "dns-source-1", Generation: "dns-source-1", GeneratedAt: now, DNSNodeID: node.ID, EdgeGroupID: node.EdgeGroupID, Zone: node.Zone, Records: []model.EdgeDNSRecord{
		{Name: "app.example.test", Type: "A", Values: []string{"192.0.2.1"}, TTL: 60, Status: "active"},
		{Name: "probe.example.test", Type: "A", Values: []string{"8.8.8.8"}, TTL: 60, Status: "active", RecordKind: "probe", EdgeGroupID: "group-a"},
	}}, server.bundleKeyring(), 10*time.Minute)
	publishFullEdgeDNSArtifactForTest(t, state, server, newEdgeDNSBundleArtifact(options, bundle, now))
	before, err := state.ListPlatformArtifacts(model.PlatformArtifactFilter{})
	if err != nil {
		t.Fatal(err)
	}
	referenceBefore, releaseBefore, found, err := state.GetActivePlatformArtifact(model.PlatformArtifactKindDNSAnswerBundle, edgeDNSBundleArtifactScopeKey(options), "full")
	if err != nil || !found {
		t.Fatal("missing published fixture", err)
	}
	good := performJSONRequest(t, server, http.MethodGet, path+compiled.DNSArtifact.ID, admin, nil)
	if good.Code != 200 {
		t.Fatal(good.Body.String())
	}
	var comparison platformDNSMigrationComparison
	mustDecodeJSON(t, good, &comparison)
	if !comparison.Equivalent || comparison.MatchingRecordCount != 2 || comparison.NodeID != node.ID || comparison.SourceGeneration != bundle.Generation || comparison.ArtifactDigest != compiled.DNSArtifact.ContentHash || comparison.SourceDigest == "" || good.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("incorrect scoped comparison: %+v", comparison)
	}
	for _, query := range []string{"?artifact_id=" + compiled.DNSArtifact.ID + "&node_id=foreign&zone=example.test", "?artifact_id=" + compiled.DNSArtifact.ID + "&node_id=dns-a&zone=other.test"} {
		r := performJSONRequest(t, server, http.MethodGet, "/v1/admin/platform-config/dns/compare"+query, admin, nil)
		if r.Code != 409 {
			t.Fatal("unsigned consumer view accepted", r.Code)
		}
	}
	after, err := state.ListPlatformArtifacts(model.PlatformArtifactFilter{})
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("comparison changed artifacts", err)
	}
	referenceAfter, releaseAfter, found, err := state.GetActivePlatformArtifact(model.PlatformArtifactKindDNSAnswerBundle, edgeDNSBundleArtifactScopeKey(options), "full")
	if err != nil || !found || !reflect.DeepEqual(referenceBefore, referenceAfter) || !reflect.DeepEqual(releaseBefore, releaseAfter) {
		t.Fatal("comparison changed serving publication", err)
	}
	invalid := compiled.DNSArtifact
	invalid.Metadata = map[string]string{}
	if _, _, err := platformDNSArtifactView(invalid, "dns-a", "example.test"); err == nil {
		t.Fatal("unbound policy lineage accepted")
	}
	bad := bundle
	bad.Version, bad.Generation = "dns-source-bad", "dns-source-bad"
	bad = signEdgeDNSBundle(bad, server.bundleKeyring(), 10*time.Minute)
	bad.Records = append([]model.EdgeDNSRecord(nil), bad.Records...)
	bad.Records[0].Values = []string{"192.0.2.99"}
	publishFullEdgeDNSArtifactForTest(t, state, server, newEdgeDNSBundleArtifact(options, bad, now))
	badSource := performJSONRequest(t, server, http.MethodGet, path+compiled.DNSArtifact.ID, admin, nil)
	if badSource.Code != 503 {
		t.Fatal("untrusted reference reported a comparison", badSource.Code)
	}
	alias := node
	alias.ID = "dns-zone-alias"
	if _, err := state.UpdateDNSHeartbeat(alias); err != nil {
		t.Fatal(err)
	}
	ambiguous := performJSONRequest(t, server, http.MethodGet, path+compiled.DNSArtifact.ID, admin, nil)
	if ambiguous.Code != 503 {
		t.Fatal("ambiguous physical zone reference accepted", ambiguous.Code)
	}
	server.bundleRevokedKeyIDs = append(server.bundleRevokedKeyIDs, compiled.DNSArtifact.Provenance.KeyID)
	rejected := performJSONRequest(t, server, http.MethodGet, path+compiled.DNSArtifact.ID, admin, nil)
	if rejected.Code != 409 {
		t.Fatal("revoked candidate accepted", rejected.Code)
	}
}
