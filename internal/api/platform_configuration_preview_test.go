package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/edgequality"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
)

func configurationPreviewFixture(t *testing.T, s *Server) (model.PlatformArtifact, platformproducer.Policy) {
	t.Helper()
	save := func(kind, scope, gen string, value any) model.PlatformArtifact {
		t.Helper()
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		var content map[string]any
		if err := json.Unmarshal(raw, &content); err != nil {
			t.Fatal(err)
		}
		a, err := s.store.CreatePlatformArtifact(model.PlatformArtifact{ArtifactKind: kind, Scope: model.PlatformArtifactScope{ScopeType: "global", Key: scope}, Generation: gen, Content: content})
		if err != nil {
			t.Fatal(err)
		}
		a, err = s.store.ValidatePlatformArtifact(a.ID, []model.PlatformArtifactValidationResult{{Name: "fixture", Pass: true}})
		if err != nil {
			t.Fatal(err)
		}
		return a
	}
	p, consumers, _ := pinnedDNSFixture()
	minimum, stale := 1, 120
	rules := []platformconfig.RoutePolicyConstraint{}
	states := []platformconfig.DNSRouteStateConstraint{}
	p.MinimumHealthyEdges, p.MaxStaleSeconds, p.RouteConstraints, p.DNSRouteStateConstraints = &minimum, &stale, &rules, &states
	p.DNSPlacementMode = platformconfig.DNSPlacementConsumerReadiness
	p.DNSQueryPolicy = &platformconfig.DNSQueryPolicy{RankingMode: "active", PreferenceMode: "runtime_locality", MinimumTTLSeconds: 60, MaximumTTLSeconds: 120,
		OrderedProjection: &platformconfig.DNSOrderedProjection{DefaultOrder: model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: []string{"edge-a"}}, Overrides: []platformconfig.DNSOrderOverride{}},
		PhysicalRoutes:    []platformconfig.PhysicalQualityRoute{{Hostname: "app.example.test", TrafficClass: "streaming", Policy: edgequality.DefaultNetworkPolicy()}},
	}
	source := save(model.PlatformArtifactKindPolicySnapshot, "global", p.Generation, p)
	if _, err := platformproducer.DecodeProjectionPolicy(source, consumers, nil); err != nil {
		t.Fatal("invalid test projection policy", err)
	}
	base := save(model.PlatformArtifactKindPlatformIntent, "global", "preview-base", platformconfig.PlatformIntent{SchemaVersion: platformconfig.SchemaVersion, Scope: "global", Generation: "preview-base",
		ApplicationDomains: &platformconfig.ApplicationDomainsIntent{AppBaseDomain: "example.test", ReservedHostnames: []string{}, DefaultDNSTTL: 60}, DNSConsumers: consumers,
		DNS: []platformconfig.DNSIntent{{Hostname: "mail._domainkey.example.test", Type: "TXT", TTL: 60, Values: []string{"public-verification-value"}}},
	})
	control := platformproducer.Policy{SchemaVersion: platformproducer.Schema, Generation: "preview-control", Mode: "serving", InputSource: "business-static-intent", TargetScope: "global", IntervalSeconds: 60, RefreshSeconds: 600,
		StaticIntentArtifactID: base.ID, StaticIntentDigest: base.ContentHash, DNSPolicyArtifactID: source.ID, DNSPolicyDigest: source.ContentHash,
		RequireApplicationDomains: true, RequireRouteDefaults: true, RequireDNSQueryPolicy: true,
		Serving: &platformproducer.ServingPolicy{CanaryRuleRef: "cohort=complete", GrayMinSeconds: 120, FullMinSeconds: 120, RolloutTimeoutSeconds: 600},
	}
	return save(model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, control.Generation, control), control
}

func TestConfigurationPreviewSurvivesUnavailableRuntimeWithoutAuthorizingPublication(t *testing.T) {
	state, s, _, admin, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	policy, control := configurationPreviewFixture(t, s)
	var reads atomic.Int32
	s.newClusterNodeClient = func() (*clusterNodeClient, error) {
		reads.Add(1)
		return nil, errors.New("runtime intentionally unavailable")
	}
	before, err := state.ListPlatformArtifacts(model.PlatformArtifactFilter{})
	if err != nil {
		t.Fatal(err)
	}
	path := "/v1/admin/platform-config/routes/project?producer_policy_artifact_id=" + policy.ID
	for _, query := range []string{"", "&view=configuration"} {
		response := performJSONRequest(t, s, http.MethodGet, path+query, admin, nil)
		if response.Code != 200 {
			t.Fatal(response.Code, response.Body.String())
		}
		var result platformIntentProjectionResponse
		mustDecodeJSON(t, response, &result)
		if reads.Load() != 0 {
			t.Fatal("configuration preview consulted unavailable runtime")
		}
		if result.PreviewMode != "configuration" || result.MigrationReady || result.BusinessSnapshotRevision == "" || len(result.OmittedRuntimeFields) == 0 {
			t.Fatal("preview lacks explicit non-serving provenance")
		}
		expected := platformconfig.RuntimeSnapshot{IntentGeneration: result.Intent.Generation, PolicyGeneration: result.Policy.Generation}
		if !reflect.DeepEqual(result.RuntimeSnapshot, expected) {
			t.Fatal("preview invented or retained runtime evidence")
		}
		found, issue := false, false
		for _, row := range result.Intent.DNS {
			if row.Hostname == "mail._domainkey.example.test" && reflect.DeepEqual(row.Values, []string{"public-verification-value"}) {
				found = true
			}
		}
		for _, row := range result.Issues {
			if row.Code == "runtime_evidence_not_collected" {
				issue = true
			}
		}
		if !found || !issue || len(result.Intent.DNSConsumers) != 1 || len(result.Policy.DNSQueryPolicy.PhysicalRoutes) != 1 {
			t.Fatal("static DNS or declared dynamic policy lost")
		}
		if response.Body.Len() > 64<<10 {
			t.Fatal("small configuration preview unexpectedly expanded", response.Body.Len())
		}
		if _, err := platformconfig.Compile(platformconfig.CompileRequest{Intent: result.Intent, Policy: result.Policy, RuntimeSnapshot: result.RuntimeSnapshot}); err == nil {
			t.Fatal("configuration-only preview substituted for publication evidence")
		}
	}
	// Full diagnostics and the producer keep the fail-closed path.
	full := performJSONRequest(t, s, http.MethodGet, path+"&view=runtime", admin, nil)
	if full.Code != 503 {
		t.Fatal("runtime view silently accepted unavailable evidence", full.Code)
	}
	if _, err := s.capturePlatformIntentForProducer(context.Background(), platformProducerPrincipal(), control); err == nil {
		t.Fatal("producer bypassed missing runtime")
	}
	after, err := state.ListPlatformArtifacts(model.PlatformArtifactFilter{})
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("preview changed stored artifacts")
	}
	for _, channel := range []string{"shadow", "gray", "full"} {
		_, _, found, err := state.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, "global", channel)
		if err != nil || found {
			t.Fatal("preview published traffic", channel, err)
		}
	}
}

func TestConfigurationPreviewRejectsInvalidViewsAndUntrustedSources(t *testing.T) {
	_, s, _, admin, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	policy, _ := configurationPreviewFixture(t, s)
	path := "/v1/admin/platform-config/routes/project?producer_policy_artifact_id=" + policy.ID
	for _, query := range []string{"&view=", "&view=full", "&view=runtime&view=configuration"} {
		r := performJSONRequest(t, s, http.MethodGet, path+query, admin, nil)
		if r.Code != 400 {
			t.Fatal(query, r.Code)
		}
	}
	s.bundleRevokedKeyIDs = append(s.bundleRevokedKeyIDs, policy.Provenance.KeyID)
	for _, view := range []string{"configuration", "runtime"} {
		r := performJSONRequest(t, s, http.MethodGet, path+"&view="+view, admin, nil)
		if r.Code != 503 {
			t.Fatal("revoked source accepted", view, r.Code)
		}
	}
}

func TestConfigurationPreviewDoesNotExportRetainedReplayEvidence(t *testing.T) {
	_, s, _, admin, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	policy, _ := configurationPreviewFixture(t, s)
	// Real captures retain large replay receipts in the process cache. A default
	// preview must not fetch, refresh or export them even when they are available.
	blob := json.RawMessage(`{"retained":"` + strings.Repeat("x", 4<<20) + `"}`)
	s.dynamicQuality.entries = map[string]dynamicQualityEntry{}
	s.dynamicQuality.entries["dns-a\x00app.example.test"] = dynamicQualityEntry{Attempted: time.Now(), Compiled: compiledPhysicalDNSSelection{Evidence: blob}}
	r := performJSONRequest(t, s, http.MethodGet, "/v1/admin/platform-config/routes/project?producer_policy_artifact_id="+policy.ID, admin, nil)
	if r.Code != 200 || r.Body.Len() > 64<<10 || strings.Contains(r.Body.String(), "physical_evidence") {
		t.Fatal("configuration preview exports replay evidence", r.Code, r.Body.Len())
	}
	if !reflect.DeepEqual(s.dynamicQuality.entries["dns-a\x00app.example.test"].Compiled.Evidence, blob) {
		t.Fatal("preview mutated evidence cache")
	}
}

func TestConfigurationPreviewDistinguishesUncollectedEvidenceFromInvalidConfiguration(t *testing.T) {
	app := model.App{ID: "app-a", TenantID: "tenant-a", Spec: model.AppSpec{Replicas: 1, RuntimeID: "runtime-a", Ports: []int{80}}}
	apps := map[string]model.App{app.ID: app}
	release := model.AppRelease{ID: "release-a", AppID: app.ID, TenantID: app.TenantID, RuntimeID: "runtime-a", Status: model.AppReleaseStatusServing, UpstreamURL: "http://app:80"}
	traffic := model.AppTrafficPolicy{ID: "traffic-a", AppID: app.ID, TenantID: app.TenantID, Mode: model.AppTrafficModeSingle, StableReleaseID: release.ID, StableWeight: 100}
	snapshot := model.EdgeRouteIntentSnapshot{GeneratedAt: time.Now().UTC(), Routes: []model.EdgeRouteIntent{{Hostname: "app.example.test", PathPrefix: "/", AppID: app.ID, TenantID: app.TenantID, RuntimeID: "runtime-a", ServicePort: 80}}}
	zone := model.HostedZone{ID: "zone-a", TenantID: app.TenantID, ZoneName: "example.test", Status: model.HostedZoneStatusActive}
	record := model.DNSRecord{ID: "record-a", ZoneID: zone.ID, TenantID: app.TenantID, FQDN: "missing.example.test", Type: model.DNSRecordTypeFUGUEAPP, Values: []string{"missing-app"}, TTL: 60}
	result, err := projectBusinessRouteDraft(snapshot, apps, apps, nil, nil, []model.AppTrafficPolicy{traffic}, []model.AppRelease{release}, []model.HostedZone{zone}, []model.DNSRecord{record}, nil)
	if err != nil {
		t.Fatal(err)
	}
	hasIssue := func(code string) bool {
		for _, issue := range result.Issues {
			if issue.Code == code {
				return true
			}
		}
		return false
	}
	if !hasIssue("release_observations_require_repair") || !hasIssue("dns_app_placement_not_projected") || !hasIssue("dns_app_reference_unresolved") {
		t.Fatal("fixture must exercise missing observations and a genuine unresolved application")
	}
	configurationPreview(&result)
	if hasIssue("release_observations_require_repair") || hasIssue("dns_app_placement_not_projected") || !hasIssue("runtime_evidence_not_collected") {
		t.Fatal("uncollected evidence was presented as a runtime failure")
	}
	if !hasIssue("dns_app_reference_unresolved") || len(result.Policy.TrafficConstraints) != 1 || result.MigrationReady {
		t.Fatal("configuration defect or signed traffic intent was hidden")
	}
}
