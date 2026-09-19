package api

import (
	"context"
	"encoding/json"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"net/http"
	"reflect"
	"testing"
)

func TestPinnedApplicationDomainsIgnoreAmbientConfiguration(t *testing.T) {
	state, s, _, admin, app, _ := setupAppDomainTestServerWithDomains(t, "apps.example.test")
	s.reservedAppHosts["reserved.customer.test"] = struct{}{}
	for _, host := range []string{"owned.apps.example.test", "alias.customer.test", "reserved.customer.test"} {
		if _, err := state.PutAppDomain(model.AppDomain{AppID: app.ID, TenantID: app.TenantID, Hostname: host, Status: model.AppDomainStatusVerified, DNSStatus: model.AppDomainDNSStatusReady, TLSStatus: model.AppDomainTLSStatusReady}); err != nil {
			t.Fatal(err)
		}
	}
	old, err := s.capturePlatformIntent(context.Background(), platformProducerPrincipal())
	if err != nil {
		t.Fatal(err)
	}
	imported, err := s.importPlatformEnvironment(map[string]string{}, "domains")
	if err != nil {
		t.Fatal(err)
	}
	base, err := state.CreatePlatformArtifact(model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindPlatformIntent, Scope: model.PlatformArtifactScope{ScopeType: "global", Key: "global"}, Generation: imported.Intent.Generation, Content: mustPlatformIntentContent(imported.Intent)})
	if err != nil {
		t.Fatal(err)
	}
	response := performJSONRequest(t, s, http.MethodPost, "/v1/admin/artifacts/"+base.ID+"/validate", admin, model.PlatformArtifactValidateRequest{})
	if response.Code != 200 {
		t.Fatal(response.Body.String())
	}
	p := platformproducer.Policy{InputSource: "business-static-intent", StaticIntentArtifactID: base.ID, StaticIntentDigest: base.ContentHash, RequireApplicationDomains: true}
	s.appBaseDomain, s.customDomainBaseDomain, s.dnsBundleTTL = "changed.test", "changed-dns.test", 1
	s.reservedAppHosts = map[string]struct{}{"alias.customer.test": {}}
	pinned, err := s.capturePlatformIntentForProducer(context.Background(), platformProducerPrincipal(), p)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(old.Intent, pinned.Intent) || !reflect.DeepEqual(old.Policy, pinned.Policy) || !reflect.DeepEqual(old.DNSExclusions, pinned.DNSExclusions) {
		t.Fatal("ambient domain configuration changed pinned projection")
	}
	kinds := map[string]string{}
	for _, r := range pinned.Intent.Routes {
		kinds[r.Hostname] = r.Kind
	}
	if kinds["owned.apps.example.test"] != model.EdgeRouteKindPlatformDomain || kinds["alias.customer.test"] != model.EdgeRouteKindCustomDomain || kinds["reserved.customer.test"] != "" {
		t.Fatal("explicit namespace not respected", kinds)
	}
	input, err := s.loadStaticPlatformIntent(base.ID, base.ContentHash)
	if err != nil {
		t.Fatal(err)
	}
	input.ApplicationDomains.DefaultDNSTTL = 180
	changed, err := s.capturePlatformIntentWithStatic(context.Background(), platformProducerPrincipal(), input)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, r := range changed.Intent.DNS {
		if r.Route != nil {
			count++
			if r.TTL != 180 {
				t.Fatal("explicit TTL clamped", r.Hostname, r.TTL)
			}
		}
	}
	if count < 3 || reflect.DeepEqual(changed.Intent, pinned.Intent) {
		t.Fatal("namespace version did not change desired DNS")
	}
	missing := createTestStaticIntent(t, s, "missing-domains", "http://static:8080")
	p.StaticIntentArtifactID, p.StaticIntentDigest = missing.ID, missing.ContentHash
	if _, err = s.capturePlatformIntentForProducer(context.Background(), platformProducerPrincipal(), p); err == nil {
		t.Fatal("required namespace fell back to ambient config")
	}
	input.ApplicationDomains.DefaultDNSTTL = 0
	if _, err = s.capturePlatformIntentWithStatic(context.Background(), platformProducerPrincipal(), input); err == nil {
		t.Fatal("invalid namespace fell back")
	}
	// Both public importer paths must expose the identical effective source.
	preview := performJSONRequest(t, s, http.MethodGet, "/v1/admin/platform-config/import-env/preview?generation=effective-domain-import", admin, nil)
	created := performJSONRequest(t, s, http.MethodPost, "/v1/admin/platform-config/import-env", admin, map[string]any{"generation": "effective-domain-import"})
	if preview.Code != 200 || created.Code != 201 {
		t.Fatal(preview.Body.String(), created.Body.String())
	}
	var result platformconfig.EnvironmentImportResult
	mustDecodeJSON(t, preview, &result)
	var out model.PlatformArtifactResponse
	mustDecodeJSON(t, created, &out)
	if !reflect.DeepEqual(out.Artifact.Content, mustPlatformIntentContent(result.Intent)) || out.Artifact.Metadata["source_digest"] != result.SourceDigest {
		t.Fatal("importer source audit mismatch")
	}
	raw, _ := json.Marshal(result.Intent.ApplicationDomains)
	if string(raw) == "null" {
		t.Fatal("effective application domains missing from importer")
	}
}

func TestApplicationDomainProjectRouteClassificationIsExplicit(t *testing.T) {
	s := &Server{appBaseDomain: "apps.example.test", customDomainBaseDomain: "dns.apps.example.test", reservedAppHosts: map[string]struct{}{"api.apps.example.test": {}}}
	cfg := s.legacyApplicationDomains()
	domains := map[string]model.AppDomain{"customer.test": {Hostname: "customer.test"}, "outside.test": {Hostname: "outside.test"}}
	for _, test := range []struct {
		host, kind, tls string
		allowed, domain bool
	}{
		{"apps.example.test", model.EdgeRouteKindPlatformDomain, model.EdgeRouteTLSPolicyPlatform, true, false},
		{"web.apps.example.test", model.EdgeRouteKindPlatformDomain, model.EdgeRouteTLSPolicyPlatform, true, false},
		{"api.apps.example.test", model.EdgeRouteKindPlatform, model.EdgeRouteTLSPolicyPlatform, true, false},
		{"dns.apps.example.test", model.EdgeRouteKindPlatform, model.EdgeRouteTLSPolicyPlatform, true, false},
		{"customer.test", model.EdgeRouteKindCustomDomain, model.EdgeRouteTLSPolicyCustomDomain, true, true},
		{"unknown.test", "", "", false, false},
	} {
		kind, tls, domain, allowed := cfg.projectRouteEdgePolicy(test.host, domains)
		if kind != test.kind || tls != test.tls || allowed != test.allowed || (domain != nil) != test.domain {
			t.Fatal("namespace classification changed", test.host, kind, tls, allowed, domain)
		}
	}
	s.appBaseDomain = "outside.test"
	kind, _, _, ok := cfg.projectRouteEdgePolicy("customer.test", domains)
	if !ok || kind != model.EdgeRouteKindCustomDomain {
		t.Fatal("explicit project namespace changed")
	}
}
