package api

import (
	"net/http"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func TestComparePlatformRouteSnapshotsReportsSemanticDrift(t *testing.T) {
	source := model.EdgeRouteIntentSnapshot{
		SchemaVersion: model.EdgeRouteIntentSchemaVersionV1,
		Generation:    "source-generation",
		Routes: []model.EdgeRouteIntent{
			{Hostname: "same.example", PathPrefix: "/", RouteKind: "platform", RoutePolicy: model.EdgeRoutePolicyEnabled, UpstreamKind: "runtime", UpstreamURL: "http://same", TLSPolicy: "platform"},
			{Hostname: "missing.example", PathPrefix: "/", RouteKind: "custom", RoutePolicy: model.EdgeRoutePolicyEnabled, UpstreamKind: "runtime", UpstreamURL: "http://missing", TLSPolicy: "custom"},
		},
	}
	artifact := model.EdgeRouteIntentSnapshot{
		SchemaVersion: model.EdgeRouteIntentSchemaVersionV1,
		Generation:    "artifact-generation",
		Routes: []model.EdgeRouteIntent{
			{Hostname: "same.example", PathPrefix: "/", RouteKind: "platform", RoutePolicy: model.EdgeRoutePolicyEnabled, UpstreamKind: "runtime", UpstreamURL: "http://changed", TLSPolicy: "platform"},
			{Hostname: "extra.example", PathPrefix: "/", RouteKind: "custom", RoutePolicy: model.EdgeRoutePolicyEnabled, UpstreamKind: "runtime", UpstreamURL: "http://extra", TLSPolicy: "custom"},
		},
	}
	result, err := comparePlatformRouteSnapshots(source, artifact)
	if err != nil {
		t.Fatal(err)
	}
	if result.Equivalent || result.MatchingRouteCount != 0 || len(result.Differences) != 3 {
		t.Fatalf("unexpected comparison: %+v", result)
	}
	if result.Differences[2].Kind != "changed" || !reflect.DeepEqual(result.Differences[2].Fields, []string{"upstream_url"}) {
		t.Fatalf("changed route was not reported precisely: %+v", result.Differences[2])
	}
}

func TestComparePlatformRouteSnapshotsIgnoresIdentityTimestamps(t *testing.T) {
	left := model.EdgeRouteIntent{Hostname: "Example.TEST.", PathPrefix: "", RouteKind: "platform", RoutePolicy: model.EdgeRoutePolicyEnabled, UpstreamKind: "runtime", UpstreamURL: "http://origin", TLSPolicy: "platform"}
	right := left
	left.Generation, right.Generation = "routeintent_old", "routeintent_new"
	left.CreatedAt = time.Now().UTC()
	right.UpdatedAt = left.CreatedAt.Add(time.Hour)
	result, err := comparePlatformRouteSnapshots(model.EdgeRouteIntentSnapshot{Routes: []model.EdgeRouteIntent{left}}, model.EdgeRouteIntentSnapshot{Routes: []model.EdgeRouteIntent{right}})
	if err != nil || !result.Equivalent || result.MatchingRouteCount != 1 {
		t.Fatalf("representation-only changes should compare equal: %+v %v", result, err)
	}
}

func TestComparePlatformRouteSnapshotsPreservesPathsAndNestedSemantics(t *testing.T) {
	base := model.EdgeRouteIntent{Hostname: "route.example", PathPrefix: "/", UpstreamURL: "http://origin"}
	path := base
	path.PathPrefix = "/api"
	source := model.EdgeRouteIntentSnapshot{Routes: []model.EdgeRouteIntent{base, path}}
	changed := path
	changed.Upstreams = []model.EdgeRouteUpstream{{UpstreamURL: "http://candidate", Weight: 10}}
	changed.ExcludedEdgeIDs = []string{"edge-test"}
	result, err := comparePlatformRouteSnapshots(source, model.EdgeRouteIntentSnapshot{Routes: []model.EdgeRouteIntent{changed, base}})
	if err != nil || result.MatchingRouteCount != 1 || len(result.Differences) != 1 || result.Differences[0].PathPrefix != "/api" ||
		!reflect.DeepEqual(result.Differences[0].Fields, []string{"excluded_edge_ids", "upstreams"}) {
		t.Fatalf("path or nested route semantics lost: %+v %v", result, err)
	}
	source.TLSAllowlist = []model.EdgeTLSAllowlistEntry{{Hostname: "route.example"}}
	source.CachePolicies = []model.CachePolicy{{ID: "test-cache"}}
	result, err = comparePlatformRouteSnapshots(source, model.EdgeRouteIntentSnapshot{Routes: source.Routes})
	if err != nil || result.Equivalent || result.MatchingRouteCount != 2 || !reflect.DeepEqual(result.SnapshotDifferences, []string{"tls_allowlist", "cache_policies"}) {
		t.Fatalf("non-route drift was hidden: %+v %v", result, err)
	}
	for _, duplicate := range []model.EdgeRouteIntentSnapshot{
		{Routes: []model.EdgeRouteIntent{base, base}},
		{Routes: []model.EdgeRouteIntent{{Hostname: ""}}},
	} {
		if _, err := comparePlatformRouteSnapshots(source, duplicate); err == nil {
			t.Fatal("ambiguous route identity accepted")
		}
	}
}

func TestRouteMigrationCachePolicyIdentityAndNestedSemantics(t *testing.T) {
	policies := defaultEdgeCachePolicies()
	source := model.EdgeRouteIntentSnapshot{CachePolicies: policies}
	artifact := model.EdgeRouteIntentSnapshot{CachePolicies: []model.CachePolicy{policies[1], policies[0]}}
	beforeSource := platformconfig.CloneCachePolicies(source.CachePolicies)
	beforeArtifact := platformconfig.CloneCachePolicies(artifact.CachePolicies)
	result, err := comparePlatformRouteSnapshots(source, artifact)
	if err != nil || !result.Equivalent || len(result.SnapshotDifferences) != 0 {
		t.Fatalf("compiler collection normalization changed cache semantics: %+v %v", result, err)
	}
	if !reflect.DeepEqual(source.CachePolicies, beforeSource) || !reflect.DeepEqual(artifact.CachePolicies, beforeArtifact) {
		t.Fatal("comparison mutated a captured snapshot")
	}
	for _, scenario := range []string{"ttl", "nested order", "missing policy", "extra policy"} {
		t.Run(scenario, func(t *testing.T) {
			changed := model.EdgeRouteIntentSnapshot{CachePolicies: platformconfig.CloneCachePolicies(artifact.CachePolicies)}
			switch scenario {
			case "ttl":
				changed.CachePolicies[0].TTLSeconds++
			case "nested order":
				patterns := changed.CachePolicies[0].PathPatterns
				patterns[0], patterns[1] = patterns[1], patterns[0]
			case "missing policy":
				changed.CachePolicies = changed.CachePolicies[:1]
			case "extra policy":
				changed.CachePolicies = append(changed.CachePolicies, model.CachePolicy{ID: "extra", Kind: model.CachePolicyKindDisabled})
			}
			result, err := comparePlatformRouteSnapshots(source, changed)
			if err != nil || result.Equivalent || !reflect.DeepEqual(result.SnapshotDifferences, []string{"cache_policies"}) {
				t.Fatalf("cache behavior change hidden: %+v %v", result, err)
			}
		})
	}
	for _, policies := range [][]model.CachePolicy{{{ID: ""}}, {{ID: " a"}}, {{ID: "duplicate"}, {ID: "DUPLICATE"}}} {
		for _, left := range []bool{true, false} {
			a, b := source, artifact
			if left {
				a.CachePolicies = policies
			} else {
				b.CachePolicies = policies
			}
			if _, err := comparePlatformRouteSnapshots(a, b); err == nil {
				t.Fatal("ambiguous cache policy collection accepted")
			}
		}
	}
	// Static routes implicitly try HTML policies in bundle order. A reorder
	// can change the selected TTL, so collection normalization must retain it.
	first := model.CachePolicy{ID: "html-a", Kind: model.CachePolicyKindHTMLDocuments, TTLSeconds: 30}
	second := model.CachePolicy{ID: "html-b", Kind: model.CachePolicyKindHTMLDocuments, TTLSeconds: 60}
	result, err = comparePlatformRouteSnapshots(model.EdgeRouteIntentSnapshot{CachePolicies: []model.CachePolicy{first, second}},
		model.EdgeRouteIntentSnapshot{CachePolicies: []model.CachePolicy{second, first}})
	if err != nil || result.Equivalent || !reflect.DeepEqual(result.SnapshotDifferences, []string{"cache_policies"}) {
		t.Fatalf("implicit HTML fallback precedence was lost: %+v %v", result, err)
	}
}

func TestPlatformRouteMigrationAPIIsReadOnlyAndRequiresTrustedArtifact(t *testing.T) {
	state, server, tenant, admin, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	response := performJSONRequest(t, server, http.MethodPost, "/v1/admin/platform-config/compile", admin, platformConfigCompileRequest{
		Intent: platformconfig.PlatformIntent{Generation: "migration-compare-test", Routes: []platformconfig.RouteIntent{
			{Hostname: "candidate.example.test", UpstreamURL: "http://candidate:8080", Enabled: true},
		}},
		Policy: platformconfig.PolicySnapshot{Generation: "migration-policy-test"},
	})
	if response.Code != http.StatusCreated {
		t.Fatal(response.Body.String())
	}
	var compiled platformConfigCompileResponse
	mustDecodeJSON(t, response, &compiled)
	// A serving artifact must not cause the diagnostic to compare itself.
	seedVerifiedPlatformArtifactAPI(t, server, admin, compiled.RouteArtifact.ID)
	before, err := state.ListPlatformArtifacts(model.PlatformArtifactFilter{})
	if err != nil {
		t.Fatal(err)
	}
	lkgBefore, err := state.GetPlatformLKG(model.PlatformArtifactKindEdgeRouteBundle, "global")
	if err != nil {
		t.Fatal(err)
	}
	path := "/v1/admin/platform-config/routes/compare?artifact_id="
	for _, test := range []struct {
		name, token, id string
		status          int
	}{
		{"anonymous", "", compiled.RouteArtifact.ID, http.StatusUnauthorized},
		{"tenant", tenant, compiled.RouteArtifact.ID, http.StatusForbidden},
		{"missing id", admin, "", http.StatusBadRequest},
		{"unknown", admin, "unknown", http.StatusNotFound},
		{"generation alias", admin, compiled.RouteArtifact.Generation, http.StatusConflict},
		{"wrong kind", admin, compiled.PolicyArtifact.ID, http.StatusConflict},
		{"valid", admin, compiled.RouteArtifact.ID, http.StatusOK},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := performJSONRequest(t, server, http.MethodGet, path+test.id, test.token, nil)
			if r.Code != test.status {
				t.Fatalf("%d: %s", r.Code, r.Body.String())
			}
			if r.Code == http.StatusOK {
				var result platformRouteMigrationComparison
				mustDecodeJSON(t, r, &result)
				if result.Equivalent || result.SourceRouteCount == 0 || result.ArtifactRouteCount != 1 || result.ArtifactDigest != compiled.RouteArtifact.ContentHash || result.SourceGeneration == compiled.RouteArtifact.Generation || r.Header().Get("Cache-Control") != "no-store" {
					t.Fatalf("incorrect migration evidence: %+v", result)
				}
			}
		})
	}
	server.bundleRevokedKeyIDs = append(server.bundleRevokedKeyIDs, compiled.RouteArtifact.Provenance.KeyID)
	rejected := performJSONRequest(t, server, http.MethodGet, path+compiled.RouteArtifact.ID, admin, nil)
	if rejected.Code != http.StatusConflict {
		t.Fatalf("revoked artifact accepted: %d %s", rejected.Code, rejected.Body.String())
	}
	after, err := state.ListPlatformArtifacts(model.PlatformArtifactFilter{})
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("read-only comparison changed artifacts", err)
	}
	lkgAfter, err := state.GetPlatformLKG(model.PlatformArtifactKindEdgeRouteBundle, "global")
	if err != nil || !reflect.DeepEqual(lkgBefore, lkgAfter) {
		t.Fatal("read-only comparison changed LKG", err)
	}
}
