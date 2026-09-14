package platformconfig

import (
	"reflect"
	"testing"

	"fugue/internal/model"
)

func TestValidateRouteBehaviorBindsCacheAndRequestPolicies(t *testing.T) {
	policies := []model.CachePolicy{{ID: "assets", Kind: model.CachePolicyKindStaticAssets, HostnameScope: "app.example", PathPatterns: []string{"/assets/*"}, MethodAllowlist: []string{"GET", "HEAD"}, StatusAllowlist: []int{200}, TTLSeconds: 60, PurgeMode: model.CachePolicyPurgeModeGeneration}}
	route := RouteIntent{Hostname: "app.example", UpstreamURL: "https://origin.example", Enabled: true, CachePolicyID: "assets", CacheNamespace: "app_gen1"}
	if err := ValidateRouteBehavior([]RouteIntent{route}, policies); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*RouteIntent, *[]model.CachePolicy){
		"missing namespace": func(r *RouteIntent, _ *[]model.CachePolicy) { r.CacheNamespace = "" },
		"unknown policy":    func(r *RouteIntent, _ *[]model.CachePolicy) { r.CachePolicyID = "missing" },
		"scope mismatch":    func(r *RouteIntent, _ *[]model.CachePolicy) { r.Hostname = "other.example" },
		"whitespace id":     func(r *RouteIntent, _ *[]model.CachePolicy) { r.CachePolicyID = " assets" },
	} {
		t.Run(name, func(t *testing.T) {
			bad := route
			mutate(&bad, &policies)
			if err := ValidateRouteBehavior([]RouteIntent{bad}, policies); err == nil {
				t.Fatal("invalid cache binding accepted")
			}
		})
	}
	disabled := []model.CachePolicy{{ID: "off", Kind: model.CachePolicyKindDisabled}}
	if err := ValidateRouteBehavior([]RouteIntent{{Hostname: "app.example", UpstreamURL: "https://origin.example", Enabled: true, CachePolicyID: "off"}}, disabled); err != nil {
		t.Fatal(err)
	}
	validBody := model.EdgeRequestBodyPolicy{Name: "upload", Methods: []string{"POST"}, Paths: []string{"/upload"}, MaxBytes: 1024, TimeoutSeconds: 10, MaxConcurrent: 2, RetryAfterSeconds: 5}
	withBody := route
	withBody.CachePolicyID = ""
	withBody.RequestBodyPolicies = []model.EdgeRequestBodyPolicy{validBody}
	if err := ValidateRouteBehavior([]RouteIntent{withBody}, nil); err != nil {
		t.Fatal(err)
	}
	badBody := withBody
	badBody.RequestBodyPolicies = []model.EdgeRequestBodyPolicy{{Name: "upload", Methods: []string{"POST"}, Paths: []string{"/upload"}, MaxBytes: 1024, TimeoutSeconds: 10, MaxConcurrent: 2}}
	if err := ValidateRouteBehavior([]RouteIntent{badBody}, nil); err == nil {
		t.Fatal("non-canonical request policy accepted")
	}
}

func TestCachePoliciesAreSortedWithoutChangingRuleOrder(t *testing.T) {
	input := PlatformIntent{Generation: "cache-order", CachePolicies: []model.CachePolicy{
		{ID: "z", Kind: model.CachePolicyKindStaticAssets, PathPatterns: []string{"/z", "/a"}},
		{ID: "a", Kind: model.CachePolicyKindHTMLDocuments, PathPatterns: []string{"/", "/*.html"}},
	}}
	normalized := NormalizePlatformIntent(input)
	if normalized.CachePolicies[0].ID != "a" || normalized.CachePolicies[1].ID != "z" || !reflect.DeepEqual(normalized.CachePolicies[1].PathPatterns, []string{"/z", "/a"}) {
		t.Fatalf("cache policies were not canonically sorted: %+v", normalized.CachePolicies)
	}
	if input.CachePolicies[0].ID != "z" {
		t.Fatal("normalization mutated caller input")
	}
}

func TestCloneCachePoliciesDeepCopiesRuleLists(t *testing.T) {
	input := []model.CachePolicy{{ID: "assets", Kind: model.CachePolicyKindStaticAssets, PathPatterns: []string{"/assets/*"}, MethodAllowlist: []string{"GET"}, StatusAllowlist: []int{200}, VaryAllowlist: []string{"Accept-Encoding"}}}
	copy := CloneCachePolicies(input)
	copy[0].PathPatterns[0], copy[0].MethodAllowlist[0], copy[0].StatusAllowlist[0], copy[0].VaryAllowlist[0] = "/changed", "HEAD", 201, "Cookie"
	if reflect.DeepEqual(input, copy) || input[0].PathPatterns[0] != "/assets/*" || input[0].StatusAllowlist[0] != 200 {
		t.Fatal("cache policy clone aliases mutable slices")
	}
}
