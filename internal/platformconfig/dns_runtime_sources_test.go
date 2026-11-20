package platformconfig_test

import (
	"encoding/json"
	"maps"
	"slices"
	"testing"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/testfixture/celldns"
)

func TestDNSRuntimeSourcePlanManagedHostPreservesEndpointAcrossPathTransitions(t *testing.T) {
	req, _, _ := celldns.SourceAuthorizedRequest(t, false)
	p := req.CellRoutePublications[0]
	original := model.PlatformDNSRouteSourcePublication{Parent: p.Parent, Route: p.Route, TLS: p.TLS, Release: celldns.Publication(p)}
	old := req.Intent.DNS[0]
	oldSource := platformconfig.CellDNSPlanSource{Intent: req.Intent, Endpoints: req.RuntimeSnapshot.DNSEdgeEndpoints, CapturedAt: req.RuntimeSnapshot.CapturedAt}
	original.Route.Content = maps.Clone(original.Route.Content)
	allowlist := []model.EdgeTLSAllowlistEntry{{Hostname: old.Hostname, AppID: old.AppID, TenantID: old.TenantID, Status: model.AppDomainStatusVerified, TLSStatus: model.AppDomainTLSStatusReady}}
	original.Route.Content["tls_allowlist"] = allowlist
	var routes []platformconfig.CompiledRoute
	raw, _ := json.Marshal(original.Route.Content["routes"])
	if err := json.Unmarshal(raw, &routes); err != nil || len(routes) != 1 {
		t.Fatalf("original route: %v, error %v", routes, err)
	}
	child := routes[0]
	child.PathPrefix = "/v1"
	routes[0].AppID = "app-frontend"
	routes = append(routes, child)
	shared := old
	shared.Type, shared.Values, shared.Application = "FUGUE_ROUTE", nil, nil
	shared.Route = &platformconfig.DNSRouteIntent{DNSApplicationIntent: *old.Application, Hostnames: []string{old.Hostname}, ManagedHost: true, Bindings: []platformconfig.DNSRouteBinding{
		{Hostname: old.Hostname, PathPrefix: "/", AppID: "app-frontend"}, {Hostname: old.Hostname, PathPrefix: "/v1", AppID: old.AppID},
	}}
	sharedSource := oldSource
	sharedSource.Intent.DNS = []platformconfig.DNSIntent{shared}
	current := original
	current.Route.Content = maps.Clone(original.Route.Content)
	current.Route.Content["routes"] = routes
	marker := map[string]any{"hostname": old.Hostname, "app_id": old.AppID, "tenant_id": old.TenantID, "bindings": shared.Route.Bindings}
	current.Route.Content["managed_host_dns_bindings"] = []any{marker}
	for _, test := range []struct {
		name   string
		source platformconfig.CellDNSPlanSource
		pub    model.PlatformDNSRouteSourcePublication
		paths  []string
	}{
		{"old_dns_observes_shared_source", oldSource, current, []string{"/", "/v1"}},
		{"new_dns_observes_retained_source", sharedSource, original, []string{"/"}},
		{"new_dns_observes_shared_source", sharedSource, current, []string{"/", "/v1"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			plan, binding, err := platformconfig.DNSRuntimeSourcePlan(test.source, req.Policy, test.pub, "cell-a")
			if err != nil || len(plan.Records) != 1 || len(plan.Records[0].Targets) != 1 {
				t.Fatalf("original endpoint unavailable: %+v, %v", plan, err)
			}
			target := plan.Records[0].Targets[0]
			if target.EdgeID != "edge-a" || target.Address != "93.184.216.34" || !target.RequireSinglePublication || len(target.ProbeIDs) != len(test.paths) || binding.RouteArtifactDigest != test.pub.Route.ContentHash {
				t.Fatalf("unexpected endpoint or authority binding: %+v", target)
			}
			paths := make([]string, 0, len(plan.Probes))
			for _, probe := range plan.Probes {
				paths = append(paths, probe.Path)
				if probe.RouteDigest == "" || probe.CellPublicationDigest == "" {
					t.Fatalf("missing exact proof binding: %+v", probe)
				}
			}
			slices.Sort(paths)
			if !slices.Equal(paths, test.paths) {
				t.Fatalf("proof paths %v, expected %v", paths, test.paths)
			}
		})
	}
	for _, test := range []string{"unmarked", "different_owner", "incomplete_paths", "tls_pending"} {
		t.Run(test, func(t *testing.T) {
			bad := current
			bad.Route.Content = maps.Clone(bad.Route.Content)
			modified := maps.Clone(marker)
			bad.Route.Content["managed_host_dns_bindings"] = []any{modified}
			switch test {
			case "unmarked":
				delete(bad.Route.Content, "managed_host_dns_bindings")
			case "different_owner":
				modified["app_id"] = "app-frontend"
			case "incomplete_paths":
				modified["bindings"] = shared.Route.Bindings[:1]
			case "tls_pending":
				pending := allowlist[0]
				pending.TLSStatus = model.AppDomainTLSStatusPending
				bad.Route.Content["tls_allowlist"] = []model.EdgeTLSAllowlistEntry{pending}
			}
			if _, _, err := platformconfig.DNSRuntimeSourcePlan(oldSource, req.Policy, bad, "cell-a"); err == nil {
				t.Fatal("an unauthorized owner or incomplete path contract was accepted")
			}
		})
	}
}

func TestCurrentSourceHardPolicyRestrictsPositivePublication(t *testing.T) {
	req, _, _ := celldns.SourceAuthorizedRequest(t, false)
	source := platformconfig.CellDNSPlanSource{Intent: req.Intent, Endpoints: req.RuntimeSnapshot.DNSEdgeEndpoints, CapturedAt: req.RuntimeSnapshot.CapturedAt}
	p := req.CellRoutePublications[0]
	pub := model.PlatformDNSRouteSourcePublication{Parent: p.Parent, Route: p.Route, TLS: p.TLS, Release: celldns.Publication(p)}
	for _, scenario := range []string{"unready-candidate", "exclusion", "higher-quorum", "disabled", "owner-mismatch"} {
		t.Run(scenario, func(t *testing.T) {
			plan, _, err := platformconfig.DNSRuntimeSourcePlan(source, req.Policy, pub, "cell-a")
			if err != nil {
				t.Fatal(err)
			}
			var current model.PlatformDNSRouteSourcePublication
			raw, _ := json.Marshal(pub)
			json.Unmarshal(raw, &current)
			var policy platformconfig.PolicySnapshot
			raw, _ = json.Marshal(current.Route.Content["policy"])
			json.Unmarshal(raw, &policy)
			rule := platformconfig.RoutePolicyConstraint{ID: "constraint", Hostname: "app.example.test", TenantID: "tenant-a", AppID: "app-a", RoutePolicy: "direct", Enabled: true}
			switch scenario {
			case "unready-candidate":
				current.Route.Content["routes"] = []any{}
			case "exclusion":
				rule.ExcludedEdgeIDs = []string{"edge-a"}
				policy.RouteConstraints = []platformconfig.RoutePolicyConstraint{rule}
			case "higher-quorum":
				policy.MinimumHealthyEdges = 3
			case "disabled":
				rule.Enabled = false
				policy.RouteConstraints = []platformconfig.RoutePolicyConstraint{rule}
			case "owner-mismatch":
				rule.TenantID = "tenant-other"
				policy.RouteConstraints = []platformconfig.RoutePolicyConstraint{rule}
			}
			current.Route.Content["policy"] = policy
			err = platformconfig.RestrictDNSRuntimeSourcePlan(plan, source, pub, []model.PlatformDNSRouteSourcePublication{current})
			if scenario == "owner-mismatch" {
				if err == nil {
					t.Fatal("foreign policy owner accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(plan.Records) != 1 {
				t.Fatal("record was silently omitted")
			}
			if scenario == "exclusion" || scenario == "disabled" {
				if len(plan.Records[0].Targets) != 0 {
					t.Fatal("hard policy bypassed")
				}
			} else if len(plan.Records[0].Targets) != 1 {
				t.Fatal("unready candidate erased positive LKG")
			}
			if scenario == "higher-quorum" && plan.Records[0].MinimumHealthyEdges != 3 {
				t.Fatal("source quorum lowered")
			}
		})
	}
}
