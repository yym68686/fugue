package api

import (
	"fmt"
	"strings"

	"fugue/internal/model"
)

type edgeRouteBundleInvariantInput struct {
	Apps                       []model.App
	Domains                    []model.AppDomain
	PlatformRoutes             []model.PlatformRoute
	HealthyEdgeGroups          map[string]bool
	ExpectedNonEmptyEdgeGroups map[string]bool
	ExpectedMinTrafficRoutes   map[string]int
	Options                    edgeRouteBundleOptions
}

func validateEdgeRouteBundleForPublish(bundle model.EdgeRouteBundle, input edgeRouteBundleInvariantInput) error {
	if strings.TrimSpace(bundle.Version) == "" || strings.TrimSpace(bundle.Generation) == "" {
		return fmt.Errorf("edge route bundle invariant failed: generation is required")
	}
	for _, route := range bundle.Routes {
		if normalizeExternalAppDomain(route.Hostname) == "" {
			return fmt.Errorf("edge route bundle invariant failed: route hostname is required")
		}
		if strings.TrimSpace(route.RouteGeneration) == "" {
			return fmt.Errorf("edge route bundle invariant failed: route generation is required for %s", route.Hostname)
		}
		if strings.EqualFold(route.Status, model.EdgeRouteStatusActive) && model.EdgeRoutePolicyAllowsTraffic(route.RoutePolicy) {
			if strings.TrimSpace(route.EdgeGroupID) == "" {
				return fmt.Errorf("edge route bundle invariant failed: active route %s missing edge group", route.Hostname)
			}
			if strings.TrimSpace(route.UpstreamURL) == "" {
				return fmt.Errorf("edge route bundle invariant failed: active route %s missing upstream", route.Hostname)
			}
		}
	}
	if len(bundle.Routes) == 0 && edgeRouteBundleExpectedRoutableHosts(input) > 0 && edgeRouteSelectorShouldHaveRoutes(input.Options, input.HealthyEdgeGroups, input.ExpectedNonEmptyEdgeGroups) {
		return fmt.Errorf("edge route bundle invariant failed: refusing to publish empty route bundle for non-empty routable inventory")
	}
	if edgeRouteBundleExpectedRoutableHosts(input) > 0 && edgeRouteSelectorShouldHaveRoutes(input.Options, input.HealthyEdgeGroups, input.ExpectedNonEmptyEdgeGroups) {
		trafficRoutes := edgeRouteBundleTrafficRouteCount(bundle, input.Options)
		if trafficRoutes == 0 {
			return fmt.Errorf("edge route bundle invariant failed: refusing to publish route bundle without traffic routes for non-empty routable inventory")
		}
		if minimum := edgeRouteExpectedMinTrafficRoutes(input.Options, input.ExpectedMinTrafficRoutes); minimum >= 5 {
			floor := (minimum*8 + 9) / 10
			if trafficRoutes < floor {
				return fmt.Errorf("edge route bundle invariant failed: refusing to publish route bundle with abnormal traffic route drop: got %d, previous %d", trafficRoutes, minimum)
			}
		}
	}
	return nil
}

func edgeRouteBundleExpectedRoutableHosts(input edgeRouteBundleInvariantInput) int {
	count := 0
	for _, app := range input.Apps {
		if app.Route != nil && normalizeExternalAppDomain(app.Route.Hostname) != "" {
			count++
		}
	}
	for _, domain := range input.Domains {
		if normalizeExternalAppDomain(domain.Hostname) != "" {
			count++
		}
	}
	for _, route := range input.PlatformRoutes {
		if normalizeExternalAppDomain(route.Hostname) != "" {
			count++
		}
	}
	return count
}

func edgeRouteSelectorShouldHaveRoutes(options edgeRouteBundleOptions, healthyEdgeGroups, expectedNonEmptyEdgeGroups map[string]bool) bool {
	if strings.TrimSpace(options.EdgeGroupID) == "" && strings.TrimSpace(options.EdgeID) == "" {
		return true
	}
	edgeGroupID := strings.TrimSpace(options.EdgeGroupID)
	if edgeGroupID == "" {
		edgeGroupID = edgeGroupIDFromEdgeID(options.EdgeID)
	}
	return edgeGroupID != "" && (expectedNonEmptyEdgeGroups[edgeGroupID] || healthyEdgeGroups[edgeGroupID])
}

func edgeRouteBundleTrafficRouteCount(bundle model.EdgeRouteBundle, options edgeRouteBundleOptions) int {
	edgeGroupID := strings.TrimSpace(options.EdgeGroupID)
	if edgeGroupID == "" {
		edgeGroupID = edgeGroupIDFromEdgeID(options.EdgeID)
	}
	count := 0
	for _, route := range bundle.Routes {
		if !model.EdgeRoutePolicyAllowsTraffic(route.RoutePolicy) {
			continue
		}
		if edgeGroupID != "" && !strings.EqualFold(strings.TrimSpace(route.EdgeGroupID), edgeGroupID) {
			continue
		}
		count++
	}
	return count
}

func edgeRouteExpectedMinTrafficRoutes(options edgeRouteBundleOptions, expected map[string]int) int {
	if len(expected) == 0 {
		return 0
	}
	edgeGroupID := strings.TrimSpace(options.EdgeGroupID)
	if edgeGroupID == "" {
		edgeGroupID = edgeGroupIDFromEdgeID(options.EdgeID)
	}
	if edgeGroupID == "" {
		minimum := 0
		for _, count := range expected {
			if count > minimum {
				minimum = count
			}
		}
		return minimum
	}
	return expected[edgeGroupID]
}

func validateEdgeDNSBundleForPublish(bundle model.EdgeDNSBundle, options edgeDNSBundleOptions, protectedRecords []model.EdgeDNSRecord) error {
	if strings.TrimSpace(bundle.Version) == "" || strings.TrimSpace(bundle.Generation) == "" {
		return fmt.Errorf("edge dns bundle invariant failed: generation is required")
	}
	if normalizeExternalAppDomain(bundle.Zone) == "" {
		return fmt.Errorf("edge dns bundle invariant failed: zone is required")
	}
	if len(bundle.Records) == 0 {
		return fmt.Errorf("edge dns bundle invariant failed: refusing to publish empty dns record bundle")
	}
	probeName := normalizeExternalAppDomain(defaultEdgeDNSProbeLabel + "." + options.Zone)
	if !edgeDNSBundleHasRecordValue(bundle.Records, probeName, model.EdgeDNSRecordTypeA, "") &&
		!edgeDNSBundleHasRecordValue(bundle.Records, probeName, model.EdgeDNSRecordTypeAAAA, "") {
		return fmt.Errorf("edge dns bundle invariant failed: probe record %s is missing", probeName)
	}
	for _, record := range protectedRecords {
		if record.RecordKind != model.EdgeDNSRecordKindProtected {
			continue
		}
		name := normalizeExternalAppDomain(record.Name)
		recordType := strings.ToUpper(strings.TrimSpace(record.Type))
		if name == "" || recordType == "" {
			continue
		}
		for _, value := range record.Values {
			if !edgeDNSBundleHasRecordValue(bundle.Records, name, recordType, value) {
				return fmt.Errorf("edge dns bundle invariant failed: protected %s %s value is missing", name, recordType)
			}
		}
	}
	return nil
}

func edgeDNSBundleHasRecordValue(records []model.EdgeDNSRecord, name, recordType, value string) bool {
	name = normalizeExternalAppDomain(name)
	recordType = strings.ToUpper(strings.TrimSpace(recordType))
	value = strings.TrimSpace(value)
	for _, record := range records {
		if normalizeExternalAppDomain(record.Name) != name || !strings.EqualFold(record.Type, recordType) {
			continue
		}
		if value == "" {
			return len(record.Values) > 0
		}
		for _, candidate := range record.Values {
			if strings.TrimSpace(candidate) == value {
				return true
			}
		}
	}
	return false
}
