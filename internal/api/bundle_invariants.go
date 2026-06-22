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
	ExplicitlyExcludedRoutes   int
	Options                    edgeRouteBundleOptions
}

type edgeDNSBundleInvariantInput struct {
	Options                       edgeDNSBundleOptions
	ProtectedRecords              []model.EdgeDNSRecord
	AnswerEdgeGroupsByIP          map[string][]string
	RouteReadyByHostnameEdgeGroup map[string]map[string]bool
	RecordRouteHostsByName        map[string][]string
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
	if len(bundle.Routes) == 0 && input.ExplicitlyExcludedRoutes == 0 && edgeRouteBundleExpectedRoutableHosts(input) > 0 && edgeRouteSelectorShouldHaveRoutes(input.Options, input.HealthyEdgeGroups, input.ExpectedNonEmptyEdgeGroups) {
		return fmt.Errorf("edge route bundle invariant failed: refusing to publish empty route bundle for non-empty routable inventory")
	}
	if edgeRouteBundleExpectedRoutableHosts(input) > 0 && edgeRouteSelectorShouldHaveRoutes(input.Options, input.HealthyEdgeGroups, input.ExpectedNonEmptyEdgeGroups) {
		trafficRoutes := edgeRouteBundleTrafficRouteCount(bundle, input.Options)
		if trafficRoutes == 0 && input.ExplicitlyExcludedRoutes == 0 {
			return fmt.Errorf("edge route bundle invariant failed: refusing to publish route bundle without traffic routes for non-empty routable inventory")
		}
		if minimum := edgeRouteExpectedMinTrafficRoutes(input.Options, input.ExpectedMinTrafficRoutes); minimum >= 5 {
			minimum -= input.ExplicitlyExcludedRoutes
			if minimum < 0 {
				minimum = 0
			}
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

func edgeRouteBundleTrafficRouteCount(bundle model.EdgeRouteBundle, _ edgeRouteBundleOptions) int {
	count := 0
	for _, route := range bundle.Routes {
		if !model.EdgeRoutePolicyAllowsTraffic(route.RoutePolicy) {
			continue
		}
		count++
	}
	return count
}

func edgeRouteExpectedMinTrafficRoutes(_ edgeRouteBundleOptions, expected map[string]int) int {
	if len(expected) == 0 {
		return 0
	}
	minimum := 0
	for _, count := range expected {
		if count > minimum {
			minimum = count
		}
	}
	return minimum
}

func validateEdgeDNSBundleForPublish(bundle model.EdgeDNSBundle, input edgeDNSBundleInvariantInput) error {
	if strings.TrimSpace(bundle.Version) == "" || strings.TrimSpace(bundle.Generation) == "" {
		return fmt.Errorf("edge dns bundle invariant failed: generation is required")
	}
	if normalizeExternalAppDomain(bundle.Zone) == "" {
		return fmt.Errorf("edge dns bundle invariant failed: zone is required")
	}
	if len(bundle.Records) == 0 {
		return fmt.Errorf("edge dns bundle invariant failed: refusing to publish empty dns record bundle")
	}
	probeName := normalizeExternalAppDomain(defaultEdgeDNSProbeLabel + "." + input.Options.Zone)
	if !edgeDNSBundleHasRecordValue(bundle.Records, probeName, model.EdgeDNSRecordTypeA, "") &&
		!edgeDNSBundleHasRecordValue(bundle.Records, probeName, model.EdgeDNSRecordTypeAAAA, "") {
		return fmt.Errorf("edge dns bundle invariant failed: probe record %s is missing", probeName)
	}
	for _, record := range input.ProtectedRecords {
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
	if err := validateEdgeDNSRouteReadyAnswers(bundle, input); err != nil {
		return err
	}
	return nil
}

func validateEdgeDNSRouteReadyAnswers(bundle model.EdgeDNSBundle, input edgeDNSBundleInvariantInput) error {
	if len(input.AnswerEdgeGroupsByIP) == 0 {
		return nil
	}
	for _, record := range bundle.Records {
		if !edgeDNSRecordRequiresRouteReady(record) {
			continue
		}
		routeHosts := edgeDNSRecordRouteHosts(record, input)
		for _, value := range record.Values {
			answerIP := normalizeEdgeDNSStaticRecordValue(record.Type, value)
			if answerIP == "" {
				continue
			}
			edgeGroups := input.AnswerEdgeGroupsByIP[answerIP]
			if len(edgeGroups) == 0 {
				continue
			}
			if record.RecordKind == model.EdgeDNSRecordKindCustomDomainTarget {
				if len(routeHosts) == 0 {
					continue
				}
				routeHosts = edgeDNSRouteHostsWithReadiness(input.RouteReadyByHostnameEdgeGroup, routeHosts)
				if len(routeHosts) == 0 {
					continue
				}
			} else if len(routeHosts) == 0 {
				return fmt.Errorf("edge dns bundle invariant failed: %s %s answer %s maps to edge groups %s but no route hostname is known", record.Name, record.Type, answerIP, strings.Join(edgeGroups, ","))
			}
			for _, edgeGroupID := range edgeGroups {
				for _, routeHost := range routeHosts {
					if !edgeDNSRouteReadyForGroup(input.RouteReadyByHostnameEdgeGroup, routeHost, edgeGroupID) {
						return fmt.Errorf("edge dns bundle invariant failed: %s %s answer %s points at edge group %s without active route for %s", record.Name, record.Type, answerIP, edgeGroupID, routeHost)
					}
				}
			}
		}
	}
	return nil
}

func edgeDNSRecordRequiresRouteReady(record model.EdgeDNSRecord) bool {
	recordType := strings.ToUpper(strings.TrimSpace(record.Type))
	if recordType != model.EdgeDNSRecordTypeA && recordType != model.EdgeDNSRecordTypeAAAA {
		return false
	}
	switch strings.TrimSpace(record.RecordKind) {
	case model.EdgeDNSRecordKindPlatform,
		model.EdgeDNSRecordKindCustomDomainTarget,
		model.EdgeDNSRecordKindPlatformDomain,
		model.EdgeDNSRecordKindPlatformRoute:
		return true
	default:
		return false
	}
}

func edgeDNSRecordRouteHosts(record model.EdgeDNSRecord, input edgeDNSBundleInvariantInput) []string {
	name := normalizeExternalAppDomain(record.Name)
	if name == "" {
		return nil
	}
	if hosts := normalizeEdgeDNSRouteHosts(input.RecordRouteHostsByName[name]); len(hosts) > 0 {
		return hosts
	}
	if record.RecordKind == model.EdgeDNSRecordKindCustomDomainTarget {
		return nil
	}
	return []string{name}
}

func normalizeEdgeDNSRouteHosts(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = normalizeExternalAppDomain(value)
		if value == "" || stringSliceContains(out, value) {
			continue
		}
		out = append(out, value)
	}
	return out
}

func edgeDNSRouteHostsWithReadiness(routeReady map[string]map[string]bool, routeHosts []string) []string {
	out := make([]string, 0, len(routeHosts))
	for _, routeHost := range routeHosts {
		routeHost = normalizeExternalAppDomain(routeHost)
		if routeHost == "" {
			continue
		}
		if len(routeReady[routeHost]) == 0 {
			continue
		}
		out = append(out, routeHost)
	}
	return out
}

func edgeDNSRouteReadyForGroup(routeReady map[string]map[string]bool, hostname, edgeGroupID string) bool {
	hostname = normalizeExternalAppDomain(hostname)
	edgeGroupID = strings.TrimSpace(edgeGroupID)
	if hostname == "" || edgeGroupID == "" {
		return false
	}
	return routeReady[hostname][edgeGroupID]
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
