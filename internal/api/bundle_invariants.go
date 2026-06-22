package api

import (
	"errors"
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

type bundleInvariantError struct {
	ArtifactKind string
	Failures     []model.RobustnessCheck
}

func (e *bundleInvariantError) Error() string {
	if e == nil {
		return ""
	}
	kind := strings.ReplaceAll(strings.TrimSpace(e.ArtifactKind), "_", " ")
	message := ""
	if len(e.Failures) > 0 {
		message = strings.TrimSpace(e.Failures[0].Message)
	}
	if message == "" {
		message = "invariant failed"
	}
	return fmt.Sprintf("%s invariant failed: %s", kind, message)
}

func bundleInvariantChecks(err error) []model.RobustnessCheck {
	var invariantErr *bundleInvariantError
	if !errors.As(err, &invariantErr) || invariantErr == nil {
		return nil
	}
	out := make([]model.RobustnessCheck, 0, len(invariantErr.Failures))
	for _, failure := range invariantErr.Failures {
		failure.Pass = false
		if strings.TrimSpace(failure.Severity) == "" {
			failure.Severity = model.RobustnessSeverityBlockPublish
		}
		out = append(out, failure)
	}
	return out
}

func newBundleInvariantError(artifactKind, name, subject, expected, observed, message string, evidence map[string]string) error {
	name = strings.TrimSpace(name)
	if name == "" {
		name = "bundle_publish_invariant"
	}
	subject = strings.TrimSpace(subject)
	if subject == "" {
		subject = strings.TrimSpace(artifactKind)
	}
	return &bundleInvariantError{
		ArtifactKind: strings.TrimSpace(artifactKind),
		Failures: []model.RobustnessCheck{{
			Name:       name,
			Pass:       false,
			Severity:   model.RobustnessSeverityBlockPublish,
			Subject:    subject,
			Expected:   strings.TrimSpace(expected),
			Observed:   strings.TrimSpace(observed),
			Evidence:   evidence,
			Message:    strings.TrimSpace(message),
			RepairHint: robustnessRepairHint(name),
		}},
	}
}

func validateEdgeRouteBundleForPublish(bundle model.EdgeRouteBundle, input edgeRouteBundleInvariantInput) error {
	if strings.TrimSpace(bundle.Version) == "" || strings.TrimSpace(bundle.Generation) == "" {
		return newBundleInvariantError("edge_route_bundle", "bundle_generation", "edge_route_bundle", "version and generation are set", fmt.Sprintf("version=%s generation=%s", bundle.Version, bundle.Generation), "generation is required", nil)
	}
	for _, route := range bundle.Routes {
		if normalizeExternalAppDomain(route.Hostname) == "" {
			return newBundleInvariantError("edge_route_bundle", "route_hostname", "edge_route_bundle", "every route has a hostname", fmt.Sprintf("route_kind=%s edge_group=%s", route.RouteKind, route.EdgeGroupID), "route hostname is required", nil)
		}
		if strings.TrimSpace(route.RouteGeneration) == "" {
			return newBundleInvariantError("edge_route_bundle", "route_generation", "hostname:"+route.Hostname, "route_generation is set", "route_generation=", fmt.Sprintf("route generation is required for %s", route.Hostname), nil)
		}
		if strings.EqualFold(route.Status, model.EdgeRouteStatusActive) && model.EdgeRoutePolicyAllowsTraffic(route.RoutePolicy) {
			if strings.TrimSpace(route.EdgeGroupID) == "" {
				return newBundleInvariantError("edge_route_bundle", "route_active", "hostname:"+route.Hostname, "active traffic route has edge group", "edge_group=", fmt.Sprintf("active route %s missing edge group", route.Hostname), nil)
			}
			if strings.TrimSpace(route.UpstreamURL) == "" {
				return newBundleInvariantError("edge_route_bundle", "route_active", "hostname:"+route.Hostname, "active traffic route has upstream", "upstream=", fmt.Sprintf("active route %s missing upstream", route.Hostname), nil)
			}
		}
	}
	if len(bundle.Routes) == 0 && input.ExplicitlyExcludedRoutes == 0 && edgeRouteBundleExpectedRoutableHosts(input) > 0 && edgeRouteSelectorShouldHaveRoutes(input.Options, input.HealthyEdgeGroups, input.ExpectedNonEmptyEdgeGroups) {
		return newBundleInvariantError("edge_route_bundle", "route_bundle_non_empty", "edge_route_bundle", "non-empty route bundle for non-empty routable inventory", fmt.Sprintf("routes=0 expected_hosts=%d excluded_routes=%d", edgeRouteBundleExpectedRoutableHosts(input), input.ExplicitlyExcludedRoutes), "refusing to publish empty route bundle for non-empty routable inventory", nil)
	}
	if edgeRouteBundleExpectedRoutableHosts(input) > 0 && edgeRouteSelectorShouldHaveRoutes(input.Options, input.HealthyEdgeGroups, input.ExpectedNonEmptyEdgeGroups) {
		trafficRoutes := edgeRouteBundleTrafficRouteCount(bundle, input.Options)
		if trafficRoutes == 0 && input.ExplicitlyExcludedRoutes == 0 {
			return newBundleInvariantError("edge_route_bundle", "route_bundle_traffic_routes", "edge_route_bundle", "at least one traffic route for non-empty routable inventory", fmt.Sprintf("traffic_routes=0 expected_hosts=%d excluded_routes=%d", edgeRouteBundleExpectedRoutableHosts(input), input.ExplicitlyExcludedRoutes), "refusing to publish route bundle without traffic routes for non-empty routable inventory", nil)
		}
		if minimum := edgeRouteExpectedMinTrafficRoutes(input.Options, input.ExpectedMinTrafficRoutes); minimum >= 5 {
			minimum -= input.ExplicitlyExcludedRoutes
			if minimum < 0 {
				minimum = 0
			}
			floor := (minimum*8 + 9) / 10
			if trafficRoutes < floor {
				return newBundleInvariantError("edge_route_bundle", "route_bundle_traffic_drop", "edge_route_bundle", fmt.Sprintf("traffic routes >= %d", floor), fmt.Sprintf("traffic_routes=%d previous_minimum=%d", trafficRoutes, minimum), fmt.Sprintf("refusing to publish route bundle with abnormal traffic route drop: got %d, previous %d", trafficRoutes, minimum), nil)
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
		return newBundleInvariantError("edge_dns_bundle", "bundle_generation", "dns:"+bundle.Zone, "version and generation are set", fmt.Sprintf("version=%s generation=%s", bundle.Version, bundle.Generation), "generation is required", nil)
	}
	if normalizeExternalAppDomain(bundle.Zone) == "" {
		return newBundleInvariantError("edge_dns_bundle", "dns_zone", "edge_dns_bundle", "zone is set", "zone=", "zone is required", nil)
	}
	if len(bundle.Records) == 0 {
		return newBundleInvariantError("edge_dns_bundle", "dns_bundle_non_empty", "dns:"+bundle.Zone, "non-empty DNS record bundle", "records=0", "refusing to publish empty dns record bundle", nil)
	}
	probeName := normalizeExternalAppDomain(defaultEdgeDNSProbeLabel + "." + input.Options.Zone)
	if !edgeDNSBundleHasRecordValue(bundle.Records, probeName, model.EdgeDNSRecordTypeA, "") &&
		!edgeDNSBundleHasRecordValue(bundle.Records, probeName, model.EdgeDNSRecordTypeAAAA, "") {
		return newBundleInvariantError("edge_dns_bundle", "dns_probe_record", "dns:"+bundle.Zone, "probe A or AAAA record is present", "probe_record=missing", fmt.Sprintf("probe record %s is missing", probeName), map[string]string{"probe_name": probeName})
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
				return newBundleInvariantError("edge_dns_bundle", "dns_protected_record", "dns-record:"+name, "protected record value is preserved", fmt.Sprintf("type=%s value=%s missing", recordType, value), fmt.Sprintf("protected %s %s value is missing", name, recordType), nil)
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
				return newBundleInvariantError("edge_dns_bundle", "route_dns_invariant", "dns-record:"+record.Name, "DNS answer has route host mapping", fmt.Sprintf("answer=%s edge_groups=%s route_hosts=0", answerIP, strings.Join(edgeGroups, ",")), fmt.Sprintf("%s %s answer %s maps to edge groups %s but no route hostname is known", record.Name, record.Type, answerIP, strings.Join(edgeGroups, ",")), nil)
			}
			for _, edgeGroupID := range edgeGroups {
				for _, routeHost := range routeHosts {
					if !edgeDNSRouteReadyForGroup(input.RouteReadyByHostnameEdgeGroup, routeHost, edgeGroupID) {
						return newBundleInvariantError("edge_dns_bundle", "route_dns_invariant", "hostname:"+routeHost, "DNS answer edge group is route-ready", fmt.Sprintf("record=%s type=%s answer=%s edge_group=%s route_ready=false", record.Name, record.Type, answerIP, edgeGroupID), fmt.Sprintf("%s %s answer %s points at edge group %s without active route for %s", record.Name, record.Type, answerIP, edgeGroupID, routeHost), nil)
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
