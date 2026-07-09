package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log"
	"math"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/store"
)

const (
	defaultEdgeDNSTTL            = 60
	defaultEdgeDNSProbeLabel     = "d-test"
	edgeDNSBundleVersionPrefix   = "dnsgen_"
	edgeDNSQualityRankingVersion = "edge-quality-scoped-v1"
)

type edgeDNSBundleOptions struct {
	DNSNodeID       string
	EdgeGroupID     string
	Zone            string
	AnswerIPs       []string
	RouteAAnswerIPs []string
	TTL             int
}

func (s *Server) handleEdgeDNSBundle(w http.ResponseWriter, r *http.Request) {
	authContext, ok := s.authorizeEdgeRequest(w, r)
	if !ok {
		return
	}

	options, err := s.edgeDNSBundleOptionsFromRequest(r)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := authContext.constrain(&options.DNSNodeID, &options.EdgeGroupID); err != nil {
		httpx.WriteError(w, http.StatusForbidden, err.Error())
		return
	}
	allowed, err := s.enforceScopedDNSNode(authContext, options.DNSNodeID, options.EdgeGroupID, options.Zone)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if !allowed {
		httpx.WriteError(w, http.StatusForbidden, "dns token cannot access another DNS zone")
		return
	}
	if bundle, ok, err := s.edgeDNSBundleArtifactForOptions(options, time.Now().UTC()); err == nil && ok {
		writeEdgeDNSBundleResponse(w, bundle)
		return
	} else if err != nil && s.log != nil {
		s.log.Printf("edge dns artifact lookup failed; falling back to read-only derive: dns_node_id=%s edge_group_id=%s zone=%s err=%v", options.DNSNodeID, options.EdgeGroupID, options.Zone, err)
	}

	bundle, err := s.deriveEdgeDNSBundle(r, options)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	writeEdgeDNSBundleResponse(w, bundle)
}

func writeEdgeDNSBundleResponse(w http.ResponseWriter, bundle model.EdgeDNSBundle) {
	etag := edgeRouteBundleETag(bundle.Version)
	w.Header().Set("ETag", etag)
	w.Header().Set("Cache-Control", "private, no-cache")
	w.Header().Set("X-Fugue-DNS-Bundle-Version", bundle.Version)

	// DNS bundles carry signed validity windows. Re-send unchanged content so
	// DNS nodes can refresh valid_until instead of going stale behind a 304.
	httpx.WriteJSON(w, http.StatusOK, bundle)
}

func (s *Server) edgeDNSBundleOptionsFromRequest(r *http.Request) (edgeDNSBundleOptions, error) {
	query := r.URL.Query()
	ttl := defaultEdgeDNSTTL
	if rawTTL := strings.TrimSpace(query.Get("ttl")); rawTTL != "" {
		parsed, err := strconv.Atoi(rawTTL)
		if err != nil || parsed <= 0 || parsed > 3600 {
			return edgeDNSBundleOptions{}, errInvalidEdgeDNSOption("ttl must be an integer between 1 and 3600")
		}
		ttl = parsed
	}

	answerIPs, err := parseEdgeDNSAnswerIPs(query["answer_ip"])
	if err != nil {
		return edgeDNSBundleOptions{}, err
	}
	routeAAnswerIPs, err := parseOptionalEdgeDNSAnswerIPs(query["route_a_answer_ip"])
	if err != nil {
		return edgeDNSBundleOptions{}, err
	}

	zone := normalizeExternalAppDomain(query.Get("zone"))
	if zone == "" {
		zone = normalizeExternalAppDomain(s.customDomainBaseDomain)
	}
	if zone == "" {
		return edgeDNSBundleOptions{}, errInvalidEdgeDNSOption("dns zone is not configured")
	}

	return edgeDNSBundleOptions{
		DNSNodeID:       strings.TrimSpace(query.Get("dns_node_id")),
		EdgeGroupID:     strings.TrimSpace(query.Get("edge_group_id")),
		Zone:            zone,
		AnswerIPs:       answerIPs,
		RouteAAnswerIPs: routeAAnswerIPs,
		TTL:             ttl,
	}, nil
}

func parseEdgeDNSAnswerIPs(values []string) ([]string, error) {
	out, err := parseOptionalEdgeDNSAnswerIPs(values)
	if err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, errInvalidEdgeDNSOption("at least one answer_ip is required")
	}
	return out, nil
}

func parseOptionalEdgeDNSAnswerIPs(values []string) ([]string, error) {
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		parts := strings.FieldsFunc(value, func(r rune) bool {
			return r == ',' || r == ';' || r == ' ' || r == '\n' || r == '\t'
		})
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			ip := net.ParseIP(part)
			if ip == nil {
				return nil, errInvalidEdgeDNSOption("answer_ip must contain only IP addresses")
			}
			normalized := ip.String()
			if _, ok := seen[normalized]; ok {
				continue
			}
			seen[normalized] = struct{}{}
			out = append(out, normalized)
		}
	}
	return out, nil
}

type errInvalidEdgeDNSOption string

func (e errInvalidEdgeDNSOption) Error() string {
	return string(e)
}

func (s *Server) deriveEdgeDNSBundle(r *http.Request, options edgeDNSBundleOptions) (model.EdgeDNSBundle, error) {
	apps, err := s.store.ListAppsMetadata("", true)
	if err != nil {
		return model.EdgeDNSBundle{}, err
	}
	domains, err := s.store.ListVerifiedAppDomains()
	if err != nil {
		return model.EdgeDNSBundle{}, err
	}
	runtimes, err := s.store.ListRuntimes("", true)
	if err != nil {
		return model.EdgeDNSBundle{}, err
	}
	policies, err := s.store.ListEdgeRoutePolicies()
	if err != nil {
		return model.EdgeDNSBundle{}, err
	}
	healthyEdgeGroups, healthyEdgeNodeIDsByGroup, err := s.edgeRouteHealthyEdgeGroupInventory()
	if err != nil {
		return model.EdgeDNSBundle{}, err
	}
	acmeChallenges, err := s.store.ListDNSACMEChallenges(options.Zone, false)
	if err != nil {
		return model.EdgeDNSBundle{}, err
	}
	var hostedZone model.HostedZone
	hostedRecords := []model.DNSRecord{}
	hostedZone, err = s.store.GetHostedZoneByName(options.Zone)
	if err == nil && hostedZone.Status != model.HostedZoneStatusDeleted {
		hostedRecords, err = s.store.ListDNSRecords(hostedZone.ID)
		if err != nil {
			return model.EdgeDNSBundle{}, err
		}
	} else if err != nil && err != store.ErrNotFound {
		return model.EdgeDNSBundle{}, err
	}

	runtimeByID := make(map[string]model.Runtime, len(runtimes))
	for _, runtimeObj := range runtimes {
		runtimeByID[strings.TrimSpace(runtimeObj.ID)] = runtimeObj
	}
	runtimeNodeLabelsByID := s.edgeRouteRuntimeNodeLabels(r.Context())
	apps = s.overlayManagedAppStatusesCached(apps)
	appByID := make(map[string]model.App, len(apps))
	for _, app := range apps {
		appByID[strings.TrimSpace(app.ID)] = app
	}
	policyByHostname := edgeRoutePolicyByHostname(policies)
	edgeAnswerIPsByGroup, err := s.edgeDNSAnswerIPsByGroup(r.Context(), options)
	if err != nil {
		return model.EdgeDNSBundle{}, err
	}
	edgeCandidateByIP, err := s.edgeDNSAnswerCandidateByIP(r.Context(), options)
	if err != nil {
		return model.EdgeDNSBundle{}, err
	}
	latencyProfiles, err := s.edgeDNSLatencyProfiles(options)
	if err != nil {
		return model.EdgeDNSBundle{}, err
	}
	applyQualityRanking := s.edgeQualityRankingActive()
	routeReadyByHostnameEdgeGroup := map[string]map[string]bool{}
	recordRouteHostsByName := map[string][]string{}

	staticRecords := edgeDNSStaticRecordsForZone(s.dnsStaticRecords, options.Zone)
	platformOverrideNames := s.edgeDNSPlatformDomainNames(domains, options.Zone)
	for hostname := range s.edgeDNSPlatformRouteNames(options.Zone) {
		platformOverrideNames[hostname] = true
	}
	staticRecords = edgeDNSStaticRecordsWithoutPlatformOverrides(staticRecords, platformOverrideNames)
	protectedNames := edgeDNSProtectedRecordNames(staticRecords)
	protectedRecordKeys := edgeDNSProtectedRecordKeys(staticRecords)
	readyCustomDomainTargets := s.edgeDNSReadyCustomDomainTargetNames(domains, appByID, options.Zone)

	acmeRecords := edgeDNSACMEChallengeRecords(acmeChallenges)

	records := make([]model.EdgeDNSRecord, 0, len(staticRecords)+len(acmeRecords)+len(hostedRecords)+len(apps)+len(domains)+len(s.platformRoutes)+1)
	records = append(records, staticRecords...)
	records = append(records, acmeRecords...)
	now := time.Now().UTC()
	records = append(records, edgeDNSRecordsForTarget(
		normalizeExternalAppDomain(defaultEdgeDNSProbeLabel+"."+options.Zone),
		options.AnswerIPs,
		options.TTL,
		model.EdgeDNSRecordKindProbe,
		model.EdgeRouteStatusActive,
		"",
		"",
		"",
		options.EdgeGroupID,
		"",
	)...)

	for _, platformRoute := range s.platformRoutes {
		hostname := normalizeExternalAppDomain(platformRoute.Hostname)
		if !edgeDNSTargetWithinZone(hostname, options.Zone) {
			continue
		}
		latencyProfile := latencyProfiles.globalProfile(hostname)
		registerEdgeDNSRouteReadyBindings(routeReadyByHostnameEdgeGroup, edgeRouteBindingsForPlatformRoute(platformRoute, healthyEdgeGroups, healthyEdgeNodeIDsByGroup))
		answerIPs := edgeDNSAnswerIPsForPlatformRoute(platformRoute, options, edgeAnswerIPsByGroup)
		if policy, ok := policyByHostname[hostname]; ok {
			answerIPs = edgeDNSFilterAnswerIPsForExclusions(answerIPs, edgeRoutePolicyActiveExclusions(policy, now), edgeCandidateByIP)
		}
		if len(answerIPs) == 0 {
			continue
		}
		edgeGroupID := strings.TrimSpace(platformRoute.EdgeGroupID)
		if edgeGroupID == "" {
			edgeGroupID = strings.TrimSpace(options.EdgeGroupID)
		}
		targetRecords := edgeDNSRecordsForTargetWithPolicy(
			hostname,
			answerIPs,
			edgeDNSPolicyTTL(platformRoute.TTL),
			model.EdgeDNSRecordKindPlatformRoute,
			platformRoute.Status,
			platformRoute.StatusReason,
			"",
			"",
			edgeGroupID,
			"",
			edgeDNSAnswerPolicy(options, edgeGroupID, "", answerIPs, edgeCandidateByIP, latencyProfile, platformRoute.TTL, applyQualityRanking),
			edgeDNSCandidatesForAnswerIPs(answerIPs, edgeCandidateByIP, routeReadyByHostnameEdgeGroup[hostname], edgeGroupID, "", latencyProfile, applyQualityRanking),
			latencyProfiles.scopedProfiles(hostname, answerIPs, edgeCandidateByIP, routeReadyByHostnameEdgeGroup[hostname], edgeGroupID, "", applyQualityRanking),
		)
		records = append(records, targetRecords...)
		registerEdgeDNSRecordRouteHost(recordRouteHostsByName, hostname, targetRecords...)
	}

	hostedStandardRecords, hostedAppRecords := partitionHostedDNSRecords(hostedRecords)
	for _, record := range hostedStandardRecords {
		if !edgeDNSTargetWithinZone(record.FQDN, options.Zone) || edgeDNSHostedRecordConflictsWithProtected(record, protectedRecordKeys) {
			continue
		}
		records = append(records, edgeDNSRecordsForHostedRecord(record)...)
	}

	for _, app := range appByID {
		if app.Route == nil || strings.TrimSpace(app.Route.Hostname) == "" {
			continue
		}
		hostname := normalizeExternalAppDomain(app.Route.Hostname)
		if !edgeDNSTargetWithinZone(hostname, options.Zone) || protectedNames[hostname] {
			continue
		}
		binding := s.deriveEdgeRouteBinding(r, app, hostname, model.EdgeRouteKindPlatform, model.EdgeRouteTLSPolicyPlatform, app.CreatedAt, app.UpdatedAt, runtimeByID, runtimeNodeLabelsByID)
		binding = applyEdgeRoutePolicy(binding, policyByHostname, healthyEdgeGroups, healthyEdgeNodeIDsByGroup, now)
		dnsBindings := expandDefaultPlatformEdgeBindings(binding, healthyEdgeGroups, healthyEdgeNodeIDsByGroup)
		registerEdgeDNSRouteReadyBindings(routeReadyByHostnameEdgeGroup, dnsBindings)
		answerIPs := edgeDNSAnswerIPsForBindings(dnsBindings, options, edgeAnswerIPsByGroup)
		answerIPs = edgeDNSFilterAnswerIPsForBinding(answerIPs, binding, edgeCandidateByIP)
		if len(answerIPs) == 0 {
			continue
		}
		latencyProfile := latencyProfiles.globalProfile(hostname)
		targetRecords := edgeDNSRecordsForTargetWithPolicy(
			hostname,
			answerIPs,
			edgeDNSPolicyTTL(options.TTL),
			model.EdgeDNSRecordKindPlatform,
			binding.Status,
			binding.StatusReason,
			app.ID,
			app.TenantID,
			binding.EdgeGroupID,
			binding.FallbackEdgeGroupID,
			edgeDNSAnswerPolicy(options, binding.EdgeGroupID, binding.FallbackEdgeGroupID, answerIPs, edgeCandidateByIP, latencyProfile, options.TTL, applyQualityRanking),
			edgeDNSCandidatesForAnswerIPs(answerIPs, edgeCandidateByIP, routeReadyByHostnameEdgeGroup[hostname], binding.EdgeGroupID, binding.FallbackEdgeGroupID, latencyProfile, applyQualityRanking),
			latencyProfiles.scopedProfiles(hostname, answerIPs, edgeCandidateByIP, routeReadyByHostnameEdgeGroup[hostname], binding.EdgeGroupID, binding.FallbackEdgeGroupID, applyQualityRanking),
		)
		records = append(records, targetRecords...)
		registerEdgeDNSRecordRouteHost(recordRouteHostsByName, hostname, targetRecords...)
	}

	for _, record := range hostedAppRecords {
		fqdn := normalizeExternalAppDomain(record.FQDN)
		if fqdn == "" || !edgeDNSTargetWithinZone(fqdn, options.Zone) || edgeDNSHostedAppRecordConflictsWithProtected(fqdn, protectedRecordKeys) || len(record.Values) == 0 {
			continue
		}
		app, ok := hostedDNSRecordApp(record, appByID)
		if !ok || app.Route == nil || strings.TrimSpace(app.Route.Hostname) == "" {
			continue
		}
		binding := s.deriveEdgeRouteBinding(r, app, fqdn, model.EdgeRouteKindCustomDomain, model.EdgeRouteTLSPolicyCustomDomain, record.CreatedAt, record.UpdatedAt, runtimeByID, runtimeNodeLabelsByID)
		binding = applyEdgeRoutePolicy(binding, policyByHostname, healthyEdgeGroups, healthyEdgeNodeIDsByGroup, now)
		dnsBindings := expandDefaultPlatformEdgeBindings(binding, healthyEdgeGroups, healthyEdgeNodeIDsByGroup)
		registerEdgeDNSRouteReadyBindings(routeReadyByHostnameEdgeGroup, dnsBindings)
		answerIPs := edgeDNSAnswerIPsForCustomDomainTarget(dnsBindings, options, edgeAnswerIPsByGroup)
		answerIPs = edgeDNSFilterAnswerIPsForBinding(answerIPs, binding, edgeCandidateByIP)
		if len(answerIPs) == 0 {
			continue
		}
		latencyProfile := latencyProfiles.globalProfile(fqdn)
		targetRecords := edgeDNSRecordsForTargetWithPolicy(
			fqdn,
			answerIPs,
			edgeDNSPolicyTTL(record.TTL),
			model.EdgeDNSRecordKindCustomDomainTarget,
			binding.Status,
			binding.StatusReason,
			app.ID,
			app.TenantID,
			binding.EdgeGroupID,
			binding.FallbackEdgeGroupID,
			edgeDNSAnswerPolicy(options, binding.EdgeGroupID, binding.FallbackEdgeGroupID, answerIPs, edgeCandidateByIP, latencyProfile, record.TTL, applyQualityRanking),
			edgeDNSCandidatesForAnswerIPs(answerIPs, edgeCandidateByIP, routeReadyByHostnameEdgeGroup[fqdn], binding.EdgeGroupID, binding.FallbackEdgeGroupID, latencyProfile, applyQualityRanking),
			latencyProfiles.scopedProfiles(fqdn, answerIPs, edgeCandidateByIP, routeReadyByHostnameEdgeGroup[fqdn], binding.EdgeGroupID, binding.FallbackEdgeGroupID, applyQualityRanking),
		)
		records = append(records, targetRecords...)
		registerEdgeDNSRecordRouteHost(recordRouteHostsByName, fqdn, targetRecords...)
	}

	for _, app := range appByID {
		if app.Route == nil || strings.TrimSpace(app.Route.Hostname) == "" {
			continue
		}
		target := s.primaryCustomDomainTarget(app)
		if target == "" {
			continue
		}
		hostname := normalizeExternalAppDomain(app.Route.Hostname)
		if hostname == "" ||
			!edgeDNSTargetWithinZone(hostname, s.appBaseDomain) ||
			!edgeDNSTargetWithinZone(target, options.Zone) ||
			readyCustomDomainTargets[target] ||
			protectedNames[target] {
			continue
		}
		binding := s.deriveEdgeRouteBinding(r, app, hostname, model.EdgeRouteKindPlatform, model.EdgeRouteTLSPolicyPlatform, app.CreatedAt, app.UpdatedAt, runtimeByID, runtimeNodeLabelsByID)
		binding = applyEdgeRoutePolicy(binding, policyByHostname, healthyEdgeGroups, healthyEdgeNodeIDsByGroup, now)
		dnsBindings := expandDefaultPlatformEdgeBindings(binding, healthyEdgeGroups, healthyEdgeNodeIDsByGroup)
		registerEdgeDNSRouteReadyBindings(routeReadyByHostnameEdgeGroup, dnsBindings)
		answerIPs := edgeDNSAnswerIPsForCustomDomainTarget(dnsBindings, options, edgeAnswerIPsByGroup)
		answerIPs = edgeDNSFilterAnswerIPsForBinding(answerIPs, binding, edgeCandidateByIP)
		if len(answerIPs) == 0 {
			continue
		}
		latencyProfile := latencyProfiles.globalProfile(hostname)
		targetRecords := edgeDNSRecordsForTargetWithPolicy(
			target,
			answerIPs,
			edgeDNSPolicyTTL(options.TTL),
			model.EdgeDNSRecordKindCustomDomainTarget,
			binding.Status,
			binding.StatusReason,
			app.ID,
			app.TenantID,
			binding.EdgeGroupID,
			binding.FallbackEdgeGroupID,
			edgeDNSAnswerPolicy(options, binding.EdgeGroupID, binding.FallbackEdgeGroupID, answerIPs, edgeCandidateByIP, latencyProfile, options.TTL, applyQualityRanking),
			edgeDNSCandidatesForAnswerIPs(answerIPs, edgeCandidateByIP, routeReadyByHostnameEdgeGroup[hostname], binding.EdgeGroupID, binding.FallbackEdgeGroupID, latencyProfile, applyQualityRanking),
			latencyProfiles.scopedProfiles(hostname, answerIPs, edgeCandidateByIP, routeReadyByHostnameEdgeGroup[hostname], binding.EdgeGroupID, binding.FallbackEdgeGroupID, applyQualityRanking),
		)
		records = append(records, targetRecords...)
		registerEdgeDNSRecordRouteHost(recordRouteHostsByName, hostname, targetRecords...)
	}

	for _, domain := range domains {
		hostname := normalizeExternalAppDomain(domain.Hostname)
		if hostname == "" {
			continue
		}
		app, ok := appByID[strings.TrimSpace(domain.AppID)]
		if !ok {
			continue
		}
		routeKind := model.EdgeRouteKindCustomDomain
		tlsPolicy := model.EdgeRouteTLSPolicyCustomDomain
		recordKind := model.EdgeDNSRecordKindCustomDomainTarget
		platformDomain := false
		switch {
		case s.isPlatformOwnedDomainBinding(hostname):
			routeKind = model.EdgeRouteKindPlatformDomain
			tlsPolicy = model.EdgeRouteTLSPolicyPlatform
			recordKind = model.EdgeDNSRecordKindPlatformDomain
			platformDomain = true
		case s.managedEdgeCustomDomain(hostname):
		default:
			continue
		}
		binding := s.deriveEdgeRouteBinding(r, app, hostname, routeKind, tlsPolicy, domain.CreatedAt, domain.UpdatedAt, runtimeByID, runtimeNodeLabelsByID)
		binding = applyEdgeRoutePolicy(binding, policyByHostname, healthyEdgeGroups, healthyEdgeNodeIDsByGroup, now)
		binding = applyCustomDomainReadiness(binding, domain)
		dnsBindings := expandDefaultPlatformEdgeBindings(binding, healthyEdgeGroups, healthyEdgeNodeIDsByGroup)
		registerEdgeDNSRouteReadyBindings(routeReadyByHostnameEdgeGroup, dnsBindings)
		if routeKind == model.EdgeRouteKindCustomDomain &&
			(domain.Status != model.AppDomainStatusVerified ||
				domain.DNSStatus != model.AppDomainDNSStatusReady ||
				domain.TLSStatus != model.AppDomainTLSStatusReady) {
			continue
		}
		target := hostname
		if !platformDomain {
			target = normalizeExternalAppDomain(domain.RouteTarget)
		}
		if target == "" && !platformDomain {
			target = normalizeExternalAppDomain(s.primaryCustomDomainTarget(app))
		}
		if !edgeDNSTargetWithinZone(target, options.Zone) || (!platformDomain && protectedNames[target]) {
			continue
		}
		answerIPs := edgeDNSAnswerIPsForBindings(dnsBindings, options, edgeAnswerIPsByGroup)
		if routeKind == model.EdgeRouteKindCustomDomain {
			answerIPs = edgeDNSAnswerIPsForCustomDomainTarget(dnsBindings, options, edgeAnswerIPsByGroup)
		}
		answerIPs = edgeDNSFilterAnswerIPsForBinding(answerIPs, binding, edgeCandidateByIP)
		if len(answerIPs) == 0 {
			continue
		}
		latencyProfile := latencyProfiles.globalProfile(hostname)
		targetRecords := edgeDNSRecordsForTargetWithPolicy(
			target,
			answerIPs,
			edgeDNSPolicyTTL(options.TTL),
			recordKind,
			binding.Status,
			binding.StatusReason,
			app.ID,
			app.TenantID,
			binding.EdgeGroupID,
			binding.FallbackEdgeGroupID,
			edgeDNSAnswerPolicy(options, binding.EdgeGroupID, binding.FallbackEdgeGroupID, answerIPs, edgeCandidateByIP, latencyProfile, options.TTL, applyQualityRanking),
			edgeDNSCandidatesForAnswerIPs(answerIPs, edgeCandidateByIP, routeReadyByHostnameEdgeGroup[hostname], binding.EdgeGroupID, binding.FallbackEdgeGroupID, latencyProfile, applyQualityRanking),
			latencyProfiles.scopedProfiles(hostname, answerIPs, edgeCandidateByIP, routeReadyByHostnameEdgeGroup[hostname], binding.EdgeGroupID, binding.FallbackEdgeGroupID, applyQualityRanking),
		)
		records = append(records, targetRecords...)
		registerEdgeDNSRecordRouteHost(recordRouteHostsByName, hostname, targetRecords...)
	}

	records = dedupeAndSortEdgeDNSRecords(records)
	bundle := model.EdgeDNSBundle{
		GeneratedAt: time.Now().UTC(),
		DNSNodeID:   options.DNSNodeID,
		EdgeGroupID: options.EdgeGroupID,
		Zone:        options.Zone,
		Records:     records,
	}
	bundle.Version = edgeDNSBundleVersion(bundle)
	bundle.Generation = bundle.Version
	answerEdgeGroupsByIP := edgeDNSAnswerGroupsByIP(edgeAnswerIPsByGroup)
	if err := validateEdgeDNSBundleForPublish(bundle, edgeDNSBundleInvariantInput{
		Options:                       options,
		ProtectedRecords:              staticRecords,
		AnswerEdgeGroupsByIP:          answerEdgeGroupsByIP,
		RouteReadyByHostnameEdgeGroup: routeReadyByHostnameEdgeGroup,
		RecordRouteHostsByName:        recordRouteHostsByName,
	}); err != nil {
		return model.EdgeDNSBundle{}, err
	}
	bundle = signEdgeDNSBundle(bundle, s.bundleKeyring(), s.discoveryBundleTTL())
	return bundle, nil
}

func (s *Server) edgeDNSPlatformDomainNames(domains []model.AppDomain, zone string) map[string]bool {
	zone = normalizeExternalAppDomain(zone)
	out := make(map[string]bool)
	for _, domain := range domains {
		hostname := normalizeExternalAppDomain(domain.Hostname)
		if hostname == "" || !s.isPlatformOwnedDomainBinding(hostname) {
			continue
		}
		if zone != "" && !edgeDNSTargetWithinZone(hostname, zone) {
			continue
		}
		out[hostname] = true
	}
	return out
}

func (s *Server) edgeDNSPlatformRouteNames(zone string) map[string]bool {
	zone = normalizeExternalAppDomain(zone)
	out := make(map[string]bool)
	for _, route := range s.platformRoutes {
		hostname := normalizeExternalAppDomain(route.Hostname)
		if hostname == "" {
			continue
		}
		if zone != "" && !edgeDNSTargetWithinZone(hostname, zone) {
			continue
		}
		out[hostname] = true
	}
	return out
}

func (s *Server) edgeDNSReadyCustomDomainTargetNames(domains []model.AppDomain, appByID map[string]model.App, zone string) map[string]bool {
	zone = normalizeExternalAppDomain(zone)
	out := make(map[string]bool)
	for _, domain := range domains {
		hostname := normalizeExternalAppDomain(domain.Hostname)
		if hostname == "" || s.isPlatformOwnedDomainBinding(hostname) || !s.managedEdgeCustomDomain(hostname) {
			continue
		}
		if domain.TLSStatus != model.AppDomainTLSStatusReady {
			continue
		}
		app, ok := appByID[strings.TrimSpace(domain.AppID)]
		if !ok {
			continue
		}
		target := normalizeExternalAppDomain(domain.RouteTarget)
		if target == "" {
			target = normalizeExternalAppDomain(s.primaryCustomDomainTarget(app))
		}
		if target == "" {
			continue
		}
		if zone != "" && !edgeDNSTargetWithinZone(target, zone) {
			continue
		}
		out[target] = true
	}
	return out
}

type edgeDNSStaticRecordsEnvelope struct {
	Records []model.EdgeDNSRecord `json:"records"`
}

func parseEdgeDNSStaticRecords(raw string, logger *log.Logger) []model.EdgeDNSRecord {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	var records []model.EdgeDNSRecord
	if err := json.Unmarshal([]byte(raw), &records); err != nil {
		var envelope edgeDNSStaticRecordsEnvelope
		if envelopeErr := json.Unmarshal([]byte(raw), &envelope); envelopeErr != nil {
			if logger != nil {
				logger.Printf("ignoring FUGUE_DNS_STATIC_RECORDS_JSON: %v", err)
			}
			return nil
		}
		records = envelope.Records
	}
	out := make([]model.EdgeDNSRecord, 0, len(records))
	for _, record := range records {
		normalized, ok := normalizeEdgeDNSStaticRecord(record)
		if ok {
			out = append(out, normalized)
		}
	}
	return dedupeAndSortEdgeDNSRecords(out)
}

func normalizeEdgeDNSStaticRecord(record model.EdgeDNSRecord) (model.EdgeDNSRecord, bool) {
	record.Name = normalizeExternalAppDomain(record.Name)
	record.Type = strings.ToUpper(strings.TrimSpace(record.Type))
	if record.Name == "" || !edgeDNSSupportedRecordType(record.Type) {
		return model.EdgeDNSRecord{}, false
	}
	if record.TTL <= 0 {
		record.TTL = defaultEdgeDNSTTL
	}
	if record.RecordKind == "" {
		record.RecordKind = model.EdgeDNSRecordKindProtected
	}
	if record.Status == "" {
		record.Status = model.EdgeRouteStatusActive
	}
	record.AppID = strings.TrimSpace(record.AppID)
	record.TenantID = strings.TrimSpace(record.TenantID)
	record.EdgeGroupID = strings.TrimSpace(record.EdgeGroupID)
	record.FallbackEdgeGroupID = strings.TrimSpace(record.FallbackEdgeGroupID)
	record.StatusReason = strings.TrimSpace(record.StatusReason)

	values := make([]string, 0, len(record.Values))
	for _, value := range record.Values {
		normalized := normalizeEdgeDNSStaticRecordValue(record.Type, value)
		if normalized == "" {
			continue
		}
		values = append(values, normalized)
	}
	record.Values = uniqueSortedStrings(values)
	if len(record.Values) == 0 {
		return model.EdgeDNSRecord{}, false
	}
	record.RecordGeneration = edgeDNSRecordGeneration(record)
	return record, true
}

func edgeDNSSupportedRecordType(recordType string) bool {
	switch strings.ToUpper(strings.TrimSpace(recordType)) {
	case model.EdgeDNSRecordTypeA,
		model.EdgeDNSRecordTypeAAAA,
		model.EdgeDNSRecordTypeCAA,
		model.EdgeDNSRecordTypeCNAME,
		model.EdgeDNSRecordTypeMX,
		model.EdgeDNSRecordTypeNS,
		model.EdgeDNSRecordTypeSRV,
		model.EdgeDNSRecordTypeTXT:
		return true
	default:
		return false
	}
}

func normalizeEdgeDNSStaticRecordValue(recordType, value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	switch strings.ToUpper(strings.TrimSpace(recordType)) {
	case model.EdgeDNSRecordTypeA:
		ip := net.ParseIP(value)
		if ip == nil || ip.To4() == nil {
			return ""
		}
		return ip.To4().String()
	case model.EdgeDNSRecordTypeAAAA:
		ip := net.ParseIP(value)
		if ip == nil || ip.To4() != nil {
			return ""
		}
		return ip.String()
	case model.EdgeDNSRecordTypeCNAME, model.EdgeDNSRecordTypeNS:
		return normalizeExternalAppDomain(value)
	default:
		return value
	}
}

func edgeDNSStaticRecordsForZone(records []model.EdgeDNSRecord, zone string) []model.EdgeDNSRecord {
	zone = normalizeExternalAppDomain(zone)
	if zone == "" || len(records) == 0 {
		return nil
	}
	out := make([]model.EdgeDNSRecord, 0, len(records))
	for _, record := range records {
		if edgeDNSTargetWithinZone(record.Name, zone) {
			out = append(out, record)
		}
	}
	return out
}

func edgeDNSStaticRecordsWithoutPlatformOverrides(records []model.EdgeDNSRecord, platformDomainNames map[string]bool) []model.EdgeDNSRecord {
	if len(records) == 0 || len(platformDomainNames) == 0 {
		return records
	}
	out := make([]model.EdgeDNSRecord, 0, len(records))
	for _, record := range records {
		name := normalizeExternalAppDomain(record.Name)
		if platformDomainNames[name] {
			switch strings.ToUpper(strings.TrimSpace(record.Type)) {
			case model.EdgeDNSRecordTypeA, model.EdgeDNSRecordTypeAAAA, model.EdgeDNSRecordTypeCNAME:
				continue
			}
		}
		out = append(out, record)
	}
	return out
}

func edgeDNSProtectedRecordNames(records []model.EdgeDNSRecord) map[string]bool {
	out := make(map[string]bool, len(records))
	for _, record := range records {
		if record.RecordKind == model.EdgeDNSRecordKindProtected {
			if name := normalizeExternalAppDomain(record.Name); name != "" && !strings.HasPrefix(name, "*.") {
				out[name] = true
			}
		}
	}
	return out
}

func edgeDNSProtectedRecordKeys(records []model.EdgeDNSRecord) map[string]bool {
	out := make(map[string]bool, len(records))
	for _, record := range records {
		if record.RecordKind != model.EdgeDNSRecordKindProtected {
			continue
		}
		name := normalizeExternalAppDomain(record.Name)
		recordType := strings.ToUpper(strings.TrimSpace(record.Type))
		if name == "" || recordType == "" {
			continue
		}
		out[name+"\x00"+recordType] = true
	}
	return out
}

func edgeDNSHostedRecordConflictsWithProtected(record model.DNSRecord, protected map[string]bool) bool {
	name := normalizeExternalAppDomain(record.FQDN)
	if name == "" {
		return true
	}
	if hostedDNSRecordNeedsFlatten(record) {
		return protected[name+"\x00"+model.EdgeDNSRecordTypeA] || protected[name+"\x00"+model.EdgeDNSRecordTypeAAAA]
	}
	recordType := model.NormalizeDNSRecordType(record.Type)
	if recordType == model.DNSRecordTypeALIAS || recordType == model.DNSRecordTypeANAME || recordType == model.DNSRecordTypeFUGUEAPP {
		return protected[name+"\x00"+model.EdgeDNSRecordTypeA] || protected[name+"\x00"+model.EdgeDNSRecordTypeAAAA]
	}
	return protected[name+"\x00"+strings.ToUpper(recordType)]
}

func edgeDNSHostedAppRecordConflictsWithProtected(fqdn string, protected map[string]bool) bool {
	fqdn = normalizeExternalAppDomain(fqdn)
	if fqdn == "" {
		return true
	}
	return protected[fqdn+"\x00"+model.EdgeDNSRecordTypeA] || protected[fqdn+"\x00"+model.EdgeDNSRecordTypeAAAA]
}

func edgeDNSACMEChallengeRecords(challenges []model.DNSACMEChallenge) []model.EdgeDNSRecord {
	if len(challenges) == 0 {
		return nil
	}
	records := make([]model.EdgeDNSRecord, 0, len(challenges))
	for _, challenge := range challenges {
		record := edgeDNSRecord(
			challenge.Name,
			model.EdgeDNSRecordTypeTXT,
			[]string{challenge.Value},
			challenge.TTL,
			model.EdgeDNSRecordKindACMEChallenge,
			model.EdgeRouteStatusActive,
			"",
			"",
			"",
			"",
			"",
		)
		records = append(records, record)
	}
	return records
}

func partitionHostedDNSRecords(records []model.DNSRecord) ([]model.DNSRecord, []model.DNSRecord) {
	standard := make([]model.DNSRecord, 0, len(records))
	app := []model.DNSRecord{}
	for _, record := range records {
		record.Type = model.NormalizeDNSRecordType(record.Type)
		record.Status = model.NormalizeDNSRecordStatus(record.Status)
		if record.Status == model.DNSRecordStatusDisabled || record.Status == model.DNSRecordStatusConflict || record.Type == "" {
			continue
		}
		if record.Type == model.DNSRecordTypeFUGUEAPP {
			app = append(app, record)
			continue
		}
		standard = append(standard, record)
	}
	return standard, app
}

func edgeDNSRecordsForHostedRecord(record model.DNSRecord) []model.EdgeDNSRecord {
	name := normalizeExternalAppDomain(record.FQDN)
	if name == "" {
		return nil
	}
	record.Type = model.NormalizeDNSRecordType(record.Type)
	record.FlattenMode = model.NormalizeDNSRecordFlattenMode(record.FlattenMode)
	status := model.EdgeRouteStatusActive
	reason := strings.TrimSpace(record.LastMessage)
	if record.Status == model.DNSRecordStatusDegraded || record.FlattenStatus == model.DNSRecordFlattenStatusDegraded || record.FlattenStatus == model.DNSRecordFlattenStatusStale || strings.TrimSpace(record.ResolveError) != "" {
		status = model.EdgeRouteStatusUnavailable
		if reason == "" {
			reason = strings.TrimSpace(record.ResolveError)
		}
		if reason == "" {
			reason = "hosted DNS record is degraded"
		}
	}
	if hostedDNSRecordNeedsFlatten(record) {
		if record.FlattenFallbackPolicy == model.DNSRecordFlattenFallbackFailClosed && len(record.FlattenedA) == 0 && len(record.FlattenedAAAA) == 0 {
			return nil
		}
		out := edgeDNSRecordsForTarget(name, append(append([]string(nil), record.FlattenedA...), record.FlattenedAAAA...), record.TTL, model.EdgeDNSRecordKindHosted, status, reason, "", record.TenantID, "", "")
		if len(out) == 0 && record.FlattenFallbackPolicy == model.DNSRecordFlattenFallbackEmptyNoError {
			return nil
		}
		return out
	}
	switch record.Type {
	case model.DNSRecordTypeA,
		model.DNSRecordTypeAAAA,
		model.DNSRecordTypeCAA,
		model.DNSRecordTypeCNAME,
		model.DNSRecordTypeMX,
		model.DNSRecordTypeNS,
		model.DNSRecordTypeSRV,
		model.DNSRecordTypeTXT:
		return []model.EdgeDNSRecord{edgeDNSRecord(name, record.Type, record.Values, record.TTL, model.EdgeDNSRecordKindHosted, status, reason, "", record.TenantID, "", "")}
	default:
		return nil
	}
}

func hostedDNSRecordNeedsFlatten(record model.DNSRecord) bool {
	switch model.NormalizeDNSRecordType(record.Type) {
	case model.DNSRecordTypeALIAS, model.DNSRecordTypeANAME:
		return true
	case model.DNSRecordTypeCNAME:
		return model.NormalizeDNSRecordFlattenMode(record.FlattenMode) != model.DNSRecordFlattenModeNone
	default:
		return false
	}
}

func hostedDNSRecordApp(record model.DNSRecord, appByID map[string]model.App) (model.App, bool) {
	if len(record.Values) == 0 {
		return model.App{}, false
	}
	value := strings.TrimSpace(record.Values[0])
	if value == "" {
		return model.App{}, false
	}
	if app, ok := appByID[value]; ok && strings.TrimSpace(app.TenantID) == strings.TrimSpace(record.TenantID) {
		return app, true
	}
	for _, app := range appByID {
		if strings.TrimSpace(app.TenantID) != strings.TrimSpace(record.TenantID) {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(app.Name), value) {
			return app, true
		}
	}
	return model.App{}, false
}

func (s *Server) edgeDNSAnswerIPsByGroup(ctx context.Context, options edgeDNSBundleOptions) (map[string][]string, error) {
	out := map[string][]string{}
	blockedGroups := map[string]bool{}
	if s.store != nil {
		nodes, _, err := s.store.ListEdgeNodes("")
		if err != nil {
			return nil, err
		}
		now := time.Now().UTC()
		liveServingByNode := s.edgeLiveServingByNode(ctx, now)
		for _, node := range nodes {
			if !edgeNodeRouteServingCapableWithLive(node, now, liveServingByNode) {
				continue
			}
			if !edgeNodeDNSEligible(node) {
				continue
			}
			if !edgeNodeDNSCacheValid(node) {
				if groupID := strings.TrimSpace(node.EdgeGroupID); groupID != "" {
					blockedGroups[groupID] = true
				}
				continue
			}
			groupID := strings.TrimSpace(node.EdgeGroupID)
			if groupID == "" {
				continue
			}
			out[groupID] = appendEdgeDNSUniqueIP(out[groupID], node.PublicIPv4)
			out[groupID] = appendEdgeDNSUniqueIP(out[groupID], node.PublicIPv6)
		}
	}
	if options.EdgeGroupID != "" && len(out[options.EdgeGroupID]) == 0 && !blockedGroups[options.EdgeGroupID] {
		out[options.EdgeGroupID] = append([]string(nil), options.AnswerIPs...)
	}
	return out, nil
}

func (s *Server) edgeDNSAnswerCandidateByIP(ctx context.Context, options edgeDNSBundleOptions) (map[string]model.EdgeDNSAnswerCandidate, error) {
	out := map[string]model.EdgeDNSAnswerCandidate{}
	blockedIPs := map[string]bool{}
	if s.store != nil {
		nodes, _, err := s.store.ListEdgeNodes("")
		if err != nil {
			return nil, err
		}
		now := time.Now().UTC()
		liveServingByNode := s.edgeLiveServingByNode(ctx, now)
		for _, node := range nodes {
			if !edgeNodeRouteServingCapableWithLive(node, now, liveServingByNode) {
				continue
			}
			if !edgeNodeDNSEligible(node) {
				continue
			}
			if !edgeNodeDNSCacheValid(node) {
				for _, ip := range []string{node.PublicIPv4, node.PublicIPv6} {
					normalized := normalizeEdgeDNSStaticRecordValue(model.EdgeDNSRecordTypeA, ip)
					if normalized == "" {
						normalized = normalizeEdgeDNSStaticRecordValue(model.EdgeDNSRecordTypeAAAA, ip)
					}
					if normalized != "" {
						blockedIPs[normalized] = true
					}
				}
				continue
			}
			for _, ip := range []string{node.PublicIPv4, node.PublicIPv6} {
				normalized := normalizeEdgeDNSStaticRecordValue(model.EdgeDNSRecordTypeA, ip)
				if normalized == "" {
					normalized = normalizeEdgeDNSStaticRecordValue(model.EdgeDNSRecordTypeAAAA, ip)
				}
				if normalized == "" {
					continue
				}
				out[normalized] = edgeDNSAnswerCandidateForNode(normalized, node, options.EdgeGroupID)
			}
		}
	}
	for _, ip := range options.AnswerIPs {
		normalized := normalizeEdgeDNSStaticRecordValue(model.EdgeDNSRecordTypeA, ip)
		if normalized == "" {
			normalized = normalizeEdgeDNSStaticRecordValue(model.EdgeDNSRecordTypeAAAA, ip)
		}
		if normalized == "" {
			continue
		}
		if blockedIPs[normalized] {
			continue
		}
		if _, ok := out[normalized]; ok {
			continue
		}
		out[normalized] = model.EdgeDNSAnswerCandidate{
			IP:          normalized,
			EdgeGroupID: strings.TrimSpace(options.EdgeGroupID),
			Priority:    edgeDNSCandidatePriority(strings.TrimSpace(options.EdgeGroupID), strings.TrimSpace(options.EdgeGroupID), ""),
			Weight:      100,
			Reason:      "local_dns_node_answer",
			Healthy:     true,
			RouteReady:  true,
			TLSReady:    true,
		}
	}
	return out, nil
}

func edgeNodeDNSCacheValid(node model.EdgeNode) bool {
	status := strings.ToLower(strings.TrimSpace(node.CacheStatus))
	if status == "" {
		return true
	}
	for _, marker := range []string{"error", "invalid", "corrupt", "expired", "max_stale"} {
		if strings.Contains(status, marker) {
			return false
		}
	}
	return true
}

func edgeDNSAnswerCandidateForNode(ip string, node model.EdgeNode, localEdgeGroupID string) model.EdgeDNSAnswerCandidate {
	groupID := strings.TrimSpace(node.EdgeGroupID)
	reason := edgeDNSCandidateReason(groupID, strings.TrimSpace(localEdgeGroupID), "")
	if model.NormalizeEdgeWorkloadMode(node.WorkloadMode) == model.EdgeWorkloadModeDynamic {
		reason = strings.TrimSpace(reason + "; dynamic_" + edgeNodeEffectiveCanaryState(node))
	}
	return model.EdgeDNSAnswerCandidate{
		IP:                strings.TrimSpace(ip),
		EdgeID:            strings.TrimSpace(node.ID),
		EdgeGroupID:       groupID,
		Region:            strings.TrimSpace(node.Region),
		Country:           strings.ToLower(strings.TrimSpace(node.Country)),
		WorkloadMode:      edgeNodeEffectiveWorkloadMode(node),
		CanaryState:       edgeNodeEffectiveCanaryState(node),
		CanaryWeight:      edgeNodeEffectiveCanaryWeight(node),
		PublicProbeStatus: edgeNodeEffectivePublicProbeStatus(node),
		ServingGeneration: strings.TrimSpace(node.ServingGeneration),
		LKGGeneration:     strings.TrimSpace(node.LKGGeneration),
		CacheStatus:       strings.TrimSpace(node.CacheStatus),
		DNSEligible:       edgeNodeDNSEligible(node),
		Priority:          edgeDNSCandidatePriority(groupID, strings.TrimSpace(localEdgeGroupID), ""),
		Weight:            edgeNodeEffectiveCanaryWeight(node),
		Reason:            reason,
		Healthy:           node.Healthy && !node.Draining,
		RouteReady:        edgeNodeHasRouteState(node),
		TLSReady:          edgeNodeTLSReadyForDNS(node),
	}
}

func edgeNodeEffectiveWorkloadMode(node model.EdgeNode) string {
	mode := model.NormalizeEdgeWorkloadMode(node.WorkloadMode)
	if mode == "" {
		return model.EdgeWorkloadModeStatic
	}
	return mode
}

func edgeNodeEffectiveCanaryState(node model.EdgeNode) string {
	state := model.NormalizeEdgeCanaryState(node.CanaryState)
	if state != "" {
		return state
	}
	if edgeNodeEffectiveWorkloadMode(node) == model.EdgeWorkloadModeDynamic {
		return model.EdgeCanaryStateJoined
	}
	return model.EdgeCanaryStateActive
}

func edgeNodeEffectivePublicProbeStatus(node model.EdgeNode) string {
	status := model.NormalizeEdgePublicProbeStatus(node.PublicProbeStatus)
	if status == "" {
		return model.EdgePublicProbeStatusUnknown
	}
	return status
}

func edgeNodeEffectiveCanaryWeight(node model.EdgeNode) int {
	state := edgeNodeEffectiveCanaryState(node)
	weight := node.CanaryWeight
	switch state {
	case model.EdgeCanaryStateActive:
		if weight <= 0 {
			return 100
		}
		if weight > 100 {
			return 100
		}
		return weight
	case model.EdgeCanaryStateCanary:
		if weight <= 0 {
			return 1
		}
		if weight > 5 {
			return 5
		}
		return weight
	default:
		return 0
	}
}

func edgeNodeDNSEligible(node model.EdgeNode) bool {
	if edgeNodeEffectiveWorkloadMode(node) != model.EdgeWorkloadModeDynamic {
		return true
	}
	if edgeNodeEffectivePublicProbeStatus(node) == model.EdgePublicProbeStatusFailing {
		return false
	}
	switch edgeNodeEffectiveCanaryState(node) {
	case model.EdgeCanaryStateCanary, model.EdgeCanaryStateActive:
		return edgeNodeEffectiveCanaryWeight(node) > 0
	default:
		return false
	}
}

func edgeNodeTLSReadyForDNS(node model.EdgeNode) bool {
	switch model.NormalizeEdgeTLSStatus(node.TLSStatus) {
	case model.EdgeTLSStatusReady:
		return true
	case model.EdgeTLSStatusPending, model.EdgeTLSStatusError:
		return false
	default:
		return edgeNodeHasRouteState(node) && strings.TrimSpace(node.CaddyLastError) == ""
	}
}

func edgeDNSAnswerGroupsByIP(edgeAnswerIPsByGroup map[string][]string) map[string][]string {
	out := make(map[string][]string)
	for groupID, ips := range edgeAnswerIPsByGroup {
		groupID = strings.TrimSpace(groupID)
		if groupID == "" {
			continue
		}
		for _, ip := range ips {
			ip = strings.TrimSpace(ip)
			if ip == "" {
				continue
			}
			if !stringSliceContains(out[ip], groupID) {
				out[ip] = append(out[ip], groupID)
			}
		}
	}
	for ip := range out {
		sort.Strings(out[ip])
	}
	return out
}

func registerEdgeDNSRouteReadyBindings(routeReady map[string]map[string]bool, bindings []model.EdgeRouteBinding) {
	for _, binding := range bindings {
		registerEdgeDNSRouteReadyBinding(routeReady, binding)
	}
}

func registerEdgeDNSRouteReadyBinding(routeReady map[string]map[string]bool, binding model.EdgeRouteBinding) {
	hostname := normalizeExternalAppDomain(binding.Hostname)
	if hostname == "" {
		return
	}
	if strings.EqualFold(binding.Status, model.EdgeRouteStatusActive) && model.EdgeRoutePolicyAllowsTraffic(binding.RoutePolicy) && strings.TrimSpace(binding.UpstreamURL) != "" {
		registerEdgeDNSRouteReady(routeReady, hostname, binding.EdgeGroupID)
		registerEdgeDNSRouteReady(routeReady, hostname, binding.FallbackEdgeGroupID)
	}
}

func registerEdgeDNSRouteReady(routeReady map[string]map[string]bool, hostname, edgeGroupID string) {
	hostname = normalizeExternalAppDomain(hostname)
	edgeGroupID = strings.TrimSpace(edgeGroupID)
	if hostname == "" || edgeGroupID == "" {
		return
	}
	if _, ok := routeReady[hostname]; !ok {
		routeReady[hostname] = make(map[string]bool)
	}
	routeReady[hostname][edgeGroupID] = true
}

func registerEdgeDNSRecordRouteHost(recordRouteHostsByName map[string][]string, routeHost string, records ...model.EdgeDNSRecord) {
	routeHost = normalizeExternalAppDomain(routeHost)
	if routeHost == "" {
		return
	}
	for _, record := range records {
		name := normalizeExternalAppDomain(record.Name)
		if name == "" {
			continue
		}
		if !stringSliceContains(recordRouteHostsByName[name], routeHost) {
			recordRouteHostsByName[name] = append(recordRouteHostsByName[name], routeHost)
			sort.Strings(recordRouteHostsByName[name])
		}
	}
}

func appendEdgeDNSUniqueIP(values []string, raw string) []string {
	ip := net.ParseIP(strings.TrimSpace(raw))
	if ip == nil {
		return values
	}
	normalized := ip.String()
	for _, existing := range values {
		if existing == normalized {
			return values
		}
	}
	return append(values, normalized)
}

func edgeDNSAnswerIPsForBinding(binding model.EdgeRouteBinding, options edgeDNSBundleOptions, edgeAnswerIPsByGroup map[string][]string) []string {
	if model.EdgeRoutePolicyAllowsTraffic(binding.RoutePolicy) {
		if binding.Status != model.EdgeRouteStatusActive {
			return nil
		}
		out := []string{}
		for _, ip := range edgeAnswerIPsByGroup[strings.TrimSpace(binding.EdgeGroupID)] {
			out = appendEdgeDNSUniqueIP(out, ip)
		}
		for _, ip := range edgeAnswerIPsByGroup[strings.TrimSpace(binding.FallbackEdgeGroupID)] {
			out = appendEdgeDNSUniqueIP(out, ip)
		}
		if len(out) > 0 {
			return out
		}
		return nil
	}
	if len(options.RouteAAnswerIPs) > 0 {
		return append([]string(nil), options.RouteAAnswerIPs...)
	}
	return append([]string(nil), options.AnswerIPs...)
}

func edgeDNSAnswerIPsForBindings(bindings []model.EdgeRouteBinding, options edgeDNSBundleOptions, edgeAnswerIPsByGroup map[string][]string) []string {
	out := []string{}
	for _, binding := range bindings {
		for _, ip := range edgeDNSAnswerIPsForBinding(binding, options, edgeAnswerIPsByGroup) {
			out = appendEdgeDNSUniqueIP(out, ip)
		}
	}
	return out
}

func edgeDNSFilterAnswerIPsForBinding(answerIPs []string, binding model.EdgeRouteBinding, candidateByIP map[string]model.EdgeDNSAnswerCandidate) []string {
	exclusions := edgeRouteExclusionsFromBinding(binding)
	return edgeDNSFilterAnswerIPsForExclusions(answerIPs, exclusions, candidateByIP)
}

func edgeDNSFilterAnswerIPsForExclusions(answerIPs []string, exclusions edgeRouteExclusions, candidateByIP map[string]model.EdgeDNSAnswerCandidate) []string {
	if exclusions.Empty() || len(answerIPs) == 0 {
		return answerIPs
	}
	out := make([]string, 0, len(answerIPs))
	for _, raw := range answerIPs {
		ip := normalizeEdgeDNSStaticRecordValue(model.EdgeDNSRecordTypeA, raw)
		if ip == "" {
			ip = normalizeEdgeDNSStaticRecordValue(model.EdgeDNSRecordTypeAAAA, raw)
		}
		if ip == "" {
			continue
		}
		candidate, ok := candidateByIP[ip]
		if ok {
			if exclusions.ExcludesEdge(candidate.EdgeID) || exclusions.ExcludesEdgeGroup(candidate.EdgeGroupID) {
				continue
			}
		}
		out = appendEdgeDNSUniqueIP(out, ip)
	}
	return out
}

func edgeDNSAnswerIPsForCustomDomainTarget(bindings []model.EdgeRouteBinding, options edgeDNSBundleOptions, edgeAnswerIPsByGroup map[string][]string) []string {
	out := []string{}
	for _, binding := range bindings {
		if model.EdgeRoutePolicyAllowsTraffic(binding.RoutePolicy) {
			for _, ip := range edgeAnswerIPsByGroup[strings.TrimSpace(binding.EdgeGroupID)] {
				out = appendEdgeDNSUniqueIP(out, ip)
			}
			for _, ip := range edgeAnswerIPsByGroup[strings.TrimSpace(binding.FallbackEdgeGroupID)] {
				out = appendEdgeDNSUniqueIP(out, ip)
			}
			continue
		}
		for _, ip := range edgeDNSAnswerIPsForBinding(binding, options, edgeAnswerIPsByGroup) {
			out = appendEdgeDNSUniqueIP(out, ip)
		}
	}
	if len(out) > 0 {
		return out
	}
	return append([]string(nil), options.AnswerIPs...)
}

func edgeDNSAnswerIPsForPlatformRoute(route model.PlatformRoute, options edgeDNSBundleOptions, edgeAnswerIPsByGroup map[string][]string) []string {
	if route.Status != model.EdgeRouteStatusActive || !model.EdgeRoutePolicyAllowsTraffic(route.RoutePolicy) {
		return nil
	}
	switch route.EdgeGroupMode {
	case model.PlatformRouteEdgeGroupModePinned:
		return append([]string(nil), edgeAnswerIPsByGroup[strings.TrimSpace(route.EdgeGroupID)]...)
	default:
		return edgeDNSAllHealthyAnswerIPs(strings.TrimSpace(options.EdgeGroupID), edgeAnswerIPsByGroup)
	}
}

func edgeDNSAllHealthyAnswerIPs(localEdgeGroupID string, edgeAnswerIPsByGroup map[string][]string) []string {
	out := []string{}
	if localEdgeGroupID != "" {
		for _, ip := range edgeAnswerIPsByGroup[localEdgeGroupID] {
			out = appendEdgeDNSUniqueIP(out, ip)
		}
	}
	groups := make([]string, 0, len(edgeAnswerIPsByGroup))
	for groupID := range edgeAnswerIPsByGroup {
		if groupID == localEdgeGroupID {
			continue
		}
		groups = append(groups, groupID)
	}
	sort.Strings(groups)
	for _, groupID := range groups {
		for _, ip := range edgeAnswerIPsByGroup[groupID] {
			out = appendEdgeDNSUniqueIP(out, ip)
		}
	}
	return out
}

func edgeDNSTargetWithinZone(target, zone string) bool {
	target = normalizeExternalAppDomain(target)
	zone = normalizeExternalAppDomain(zone)
	return target != "" && zone != "" && (target == zone || strings.HasSuffix(target, "."+zone))
}

func edgeDNSRecordsForTarget(name string, answerIPs []string, ttl int, kind, status, reason, appID, tenantID, edgeGroupID, fallbackEdgeGroupID string) []model.EdgeDNSRecord {
	aValues := make([]string, 0, len(answerIPs))
	aaaaValues := make([]string, 0, len(answerIPs))
	for _, value := range answerIPs {
		ip := net.ParseIP(value)
		if ip == nil {
			continue
		}
		if ip.To4() != nil {
			aValues = append(aValues, ip.String())
		} else {
			aaaaValues = append(aaaaValues, ip.String())
		}
	}

	records := make([]model.EdgeDNSRecord, 0, 2)
	if len(aValues) > 0 {
		records = append(records, edgeDNSRecord(name, model.EdgeDNSRecordTypeA, aValues, ttl, kind, status, reason, appID, tenantID, edgeGroupID, fallbackEdgeGroupID))
	}
	if len(aaaaValues) > 0 {
		records = append(records, edgeDNSRecord(name, model.EdgeDNSRecordTypeAAAA, aaaaValues, ttl, kind, status, reason, appID, tenantID, edgeGroupID, fallbackEdgeGroupID))
	}
	return records
}

func edgeDNSRecordsForTargetWithPolicy(name string, answerIPs []string, ttl int, kind, status, reason, appID, tenantID, edgeGroupID, fallbackEdgeGroupID string, policy model.DNSAnswerPolicy, candidates []model.EdgeDNSAnswerCandidate, scopedCandidates []model.EdgeDNSScopedAnswerCandidates) []model.EdgeDNSRecord {
	records := edgeDNSRecordsForTarget(name, answerIPs, ttl, kind, status, reason, appID, tenantID, edgeGroupID, fallbackEdgeGroupID)
	for index := range records {
		records[index].AnswerPolicy = policy
		records[index].Candidates = edgeDNSCandidatesForRecordType(records[index].Type, candidates)
		records[index].ScopedCandidates = edgeDNSScopedCandidatesForRecordType(records[index].Type, scopedCandidates)
		records[index].RecordGeneration = edgeDNSRecordGeneration(records[index])
	}
	return records
}

func edgeDNSCandidatesForRecordType(recordType string, candidates []model.EdgeDNSAnswerCandidate) []model.EdgeDNSAnswerCandidate {
	recordType = strings.ToUpper(strings.TrimSpace(recordType))
	out := make([]model.EdgeDNSAnswerCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		ip := net.ParseIP(strings.TrimSpace(candidate.IP))
		if ip == nil {
			continue
		}
		if recordType == model.EdgeDNSRecordTypeA && ip.To4() == nil {
			continue
		}
		if recordType == model.EdgeDNSRecordTypeAAAA && ip.To4() != nil {
			continue
		}
		out = append(out, candidate)
	}
	return out
}

func edgeDNSScopedCandidatesForRecordType(recordType string, scoped []model.EdgeDNSScopedAnswerCandidates) []model.EdgeDNSScopedAnswerCandidates {
	recordType = strings.ToUpper(strings.TrimSpace(recordType))
	out := make([]model.EdgeDNSScopedAnswerCandidates, 0, len(scoped))
	for _, profile := range scoped {
		candidates := edgeDNSCandidatesForRecordType(recordType, profile.Candidates)
		if len(candidates) == 0 {
			continue
		}
		profile.Candidates = candidates
		out = append(out, profile)
	}
	return out
}

const (
	edgeDNSLatencyWindow              = 24 * time.Hour
	edgeDNSLatencyMinGroupSamples     = 3
	edgeDNSLatencyMinGroups           = 2
	edgeDNSLatencyMinScoreDelta       = 75.0
	edgeDNSLatencyMinScoreRatio       = 0.20
	edgeDNSLatencyMinSwitchConfidence = 0.10
	edgeDNSLatencyWeightMin           = 20
	edgeDNSLatencyWeightMax           = 200
	edgeDNSExplorationPercent         = 5
	edgeDNSDecisionCooldown           = 30 * time.Minute
)

type edgeDNSLatencyProfileCatalog struct {
	Global map[string]*edgeDNSLatencyProfile
	Scoped map[string][]edgeDNSLatencyProfile
}

type edgeDNSLatencyProfile struct {
	Hostname              string
	Scope                 edgeDNSLatencyScope
	Enabled               bool
	Reason                string
	Weight                int
	BestEdgeGroupID       string
	ShadowBestEdgeGroupID string
	ShadowReason          string
	Candidates            map[string]edgeDNSLatencyCandidateProfile
	NodeCandidates        map[string]edgeDNSLatencyCandidateProfile
	CooldownUntil         time.Time
}

type edgeDNSLatencyCandidateProfile struct {
	EdgeGroupID               string
	EdgeID                    string
	Weight                    int
	Reason                    string
	Score                     float64
	ScoreBreakdown            map[string]float64
	TrafficClass              string
	TTFBMS                    float64
	UpstreamMS                float64
	TotalMS                   float64
	HitRatio                  float64
	ErrorRate                 float64
	UploadBPS                 float64
	BodyReadMS                float64
	MaxReadGapMS              float64
	BodyIncompleteRate        float64
	BodyReadErrorRate         float64
	ResponseEgressBPS         float64
	ResponseWriteMS           float64
	OriginConnectMS           float64
	OriginWriteMS             float64
	OriginWaitMS              float64
	OriginTTFBMS              float64
	OriginTotalMS             float64
	ActiveRequests            float64
	ActiveBodyBuffers         float64
	ClientTCPRTTMS            float64
	ClientTCPMinRTTMS         float64
	ClientTCPRTTVarMS         float64
	ClientTCPRetransRate      float64
	ClientTCPBytesRetransRate float64
	ClientTCPRTORate          float64
	ClientTCPDeliveryBPS      float64
	Confidence                float64
	ConfidencePenalty         float64
	SampleCount               int
	BodySampleCount           int
	Country                   string
	Region                    string
	ASN                       string
}

type edgeDNSLatencyGroupAccumulator struct {
	EdgeGroupID                       string
	EdgeID                            string
	SampleCount                       int
	TTFBWeightedMS                    float64
	UpstreamWeightedMS                float64
	TotalWeightedMS                   float64
	CacheHitCount                     int
	CacheObservationCount             int
	ErrorCount                        int
	UploadWeightedBPS                 float64
	UploadSampleCount                 int
	BodyReadWeightedMS                float64
	BodyReadSampleCount               int
	MaxReadGapWeightedMS              float64
	MaxReadGapSampleCount             int
	BodyIncompleteCount               int
	BodyReadErrorCount                int
	ResponseEgressWeightedBPS         float64
	ResponseEgressSampleCount         int
	ResponseWriteWeightedMS           float64
	ResponseWriteSampleCount          int
	OriginConnectWeightedMS           float64
	OriginConnectSampleCount          int
	OriginWriteWeightedMS             float64
	OriginWriteSampleCount            int
	OriginWaitWeightedMS              float64
	OriginWaitSampleCount             int
	OriginTTFBWeightedMS              float64
	OriginTTFBSampleCount             int
	OriginTotalWeightedMS             float64
	OriginTotalSampleCount            int
	ActiveRequestsWeighted            float64
	ActiveBodyWeighted                float64
	SaturationSampleCount             int
	ClientTCPRTTWeighted              float64
	ClientTCPMinRTTWeighted           float64
	ClientTCPRTTVarWeighted           float64
	ClientTCPMetricSampleCount        int
	ClientTCPTotalRetrans             int64
	ClientTCPBytesRetrans             int64
	ClientTCPTotalRTO                 int64
	ClientTCPRetransRateWeighted      float64
	ClientTCPBytesRetransRateWeighted float64
	ClientTCPRTORateWeighted          float64
	ClientTCPRateSampleCount          int
	ClientTCPDeliveryWeighted         float64
	ClientTCPDeliverySampleCount      int
	TrafficClassCounts                map[string]int
	CountryCounts                     map[string]int
	RegionCounts                      map[string]int
	ASNCounts                         map[string]int
	NodeAccumulators                  map[string]*edgeDNSLatencyGroupAccumulator
}

type edgeDNSLatencyScope struct {
	Country string
	Region  string
	ASN     string
}

func (s *Server) edgeDNSLatencyProfiles(_ edgeDNSBundleOptions) (edgeDNSLatencyProfileCatalog, error) {
	if s.store == nil || s.edgeQualityRankingDisabled() {
		return edgeDNSLatencyProfileCatalog{}, nil
	}
	now := time.Now().UTC()
	samples, err := s.store.ListEdgePerformanceSamples("", now.Add(-edgeDNSLatencyWindow))
	if err != nil {
		return edgeDNSLatencyProfileCatalog{}, err
	}
	decisions, err := s.store.ListEdgeDNSRoutingDecisions("")
	if err != nil {
		return edgeDNSLatencyProfileCatalog{}, err
	}
	catalog, _ := edgeDNSLatencyProfilesByHostname(samples, decisions, now)
	edgeDNSApplySevereDegradeToCatalog(&catalog, samples, now)
	return catalog, nil
}

func (s *Server) reconcileEdgeDNSRoutingDecisions(now time.Time) (int, error) {
	if s.store == nil || s.edgeQualityRankingDisabled() {
		return 0, nil
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	samples, err := s.store.ListEdgePerformanceSamples("", now.Add(-edgeDNSLatencyWindow))
	if err != nil {
		return 0, err
	}
	decisions, err := s.store.ListEdgeDNSRoutingDecisions("")
	if err != nil {
		return 0, err
	}
	_, updates := edgeDNSLatencyProfilesByHostname(samples, decisions, now)
	if len(updates) > 0 {
		sortEdgeDNSRoutingDecisionUpdates(updates)
		if err := s.store.UpsertEdgeDNSRoutingDecisions(updates); err != nil {
			return 0, err
		}
	}
	return len(updates), nil
}

func edgeDNSLatencyProfilesByHostname(samples []model.EdgePerformanceSample, decisions []model.EdgeDNSRoutingDecision, now time.Time) (edgeDNSLatencyProfileCatalog, []model.EdgeDNSRoutingDecision) {
	byHostnameScope := make(map[string]map[string]map[string]*edgeDNSLatencyGroupAccumulator)
	for _, sample := range samples {
		hostname := normalizeExternalAppDomain(sample.Hostname)
		edgeGroupID := strings.TrimSpace(sample.EdgeGroupID)
		if hostname == "" || edgeGroupID == "" {
			continue
		}
		for _, scope := range edgeDNSLatencyScopesForSample(sample) {
			if _, ok := byHostnameScope[hostname]; !ok {
				byHostnameScope[hostname] = make(map[string]map[string]*edgeDNSLatencyGroupAccumulator)
			}
			scopeKey := scope.key()
			if _, ok := byHostnameScope[hostname][scopeKey]; !ok {
				byHostnameScope[hostname][scopeKey] = make(map[string]*edgeDNSLatencyGroupAccumulator)
			}
			edgeDNSLatencyAccumulate(byHostnameScope[hostname][scopeKey], edgeGroupID, sample)
		}
	}

	decisionByKey := make(map[string]model.EdgeDNSRoutingDecision, len(decisions))
	for _, decision := range decisions {
		key := edgeDNSRoutingDecisionKey(normalizeExternalAppDomain(decision.Hostname), strings.TrimSpace(decision.ScopeKey))
		if key != "" {
			decisionByKey[key] = decision
		}
	}

	catalog := edgeDNSLatencyProfileCatalog{
		Global: make(map[string]*edgeDNSLatencyProfile),
		Scoped: make(map[string][]edgeDNSLatencyProfile),
	}
	updates := []model.EdgeDNSRoutingDecision{}
	hostnames := make([]string, 0, len(byHostnameScope))
	for hostname := range byHostnameScope {
		hostnames = append(hostnames, hostname)
	}
	sort.Strings(hostnames)
	for _, hostname := range hostnames {
		scopes := byHostnameScope[hostname]
		scopeKeys := make([]string, 0, len(scopes))
		for scopeKey := range scopes {
			scopeKeys = append(scopeKeys, scopeKey)
		}
		sort.Strings(scopeKeys)
		for _, scopeKey := range scopeKeys {
			groups := scopes[scopeKey]
			scope := edgeDNSLatencyScopeFromKey(scopeKey)
			profile := buildEdgeDNSLatencyProfile(hostname, scope, groups)
			if profile == nil || !profile.Enabled {
				continue
			}
			decisionKey := edgeDNSRoutingDecisionKey(hostname, scopeKey)
			profile, decision := applyEdgeDNSRoutingDecision(profile, decisionByKey[decisionKey], now)
			updates = append(updates, decision)
			if profile.Scope.global() {
				catalog.Global[hostname] = profile
				continue
			}
			catalog.Scoped[hostname] = append(catalog.Scoped[hostname], *profile)
		}
	}
	sortEdgeDNSRoutingDecisionUpdates(updates)
	for hostname := range catalog.Scoped {
		sort.Slice(catalog.Scoped[hostname], func(i, j int) bool {
			return catalog.Scoped[hostname][i].Scope.key() < catalog.Scoped[hostname][j].Scope.key()
		})
	}
	return catalog, updates
}

func sortEdgeDNSRoutingDecisionUpdates(decisions []model.EdgeDNSRoutingDecision) {
	sort.Slice(decisions, func(i, j int) bool {
		leftHost := normalizeExternalAppDomain(decisions[i].Hostname)
		rightHost := normalizeExternalAppDomain(decisions[j].Hostname)
		if leftHost != rightHost {
			return leftHost < rightHost
		}
		return strings.TrimSpace(strings.ToLower(decisions[i].ScopeKey)) < strings.TrimSpace(strings.ToLower(decisions[j].ScopeKey))
	})
}

func edgeDNSRoutingDecisionKey(hostname, scopeKey string) string {
	hostname = normalizeExternalAppDomain(hostname)
	scopeKey = strings.TrimSpace(strings.ToLower(scopeKey))
	if hostname == "" || scopeKey == "" {
		return ""
	}
	return hostname + "\x00" + scopeKey
}

func edgeDNSLatencyAccumulate(groups map[string]*edgeDNSLatencyGroupAccumulator, edgeGroupID string, sample model.EdgePerformanceSample) {
	accumulator := groups[edgeGroupID]
	if accumulator == nil {
		accumulator = &edgeDNSLatencyGroupAccumulator{
			EdgeGroupID:        edgeGroupID,
			CountryCounts:      make(map[string]int),
			RegionCounts:       make(map[string]int),
			ASNCounts:          make(map[string]int),
			TrafficClassCounts: make(map[string]int),
			NodeAccumulators:   make(map[string]*edgeDNSLatencyGroupAccumulator),
		}
		groups[edgeGroupID] = accumulator
	}
	edgeDNSLatencyAccumulateInto(accumulator, sample)
	edgeID := strings.TrimSpace(sample.EdgeID)
	if edgeID != "" {
		nodeAccumulator := accumulator.NodeAccumulators[edgeID]
		if nodeAccumulator == nil {
			nodeAccumulator = &edgeDNSLatencyGroupAccumulator{
				EdgeGroupID:        edgeGroupID,
				EdgeID:             edgeID,
				CountryCounts:      make(map[string]int),
				RegionCounts:       make(map[string]int),
				ASNCounts:          make(map[string]int),
				TrafficClassCounts: make(map[string]int),
			}
			accumulator.NodeAccumulators[edgeID] = nodeAccumulator
		}
		edgeDNSLatencyAccumulateInto(nodeAccumulator, sample)
	}
}

func edgeDNSLatencyAccumulateInto(accumulator *edgeDNSLatencyGroupAccumulator, sample model.EdgePerformanceSample) {
	if accumulator == nil {
		return
	}
	sampleCount := sample.SampleCount
	if sampleCount <= 0 {
		sampleCount = 1
	}
	accumulator.SampleCount += sampleCount
	accumulator.TTFBWeightedMS += float64(sample.TTFBMS) * float64(sampleCount)
	accumulator.UpstreamWeightedMS += float64(sample.UpstreamMS) * float64(sampleCount)
	accumulator.TotalWeightedMS += float64(sample.TotalMS) * float64(sampleCount)
	accumulator.CacheHitCount += sample.CacheHitCount
	accumulator.CacheObservationCount += sample.CacheObservationCount
	accumulator.ErrorCount += sample.ErrorCount
	if uploadBPS := edgeDNSPerformanceUploadBPS(sample); uploadBPS > 0 {
		accumulator.UploadWeightedBPS += float64(uploadBPS) * float64(sampleCount)
		accumulator.UploadSampleCount += sampleCount
	}
	if sample.BodyReadBlockMS > 0 {
		accumulator.BodyReadWeightedMS += float64(sample.BodyReadBlockMS) * float64(sampleCount)
		accumulator.BodyReadSampleCount += sampleCount
	}
	if sample.MaxReadGapMS > 0 {
		accumulator.MaxReadGapWeightedMS += float64(sample.MaxReadGapMS) * float64(sampleCount)
		accumulator.MaxReadGapSampleCount += sampleCount
	}
	accumulator.BodyIncompleteCount += sample.BodyIncompleteCount
	accumulator.BodyReadErrorCount += sample.BodyReadErrorCount
	if sample.ResponseEgressBPS > 0 {
		accumulator.ResponseEgressWeightedBPS += float64(sample.ResponseEgressBPS) * float64(sampleCount)
		accumulator.ResponseEgressSampleCount += sampleCount
	}
	if sample.ResponseWriteMS > 0 {
		accumulator.ResponseWriteWeightedMS += float64(sample.ResponseWriteMS) * float64(sampleCount)
		accumulator.ResponseWriteSampleCount += sampleCount
	}
	if sample.OriginConnectMS > 0 {
		accumulator.OriginConnectWeightedMS += float64(sample.OriginConnectMS) * float64(sampleCount)
		accumulator.OriginConnectSampleCount += sampleCount
	}
	if sample.OriginRequestWriteMS > 0 {
		accumulator.OriginWriteWeightedMS += float64(sample.OriginRequestWriteMS) * float64(sampleCount)
		accumulator.OriginWriteSampleCount += sampleCount
	}
	if sample.OriginResponseWaitMS > 0 {
		accumulator.OriginWaitWeightedMS += float64(sample.OriginResponseWaitMS) * float64(sampleCount)
		accumulator.OriginWaitSampleCount += sampleCount
	}
	if sample.OriginTTFBMS > 0 {
		accumulator.OriginTTFBWeightedMS += float64(sample.OriginTTFBMS) * float64(sampleCount)
		accumulator.OriginTTFBSampleCount += sampleCount
	}
	if sample.OriginTotalMS > 0 {
		accumulator.OriginTotalWeightedMS += float64(sample.OriginTotalMS) * float64(sampleCount)
		accumulator.OriginTotalSampleCount += sampleCount
	}
	if sample.ActiveRequests > 0 || sample.ActiveBodyBuffers > 0 {
		accumulator.ActiveRequestsWeighted += float64(sample.ActiveRequests) * float64(sampleCount)
		accumulator.ActiveBodyWeighted += float64(sample.ActiveBodyBuffers) * float64(sampleCount)
		accumulator.SaturationSampleCount += sampleCount
	}
	if sample.ClientTCPRTTMS > 0 || sample.ClientTCPMinRTTMS > 0 || sample.ClientTCPRTTVarMS > 0 {
		accumulator.ClientTCPRTTWeighted += sample.ClientTCPRTTMS * float64(sampleCount)
		accumulator.ClientTCPMinRTTWeighted += sample.ClientTCPMinRTTMS * float64(sampleCount)
		accumulator.ClientTCPRTTVarWeighted += sample.ClientTCPRTTVarMS * float64(sampleCount)
		accumulator.ClientTCPMetricSampleCount += sampleCount
	}
	accumulator.ClientTCPTotalRetrans += sample.ClientTCPTotalRetrans
	accumulator.ClientTCPBytesRetrans += sample.ClientTCPBytesRetrans
	accumulator.ClientTCPTotalRTO += sample.ClientTCPTotalRTO
	if sample.ClientTCPRetransRate > 0 || sample.ClientTCPBytesRetransRate > 0 || sample.ClientTCPRTORate > 0 {
		accumulator.ClientTCPRetransRateWeighted += sample.ClientTCPRetransRate * float64(sampleCount)
		accumulator.ClientTCPBytesRetransRateWeighted += sample.ClientTCPBytesRetransRate * float64(sampleCount)
		accumulator.ClientTCPRTORateWeighted += sample.ClientTCPRTORate * float64(sampleCount)
		accumulator.ClientTCPRateSampleCount += sampleCount
	}
	if sample.ClientTCPDeliveryBPS > 0 {
		accumulator.ClientTCPDeliveryWeighted += float64(sample.ClientTCPDeliveryBPS) * float64(sampleCount)
		accumulator.ClientTCPDeliverySampleCount += sampleCount
	}
	if value := strings.TrimSpace(sample.TrafficClass); value != "" {
		accumulator.TrafficClassCounts[value] += sampleCount
	}
	if value := strings.ToLower(strings.TrimSpace(sample.ClientCountry)); value != "" {
		accumulator.CountryCounts[value] += sampleCount
	}
	if value := strings.TrimSpace(sample.ClientRegion); value != "" {
		accumulator.RegionCounts[value] += sampleCount
	}
	if value := strings.TrimSpace(sample.ClientASN); value != "" {
		accumulator.ASNCounts[value] += sampleCount
	}
}

func edgeDNSPerformanceUploadBPS(sample model.EdgePerformanceSample) int64 {
	uploadBPS := sample.UploadEffectiveBPS
	if sample.MinWindowBPS > 0 && (uploadBPS <= 0 || sample.MinWindowBPS < uploadBPS) {
		uploadBPS = sample.MinWindowBPS
	}
	return uploadBPS
}

func edgeDNSLatencyScopesForSample(sample model.EdgePerformanceSample) []edgeDNSLatencyScope {
	country := strings.ToLower(strings.TrimSpace(sample.ClientCountry))
	region := strings.TrimSpace(sample.ClientRegion)
	asn := strings.TrimSpace(sample.ClientASN)
	scopes := []edgeDNSLatencyScope{{}}
	if !edgeDNSPerformanceSampleHasClientScope(sample) {
		return scopes
	}
	if country != "" {
		scopes = append(scopes, edgeDNSLatencyScope{Country: country})
	}
	if country != "" && region != "" {
		scopes = append(scopes, edgeDNSLatencyScope{Country: country, Region: region})
	}
	if asn != "" {
		scopes = append(scopes, edgeDNSLatencyScope{ASN: asn})
	}
	return scopes
}

func edgeDNSPerformanceSampleHasClientScope(sample model.EdgePerformanceSample) bool {
	if strings.TrimSpace(sample.ClientCountry) != "" || strings.TrimSpace(sample.ClientRegion) != "" || strings.TrimSpace(sample.ClientASN) != "" {
		return true
	}
	return strings.Contains(strings.ToLower(strings.TrimSpace(sample.DNSPolicy)), "client_scope")
}

func (scope edgeDNSLatencyScope) key() string {
	country := strings.ToLower(strings.TrimSpace(scope.Country))
	region := strings.ToLower(strings.TrimSpace(scope.Region))
	asn := strings.ToLower(strings.TrimSpace(scope.ASN))
	switch {
	case asn != "":
		return "asn:" + asn
	case country != "" && region != "":
		return "region:" + country + ":" + region
	case country != "":
		return "country:" + country
	default:
		return "global"
	}
}

func (scope edgeDNSLatencyScope) global() bool {
	return scope.key() == "global"
}

func edgeDNSLatencyScopeFromKey(key string) edgeDNSLatencyScope {
	key = strings.ToLower(strings.TrimSpace(key))
	if key == "" || key == "global" {
		return edgeDNSLatencyScope{}
	}
	if strings.HasPrefix(key, "asn:") {
		return edgeDNSLatencyScope{ASN: strings.TrimPrefix(key, "asn:")}
	}
	if strings.HasPrefix(key, "region:") {
		parts := strings.SplitN(strings.TrimPrefix(key, "region:"), ":", 2)
		if len(parts) == 2 {
			return edgeDNSLatencyScope{Country: parts[0], Region: parts[1]}
		}
	}
	if strings.HasPrefix(key, "country:") {
		return edgeDNSLatencyScope{Country: strings.TrimPrefix(key, "country:")}
	}
	return edgeDNSLatencyScope{}
}

func buildEdgeDNSLatencyProfile(hostname string, scope edgeDNSLatencyScope, groups map[string]*edgeDNSLatencyGroupAccumulator) *edgeDNSLatencyProfile {
	candidates := make([]edgeDNSLatencyCandidateProfile, 0, len(groups))
	for _, accumulator := range groups {
		if accumulator == nil || accumulator.SampleCount < edgeDNSLatencyMinGroupSamples {
			continue
		}
		candidate := edgeDNSLatencyCandidateFromAccumulator(accumulator)
		candidates = append(candidates, candidate)
	}
	if len(candidates) < edgeDNSLatencyMinGroups {
		return nil
	}
	edgeDNSApplyPeerRelativePenalties(candidates)
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Score != candidates[j].Score {
			return candidates[i].Score < candidates[j].Score
		}
		return candidates[i].EdgeGroupID < candidates[j].EdgeGroupID
	})
	best := candidates[0]
	second := candidates[1]
	if best.Confidence < edgeDNSLatencyMinSwitchConfidence {
		return nil
	}
	scoreDelta := second.Score - best.Score
	minDelta := edgeDNSLatencyMinScoreDelta
	if ratioDelta := best.Score * edgeDNSLatencyMinScoreRatio; ratioDelta > minDelta {
		minDelta = ratioDelta
	}
	if scoreDelta < minDelta {
		return nil
	}
	worstScore := candidates[len(candidates)-1].Score
	scoreSpan := worstScore - best.Score
	if scoreSpan <= 0 {
		scoreSpan = minDelta
	}

	profile := &edgeDNSLatencyProfile{
		Hostname:        normalizeExternalAppDomain(hostname),
		Scope:           scope,
		Enabled:         true,
		Reason:          "latency_aware_stable_window_24h",
		BestEdgeGroupID: best.EdgeGroupID,
		Candidates:      make(map[string]edgeDNSLatencyCandidateProfile, len(candidates)),
		NodeCandidates:  make(map[string]edgeDNSLatencyCandidateProfile),
	}
	for _, candidate := range candidates {
		penalty := candidate.Score - best.Score
		weight := edgeDNSClampLatencyWeight(edgeDNSLatencyWeightMax - int((penalty/scoreSpan)*float64(edgeDNSLatencyWeightMax-edgeDNSLatencyWeightMin)))
		if candidate.EdgeGroupID == best.EdgeGroupID {
			weight = edgeDNSLatencyWeightMax
			candidate.Reason = edgeDNSLatencyReason("latency_fast", candidate)
		} else {
			candidate.Reason = edgeDNSLatencyReason("latency_penalized", candidate)
		}
		candidate.Weight = weight
		profile.Candidates[candidate.EdgeGroupID] = candidate
		if groupAccumulator := groups[candidate.EdgeGroupID]; groupAccumulator != nil {
			for edgeID, nodeAccumulator := range groupAccumulator.NodeAccumulators {
				if nodeAccumulator == nil || nodeAccumulator.SampleCount < edgeDNSLatencyMinGroupSamples {
					continue
				}
				nodeCandidate := edgeDNSLatencyCandidateFromAccumulator(nodeAccumulator)
				nodeCandidate.Weight = weight
				nodeCandidate.Reason = edgeDNSLatencyReason("node_quality", nodeCandidate)
				profile.NodeCandidates[edgeID] = nodeCandidate
			}
		}
	}
	if bestCandidate, ok := profile.Candidates[profile.BestEdgeGroupID]; ok {
		profile.Weight = bestCandidate.Weight
	}
	return profile
}

func applyEdgeDNSRoutingDecision(profile *edgeDNSLatencyProfile, existing model.EdgeDNSRoutingDecision, now time.Time) (*edgeDNSLatencyProfile, model.EdgeDNSRoutingDecision) {
	if profile == nil {
		return profile, model.EdgeDNSRoutingDecision{}
	}
	previous := strings.TrimSpace(existing.SelectedEdgeGroupID)
	selected := strings.TrimSpace(profile.BestEdgeGroupID)
	profile.ShadowBestEdgeGroupID = selected
	profile.ShadowReason = profile.Reason
	cooldownUntil := existing.CooldownUntil
	switchedAt := existing.SwitchedAt
	if previous == "" {
		cooldownUntil = now.Add(edgeDNSDecisionCooldown)
		switchedAt = now
	} else if previous != selected {
		if now.Before(existing.CooldownUntil) {
			if _, ok := profile.Candidates[previous]; ok {
				selected = previous
				profile.BestEdgeGroupID = previous
				profile.CooldownUntil = existing.CooldownUntil
				profile.Reason = "latency_aware_cooldown_hold"
				profile.promoteSelected(previous, "latency_cooldown_hold")
			}
		} else {
			cooldownUntil = now.Add(edgeDNSDecisionCooldown)
			switchedAt = now
		}
	}
	if selected == "" {
		selected = profile.BestEdgeGroupID
	}
	if cooldownUntil.IsZero() {
		cooldownUntil = now.Add(edgeDNSDecisionCooldown)
	}
	if switchedAt.IsZero() {
		switchedAt = now
	}
	profile.BestEdgeGroupID = selected
	profile.CooldownUntil = cooldownUntil
	if candidate, ok := profile.Candidates[selected]; ok {
		profile.Weight = candidate.Weight
	}
	return profile, model.EdgeDNSRoutingDecision{
		Hostname:            profile.Hostname,
		ScopeKey:            profile.Scope.key(),
		Country:             profile.Scope.Country,
		Region:              profile.Scope.Region,
		ASN:                 profile.Scope.ASN,
		SelectedEdgeGroupID: selected,
		PreviousEdgeGroupID: previous,
		Reason:              profile.Reason,
		Score:               profile.selectedScore(),
		SampleCount:         profile.selectedSampleCount(),
		SwitchedAt:          switchedAt,
		CooldownUntil:       cooldownUntil,
		CreatedAt:           firstNonZeroTime(existing.CreatedAt, now),
		UpdatedAt:           now,
	}
}

func edgeDNSApplySevereDegradeToCatalog(catalog *edgeDNSLatencyProfileCatalog, samples []model.EdgePerformanceSample, now time.Time) {
	if catalog == nil {
		return
	}
	degraded := edgeDNSSevereDegradeGroups(samples, now)
	if len(degraded) == 0 {
		return
	}
	for hostname, profile := range catalog.Global {
		if edgeDNSApplySevereDegradeToProfile(profile, degraded) {
			catalog.Global[hostname] = profile
		}
	}
	for hostname, profiles := range catalog.Scoped {
		for index := range profiles {
			edgeDNSApplySevereDegradeToProfile(&profiles[index], degraded)
		}
		catalog.Scoped[hostname] = profiles
	}
}

func edgeDNSSevereDegradeGroups(samples []model.EdgePerformanceSample, now time.Time) map[string]float64 {
	startedAt := now.Add(-5 * time.Minute)
	rollups := buildEdgeQualityRollupsForWindow(samples, "5m", startedAt, now, now)
	out := map[string]float64{}
	for _, rollup := range rollups {
		if strings.TrimSpace(rollup.EdgeID) != "" || rollup.ClientScopeKind != "global" {
			continue
		}
		penalty, _ := edgeQualitySevereDegradePenalty(rollup)
		if penalty <= 0 {
			continue
		}
		groupID := strings.TrimSpace(rollup.EdgeGroupID)
		if penalty > out[groupID] {
			out[groupID] = penalty
		}
	}
	return out
}

func edgeDNSApplySevereDegradeToProfile(profile *edgeDNSLatencyProfile, degraded map[string]float64) bool {
	if profile == nil || len(profile.Candidates) == 0 {
		return false
	}
	changed := false
	for groupID, candidate := range profile.Candidates {
		penalty := degraded[strings.TrimSpace(groupID)]
		if penalty <= 0 {
			continue
		}
		if candidate.ScoreBreakdown == nil {
			candidate.ScoreBreakdown = map[string]float64{}
		}
		candidate.ScoreBreakdown["severe_degrade"] = penalty
		candidate.Score += penalty
		candidate.Weight = edgeDNSLatencyWeightMin
		candidate.Reason = edgeDNSLatencyReason("severe_degrade_5m", candidate)
		profile.Candidates[groupID] = candidate
		changed = true
	}
	if !changed {
		return false
	}
	bestGroupID := ""
	bestScore := math.MaxFloat64
	worstScore := 0.0
	for groupID, candidate := range profile.Candidates {
		if candidate.Score < bestScore {
			bestScore = candidate.Score
			bestGroupID = groupID
		}
		if candidate.Score > worstScore {
			worstScore = candidate.Score
		}
	}
	if bestGroupID == "" {
		return true
	}
	scoreSpan := worstScore - bestScore
	if scoreSpan <= 0 {
		scoreSpan = 1
	}
	for groupID, candidate := range profile.Candidates {
		if groupID == bestGroupID {
			candidate.Weight = edgeDNSLatencyWeightMax
			candidate.Reason = edgeDNSLatencyReason("latency_fast_after_5m_degrade", candidate)
		} else if degraded[groupID] <= 0 {
			penalty := candidate.Score - bestScore
			candidate.Weight = edgeDNSClampLatencyWeight(edgeDNSLatencyWeightMax - int((penalty/scoreSpan)*float64(edgeDNSLatencyWeightMax-edgeDNSLatencyWeightMin)))
		}
		profile.Candidates[groupID] = candidate
	}
	profile.BestEdgeGroupID = bestGroupID
	profile.Reason = "latency_aware_5m_severe_degrade"
	profile.Weight = profile.Candidates[bestGroupID].Weight
	return true
}

func (profile *edgeDNSLatencyProfile) promoteSelected(edgeGroupID, reasonPrefix string) {
	if profile == nil {
		return
	}
	for key, candidate := range profile.Candidates {
		if key == edgeGroupID {
			candidate.Weight = edgeDNSLatencyWeightMax
			candidate.Reason = edgeDNSLatencyReason(reasonPrefix, candidate)
		} else if candidate.Weight >= edgeDNSLatencyWeightMax {
			candidate.Weight = edgeDNSLatencyWeightMax - 1
		}
		profile.Candidates[key] = candidate
	}
}

func (profile *edgeDNSLatencyProfile) selectedScore() float64 {
	if profile == nil {
		return 0
	}
	if candidate, ok := profile.Candidates[profile.BestEdgeGroupID]; ok {
		return candidate.Score
	}
	return 0
}

func (profile *edgeDNSLatencyProfile) selectedSampleCount() int {
	if profile == nil {
		return 0
	}
	if candidate, ok := profile.Candidates[profile.BestEdgeGroupID]; ok {
		return candidate.SampleCount
	}
	return 0
}

func firstNonZeroTime(values ...time.Time) time.Time {
	for _, value := range values {
		if !value.IsZero() {
			return value
		}
	}
	return time.Time{}
}

func (catalog edgeDNSLatencyProfileCatalog) globalProfile(hostname string) *edgeDNSLatencyProfile {
	hostname = normalizeExternalAppDomain(hostname)
	if hostname == "" || catalog.Global == nil {
		return nil
	}
	return catalog.Global[hostname]
}

func (catalog edgeDNSLatencyProfileCatalog) scopedProfiles(hostname string, answerIPs []string, candidateByIP map[string]model.EdgeDNSAnswerCandidate, routeReady map[string]bool, preferredEdgeGroupID, fallbackEdgeGroupID string, applyLatency bool) []model.EdgeDNSScopedAnswerCandidates {
	if !applyLatency {
		return nil
	}
	hostname = normalizeExternalAppDomain(hostname)
	if hostname == "" || len(catalog.Scoped[hostname]) == 0 {
		return nil
	}
	out := make([]model.EdgeDNSScopedAnswerCandidates, 0, len(catalog.Scoped[hostname]))
	for _, profile := range catalog.Scoped[hostname] {
		candidates := edgeDNSCandidatesForAnswerIPs(answerIPs, candidateByIP, routeReady, preferredEdgeGroupID, fallbackEdgeGroupID, &profile, applyLatency)
		if len(candidates) == 0 {
			continue
		}
		selectedEdgeGroupID := strings.TrimSpace(profile.BestEdgeGroupID)
		if selectedEdgeGroupID != "" && !edgeDNSCandidatesContainGroup(candidates, selectedEdgeGroupID) {
			selectedEdgeGroupID = ""
		}
		out = append(out, model.EdgeDNSScopedAnswerCandidates{
			ScopeKey:            profile.Scope.key(),
			Country:             profile.Scope.Country,
			Region:              profile.Scope.Region,
			ASN:                 profile.Scope.ASN,
			PolicyKind:          model.DNSAnswerPolicyKindLatencyAware,
			Reason:              profile.Reason,
			SelectedEdgeGroupID: selectedEdgeGroupID,
			CooldownUntil:       profile.CooldownUntil,
			Candidates:          candidates,
		})
	}
	return out
}

func edgeDNSCandidatesContainGroup(candidates []model.EdgeDNSAnswerCandidate, edgeGroupID string) bool {
	edgeGroupID = strings.TrimSpace(edgeGroupID)
	if edgeGroupID == "" {
		return false
	}
	for _, candidate := range candidates {
		if strings.TrimSpace(candidate.EdgeGroupID) == edgeGroupID {
			return true
		}
	}
	return false
}

func edgeDNSLatencyCandidateFromAccumulator(accumulator *edgeDNSLatencyGroupAccumulator) edgeDNSLatencyCandidateProfile {
	candidate := edgeDNSLatencyCandidateProfile{
		EdgeGroupID:          strings.TrimSpace(accumulator.EdgeGroupID),
		EdgeID:               strings.TrimSpace(accumulator.EdgeID),
		SampleCount:          accumulator.SampleCount,
		BodySampleCount:      accumulator.BodyReadSampleCount,
		TTFBMS:               edgeDNSLatencyAverage(accumulator.TTFBWeightedMS, accumulator.SampleCount),
		UpstreamMS:           edgeDNSLatencyAverage(accumulator.UpstreamWeightedMS, accumulator.SampleCount),
		TotalMS:              edgeDNSLatencyAverage(accumulator.TotalWeightedMS, accumulator.SampleCount),
		UploadBPS:            edgeDNSLatencyAverage(accumulator.UploadWeightedBPS, accumulator.UploadSampleCount),
		BodyReadMS:           edgeDNSLatencyAverage(accumulator.BodyReadWeightedMS, accumulator.BodyReadSampleCount),
		MaxReadGapMS:         edgeDNSLatencyAverage(accumulator.MaxReadGapWeightedMS, accumulator.MaxReadGapSampleCount),
		ResponseEgressBPS:    edgeDNSLatencyAverage(accumulator.ResponseEgressWeightedBPS, accumulator.ResponseEgressSampleCount),
		ResponseWriteMS:      edgeDNSLatencyAverage(accumulator.ResponseWriteWeightedMS, accumulator.ResponseWriteSampleCount),
		OriginConnectMS:      edgeDNSLatencyAverage(accumulator.OriginConnectWeightedMS, accumulator.OriginConnectSampleCount),
		OriginWriteMS:        edgeDNSLatencyAverage(accumulator.OriginWriteWeightedMS, accumulator.OriginWriteSampleCount),
		OriginWaitMS:         edgeDNSLatencyAverage(accumulator.OriginWaitWeightedMS, accumulator.OriginWaitSampleCount),
		OriginTTFBMS:         edgeDNSLatencyAverage(accumulator.OriginTTFBWeightedMS, accumulator.OriginTTFBSampleCount),
		OriginTotalMS:        edgeDNSLatencyAverage(accumulator.OriginTotalWeightedMS, accumulator.OriginTotalSampleCount),
		ActiveRequests:       edgeDNSLatencyAverage(accumulator.ActiveRequestsWeighted, accumulator.SaturationSampleCount),
		ActiveBodyBuffers:    edgeDNSLatencyAverage(accumulator.ActiveBodyWeighted, accumulator.SaturationSampleCount),
		ClientTCPRTTMS:       edgeDNSLatencyAverage(accumulator.ClientTCPRTTWeighted, accumulator.ClientTCPMetricSampleCount),
		ClientTCPMinRTTMS:    edgeDNSLatencyAverage(accumulator.ClientTCPMinRTTWeighted, accumulator.ClientTCPMetricSampleCount),
		ClientTCPRTTVarMS:    edgeDNSLatencyAverage(accumulator.ClientTCPRTTVarWeighted, accumulator.ClientTCPMetricSampleCount),
		ClientTCPDeliveryBPS: edgeDNSLatencyAverage(accumulator.ClientTCPDeliveryWeighted, accumulator.ClientTCPDeliverySampleCount),
		TrafficClass:         edgeDNSDominantStringCount(accumulator.TrafficClassCounts, "html_dynamic"),
		Country:              edgeDNSDominantStringCount(accumulator.CountryCounts, ""),
		Region:               edgeDNSDominantStringCount(accumulator.RegionCounts, ""),
		ASN:                  edgeDNSDominantStringCount(accumulator.ASNCounts, ""),
		ScoreBreakdown:       map[string]float64{},
	}
	if candidate.TotalMS <= 0 {
		candidate.TotalMS = candidate.TTFBMS
	}
	if candidate.TTFBMS <= 0 {
		candidate.TTFBMS = candidate.TotalMS
	}
	if accumulator.CacheObservationCount > 0 {
		candidate.HitRatio = float64(accumulator.CacheHitCount) / float64(accumulator.CacheObservationCount)
	}
	if accumulator.SampleCount > 0 {
		candidate.ErrorRate = float64(accumulator.ErrorCount) / float64(accumulator.SampleCount)
		candidate.BodyIncompleteRate = float64(accumulator.BodyIncompleteCount) / float64(accumulator.SampleCount)
		candidate.BodyReadErrorRate = float64(accumulator.BodyReadErrorCount) / float64(accumulator.SampleCount)
		candidate.ClientTCPRetransRate = float64(accumulator.ClientTCPTotalRetrans) / float64(accumulator.SampleCount)
		candidate.ClientTCPBytesRetransRate = float64(accumulator.ClientTCPBytesRetrans) / float64(accumulator.SampleCount)
		candidate.ClientTCPRTORate = float64(accumulator.ClientTCPTotalRTO) / float64(accumulator.SampleCount)
	}
	if accumulator.ClientTCPRateSampleCount > 0 {
		candidate.ClientTCPRetransRate = edgeDNSLatencyAverage(accumulator.ClientTCPRetransRateWeighted, accumulator.ClientTCPRateSampleCount)
		candidate.ClientTCPBytesRetransRate = edgeDNSLatencyAverage(accumulator.ClientTCPBytesRetransRateWeighted, accumulator.ClientTCPRateSampleCount)
		candidate.ClientTCPRTORate = edgeDNSLatencyAverage(accumulator.ClientTCPRTORateWeighted, accumulator.ClientTCPRateSampleCount)
	}
	candidate.Confidence = edgeDNSLatencyCandidateConfidence(candidate, accumulator)
	candidate.ConfidencePenalty = edgeDNSLatencyConfidencePenalty(candidate.Confidence)
	candidate.Score = edgeDNSLatencyScore(candidate)
	return candidate
}

func edgeDNSLatencyScore(candidate edgeDNSLatencyCandidateProfile) float64 {
	breakdown := candidate.ScoreBreakdown
	if breakdown == nil {
		breakdown = map[string]float64{}
	}
	profile := edgeDNSQualityTrafficProfile(candidate.TrafficClass)
	network := candidate.ClientTCPRetransRate*profile.networkRetransWeight +
		candidate.ClientTCPBytesRetransRate*profile.networkBytesRetransWeight +
		candidate.ClientTCPRTORate*profile.networkRTOWeight +
		candidate.ClientTCPRTTVarMS*profile.networkRTTVarWeight +
		candidate.ClientTCPRTTMS*profile.networkRTTWeight
	if candidate.ClientTCPDeliveryBPS > 0 && candidate.ClientTCPDeliveryBPS < profile.deliveryFloorBPS {
		network += ((profile.deliveryFloorBPS - candidate.ClientTCPDeliveryBPS) / 1024) * profile.deliveryDeficitWeight
	}
	if network > profile.networkCap {
		network = profile.networkCap
	}
	breakdown["network"] = network
	latency := candidate.TTFBMS*profile.ttfbWeight + candidate.UpstreamMS*profile.upstreamWeight + candidate.TotalMS*profile.totalWeight
	breakdown["latency"] = latency
	errorPenalty := candidate.ErrorRate * profile.errorWeight
	breakdown["availability"] = errorPenalty
	origin := candidate.OriginConnectMS*profile.originConnectWeight +
		candidate.OriginWriteMS*profile.originWriteWeight +
		candidate.OriginWaitMS*profile.originWaitWeight +
		candidate.OriginTTFBMS*profile.originTTFBWeight +
		candidate.OriginTotalMS*profile.originTotalWeight
	breakdown["origin"] = origin
	upload := 0.0
	if candidate.BodySampleCount > 0 || profile.uploadAlways {
		if candidate.UploadBPS <= 0 {
			upload += profile.missingUploadPenalty
		} else if candidate.UploadBPS < profile.uploadFloorBPS {
			upload += ((profile.uploadFloorBPS - candidate.UploadBPS) / 1024) * profile.uploadDeficitWeight
		}
		upload += candidate.BodyReadMS * profile.bodyReadWeight
		upload += candidate.MaxReadGapMS * profile.maxReadGapWeight
		upload += candidate.BodyIncompleteRate * profile.bodyIncompleteWeight
		upload += candidate.BodyReadErrorRate * profile.bodyReadErrorWeight
		if upload > profile.uploadCap {
			upload = profile.uploadCap
		}
	}
	breakdown["upload"] = upload
	download := 0.0
	if profile.downloadAlways || candidate.ResponseEgressBPS > 0 {
		if candidate.ResponseEgressBPS > 0 && candidate.ResponseEgressBPS < profile.downloadFloorBPS {
			download += ((profile.downloadFloorBPS - candidate.ResponseEgressBPS) / 1024) * profile.downloadDeficitWeight
		}
		download += candidate.ResponseWriteMS * profile.responseWriteWeight
		if download > profile.downloadCap {
			download = profile.downloadCap
		}
	}
	breakdown["download"] = download
	cache := 0.0
	if profile.cacheAware {
		if candidate.HitRatio > 0 && candidate.HitRatio < 1 {
			cache = (1 - candidate.HitRatio) * profile.cacheMissWeight
		} else if candidate.HitRatio == 0 {
			cache = profile.cacheMissWeight
		}
	}
	breakdown["cache"] = cache
	saturation := candidate.ActiveRequests*0.20 + candidate.ActiveBodyBuffers*5
	breakdown["saturation"] = saturation
	breakdown["confidence"] = candidate.ConfidencePenalty
	return network + latency + errorPenalty + origin + upload + download + cache + saturation + candidate.ConfidencePenalty
}

type edgeDNSQualityTrafficWeights struct {
	networkRetransWeight      float64
	networkBytesRetransWeight float64
	networkRTOWeight          float64
	networkRTTVarWeight       float64
	networkRTTWeight          float64
	networkCap                float64
	deliveryFloorBPS          float64
	deliveryDeficitWeight     float64
	ttfbWeight                float64
	upstreamWeight            float64
	totalWeight               float64
	errorWeight               float64
	originConnectWeight       float64
	originWriteWeight         float64
	originWaitWeight          float64
	originTTFBWeight          float64
	originTotalWeight         float64
	uploadAlways              bool
	uploadFloorBPS            float64
	uploadDeficitWeight       float64
	missingUploadPenalty      float64
	bodyReadWeight            float64
	maxReadGapWeight          float64
	bodyIncompleteWeight      float64
	bodyReadErrorWeight       float64
	uploadCap                 float64
	downloadAlways            bool
	downloadFloorBPS          float64
	downloadDeficitWeight     float64
	responseWriteWeight       float64
	downloadCap               float64
	cacheAware                bool
	cacheMissWeight           float64
}

func edgeDNSQualityTrafficProfile(trafficClass string) edgeDNSQualityTrafficWeights {
	base := edgeDNSQualityTrafficWeights{
		networkRetransWeight:      80,
		networkBytesRetransWeight: 120,
		networkRTOWeight:          120,
		networkRTTVarWeight:       0.20,
		networkRTTWeight:          0.04,
		networkCap:                450,
		deliveryFloorBPS:          256 * 1024,
		deliveryDeficitWeight:     0.12,
		ttfbWeight:                0.35,
		upstreamWeight:            0.20,
		totalWeight:               0.15,
		errorWeight:               700,
		originConnectWeight:       0.10,
		originWriteWeight:         0.20,
		originWaitWeight:          0.15,
		originTTFBWeight:          0.10,
		originTotalWeight:         0.05,
		uploadFloorBPS:            256 * 1024,
		uploadDeficitWeight:       1.00,
		missingUploadPenalty:      150,
		bodyReadWeight:            0.03,
		maxReadGapWeight:          0.08,
		bodyIncompleteWeight:      700,
		bodyReadErrorWeight:       700,
		uploadCap:                 700,
		downloadFloorBPS:          512 * 1024,
		downloadDeficitWeight:     0.50,
		responseWriteWeight:       0.05,
		downloadCap:               450,
	}
	switch strings.ToLower(strings.TrimSpace(trafficClass)) {
	case "large_body_api":
		base.networkRetransWeight = 140
		base.networkBytesRetransWeight = 180
		base.networkRTOWeight = 180
		base.networkCap = 650
		base.uploadAlways = true
		base.uploadFloorBPS = 512 * 1024
		base.uploadDeficitWeight = 1.25
		base.bodyReadWeight = 0.05
		base.maxReadGapWeight = 0.12
		base.bodyIncompleteWeight = 900
		base.bodyReadErrorWeight = 900
		base.uploadCap = 900
	case "static_cacheable":
		base.networkRetransWeight = 110
		base.networkBytesRetransWeight = 160
		base.networkRTOWeight = 150
		base.downloadAlways = true
		base.downloadFloorBPS = 1024 * 1024
		base.downloadDeficitWeight = 0.70
		base.cacheAware = true
		base.cacheMissWeight = 240
		base.originConnectWeight = 0.05
		base.originWriteWeight = 0.05
		base.originWaitWeight = 0.05
		base.originTTFBWeight = 0.05
		base.originTotalWeight = 0.03
	case "streaming", "sse", "websocket":
		base.networkRetransWeight = 150
		base.networkBytesRetransWeight = 190
		base.networkRTOWeight = 220
		base.networkCap = 750
		base.totalWeight = 0.02
		base.errorWeight = 850
		base.originTotalWeight = 0.01
	case "html_dynamic":
		base.ttfbWeight = 0.45
		base.originWaitWeight = 0.20
		base.originTTFBWeight = 0.12
	case "dynamic_api", "small_api":
		base.ttfbWeight = 0.42
		base.upstreamWeight = 0.22
		base.errorWeight = 800
	}
	return base
}

func edgeDNSLatencyCandidateConfidence(candidate edgeDNSLatencyCandidateProfile, accumulator *edgeDNSLatencyGroupAccumulator) float64 {
	if accumulator == nil || candidate.SampleCount <= 0 {
		return 0
	}
	sampleConfidence := float64(candidate.SampleCount) / 50.0
	if sampleConfidence > 1 {
		sampleConfidence = 1
	}
	metricCompleteness := 0.55
	if candidate.TTFBMS > 0 || candidate.TotalMS > 0 {
		metricCompleteness += 0.15
	}
	if candidate.UploadBPS > 0 || candidate.ResponseEgressBPS > 0 {
		metricCompleteness += 0.10
	}
	if candidate.ClientTCPRTTMS > 0 || candidate.ClientTCPRetransRate > 0 || candidate.ClientTCPRTORate > 0 {
		metricCompleteness += 0.15
	}
	if candidate.HitRatio > 0 || accumulator.CacheObservationCount > 0 {
		metricCompleteness += 0.05
	}
	if metricCompleteness > 1 {
		metricCompleteness = 1
	}
	confidence := sampleConfidence * metricCompleteness
	if confidence < 0 {
		return 0
	}
	if confidence > 1 {
		return 1
	}
	return confidence
}

func edgeDNSLatencyConfidencePenalty(confidence float64) float64 {
	if confidence >= 0.85 {
		return 0
	}
	if confidence <= 0 {
		return 250
	}
	return (0.85 - confidence) * 220
}

func edgeDNSApplyPeerRelativePenalties(candidates []edgeDNSLatencyCandidateProfile) {
	bestUpload := 0.0
	bestDownload := 0.0
	for _, candidate := range candidates {
		if candidate.UploadBPS > bestUpload {
			bestUpload = candidate.UploadBPS
		}
		if candidate.ResponseEgressBPS > bestDownload {
			bestDownload = candidate.ResponseEgressBPS
		}
	}
	for index := range candidates {
		breakdown := candidates[index].ScoreBreakdown
		if breakdown == nil {
			breakdown = map[string]float64{}
			candidates[index].ScoreBreakdown = breakdown
		}
		if bestUpload > 0 && candidates[index].UploadBPS > 0 && candidates[index].UploadBPS < bestUpload*0.5 {
			penalty := ((bestUpload / candidates[index].UploadBPS) - 1) * 80
			if penalty > 300 {
				penalty = 300
			}
			breakdown["upload_peer"] = penalty
			candidates[index].Score += penalty
		}
		if bestDownload > 0 && candidates[index].ResponseEgressBPS > 0 && candidates[index].ResponseEgressBPS < bestDownload*0.5 {
			penalty := ((bestDownload / candidates[index].ResponseEgressBPS) - 1) * 50
			if penalty > 200 {
				penalty = 200
			}
			breakdown["download_peer"] = penalty
			candidates[index].Score += penalty
		}
	}
}

func edgeDNSLatencyAverage(sum float64, count int) float64 {
	if count <= 0 || sum <= 0 {
		return 0
	}
	return sum / float64(count)
}

func edgeDNSDominantStringCount(counts map[string]int, fallback string) string {
	bestKey := strings.TrimSpace(fallback)
	bestCount := -1
	for key, count := range counts {
		if count > bestCount || (count == bestCount && key < bestKey) {
			bestKey = key
			bestCount = count
		}
	}
	if bestKey == "" {
		return fallback
	}
	return bestKey
}

func edgeDNSClampLatencyWeight(weight int) int {
	if weight < edgeDNSLatencyWeightMin {
		return edgeDNSLatencyWeightMin
	}
	if weight > edgeDNSLatencyWeightMax {
		return edgeDNSLatencyWeightMax
	}
	return weight
}

func edgeDNSLatencyReason(prefix string, candidate edgeDNSLatencyCandidateProfile) string {
	parts := []string{
		prefix,
		"ttfb_" + strconv.Itoa(int(candidate.TTFBMS+0.5)) + "ms",
		"upstream_" + strconv.Itoa(int(candidate.UpstreamMS+0.5)) + "ms",
		"class_" + strings.TrimSpace(candidate.TrafficClass),
		"upload_" + strconv.Itoa(int(candidate.UploadBPS+0.5)) + "bps",
		"hit_" + strconv.Itoa(int(candidate.HitRatio*100+0.5)) + "pct",
		"error_" + strconv.Itoa(int(candidate.ErrorRate*100+0.5)) + "pct",
		"tcp_retrans_" + strconv.Itoa(int(candidate.ClientTCPRetransRate*100+0.5)) + "pct",
		"confidence_" + strconv.Itoa(int(candidate.Confidence*100+0.5)) + "pct",
	}
	if candidate.Country != "" {
		parts = append(parts, "country_"+candidate.Country)
	}
	if candidate.Region != "" {
		parts = append(parts, "region_"+candidate.Region)
	}
	if candidate.ASN != "" {
		parts = append(parts, "asn_"+candidate.ASN)
	}
	return strings.Join(parts, "_")
}

func edgeDNSLatencyCandidateReason(profile *edgeDNSLatencyProfile, candidate edgeDNSLatencyCandidateProfile, groupID, preferredEdgeGroupID string) string {
	reason := strings.TrimSpace(candidate.Reason)
	if profile == nil || !profile.Enabled {
		return reason
	}
	if groupID == profile.BestEdgeGroupID {
		return reason
	}
	if preferredEdgeGroupID != "" && groupID == preferredEdgeGroupID {
		return strings.Replace(reason, "latency_penalized", "geo_close_but_slow", 1)
	}
	return reason
}

func edgeDNSCandidatesForAnswerIPs(answerIPs []string, candidateByIP map[string]model.EdgeDNSAnswerCandidate, routeReady map[string]bool, preferredEdgeGroupID, fallbackEdgeGroupID string, latencyProfile *edgeDNSLatencyProfile, applyLatency bool) []model.EdgeDNSAnswerCandidate {
	out := make([]model.EdgeDNSAnswerCandidate, 0, len(answerIPs))
	seen := map[string]bool{}
	for _, raw := range answerIPs {
		ip := normalizeEdgeDNSStaticRecordValue(model.EdgeDNSRecordTypeA, raw)
		if ip == "" {
			ip = normalizeEdgeDNSStaticRecordValue(model.EdgeDNSRecordTypeAAAA, raw)
		}
		if ip == "" || seen[ip] {
			continue
		}
		seen[ip] = true
		candidate, ok := candidateByIP[ip]
		if !ok {
			candidate = model.EdgeDNSAnswerCandidate{IP: ip, Healthy: true, RouteReady: true, TLSReady: true}
		}
		groupID := strings.TrimSpace(candidate.EdgeGroupID)
		if routeReady != nil {
			candidate.RouteReady = routeReady[groupID]
		}
		candidate.Priority = edgeDNSCandidatePriority(groupID, preferredEdgeGroupID, fallbackEdgeGroupID)
		candidate.Reason = edgeDNSCandidateReason(groupID, preferredEdgeGroupID, fallbackEdgeGroupID)
		if applyLatency && latencyProfile != nil && latencyProfile.Enabled {
			latencyCandidate, hasLatencyCandidate := latencyProfile.Candidates[groupID]
			if hasLatencyCandidate {
				candidate.Weight = latencyCandidate.Weight
				candidate.Reason = edgeDNSLatencyCandidateReason(latencyProfile, latencyCandidate, groupID, preferredEdgeGroupID)
				candidate.TrafficClass = latencyCandidate.TrafficClass
				candidate.Score = latencyCandidate.Score
				candidate.ScoreBreakdown = latencyCandidate.ScoreBreakdown
			}
			if nodeCandidate, ok := latencyProfile.NodeCandidates[strings.TrimSpace(candidate.EdgeID)]; ok {
				if nodeCandidate.Weight > 0 {
					candidate.Weight = nodeCandidate.Weight
				}
				if nodeCandidate.Reason != "" {
					candidate.Reason = edgeDNSLatencyCandidateReason(latencyProfile, nodeCandidate, groupID, preferredEdgeGroupID)
				}
				if nodeCandidate.TrafficClass != "" {
					candidate.TrafficClass = nodeCandidate.TrafficClass
				}
				candidate.Score = nodeCandidate.Score
				candidate.ScoreBreakdown = nodeCandidate.ScoreBreakdown
			}
		}
		if candidate.Weight <= 0 {
			candidate.Weight = 100
		}
		out = append(out, candidate)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Priority != out[j].Priority {
			return out[i].Priority < out[j].Priority
		}
		if out[i].Weight != out[j].Weight {
			return out[i].Weight > out[j].Weight
		}
		if out[i].Score > 0 && out[j].Score > 0 && out[i].Score != out[j].Score {
			return out[i].Score < out[j].Score
		}
		if out[i].EdgeGroupID != out[j].EdgeGroupID {
			return out[i].EdgeGroupID < out[j].EdgeGroupID
		}
		return out[i].IP < out[j].IP
	})
	return out
}

func edgeDNSAnswerPolicy(options edgeDNSBundleOptions, preferredEdgeGroupID, fallbackEdgeGroupID string, answerIPs []string, candidateByIP map[string]model.EdgeDNSAnswerCandidate, latencyProfile *edgeDNSLatencyProfile, ttl int, applyLatency bool) model.DNSAnswerPolicy {
	allowed := make([]string, 0, len(answerIPs))
	for _, ip := range answerIPs {
		normalized := normalizeEdgeDNSStaticRecordValue(model.EdgeDNSRecordTypeA, ip)
		if normalized == "" {
			normalized = normalizeEdgeDNSStaticRecordValue(model.EdgeDNSRecordTypeAAAA, ip)
		}
		candidate, ok := candidateByIP[normalized]
		if !ok {
			continue
		}
		if groupID := strings.TrimSpace(candidate.EdgeGroupID); groupID != "" && !stringSliceContains(allowed, groupID) {
			allowed = append(allowed, groupID)
		}
	}
	sort.Strings(allowed)
	preferred := uniqueSortedNonEmptyStrings(preferredEdgeGroupID, strings.TrimSpace(options.EdgeGroupID))
	fallback := uniqueSortedNonEmptyStrings(fallbackEdgeGroupID)
	policyKind := model.DNSAnswerPolicyKindGeo
	reason := "geo_healthy_route_ready"
	weight := 0
	selectedEdgeGroupID := edgeDNSLegacySelectedEdgeGroup(preferred, allowed)
	shadowSelectedEdgeGroupID := ""
	shadowReason := ""
	rankingVersion := ""
	rankingScope := ""
	if latencyProfile != nil && latencyProfile.Enabled {
		rankingVersion = edgeDNSQualityRankingVersion
		rankingScope = latencyProfile.Scope.key()
		shadowSelectedEdgeGroupID = strings.TrimSpace(latencyProfile.BestEdgeGroupID)
		shadowReason = "shadow_" + strings.TrimSpace(latencyProfile.Reason)
	}
	if applyLatency && latencyProfile != nil && latencyProfile.Enabled {
		policyKind = model.DNSAnswerPolicyKindLatencyAware
		reason = latencyProfile.Reason
		weight = latencyProfile.Weight
		selectedEdgeGroupID = strings.TrimSpace(latencyProfile.BestEdgeGroupID)
		shadowSelectedEdgeGroupID = strings.TrimSpace(latencyProfile.ShadowBestEdgeGroupID)
		shadowReason = strings.TrimSpace(latencyProfile.ShadowReason)
		if preferredEdgeGroupID != "" && latencyProfile.BestEdgeGroupID != "" && preferredEdgeGroupID != latencyProfile.BestEdgeGroupID {
			reason = "latency_aware_geo_close_but_slow"
		}
	}
	if selectedEdgeGroupID != "" && !stringSliceContains(allowed, selectedEdgeGroupID) {
		shadowSelectedEdgeGroupID = selectedEdgeGroupID
		if shadowReason == "" {
			shadowReason = "latency selected edge group is not in allowed answers"
		}
		selectedEdgeGroupID = ""
	}
	return model.DNSAnswerPolicy{
		PolicyKind:                policyKind,
		AllowedEdgeGroups:         allowed,
		PreferredEdgeGroups:       preferred,
		FallbackEdgeGroups:        fallback,
		TTLSeconds:                edgeDNSPolicyTTL(ttl),
		ECSEnabled:                true,
		HealthRequired:            true,
		RouteReadyRequired:        true,
		ExplorationPercent:        edgeDNSExplorationPercent,
		SwitchCooldownSec:         int(edgeDNSDecisionCooldown.Seconds()),
		RankingVersion:            rankingVersion,
		RankingScope:              rankingScope,
		Weight:                    weight,
		Reason:                    reason,
		SelectedEdgeGroupID:       selectedEdgeGroupID,
		ShadowSelectedEdgeGroupID: shadowSelectedEdgeGroupID,
		ShadowReason:              shadowReason,
	}
}

func edgeDNSLegacySelectedEdgeGroup(preferred, allowed []string) string {
	for _, groupID := range preferred {
		groupID = strings.TrimSpace(groupID)
		if groupID != "" && stringSliceContains(allowed, groupID) {
			return groupID
		}
	}
	if len(allowed) == 1 {
		return strings.TrimSpace(allowed[0])
	}
	return ""
}

func edgeDNSCandidatePriority(groupID, preferredEdgeGroupID, fallbackEdgeGroupID string) int {
	groupID = strings.TrimSpace(groupID)
	preferredEdgeGroupID = strings.TrimSpace(preferredEdgeGroupID)
	fallbackEdgeGroupID = strings.TrimSpace(fallbackEdgeGroupID)
	switch {
	case groupID != "" && preferredEdgeGroupID != "" && groupID == preferredEdgeGroupID:
		return 0
	case groupID != "" && fallbackEdgeGroupID != "" && groupID == fallbackEdgeGroupID:
		return 100
	default:
		return 50
	}
}

func edgeDNSCandidateReason(groupID, preferredEdgeGroupID, fallbackEdgeGroupID string) string {
	groupID = strings.TrimSpace(groupID)
	preferredEdgeGroupID = strings.TrimSpace(preferredEdgeGroupID)
	fallbackEdgeGroupID = strings.TrimSpace(fallbackEdgeGroupID)
	switch {
	case groupID != "" && preferredEdgeGroupID != "" && groupID == preferredEdgeGroupID:
		return "same_region"
	case groupID != "" && fallbackEdgeGroupID != "" && groupID == fallbackEdgeGroupID:
		return "fallback_healthy"
	default:
		return "global_route_ready"
	}
}

func edgeDNSPolicyTTL(ttl int) int {
	if ttl <= 0 {
		return defaultEdgeDNSTTL
	}
	if ttl < 60 {
		return 60
	}
	if ttl > 120 {
		return 120
	}
	return ttl
}

func uniqueSortedNonEmptyStrings(values ...string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || stringSliceContains(out, value) {
			continue
		}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func edgeDNSRecord(name, recordType string, values []string, ttl int, kind, status, reason, appID, tenantID, edgeGroupID, fallbackEdgeGroupID string) model.EdgeDNSRecord {
	record := model.EdgeDNSRecord{
		Name:                normalizeExternalAppDomain(name),
		Type:                strings.ToUpper(strings.TrimSpace(recordType)),
		Values:              append([]string(nil), values...),
		TTL:                 ttl,
		RecordKind:          kind,
		AppID:               strings.TrimSpace(appID),
		TenantID:            strings.TrimSpace(tenantID),
		EdgeGroupID:         strings.TrimSpace(edgeGroupID),
		FallbackEdgeGroupID: strings.TrimSpace(fallbackEdgeGroupID),
		Status:              strings.TrimSpace(status),
		StatusReason:        strings.TrimSpace(reason),
	}
	record.RecordGeneration = edgeDNSRecordGeneration(record)
	return record
}

func dedupeAndSortEdgeDNSRecords(records []model.EdgeDNSRecord) []model.EdgeDNSRecord {
	byKey := make(map[string]model.EdgeDNSRecord, len(records))
	for _, record := range records {
		key := record.Name + "\x00" + record.Type
		if existing, ok := byKey[key]; ok {
			record = mergeEdgeDNSRecords(existing, record)
		}
		byKey[key] = record
	}
	out := make([]model.EdgeDNSRecord, 0, len(byKey))
	for _, record := range byKey {
		if len(record.Values) == 0 {
			continue
		}
		out = append(out, record)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}
		return out[i].Type < out[j].Type
	})
	return out
}

func mergeEdgeDNSRecords(existing, incoming model.EdgeDNSRecord) model.EdgeDNSRecord {
	record := incoming
	constrainSharedTarget := edgeDNSRecordsShareConstrainedTarget(existing, incoming)
	if constrainSharedTarget {
		record.Values = intersectSortedStrings(existing.Values, incoming.Values)
	} else {
		record.Values = uniqueSortedStrings(append(existing.Values, record.Values...))
	}
	record.Candidates = mergeEdgeDNSAnswerCandidates(existing.Candidates, record.Candidates)
	record.ScopedCandidates = mergeEdgeDNSScopedAnswerCandidates(existing.ScopedCandidates, record.ScopedCandidates)
	record.AnswerPolicy = mergeEdgeDNSAnswerPolicy(existing.AnswerPolicy, record.AnswerPolicy)
	if constrainSharedTarget {
		record = constrainEdgeDNSRecordToValues(record)
	}
	record.RecordGeneration = edgeDNSRecordGeneration(record)
	return record
}

func edgeDNSRecordsShareConstrainedTarget(left, right model.EdgeDNSRecord) bool {
	recordType := strings.ToUpper(strings.TrimSpace(left.Type))
	if recordType == "" || recordType != strings.ToUpper(strings.TrimSpace(right.Type)) {
		return false
	}
	if recordType != model.EdgeDNSRecordTypeA && recordType != model.EdgeDNSRecordTypeAAAA {
		return false
	}
	return strings.TrimSpace(left.RecordKind) == model.EdgeDNSRecordKindCustomDomainTarget &&
		strings.TrimSpace(right.RecordKind) == model.EdgeDNSRecordKindCustomDomainTarget
}

func constrainEdgeDNSRecordToValues(record model.EdgeDNSRecord) model.EdgeDNSRecord {
	record.Values = uniqueSortedStrings(record.Values)
	record.Candidates = filterEdgeDNSAnswerCandidatesByValues(record.Candidates, record.Values)
	record.ScopedCandidates = filterEdgeDNSScopedCandidatesByValues(record.ScopedCandidates, record.Values)
	record.AnswerPolicy = constrainEdgeDNSAnswerPolicy(record.AnswerPolicy, record.Candidates)
	return record
}

func filterEdgeDNSAnswerCandidatesByValues(candidates []model.EdgeDNSAnswerCandidate, values []string) []model.EdgeDNSAnswerCandidate {
	allowed := edgeDNSValueSet(values)
	if len(allowed) == 0 {
		return nil
	}
	out := make([]model.EdgeDNSAnswerCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		ip := normalizeEdgeDNSStaticRecordValue(model.EdgeDNSRecordTypeA, candidate.IP)
		if ip == "" {
			ip = normalizeEdgeDNSStaticRecordValue(model.EdgeDNSRecordTypeAAAA, candidate.IP)
		}
		if ip == "" || !allowed[ip] {
			continue
		}
		out = append(out, candidate)
	}
	return out
}

func filterEdgeDNSScopedCandidatesByValues(scoped []model.EdgeDNSScopedAnswerCandidates, values []string) []model.EdgeDNSScopedAnswerCandidates {
	out := make([]model.EdgeDNSScopedAnswerCandidates, 0, len(scoped))
	for _, profile := range scoped {
		profile.Candidates = filterEdgeDNSAnswerCandidatesByValues(profile.Candidates, values)
		if len(profile.Candidates) == 0 {
			continue
		}
		allowedGroups := edgeDNSCandidateGroups(profile.Candidates)
		if profile.SelectedEdgeGroupID != "" && !stringSliceContains(allowedGroups, profile.SelectedEdgeGroupID) {
			profile.SelectedEdgeGroupID = ""
		}
		out = append(out, profile)
	}
	return out
}

func constrainEdgeDNSAnswerPolicy(policy model.DNSAnswerPolicy, candidates []model.EdgeDNSAnswerCandidate) model.DNSAnswerPolicy {
	allowedGroups := edgeDNSCandidateGroups(candidates)
	policy.AllowedEdgeGroups = allowedGroups
	policy.PreferredEdgeGroups = filterEdgeDNSGroups(policy.PreferredEdgeGroups, allowedGroups)
	policy.FallbackEdgeGroups = filterEdgeDNSGroups(policy.FallbackEdgeGroups, allowedGroups)
	if policy.SelectedEdgeGroupID != "" && !stringSliceContains(allowedGroups, policy.SelectedEdgeGroupID) {
		if policy.ShadowReason == "" {
			policy.ShadowReason = "selected edge group removed by shared DNS target constraints"
		}
		policy.ShadowSelectedEdgeGroupID = policy.SelectedEdgeGroupID
		policy.SelectedEdgeGroupID = ""
	}
	if policy.ShadowSelectedEdgeGroupID != "" && !stringSliceContains(allowedGroups, policy.ShadowSelectedEdgeGroupID) {
		policy.ShadowSelectedEdgeGroupID = ""
		policy.ShadowReason = ""
	}
	return policy
}

func edgeDNSCandidateGroups(candidates []model.EdgeDNSAnswerCandidate) []string {
	seen := map[string]struct{}{}
	for _, candidate := range candidates {
		groupID := strings.TrimSpace(candidate.EdgeGroupID)
		if groupID == "" {
			continue
		}
		seen[groupID] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for groupID := range seen {
		out = append(out, groupID)
	}
	sort.Strings(out)
	return out
}

func filterEdgeDNSGroups(values, allowed []string) []string {
	if len(values) == 0 || len(allowed) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || !stringSliceContains(allowed, value) || stringSliceContains(out, value) {
			continue
		}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func edgeDNSValueSet(values []string) map[string]bool {
	out := make(map[string]bool, len(values))
	for _, raw := range values {
		value := normalizeEdgeDNSStaticRecordValue(model.EdgeDNSRecordTypeA, raw)
		if value == "" {
			value = normalizeEdgeDNSStaticRecordValue(model.EdgeDNSRecordTypeAAAA, raw)
		}
		if value != "" {
			out[value] = true
		}
	}
	return out
}

func mergeEdgeDNSAnswerPolicy(existing, incoming model.DNSAnswerPolicy) model.DNSAnswerPolicy {
	if strings.TrimSpace(incoming.PolicyKind) == "" {
		return existing
	}
	if strings.TrimSpace(existing.PolicyKind) == "" {
		return incoming
	}
	existingRank := edgeDNSAnswerPolicyKindRank(existing.PolicyKind)
	incomingRank := edgeDNSAnswerPolicyKindRank(incoming.PolicyKind)
	if existingRank > incomingRank {
		return existing
	}
	return incoming
}

func edgeDNSAnswerPolicyKindRank(kind string) int {
	switch strings.TrimSpace(kind) {
	case model.DNSAnswerPolicyKindPinned, model.DNSAnswerPolicyKindDisabled:
		return 50
	case model.DNSAnswerPolicyKindLatencyAware:
		return 40
	case model.DNSAnswerPolicyKindWeighted:
		return 30
	case model.DNSAnswerPolicyKindGeo:
		return 20
	case model.DNSAnswerPolicyKindGlobal:
		return 10
	default:
		return 0
	}
}

func mergeEdgeDNSScopedAnswerCandidates(left, right []model.EdgeDNSScopedAnswerCandidates) []model.EdgeDNSScopedAnswerCandidates {
	byScope := make(map[string]model.EdgeDNSScopedAnswerCandidates, len(left)+len(right))
	for _, scoped := range append(append([]model.EdgeDNSScopedAnswerCandidates(nil), left...), right...) {
		key := strings.TrimSpace(scoped.ScopeKey)
		if key == "" {
			continue
		}
		if existing, ok := byScope[key]; ok {
			existing.Candidates = mergeEdgeDNSAnswerCandidates(existing.Candidates, scoped.Candidates)
			if existing.PolicyKind == "" {
				existing.PolicyKind = scoped.PolicyKind
			}
			if existing.Reason == "" {
				existing.Reason = scoped.Reason
			}
			if existing.SelectedEdgeGroupID == "" {
				existing.SelectedEdgeGroupID = scoped.SelectedEdgeGroupID
			}
			if existing.CooldownUntil.IsZero() {
				existing.CooldownUntil = scoped.CooldownUntil
			}
			byScope[key] = existing
			continue
		}
		byScope[key] = scoped
	}
	out := make([]model.EdgeDNSScopedAnswerCandidates, 0, len(byScope))
	for _, scoped := range byScope {
		out = append(out, scoped)
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].ScopeKey < out[j].ScopeKey
	})
	return out
}

func mergeEdgeDNSAnswerCandidates(left, right []model.EdgeDNSAnswerCandidate) []model.EdgeDNSAnswerCandidate {
	out := make([]model.EdgeDNSAnswerCandidate, 0, len(left)+len(right))
	seen := map[string]int{}
	for _, candidates := range [][]model.EdgeDNSAnswerCandidate{left, right} {
		for _, candidate := range candidates {
			key := strings.TrimSpace(candidate.IP) + "\x00" + strings.TrimSpace(candidate.EdgeGroupID)
			if key == "\x00" {
				continue
			}
			if existingIndex, ok := seen[key]; ok {
				if edgeDNSAnswerCandidateMetadataRank(candidate) > edgeDNSAnswerCandidateMetadataRank(out[existingIndex]) {
					out[existingIndex] = candidate
				}
				continue
			}
			seen[key] = len(out)
			out = append(out, candidate)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Priority != out[j].Priority {
			return out[i].Priority < out[j].Priority
		}
		if out[i].Weight != out[j].Weight {
			return out[i].Weight > out[j].Weight
		}
		if out[i].EdgeGroupID != out[j].EdgeGroupID {
			return out[i].EdgeGroupID < out[j].EdgeGroupID
		}
		return out[i].IP < out[j].IP
	})
	return out
}

func edgeDNSAnswerCandidateMetadataRank(candidate model.EdgeDNSAnswerCandidate) int {
	rank := 0
	if candidate.Score > 0 {
		rank += 16
	}
	if len(candidate.ScoreBreakdown) > 0 {
		rank += 8
	}
	if strings.TrimSpace(candidate.TrafficClass) != "" {
		rank += 4
	}
	if strings.TrimSpace(candidate.Reason) != "" {
		rank += 2
	}
	if candidate.Weight > 0 {
		rank++
	}
	return rank
}

func uniqueSortedStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func intersectSortedStrings(left, right []string) []string {
	rightSet := map[string]struct{}{}
	for _, value := range right {
		value = strings.TrimSpace(value)
		if value != "" {
			rightSet[value] = struct{}{}
		}
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(left))
	for _, value := range left {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := rightSet[value]; !ok {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

type edgeDNSRecordVersionMaterial struct {
	Name                string                                `json:"name"`
	Type                string                                `json:"type"`
	Values              []string                              `json:"values"`
	TTL                 int                                   `json:"ttl"`
	RecordKind          string                                `json:"record_kind"`
	AppID               string                                `json:"app_id,omitempty"`
	TenantID            string                                `json:"tenant_id,omitempty"`
	EdgeGroupID         string                                `json:"edge_group_id,omitempty"`
	FallbackEdgeGroupID string                                `json:"fallback_edge_group_id,omitempty"`
	Status              string                                `json:"status"`
	StatusReason        string                                `json:"status_reason,omitempty"`
	AnswerPolicy        model.DNSAnswerPolicy                 `json:"answer_policy,omitempty"`
	Candidates          []model.EdgeDNSAnswerCandidate        `json:"candidates,omitempty"`
	ScopedCandidates    []model.EdgeDNSScopedAnswerCandidates `json:"scoped_candidates,omitempty"`
}

type edgeDNSBundleVersionMaterial struct {
	Zone    string                         `json:"zone"`
	Records []edgeDNSRecordVersionMaterial `json:"records"`
}

func edgeDNSBundleVersion(bundle model.EdgeDNSBundle) string {
	records := make([]edgeDNSRecordVersionMaterial, len(bundle.Records))
	for index, record := range bundle.Records {
		records[index] = edgeDNSRecordVersionMaterialFromRecord(record)
	}
	material := edgeDNSBundleVersionMaterial{
		Zone:    normalizeExternalAppDomain(bundle.Zone),
		Records: records,
	}
	payload, _ := json.Marshal(material)
	sum := sha256.Sum256(payload)
	return edgeDNSBundleVersionPrefix + hex.EncodeToString(sum[:])[:16]
}

func edgeDNSRecordGeneration(record model.EdgeDNSRecord) string {
	payload, _ := json.Marshal(edgeDNSRecordVersionMaterialFromRecord(record))
	sum := sha256.Sum256(payload)
	return edgeDNSBundleVersionPrefix + hex.EncodeToString(sum[:])[:16]
}

func edgeDNSRecordVersionMaterialFromRecord(record model.EdgeDNSRecord) edgeDNSRecordVersionMaterial {
	return edgeDNSRecordVersionMaterial{
		Name:                record.Name,
		Type:                record.Type,
		Values:              append([]string(nil), record.Values...),
		TTL:                 record.TTL,
		RecordKind:          record.RecordKind,
		AppID:               record.AppID,
		TenantID:            record.TenantID,
		EdgeGroupID:         record.EdgeGroupID,
		FallbackEdgeGroupID: record.FallbackEdgeGroupID,
		Status:              record.Status,
		StatusReason:        record.StatusReason,
		AnswerPolicy:        record.AnswerPolicy,
		Candidates:          append([]model.EdgeDNSAnswerCandidate(nil), record.Candidates...),
		ScopedCandidates:    append([]model.EdgeDNSScopedAnswerCandidates(nil), record.ScopedCandidates...),
	}
}
