package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"fugue/internal/model"
	"fugue/internal/store"
	"strings"
	"time"
)

// Frozen legacy compiler reference for migration equivalence regressions.
// It is not linked into the API or any production consumer.
type edgeDNSBundleCompileSnapshot struct {
	generatedAt                  time.Time
	apps                         []model.App
	domains                      []model.AppDomain
	runtimeByID                  map[string]model.Runtime
	runtimeNodeLabelsByID        map[string]map[string]string
	policyByHostname             map[string]model.EdgeRoutePolicy
	healthyEdgeGroups            map[string]bool
	healthyEdgeNodeIDsByGroup    map[string][]string
	hostedZoneNames              []string
	zoneDataByName               map[string]edgeDNSZoneCompileData
	edgeAnswerIPsByGroup         map[string][]string
	edgeCandidateByIP            map[string]model.EdgeDNSAnswerCandidate
	latencyProfiles              edgeDNSLatencyProfileCatalog
	applyQualityRanking          bool
	routeBindingByCompilationKey map[string]model.EdgeRouteBinding
}

type edgeDNSZoneCompileData struct {
	acmeChallenges []model.DNSACMEChallenge
	hostedRecords  []model.DNSRecord
}

func (s *Server) loadEdgeDNSBundleCompileSnapshot(ctx context.Context, zones []string, now time.Time) (*edgeDNSBundleCompileSnapshot, error) {
	if s == nil || s.store == nil {
		return nil, errors.New("edge DNS compile store is unavailable")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}
	apps, err := s.store.ListAppsMetadata("", true)
	if err != nil {
		return nil, err
	}
	domains, err := s.store.ListVerifiedAppDomains()
	if err != nil {
		return nil, err
	}
	runtimes, err := s.store.ListRuntimes("", true)
	if err != nil {
		return nil, err
	}
	policies, err := s.store.ListEdgeRoutePolicies()
	if err != nil {
		return nil, err
	}
	healthyEdgeGroups, healthyEdgeNodeIDsByGroup, err := s.edgeRouteHealthyEdgeGroupInventory(ctx)
	if err != nil {
		return nil, err
	}
	hostedZones, err := s.store.ListHostedZones("", true)
	if err != nil {
		return nil, err
	}

	runtimeByID := make(map[string]model.Runtime, len(runtimes))
	for _, runtimeObj := range runtimes {
		runtimeByID[strings.TrimSpace(runtimeObj.ID)] = runtimeObj
	}
	apps = s.overlayManagedAppStatusesCached(apps)
	edgeAnswerIPsByGroup, edgeCandidateByIP, err := s.edgeDNSAnswerInventory(ctx, "", now)
	if err != nil {
		return nil, err
	}
	latencyProfiles, err := s.edgeDNSLatencyProfilesWithContext(ctx, now)
	if err != nil {
		return nil, err
	}

	snapshot := &edgeDNSBundleCompileSnapshot{
		generatedAt:                  now,
		apps:                         apps,
		domains:                      domains,
		runtimeByID:                  runtimeByID,
		runtimeNodeLabelsByID:        s.edgeRouteRuntimeNodeLabels(ctx),
		policyByHostname:             edgeRoutePolicyByHostname(policies),
		healthyEdgeGroups:            healthyEdgeGroups,
		healthyEdgeNodeIDsByGroup:    healthyEdgeNodeIDsByGroup,
		hostedZoneNames:              edgeDNSPublishableHostedZoneNames(hostedZones),
		zoneDataByName:               make(map[string]edgeDNSZoneCompileData),
		edgeAnswerIPsByGroup:         edgeAnswerIPsByGroup,
		edgeCandidateByIP:            edgeCandidateByIP,
		latencyProfiles:              latencyProfiles,
		applyQualityRanking:          s.edgeQualityRankingActive(),
		routeBindingByCompilationKey: make(map[string]model.EdgeRouteBinding),
	}
	for _, zone := range uniqueSortedStrings(zones) {
		zone = normalizeExternalAppDomain(zone)
		if zone == "" {
			continue
		}
		acmeChallenges, err := s.store.ListDNSACMEChallenges(zone, false)
		if err != nil {
			return nil, err
		}
		hostedRecords := []model.DNSRecord{}
		hostedZone, err := s.store.GetHostedZoneByName(zone)
		if err == nil && hostedZone.Status != model.HostedZoneStatusDeleted {
			hostedRecords, err = s.store.ListDNSRecords(hostedZone.ID)
			if err != nil {
				return nil, err
			}
		} else if err != nil && !errors.Is(err, store.ErrNotFound) {
			return nil, err
		}
		snapshot.zoneDataByName[zone] = edgeDNSZoneCompileData{
			acmeChallenges: acmeChallenges,
			hostedRecords:  hostedRecords,
		}
	}
	return snapshot, nil
}

func edgeDNSRouteBindingCompilationKey(app model.App, hostname, routeKind, tlsPolicy string, createdAt, updatedAt time.Time) string {
	return strings.Join([]string{
		strings.TrimSpace(app.ID),
		normalizeExternalAppDomain(hostname),
		strings.TrimSpace(routeKind),
		strings.TrimSpace(tlsPolicy),
		createdAt.UTC().Format(time.RFC3339Nano),
		updatedAt.UTC().Format(time.RFC3339Nano),
	}, "\x00")
}

func (snapshot *edgeDNSBundleCompileSnapshot) compileRouteBinding(s *Server, ctx context.Context, app model.App, hostname, routeKind, tlsPolicy string, createdAt, updatedAt time.Time) model.EdgeRouteBinding {
	key := edgeDNSRouteBindingCompilationKey(app, hostname, routeKind, tlsPolicy, createdAt, updatedAt)
	if binding, ok := snapshot.routeBindingByCompilationKey[key]; ok {
		return binding
	}
	binding := s.compileTrafficEpochRouteBinding(ctx, app, hostname, routeKind, tlsPolicy, createdAt, updatedAt, snapshot.runtimeByID, snapshot.runtimeNodeLabelsByID)
	snapshot.routeBindingByCompilationKey[key] = binding
	return binding
}

func (s *Server) compileEdgeDNSBundle(ctx context.Context, options edgeDNSBundleOptions, snapshot *edgeDNSBundleCompileSnapshot) (model.EdgeDNSBundle, error) {
	if snapshot == nil {
		return model.EdgeDNSBundle{}, errors.New("edge DNS compile snapshot is unavailable")
	}
	zoneData, ok := snapshot.zoneDataByName[normalizeExternalAppDomain(options.Zone)]
	if !ok {
		return model.EdgeDNSBundle{}, fmt.Errorf("edge DNS compile snapshot does not contain zone %s", options.Zone)
	}
	apps := snapshot.apps
	domains := snapshot.domains
	policyByHostname := snapshot.policyByHostname
	healthyEdgeGroups := snapshot.healthyEdgeGroups
	healthyEdgeNodeIDsByGroup := snapshot.healthyEdgeNodeIDsByGroup
	hostedZoneNames := snapshot.hostedZoneNames
	edgeAnswerIPsByGroup := snapshot.edgeAnswerIPsByGroup
	edgeCandidateByIP := snapshot.edgeCandidateByIP
	latencyProfiles := snapshot.latencyProfiles
	applyQualityRanking := snapshot.applyQualityRanking
	now := snapshot.generatedAt
	acmeChallenges := zoneData.acmeChallenges
	hostedRecords := zoneData.hostedRecords

	appByID := make(map[string]model.App, len(apps))
	for _, app := range apps {
		appByID[strings.TrimSpace(app.ID)] = app
	}
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
		registerEdgeDNSRouteReadyBindings(routeReadyByHostnameEdgeGroup, edgeRouteBindingsForPlatformRoute(platformRoute, healthyEdgeGroups, healthyEdgeGroups, healthyEdgeNodeIDsByGroup))
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
		binding := snapshot.compileRouteBinding(s, ctx, app, hostname, model.EdgeRouteKindPlatform, model.EdgeRouteTLSPolicyPlatform, app.CreatedAt, app.UpdatedAt)
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
		binding := snapshot.compileRouteBinding(s, ctx, app, fqdn, model.EdgeRouteKindCustomDomain, model.EdgeRouteTLSPolicyCustomDomain, record.CreatedAt, record.UpdatedAt)
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
		binding := snapshot.compileRouteBinding(s, ctx, app, hostname, model.EdgeRouteKindPlatform, model.EdgeRouteTLSPolicyPlatform, app.CreatedAt, app.UpdatedAt)
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
		binding := snapshot.compileRouteBinding(s, ctx, app, hostname, routeKind, tlsPolicy, domain.CreatedAt, domain.UpdatedAt)
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
		GeneratedAt: now,
		DNSNodeID:   options.DNSNodeID,
		EdgeGroupID: options.EdgeGroupID,
		Zone:        options.Zone,
		HostedZones: hostedZoneNames,
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
	bundle.Generation = edgeDNSBundleEnvelopeGeneration(bundle)
	bundle = signEdgeDNSBundle(bundle, s.bundleKeyring(), s.discoveryBundleTTL())
	return bundle, nil
}

func edgeDNSBundleEnvelopeGeneration(bundle model.EdgeDNSBundle) string {
	payload, _ := json.Marshal(bundle)
	sum := sha256.Sum256(payload)
	return "dnsenv_" + hex.EncodeToString(sum[:])
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
