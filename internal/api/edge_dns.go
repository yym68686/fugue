package api

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
)

const (
	defaultEdgeDNSTTL          = 60
	defaultEdgeDNSProbeLabel   = "d-test"
	edgeDNSBundleVersionPrefix = "dnsgen_"
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
	bundle, err := s.deriveEdgeDNSBundle(r, options)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}

	etag := edgeRouteBundleETag(bundle.Version)
	w.Header().Set("ETag", etag)
	w.Header().Set("Cache-Control", "private, no-cache")
	w.Header().Set("X-Fugue-DNS-Bundle-Version", bundle.Version)
	if edgeRouteBundleETagMatches(r.Header.Get("If-None-Match"), bundle.Version) {
		w.WriteHeader(http.StatusNotModified)
		return
	}
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
	healthyEdgeGroups, err := s.edgeRouteHealthyEdgeGroups()
	if err != nil {
		return model.EdgeDNSBundle{}, err
	}
	acmeChallenges, err := s.store.ListDNSACMEChallenges(options.Zone, false)
	if err != nil {
		return model.EdgeDNSBundle{}, err
	}

	runtimeByID := make(map[string]model.Runtime, len(runtimes))
	for _, runtimeObj := range runtimes {
		runtimeByID[strings.TrimSpace(runtimeObj.ID)] = runtimeObj
	}
	runtimeNodeLabelsByID := s.edgeRouteRuntimeNodeLabels(r.Context())
	appByID := make(map[string]model.App, len(apps))
	for _, app := range apps {
		app = s.overlayManagedAppStatusCached(app)
		appByID[strings.TrimSpace(app.ID)] = app
	}
	policyByHostname := edgeRoutePolicyByHostname(policies)
	edgeAnswerIPsByGroup, err := s.edgeDNSAnswerIPsByGroup(options)
	if err != nil {
		return model.EdgeDNSBundle{}, err
	}

	staticRecords := edgeDNSStaticRecordsForZone(s.dnsStaticRecords, options.Zone)
	platformOverrideNames := s.edgeDNSPlatformDomainNames(domains, options.Zone)
	for hostname := range s.edgeDNSPlatformRouteNames(options.Zone) {
		platformOverrideNames[hostname] = true
	}
	staticRecords = edgeDNSStaticRecordsWithoutPlatformOverrides(staticRecords, platformOverrideNames)
	protectedNames := edgeDNSProtectedRecordNames(staticRecords)

	acmeRecords := edgeDNSACMEChallengeRecords(acmeChallenges)

	records := make([]model.EdgeDNSRecord, 0, len(staticRecords)+len(acmeRecords)+len(apps)+len(domains)+len(s.platformRoutes)+1)
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
		answerIPs := edgeDNSAnswerIPsForPlatformRoute(platformRoute, options, edgeAnswerIPsByGroup)
		if len(answerIPs) == 0 {
			continue
		}
		edgeGroupID := strings.TrimSpace(platformRoute.EdgeGroupID)
		if edgeGroupID == "" {
			edgeGroupID = strings.TrimSpace(options.EdgeGroupID)
		}
		records = append(records, edgeDNSRecordsForTarget(
			hostname,
			answerIPs,
			platformRoute.TTL,
			model.EdgeDNSRecordKindPlatformRoute,
			platformRoute.Status,
			platformRoute.StatusReason,
			"",
			"",
			edgeGroupID,
			"",
		)...)
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
		binding = applyEdgeRoutePolicy(binding, policyByHostname, healthyEdgeGroups)
		answerIPs := edgeDNSAnswerIPsForBinding(binding, options, edgeAnswerIPsByGroup)
		if len(answerIPs) == 0 {
			continue
		}
		records = append(records, edgeDNSRecordsForTarget(
			hostname,
			answerIPs,
			options.TTL,
			model.EdgeDNSRecordKindPlatform,
			binding.Status,
			binding.StatusReason,
			app.ID,
			app.TenantID,
			binding.EdgeGroupID,
			binding.FallbackEdgeGroupID,
		)...)
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
		binding = applyEdgeRoutePolicy(binding, policyByHostname, healthyEdgeGroups)
		binding = applyCustomDomainReadiness(binding, domain)
		if routeKind == model.EdgeRouteKindCustomDomain && binding.Status != model.EdgeRouteStatusActive {
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
		answerIPs := edgeDNSAnswerIPsForBinding(binding, options, edgeAnswerIPsByGroup)
		if len(answerIPs) == 0 {
			continue
		}
		records = append(records, edgeDNSRecordsForTarget(
			target,
			answerIPs,
			options.TTL,
			recordKind,
			binding.Status,
			binding.StatusReason,
			app.ID,
			app.TenantID,
			binding.EdgeGroupID,
			binding.FallbackEdgeGroupID,
		)...)
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
	if err := validateEdgeDNSBundleForPublish(bundle, options, staticRecords); err != nil {
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

func (s *Server) edgeDNSAnswerIPsByGroup(options edgeDNSBundleOptions) (map[string][]string, error) {
	out := map[string][]string{}
	if s.store != nil {
		nodes, _, err := s.store.ListEdgeNodes("")
		if err != nil {
			return nil, err
		}
		for _, node := range nodes {
			if !node.Healthy || node.Draining || !strings.EqualFold(strings.TrimSpace(node.Status), model.EdgeHealthHealthy) {
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
	if options.EdgeGroupID != "" && len(out[options.EdgeGroupID]) == 0 {
		out[options.EdgeGroupID] = append([]string(nil), options.AnswerIPs...)
	}
	return out, nil
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
	if binding.Status == model.EdgeRouteStatusActive && model.EdgeRoutePolicyAllowsTraffic(binding.RoutePolicy) {
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
	}
	if len(options.RouteAAnswerIPs) > 0 {
		return append([]string(nil), options.RouteAAnswerIPs...)
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
			record.Values = uniqueSortedStrings(append(existing.Values, record.Values...))
			record.RecordGeneration = edgeDNSRecordGeneration(record)
		}
		byKey[key] = record
	}
	out := make([]model.EdgeDNSRecord, 0, len(byKey))
	for _, record := range byKey {
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

type edgeDNSRecordVersionMaterial struct {
	Name                string   `json:"name"`
	Type                string   `json:"type"`
	Values              []string `json:"values"`
	TTL                 int      `json:"ttl"`
	RecordKind          string   `json:"record_kind"`
	AppID               string   `json:"app_id,omitempty"`
	TenantID            string   `json:"tenant_id,omitempty"`
	EdgeGroupID         string   `json:"edge_group_id,omitempty"`
	FallbackEdgeGroupID string   `json:"fallback_edge_group_id,omitempty"`
	Status              string   `json:"status"`
	StatusReason        string   `json:"status_reason,omitempty"`
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
	}
}
