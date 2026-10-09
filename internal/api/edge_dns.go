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
	"fugue/internal/platformconfig"
)

const (
	defaultEdgeDNSTTL          = 60
	defaultEdgeDNSProbeLabel   = "d-test"
	edgeDNSBundleVersionPrefix = "dnsgen_"
)

type edgeDNSBundleOptions struct {
	DNSNodeID        string
	EdgeGroupID      string
	AuthorityService string
	Zone             string
	AnswerIPs        []string
	RouteAAnswerIPs  []string
	TTL              int
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
	w.Header().Set("Cache-Control", "no-store")
	httpx.WriteError(w, http.StatusGone, "legacy DNS bundle serving is retired; use a signed TrafficReleaseSet and retain the current verified LKG")
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
		DNSNodeID:        strings.TrimSpace(query.Get("dns_node_id")),
		EdgeGroupID:      strings.TrimSpace(query.Get("edge_group_id")),
		AuthorityService: strings.TrimSpace(query.Get("authority_service")),
		Zone:             zone,
		AnswerIPs:        answerIPs,
		RouteAAnswerIPs:  routeAAnswerIPs,
		TTL:              ttl,
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

func firstNonZeroTime(values ...time.Time) time.Time {
	for _, value := range values {
		if !value.IsZero() {
			return value
		}
	}
	return time.Time{}
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
	byKey := make(map[string][]model.EdgeDNSRecord, len(records))
	for _, record := range records {
		key := record.Name + "\x00" + record.Type
		byKey[key] = append(byKey[key], record)
	}
	out := make([]model.EdgeDNSRecord, 0, len(byKey))
	for _, groupedRecords := range byKey {
		if len(groupedRecords) == 0 {
			continue
		}
		record := groupedRecords[0]
		if edgeDNSRecordsFormSharedConstrainedTarget(groupedRecords) {
			record = mergeSharedEdgeDNSTargetRecords(groupedRecords)
		} else {
			for _, incoming := range groupedRecords[1:] {
				record = mergeEdgeDNSRecords(record, incoming)
			}
		}
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

func edgeDNSRecordsFormSharedConstrainedTarget(records []model.EdgeDNSRecord) bool {
	if len(records) < 2 {
		return false
	}
	for _, record := range records[1:] {
		if !edgeDNSRecordsShareConstrainedTarget(records[0], record) {
			return false
		}
	}
	return true
}

func mergeSharedEdgeDNSTargetRecords(records []model.EdgeDNSRecord) model.EdgeDNSRecord {
	if len(records) == 0 {
		return model.EdgeDNSRecord{}
	}
	preferred := records[0]
	values := append([]string(nil), records[0].Values...)
	for _, record := range records[1:] {
		preferred = edgeDNSPreferredSharedTargetRoutingRecord(preferred, record)
		values = intersectSortedStrings(values, record.Values)
	}
	preferred.Values = values
	preferred = constrainEdgeDNSRecordToValues(preferred)
	preferred.RecordGeneration = edgeDNSRecordGeneration(preferred)
	return preferred
}

func mergeEdgeDNSRecords(existing, incoming model.EdgeDNSRecord) model.EdgeDNSRecord {
	constrainSharedTarget := edgeDNSRecordsShareConstrainedTarget(existing, incoming)
	if constrainSharedTarget {
		record := edgeDNSPreferredSharedTargetRoutingRecord(existing, incoming)
		record.Values = intersectSortedStrings(existing.Values, incoming.Values)
		record = constrainEdgeDNSRecordToValues(record)
		record.RecordGeneration = edgeDNSRecordGeneration(record)
		return record
	}
	record := incoming
	record.Values = uniqueSortedStrings(append(existing.Values, record.Values...))
	record.Candidates = mergeEdgeDNSAnswerCandidates(existing.Candidates, record.Candidates)
	record.ScopedCandidates = mergeEdgeDNSScopedAnswerCandidates(existing.ScopedCandidates, record.ScopedCandidates)
	record.AnswerPolicy = mergeEdgeDNSAnswerPolicy(existing.AnswerPolicy, record.AnswerPolicy)
	record.RecordGeneration = edgeDNSRecordGeneration(record)
	return record
}

func edgeDNSPreferredSharedTargetRoutingRecord(left, right model.EdgeDNSRecord) model.EdgeDNSRecord {
	leftTrafficPriority := edgeDNSSharedTargetTrafficPriority(left)
	rightTrafficPriority := edgeDNSSharedTargetTrafficPriority(right)
	if leftTrafficPriority != rightTrafficPriority {
		if leftTrafficPriority > rightTrafficPriority {
			return left
		}
		return right
	}
	leftPolicyPriority := edgeDNSAnswerPolicyKindRank(left.AnswerPolicy.PolicyKind)
	rightPolicyPriority := edgeDNSAnswerPolicyKindRank(right.AnswerPolicy.PolicyKind)
	if leftPolicyPriority != rightPolicyPriority {
		if leftPolicyPriority > rightPolicyPriority {
			return left
		}
		return right
	}
	// Selection identity must not depend on heartbeat generations, readiness
	// flags, or the transient pre-intersection address set. Compare stable route
	// preferences first; ranking observations break otherwise identical ties.
	if edgeDNSSharedTargetPolicyKey(left) != edgeDNSSharedTargetPolicyKey(right) {
		if edgeDNSSharedTargetPolicyKey(left) < edgeDNSSharedTargetPolicyKey(right) {
			return left
		}
		return right
	}
	if edgeDNSSharedTargetRankKey(left) <= edgeDNSSharedTargetRankKey(right) {
		return left
	}
	return right
}

func edgeDNSSharedTargetPolicyKey(r model.EdgeDNSRecord) string {
	p := r.AnswerPolicy
	raw, _ := json.Marshal(struct {
		Primary     string
		Fallback    string
		Preferred   []string
		Fallbacks   []string
		Mode        string
		ECS         bool
		TTL         int
		Exploration int
		Cooldown    int
	}{r.EdgeGroupID, r.FallbackEdgeGroupID, p.PreferredEdgeGroups, p.FallbackEdgeGroups, p.PolicyKind, p.ECSEnabled, r.TTL, p.ExplorationPercent, p.SwitchCooldownSec})
	return string(raw)
}
func edgeDNSSharedTargetRankKey(r model.EdgeDNSRecord) string {
	type scope struct {
		Mode string
		Fact platformconfig.DNSSelectionScope
	}
	scopes := []scope{}
	for _, s := range r.ScopedCandidates {
		scopes = append(scopes, scope{Mode: s.PolicyKind, Fact: platformconfig.DNSSelectionScope{ScopeKey: s.ScopeKey, Country: s.Country, Region: s.Region, ASN: s.ASN, SelectedEdgeGroupID: s.SelectedEdgeGroupID, CooldownUntil: s.CooldownUntil, Reason: s.Reason, Candidates: dnsSelectionCandidates(s.Candidates)}})
	}
	raw, _ := json.Marshal(struct {
		Policy     model.DNSAnswerPolicy
		Candidates []platformconfig.DNSSelectionCandidate
		Scopes     []scope
	}{r.AnswerPolicy, dnsSelectionCandidates(r.Candidates), scopes})
	return string(raw)
}

func edgeDNSSharedTargetTrafficPriority(record model.EdgeDNSRecord) int {
	// TrafficClass is produced by the existing quality-ranking pipeline. Keep the
	// distinction intentionally binary: a cache-specific static profile must not
	// replace an observed non-static profile for an authority shared by both.
	hasStaticProfile := false
	hasNonStaticProfile := false
	observe := func(trafficClass string) {
		trafficClass = strings.ToLower(strings.TrimSpace(trafficClass))
		if trafficClass == "" {
			return
		}
		if trafficClass == "static_cacheable" {
			hasStaticProfile = true
			return
		}
		hasNonStaticProfile = true
	}
	for _, candidate := range record.Candidates {
		observe(candidate.TrafficClass)
	}
	for _, scoped := range record.ScopedCandidates {
		for _, candidate := range scoped.Candidates {
			observe(candidate.TrafficClass)
		}
	}
	if hasNonStaticProfile {
		return 2
	}
	if hasStaticProfile {
		return 1
	}
	return 0
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
	Zone        string                         `json:"zone"`
	HostedZones []string                       `json:"hosted_zones,omitempty"`
	Records     []edgeDNSRecordVersionMaterial `json:"records"`
}

func edgeDNSBundleVersion(bundle model.EdgeDNSBundle) string {
	records := make([]edgeDNSRecordVersionMaterial, len(bundle.Records))
	for index, record := range bundle.Records {
		records[index] = edgeDNSRecordVersionMaterialFromRecord(record)
	}
	material := edgeDNSBundleVersionMaterial{
		Zone:        normalizeExternalAppDomain(bundle.Zone),
		HostedZones: append([]string(nil), bundle.HostedZones...),
		Records:     records,
	}
	payload, _ := json.Marshal(material)
	sum := sha256.Sum256(payload)
	return edgeDNSBundleVersionPrefix + hex.EncodeToString(sum[:])[:16]
}

func edgeDNSPublishableHostedZoneNames(zones []model.HostedZone) []string {
	seen := make(map[string]struct{}, len(zones))
	out := make([]string, 0, len(zones))
	for _, zone := range zones {
		status := model.NormalizeHostedZoneStatus(zone.Status)
		switch status {
		case model.HostedZoneStatusPendingDelegation, model.HostedZoneStatusActive, model.HostedZoneStatusDegraded:
		default:
			continue
		}
		name := normalizeExternalAppDomain(zone.ZoneName)
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	sort.Strings(out)
	return out
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
