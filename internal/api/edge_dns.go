package api

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
	DNSNodeID   string
	EdgeGroupID string
	Zone        string
	AnswerIPs   []string
	TTL         int
}

func (s *Server) handleEdgeDNSBundle(w http.ResponseWriter, r *http.Request) {
	if !s.authorizeEdgeToken(w, r) {
		return
	}

	options, err := s.edgeDNSBundleOptionsFromRequest(r)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
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

	zone := normalizeExternalAppDomain(query.Get("zone"))
	if zone == "" {
		zone = normalizeExternalAppDomain(s.customDomainBaseDomain)
	}
	if zone == "" {
		return edgeDNSBundleOptions{}, errInvalidEdgeDNSOption("dns zone is not configured")
	}

	return edgeDNSBundleOptions{
		DNSNodeID:   strings.TrimSpace(query.Get("dns_node_id")),
		EdgeGroupID: strings.TrimSpace(query.Get("edge_group_id")),
		Zone:        zone,
		AnswerIPs:   answerIPs,
		TTL:         ttl,
	}, nil
}

func parseEdgeDNSAnswerIPs(values []string) ([]string, error) {
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
	if len(out) == 0 {
		return nil, errInvalidEdgeDNSOption("at least one answer_ip is required")
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

	runtimeByID := make(map[string]model.Runtime, len(runtimes))
	for _, runtimeObj := range runtimes {
		runtimeByID[strings.TrimSpace(runtimeObj.ID)] = runtimeObj
	}
	appByID := make(map[string]model.App, len(apps))
	for _, app := range apps {
		app = s.overlayManagedAppStatusCached(app)
		appByID[strings.TrimSpace(app.ID)] = app
	}

	records := make([]model.EdgeDNSRecord, 0, len(domains)+1)
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

	for _, domain := range domains {
		hostname := normalizeExternalAppDomain(domain.Hostname)
		if hostname == "" || !s.managedEdgeCustomDomain(hostname) {
			continue
		}
		app, ok := appByID[strings.TrimSpace(domain.AppID)]
		if !ok {
			continue
		}
		binding := s.deriveEdgeRouteBinding(r, app, hostname, model.EdgeRouteKindCustomDomain, model.EdgeRouteTLSPolicyCustomDomain, domain.CreatedAt, domain.UpdatedAt, runtimeByID)
		if !edgeRouteMatchesSelector(binding, edgeRouteBundleOptions{EdgeGroupID: options.EdgeGroupID}) {
			continue
		}
		target := normalizeExternalAppDomain(domain.RouteTarget)
		if target == "" {
			target = normalizeExternalAppDomain(s.primaryCustomDomainTarget(app))
		}
		if !edgeDNSTargetWithinZone(target, options.Zone) {
			continue
		}
		records = append(records, edgeDNSRecordsForTarget(
			target,
			options.AnswerIPs,
			options.TTL,
			model.EdgeDNSRecordKindCustomDomainTarget,
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
	return bundle, nil
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
	sort.Strings(record.Values)
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
