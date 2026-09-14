package api

import (
	"sort"
	"strings"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

type platformDNSExclusion struct {
	RecordID string `json:"record_id"`
	ZoneID   string `json:"zone_id"`
	Hostname string `json:"hostname"`
	Reason   string `json:"reason"`
}

func projectBusinessDNSDraft(result *platformIntentProjectionResponse, apps map[string]model.App, zones []model.HostedZone, records []model.DNSRecord, static []model.EdgeDNSRecord) ([]platformconfig.DNSIntent, []platformDNSExclusion) {
	byID := make(map[string]model.HostedZone, len(zones))
	for _, zone := range zones {
		byID[zone.ID] = zone
	}
	out := []platformconfig.DNSIntent{}
	exclusions := []platformDNSExclusion{}
	issue := func(code, hostname string) {
		result.Issues = append(result.Issues, platformProjectionIssue{Code: code, Hostname: hostname})
	}
	for _, record := range static {
		out = append(out, platformconfig.DNSIntent{Hostname: record.Name, Type: record.Type, Values: append([]string(nil), record.Values...), TTL: record.TTL, RecordKind: record.RecordKind, Status: record.Status, StatusReason: record.StatusReason, AppID: record.AppID, TenantID: record.TenantID, EdgeGroupID: record.EdgeGroupID, FallbackEdgeGroupID: record.FallbackEdgeGroupID})
	}
	for _, record := range records {
		host := normalizeExternalAppDomain(record.FQDN)
		zone, exists := byID[record.ZoneID]
		reason := ""
		switch {
		case !exists:
			reason = "dns_zone_missing"
		case zone.Status == model.HostedZoneStatusDeleted:
			reason = "dns_zone_deleted"
		case zone.Status == model.HostedZoneStatusSuspended:
			reason = "dns_zone_suspended"
		case record.Status == model.DNSRecordStatusDisabled || record.Status == model.DNSRecordStatusConflict:
			reason = "dns_record_inactive"
		case record.TenantID != zone.TenantID:
			reason = "dns_zone_owner_mismatch"
		case host == "" || (host != zone.ZoneName && !strings.HasSuffix(host, "."+zone.ZoneName)):
			reason = "dns_record_outside_zone"
		}
		if reason != "" {
			exclusions = append(exclusions, platformDNSExclusion{RecordID: record.ID, ZoneID: record.ZoneID, Hostname: host, Reason: reason})
			if reason != "dns_zone_deleted" && reason != "dns_zone_suspended" && reason != "dns_record_inactive" {
				issue(reason, host)
			}
			continue
		}
		desired := platformconfig.DNSIntent{Hostname: host, Type: record.Type, Values: append([]string(nil), record.Values...), TTL: record.TTL, RecordKind: model.EdgeDNSRecordKindHosted, TenantID: record.TenantID}
		if record.Type == model.DNSRecordTypeFUGUEAPP {
			if app, ok := resolveDNSIntentApp(record, apps); ok {
				desired.AppID = app.ID
			} else {
				issue("dns_app_reference_unresolved", host)
			}
			issue("dns_app_placement_not_projected", host)
		}
		if hostedDNSRecordNeedsFlatten(record) {
			desired.Flatten = &platformconfig.DNSFlattenIntent{Mode: record.FlattenMode, Target: record.FlattenTarget, IPv4Policy: record.FlattenIPv4Policy, IPv6Policy: record.FlattenIPv6Policy, TTLPolicy: record.FlattenTTLPolicy, FallbackPolicy: record.FlattenFallbackPolicy}
			issue("dns_flatten_not_projected", host)
		}
		// Never copy flattened answers, resolver status or LastPublishedAt into
		// desired configuration. They require separately captured runtime facts.
		out = append(out, desired)
	}
	sort.Slice(exclusions, func(i, j int) bool { return exclusions[i].RecordID < exclusions[j].RecordID })
	return platformconfig.NormalizePlatformIntent(platformconfig.PlatformIntent{DNS: out}).DNS, exclusions
}

// Names are accepted by the legacy UI, but ambiguous names must not pick a
// random app from map iteration. Persist the unique application identity.
func resolveDNSIntentApp(record model.DNSRecord, apps map[string]model.App) (model.App, bool) {
	if len(record.Values) != 1 {
		return model.App{}, false
	}
	value := strings.TrimSpace(record.Values[0])
	if app, ok := apps[value]; ok {
		return app, app.TenantID == record.TenantID
	}
	var found model.App
	for _, app := range apps {
		if app.TenantID != record.TenantID || !strings.EqualFold(app.Name, value) {
			continue
		}
		if found.ID != "" {
			return model.App{}, false
		}
		found = app
	}
	return found, found.ID != ""
}
