package api

import (
	"fmt"
	"sort"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

// This migration projection preserves the legacy explicit-platform-entry
// priority over static addresses. It never copies a selected address or health.
func projectPlatformEntryDNS(result *platformIntentProjectionResponse, entries []model.PlatformRoute, static []model.EdgeDNSRecord, zones []string) error {
	configured := map[string]model.PlatformRoute{}
	for _, entry := range entries {
		host := normalizeExternalAppDomain(entry.Hostname)
		managed := false
		for _, zone := range zones {
			managed = managed || edgeDNSTargetWithinZone(host, zone)
		}
		if !managed {
			continue
		}
		if _, duplicate := configured[host]; duplicate {
			return fmt.Errorf("duplicate platform DNS entry")
		}
		configured[host] = entry
	}
	staticRecords, _ := projectBusinessDNSDraft(&platformIntentProjectionResponse{}, nil, nil, nil, static)
	staticAddress := map[string]int{}
	for _, source := range staticRecords {
		if source.Type == "A" || source.Type == "AAAA" || source.Type == "CNAME" {
			digest, err := platformconfig.Digest(source)
			if err != nil {
				return err
			}
			staticAddress[digest]++
		}
	}
	remaining := make([]platformconfig.DNSIntent, 0, len(result.Intent.DNS)+len(configured))
	for _, record := range result.Intent.DNS {
		_, replaced := configured[record.Hostname]
		if replaced && (record.Type == "A" || record.Type == "AAAA" || record.Type == "CNAME") {
			digest, err := platformconfig.Digest(record)
			if err != nil {
				return err
			}
			if staticAddress[digest] > 0 {
				staticAddress[digest]--
				result.DNSExclusions = append(result.DNSExclusions, platformDNSExclusion{RecordID: "static:" + digest, Hostname: record.Hostname, Reason: "static_address_replaced_by_platform_entry"})
				continue
			}
			result.Issues = append(result.Issues, platformProjectionIssue{Code: "platform_dns_conflicting_business_record", Hostname: record.Hostname})
		}
		remaining = append(remaining, record)
	}
	for host, entry := range configured {
		exists := false
		for _, route := range result.Intent.Routes {
			if route.Hostname != host {
				continue
			}
			if route.AppID != "" || route.TenantID != "" {
				return fmt.Errorf("platform DNS entry has a business route owner")
			}
			exists = true
		}
		if !exists {
			return fmt.Errorf("platform DNS entry has no route")
		}
		desired := platformconfig.DNSIntent{Hostname: host, Type: "FUGUE_ROUTE", Values: []string{}, TTL: edgeDNSPolicyTTL(entry.TTL), RecordKind: model.EdgeDNSRecordKindPlatformRoute, Status: entry.Status, Route: &platformconfig.DNSRouteIntent{Hostnames: []string{host}, DNSApplicationIntent: platformconfig.DNSApplicationIntent{IPv4Policy: "auto", IPv6Policy: "auto", TTLPolicy: "record", FallbackPolicy: "fail_closed"}}}
		if entry.EdgeGroupMode == model.PlatformRouteEdgeGroupModePinned {
			desired.EdgeGroupID = entry.EdgeGroupID
		}
		remaining = append(remaining, desired)
	}
	result.Intent.DNS = platformconfig.NormalizePlatformIntent(platformconfig.PlatformIntent{DNS: remaining}).DNS
	result.Intent.Generation = ""
	gen, err := platformconfig.PlatformIntentGeneration(result.Intent)
	if err != nil {
		return err
	}
	result.Intent.Generation, result.RuntimeSnapshot.IntentGeneration = gen, gen
	if err := platformconfig.ValidatePlatformIntent(result.Intent); err != nil {
		result.Issues = append(result.Issues, platformProjectionIssue{Code: "intent_requires_validation_repair"})
	}
	sort.Slice(result.DNSExclusions, func(i, j int) bool { return result.DNSExclusions[i].RecordID < result.DNSExclusions[j].RecordID })
	return nil
}
