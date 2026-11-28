package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"net"
	"sort"
	"strconv"
	"strings"

	"fugue/internal/model"
)

const (
	defaultHostedDNSTTL = 300
	minHostedDNSTTL     = 30
	maxHostedDNSTTL     = 86400
)

func (s *Store) ListHostedZones(tenantID string, platformAdmin bool) ([]model.HostedZone, error) {
	tenantID = strings.TrimSpace(tenantID)
	return s.pgListHostedZones(tenantID, platformAdmin)
}

func (s *Store) GetHostedZoneByName(zoneName string) (model.HostedZone, error) {
	zoneName = normalizeDNSZone(zoneName)
	if zoneName == "" {
		return model.HostedZone{}, ErrInvalidInput
	}
	return s.pgGetHostedZoneByName(zoneName)
}

// PutHostedZone registers a new zone. Repeated registration cannot update its owner or state.
func (s *Store) PutHostedZone(zone model.HostedZone, actors ...model.Principal) (model.HostedZone, error) {
	if zone.ID == "" {
		zone.ID = model.NewSecret("dnszone")
	}
	zone, err := normalizeHostedZoneForStore(zone)
	if err != nil {
		return model.HostedZone{}, err
	}
	return s.pgPutHostedZone(zone, actors)
}

// UpdateHostedZoneStatus only updates the live instance observed by the caller.
func (s *Store) UpdateHostedZoneStatus(zone model.HostedZone) (model.HostedZone, error) {
	return s.pgUpdateHostedZoneStatus(zone)
}

func (s *Store) DeleteHostedZone(zone model.HostedZone, actors ...model.Principal) (model.HostedZone, error) {
	zoneName := normalizeDNSZone(zone.ZoneName)
	if zoneName == "" {
		return model.HostedZone{}, ErrInvalidInput
	}
	return s.pgDeleteHostedZone(zone, actors)
}

func (s *Store) FindHostedZoneForHostname(tenantID, hostname string, platformAdmin bool) (model.HostedZone, error) {
	hostname = normalizeDNSZone(hostname)
	if hostname == "" {
		return model.HostedZone{}, ErrInvalidInput
	}
	zones, err := s.ListHostedZones(tenantID, platformAdmin)
	if err != nil {
		return model.HostedZone{}, err
	}
	var match model.HostedZone
	for _, zone := range zones {
		if zone.Status == model.HostedZoneStatusDeleted {
			continue
		}
		if hostname == zone.ZoneName || strings.HasSuffix(hostname, "."+zone.ZoneName) {
			if match.ZoneName == "" || len(zone.ZoneName) > len(match.ZoneName) {
				match = zone
			}
		}
	}
	if match.ZoneName == "" {
		return model.HostedZone{}, ErrNotFound
	}
	return match, nil
}

func (s *Store) ListDNSRecords(zoneID string) ([]model.DNSRecord, error) {
	zoneID = strings.TrimSpace(zoneID)
	if zoneID == "" {
		return nil, ErrInvalidInput
	}
	return s.pgListDNSRecords(zoneID)
}

func (s *Store) GetDNSRecord(zoneID, recordID string) (model.DNSRecord, error) {
	zoneID = strings.TrimSpace(zoneID)
	recordID = strings.TrimSpace(recordID)
	if zoneID == "" || recordID == "" {
		return model.DNSRecord{}, ErrInvalidInput
	}
	return s.pgGetDNSRecord(zoneID, recordID)
}

func (s *Store) PutDNSRecord(zone model.HostedZone, record model.DNSRecord, overwrite bool) (model.DNSRecord, error) {
	zone = normalizeHostedZoneForRead(zone)
	record, err := normalizeDNSRecordForStore(zone, record)
	if err != nil {
		return model.DNSRecord{}, err
	}
	return s.pgPutDNSRecord(zone, record, overwrite)
}

func (s *Store) PatchDNSRecord(zone model.HostedZone, recordID string, patch model.DNSRecordPatch, overwrite bool) (model.DNSRecord, error) {
	zone = normalizeHostedZoneForRead(zone)
	recordID = strings.TrimSpace(recordID)
	if zone.ID == "" || recordID == "" {
		return model.DNSRecord{}, ErrInvalidInput
	}
	return s.pgWriteDNSRecord(zone, overwrite, func(ctx context.Context, tx *sql.Tx) (model.DNSRecord, error) {
		current, err := scanDNSRecord(tx.QueryRowContext(ctx, dnsRecordSelect()+`
WHERE zone_id=$1 AND tenant_id=$2 AND id=$3 FOR UPDATE
`, zone.ID, zone.TenantID, recordID))
		if err != nil {
			return model.DNSRecord{}, mapDBErr(err)
		}
		return normalizeDNSRecordForStore(zone, applyHostedDNSRecordPatch(current, patch))
	})
}

func applyHostedDNSRecordPatch(current model.DNSRecord, patch model.DNSRecordPatch) model.DNSRecord {
	if patch.Values != nil {
		current.Values = append([]string(nil), (*patch.Values)...)
	}
	if patch.TTL != nil {
		current.TTL = *patch.TTL
	}
	if patch.FlattenMode != nil {
		current.FlattenMode = *patch.FlattenMode
	}
	if patch.FlattenTarget != nil {
		current.FlattenTarget = *patch.FlattenTarget
	}
	if patch.FlattenIPv4Policy != nil {
		current.FlattenIPv4Policy = *patch.FlattenIPv4Policy
	}
	if patch.FlattenIPv6Policy != nil {
		current.FlattenIPv6Policy = *patch.FlattenIPv6Policy
	}
	if patch.FlattenTTLPolicy != nil {
		current.FlattenTTLPolicy = *patch.FlattenTTLPolicy
	}
	if patch.FlattenFallbackPolicy != nil {
		current.FlattenFallbackPolicy = *patch.FlattenFallbackPolicy
	}
	if patch.Status != nil {
		current.Status = *patch.Status
	}
	return current
}

func (s *Store) DeleteDNSRecord(zoneID, recordID string) (model.DNSRecord, error) {
	zoneID = strings.TrimSpace(zoneID)
	recordID = strings.TrimSpace(recordID)
	if zoneID == "" || recordID == "" {
		return model.DNSRecord{}, ErrInvalidInput
	}
	return s.pgDeleteDNSRecord(zoneID, recordID)
}

func (s *Store) DeleteDNSRecordsBySourceRef(zoneID, source, sourceRefType, sourceRefID string) ([]model.DNSRecord, error) {
	zoneID = strings.TrimSpace(zoneID)
	source = model.NormalizeDNSRecordSource(source)
	sourceRefType = strings.TrimSpace(sourceRefType)
	sourceRefID = strings.TrimSpace(sourceRefID)
	if zoneID == "" || source == "" || sourceRefID == "" {
		return nil, ErrInvalidInput
	}
	return s.pgDeleteDNSRecordsBySourceRef(zoneID, source, sourceRefType, sourceRefID)
}

func normalizeHostedZoneForStore(zone model.HostedZone) (model.HostedZone, error) {
	zone.ID = strings.TrimSpace(zone.ID)
	zone.TenantID = strings.TrimSpace(zone.TenantID)
	zone.ProjectID = strings.TrimSpace(zone.ProjectID)
	zone.ZoneName = normalizeDNSZone(zone.ZoneName)
	zone.Status = model.NormalizeHostedZoneStatus(zone.Status)
	zone.DelegationStatus = model.NormalizeHostedZoneDelegationStatus(zone.DelegationStatus)
	zone.ParentNameservers = normalizeNameserverList(zone.ParentNameservers)
	zone.ExpectedNameservers = normalizeNameserverList(zone.ExpectedNameservers)
	zone.CreatedBy = strings.TrimSpace(zone.CreatedBy)
	zone.LastMessage = strings.TrimSpace(zone.LastMessage)
	if zone.ZoneName == "" || zone.TenantID == "" || zone.Status == "" || zone.DelegationStatus == "" {
		return model.HostedZone{}, ErrInvalidInput
	}
	if zone.ID == "" {
		return model.HostedZone{}, ErrInvalidInput
	}
	return zone, nil
}

func normalizeHostedZoneForRead(zone model.HostedZone) model.HostedZone {
	zone.ID = strings.TrimSpace(zone.ID)
	zone.TenantID = strings.TrimSpace(zone.TenantID)
	zone.ProjectID = strings.TrimSpace(zone.ProjectID)
	zone.ZoneName = normalizeDNSZone(zone.ZoneName)
	zone.Status = model.NormalizeHostedZoneStatus(zone.Status)
	if zone.Status == "" {
		zone.Status = model.HostedZoneStatusPendingDelegation
	}
	zone.DelegationStatus = model.NormalizeHostedZoneDelegationStatus(zone.DelegationStatus)
	if zone.DelegationStatus == "" {
		zone.DelegationStatus = model.HostedZoneDelegationStatusPending
	}
	zone.ParentNameservers = normalizeNameserverList(zone.ParentNameservers)
	zone.ExpectedNameservers = normalizeNameserverList(zone.ExpectedNameservers)
	zone.CreatedBy = strings.TrimSpace(zone.CreatedBy)
	zone.LastMessage = strings.TrimSpace(zone.LastMessage)
	return zone
}

func normalizeDNSRecordForStore(zone model.HostedZone, record model.DNSRecord) (model.DNSRecord, error) {
	zone = normalizeHostedZoneForRead(zone)
	record.ID = strings.TrimSpace(record.ID)
	record.ZoneID = strings.TrimSpace(firstNonEmpty(record.ZoneID, zone.ID))
	record.TenantID = strings.TrimSpace(firstNonEmpty(record.TenantID, zone.TenantID))
	record.Name = normalizeHostedDNSRecordName(record.Name, zone.ZoneName)
	record.FQDN = hostedDNSRecordFQDN(zone.ZoneName, record.Name)
	record.Type = model.NormalizeDNSRecordType(record.Type)
	record.TTL = normalizeHostedDNSTTL(record.TTL)
	record.FlattenMode = model.NormalizeDNSRecordFlattenMode(record.FlattenMode)
	record.FlattenTarget = normalizeDNSZone(record.FlattenTarget)
	record.FlattenIPv4Policy = model.NormalizeDNSRecordFlattenIPPolicy(record.FlattenIPv4Policy)
	record.FlattenIPv6Policy = model.NormalizeDNSRecordFlattenIPPolicy(record.FlattenIPv6Policy)
	record.FlattenTTLPolicy = model.NormalizeDNSRecordFlattenTTLPolicy(record.FlattenTTLPolicy)
	record.FlattenFallbackPolicy = model.NormalizeDNSRecordFlattenFallbackPolicy(record.FlattenFallbackPolicy)
	record.FlattenStatus = model.NormalizeDNSRecordFlattenStatus(record.FlattenStatus)
	record.FlattenedA = normalizeHostedDNSIPs(record.FlattenedA, true)
	record.FlattenedAAAA = normalizeHostedDNSIPs(record.FlattenedAAAA, false)
	record.ResolveError = strings.TrimSpace(record.ResolveError)
	record.Source = model.NormalizeDNSRecordSource(record.Source)
	record.SourceRefType = strings.TrimSpace(record.SourceRefType)
	record.SourceRefID = strings.TrimSpace(record.SourceRefID)
	record.Status = model.NormalizeDNSRecordStatus(record.Status)
	record.CreatedBy = strings.TrimSpace(record.CreatedBy)
	record.LastMessage = strings.TrimSpace(record.LastMessage)
	if record.ZoneID == "" || record.TenantID == "" || record.Name == "" || record.FQDN == "" || record.Type == "" || record.Source == "" || record.Status == "" {
		return model.DNSRecord{}, ErrInvalidInput
	}
	if record.Source == model.DNSRecordSourceAppDomain && (record.SourceRefType == "" || record.SourceRefID == "") {
		return model.DNSRecord{}, ErrInvalidInput
	}
	if record.FlattenMode == "" || record.FlattenIPv4Policy == "" || record.FlattenIPv6Policy == "" || record.FlattenTTLPolicy == "" || record.FlattenFallbackPolicy == "" || record.FlattenStatus == "" {
		return model.DNSRecord{}, ErrInvalidInput
	}
	record.Values = normalizeHostedDNSValues(record)
	switch record.Type {
	case model.DNSRecordTypeCNAME:
		if record.FlattenMode != model.DNSRecordFlattenModeNone && record.FlattenTarget == "" && len(record.Values) == 1 {
			record.FlattenTarget = record.Values[0]
		}
	case model.DNSRecordTypeALIAS, model.DNSRecordTypeANAME:
		if record.FlattenTarget == "" && len(record.Values) == 1 {
			record.FlattenTarget = record.Values[0]
		}
		if record.FlattenMode == model.DNSRecordFlattenModeNone {
			record.FlattenMode = model.DNSRecordFlattenModeAlways
		}
	case model.DNSRecordTypeFUGUEAPP:
		record.FlattenMode = model.DNSRecordFlattenModeApp
	}
	if err := validateHostedDNSRecordSemantics(zone, record); err != nil {
		return model.DNSRecord{}, err
	}
	if record.ID == "" {
		record.ID = dnsRecordID(record.ZoneID, record.FQDN, record.Type, record.Source, record.SourceRefType, record.SourceRefID)
	}
	return record, nil
}

func normalizeDNSRecordForRead(record model.DNSRecord) model.DNSRecord {
	record.ID = strings.TrimSpace(record.ID)
	record.ZoneID = strings.TrimSpace(record.ZoneID)
	record.TenantID = strings.TrimSpace(record.TenantID)
	record.Name = strings.TrimSpace(record.Name)
	record.FQDN = normalizeDNSZone(record.FQDN)
	record.Type = model.NormalizeDNSRecordType(record.Type)
	record.TTL = normalizeHostedDNSTTL(record.TTL)
	record.FlattenMode = model.NormalizeDNSRecordFlattenMode(record.FlattenMode)
	if record.FlattenMode == "" {
		record.FlattenMode = model.DNSRecordFlattenModeNone
	}
	record.FlattenTarget = normalizeDNSZone(record.FlattenTarget)
	record.FlattenIPv4Policy = model.NormalizeDNSRecordFlattenIPPolicy(record.FlattenIPv4Policy)
	record.FlattenIPv6Policy = model.NormalizeDNSRecordFlattenIPPolicy(record.FlattenIPv6Policy)
	record.FlattenTTLPolicy = model.NormalizeDNSRecordFlattenTTLPolicy(record.FlattenTTLPolicy)
	record.FlattenFallbackPolicy = model.NormalizeDNSRecordFlattenFallbackPolicy(record.FlattenFallbackPolicy)
	record.FlattenStatus = model.NormalizeDNSRecordFlattenStatus(record.FlattenStatus)
	record.FlattenedA = normalizeHostedDNSIPs(record.FlattenedA, true)
	record.FlattenedAAAA = normalizeHostedDNSIPs(record.FlattenedAAAA, false)
	record.ResolveError = strings.TrimSpace(record.ResolveError)
	record.Source = model.NormalizeDNSRecordSource(record.Source)
	record.SourceRefType = strings.TrimSpace(record.SourceRefType)
	record.SourceRefID = strings.TrimSpace(record.SourceRefID)
	record.Status = model.NormalizeDNSRecordStatus(record.Status)
	record.CreatedBy = strings.TrimSpace(record.CreatedBy)
	record.LastMessage = strings.TrimSpace(record.LastMessage)
	record.Values = normalizeHostedDNSValues(record)
	return record
}

func normalizeHostedDNSRecordName(name, zoneName string) string {
	name = strings.Trim(strings.TrimSpace(strings.ToLower(name)), ".")
	zoneName = normalizeDNSZone(zoneName)
	switch name {
	case "", "@", zoneName:
		return "@"
	}
	if strings.HasSuffix(name, "."+zoneName) {
		name = strings.TrimSuffix(name, "."+zoneName)
	}
	if name == "*" || strings.HasPrefix(name, "*.") {
		if name == "*" {
			return "*"
		}
	}
	return name
}

func hostedDNSRecordFQDN(zoneName, name string) string {
	zoneName = normalizeDNSZone(zoneName)
	name = normalizeHostedDNSRecordName(name, zoneName)
	if zoneName == "" || name == "" {
		return ""
	}
	if name == "@" {
		return zoneName
	}
	return normalizeDNSZone(name + "." + zoneName)
}

func normalizeHostedDNSTTL(ttl int) int {
	if ttl <= 0 {
		return defaultHostedDNSTTL
	}
	if ttl < minHostedDNSTTL {
		return minHostedDNSTTL
	}
	if ttl > maxHostedDNSTTL {
		return maxHostedDNSTTL
	}
	return ttl
}

func normalizeHostedDNSValues(record model.DNSRecord) []string {
	values := []string{}
	for _, value := range record.Values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		switch record.Type {
		case model.DNSRecordTypeA:
			if ip := net.ParseIP(value); ip != nil && ip.To4() != nil {
				value = ip.To4().String()
			} else {
				continue
			}
		case model.DNSRecordTypeAAAA:
			if ip := net.ParseIP(value); ip != nil && ip.To4() == nil {
				value = ip.String()
			} else {
				continue
			}
		case model.DNSRecordTypeCNAME, model.DNSRecordTypeNS, model.DNSRecordTypeALIAS, model.DNSRecordTypeANAME:
			value = normalizeDNSZone(value)
			if value == "" {
				continue
			}
		case model.DNSRecordTypeFUGUEAPP:
			value = strings.TrimSpace(value)
		}
		if !storeStringSliceContains(values, value) {
			values = append(values, value)
		}
	}
	sort.Strings(values)
	return values
}

func normalizeHostedDNSIPs(raw []string, wantIPv4 bool) []string {
	out := []string{}
	for _, value := range raw {
		ip := net.ParseIP(strings.TrimSpace(value))
		if ip == nil {
			continue
		}
		if wantIPv4 {
			if v4 := ip.To4(); v4 != nil {
				value = v4.String()
			} else {
				continue
			}
		} else if ip.To4() == nil {
			value = ip.String()
		} else {
			continue
		}
		if !storeStringSliceContains(out, value) {
			out = append(out, value)
		}
	}
	sort.Strings(out)
	return out
}

func validateHostedDNSRecordSemantics(zone model.HostedZone, record model.DNSRecord) error {
	if !edgeDNSTargetWithinStoreZone(record.FQDN, zone.ZoneName) {
		return ErrInvalidInput
	}
	switch record.Type {
	case model.DNSRecordTypeA, model.DNSRecordTypeAAAA, model.DNSRecordTypeCAA, model.DNSRecordTypeMX, model.DNSRecordTypeNS, model.DNSRecordTypeSRV, model.DNSRecordTypeTXT:
		if len(record.Values) == 0 {
			return ErrInvalidInput
		}
		if record.FlattenMode != model.DNSRecordFlattenModeNone {
			return ErrInvalidInput
		}
		if record.Type == model.DNSRecordTypeSRV {
			for _, value := range record.Values {
				if !parseSRVValue(value) {
					return ErrInvalidInput
				}
			}
		}
	case model.DNSRecordTypeCNAME:
		if len(record.Values) != 1 {
			return ErrInvalidInput
		}
		if record.FQDN == zone.ZoneName && record.FlattenMode == model.DNSRecordFlattenModeNone {
			return ErrInvalidInput
		}
		if record.FlattenMode != model.DNSRecordFlattenModeNone && record.FlattenTarget == "" {
			return ErrInvalidInput
		}
	case model.DNSRecordTypeALIAS, model.DNSRecordTypeANAME:
		if len(record.Values) != 1 {
			return ErrInvalidInput
		}
		if record.FlattenTarget == "" {
			return ErrInvalidInput
		}
	case model.DNSRecordTypeFUGUEAPP:
		if len(record.Values) != 1 {
			return ErrInvalidInput
		}
		if record.FlattenMode != model.DNSRecordFlattenModeApp {
			return ErrInvalidInput
		}
	default:
		return ErrInvalidInput
	}
	return nil
}

func validateDNSRecordConflicts(record model.DNSRecord, existing []model.DNSRecord, overwrite bool) error {
	for _, other := range existing {
		if other.ID == record.ID {
			continue
		}
		if !strings.EqualFold(other.FQDN, record.FQDN) {
			continue
		}
		if other.Source == model.DNSRecordSourceSystem {
			return ErrConflict
		}
		if strings.EqualFold(other.Type, record.Type) {
			if overwrite {
				continue
			}
			return ErrConflict
		}
		if record.Type == model.DNSRecordTypeCNAME && record.FlattenMode == model.DNSRecordFlattenModeNone {
			return ErrConflict
		}
		if other.Type == model.DNSRecordTypeCNAME && other.FlattenMode == model.DNSRecordFlattenModeNone {
			return ErrConflict
		}
		if hostedDNSRecordPublishesApexAddress(record) && (other.Type == model.DNSRecordTypeA || other.Type == model.DNSRecordTypeAAAA || hostedDNSRecordPublishesApexAddress(other)) {
			return ErrConflict
		}
	}
	return nil
}

func hostedDNSRecordPublishesApexAddress(record model.DNSRecord) bool {
	switch record.Type {
	case model.DNSRecordTypeALIAS, model.DNSRecordTypeANAME, model.DNSRecordTypeFUGUEAPP:
		return true
	case model.DNSRecordTypeCNAME:
		return record.FlattenMode != model.DNSRecordFlattenModeNone
	default:
		return false
	}
}

func dnsRecordID(zoneID, fqdn, recordType, source, sourceRefType, sourceRefID string) string {
	material := strings.Join([]string{
		strings.TrimSpace(zoneID),
		normalizeDNSZone(fqdn),
		model.NormalizeDNSRecordType(recordType),
		model.NormalizeDNSRecordSource(source),
		strings.TrimSpace(sourceRefType),
		strings.TrimSpace(sourceRefID),
	}, "\x00")
	return "dnsrec_" + shortHostedDNSHash(material)
}

func shortHostedDNSHash(material string) string {
	sum := sha256.Sum256([]byte(material))
	return hex.EncodeToString(sum[:])[:20]
}

func normalizeNameserverList(values []string) []string {
	out := []string{}
	for _, value := range values {
		value = normalizeDNSZone(value)
		if value == "" {
			continue
		}
		if !storeStringSliceContains(out, value) {
			out = append(out, value)
		}
	}
	sort.Strings(out)
	return out
}

func parseSRVValue(value string) bool {
	fields := strings.Fields(strings.TrimSpace(value))
	if len(fields) != 4 {
		return false
	}
	for i := 0; i < 3; i++ {
		if _, err := strconv.ParseUint(fields[i], 10, 16); err != nil {
			return false
		}
	}
	return normalizeDNSZone(fields[3]) != ""
}

func storeStringSliceContains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
