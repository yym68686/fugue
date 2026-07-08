package store

import (
	"crypto/sha256"
	"encoding/hex"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/model"
)

const (
	defaultHostedDNSTTL = 300
	minHostedDNSTTL     = 30
	maxHostedDNSTTL     = 86400
)

func (s *Store) ListHostedZones(tenantID string, platformAdmin bool) ([]model.HostedZone, error) {
	tenantID = strings.TrimSpace(tenantID)
	if s.usingDatabase() {
		return s.pgListHostedZones(tenantID, platformAdmin)
	}
	zones := []model.HostedZone{}
	err := s.withLockedState(false, func(state *model.State) error {
		for _, zone := range state.HostedZones {
			zone = normalizeHostedZoneForRead(zone)
			if zone.Status == model.HostedZoneStatusDeleted {
				continue
			}
			if !platformAdmin && tenantID != "" && zone.TenantID != tenantID {
				continue
			}
			zones = append(zones, zone)
		}
		sortHostedZones(zones)
		return nil
	})
	return zones, err
}

func (s *Store) GetHostedZoneByName(zoneName string) (model.HostedZone, error) {
	zoneName = normalizeDNSZone(zoneName)
	if zoneName == "" {
		return model.HostedZone{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgGetHostedZoneByName(zoneName)
	}
	var zone model.HostedZone
	err := s.withLockedState(false, func(state *model.State) error {
		index := findHostedZoneByName(state, zoneName)
		if index < 0 {
			return ErrNotFound
		}
		zone = normalizeHostedZoneForRead(state.HostedZones[index])
		if zone.Status == model.HostedZoneStatusDeleted {
			return ErrNotFound
		}
		return nil
	})
	return zone, err
}

func (s *Store) PutHostedZone(zone model.HostedZone) (model.HostedZone, error) {
	zone, err := normalizeHostedZoneForStore(zone)
	if err != nil {
		return model.HostedZone{}, err
	}
	if s.usingDatabase() {
		return s.pgPutHostedZone(zone)
	}
	var out model.HostedZone
	err = s.withLockedState(true, func(state *model.State) error {
		now := time.Now().UTC()
		index := findHostedZoneByName(state, zone.ZoneName)
		if index >= 0 {
			existing := normalizeHostedZoneForRead(state.HostedZones[index])
			if existing.ID != zone.ID && zone.ID != "" {
				return ErrConflict
			}
			if zone.ID == "" {
				zone.ID = existing.ID
			}
			if zone.CreatedAt.IsZero() {
				zone.CreatedAt = existing.CreatedAt
			}
			if zone.CreatedBy == "" {
				zone.CreatedBy = existing.CreatedBy
			}
		} else {
			if zone.ID == "" {
				zone.ID = model.NewID("dnszone")
			}
			if zone.CreatedAt.IsZero() {
				zone.CreatedAt = now
			}
		}
		zone.UpdatedAt = now
		if index >= 0 {
			state.HostedZones[index] = zone
		} else {
			state.HostedZones = append(state.HostedZones, zone)
		}
		out = normalizeHostedZoneForRead(zone)
		return nil
	})
	return out, err
}

func (s *Store) DeleteHostedZone(zoneName string) (model.HostedZone, error) {
	zoneName = normalizeDNSZone(zoneName)
	if zoneName == "" {
		return model.HostedZone{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgDeleteHostedZone(zoneName)
	}
	var removed model.HostedZone
	err := s.withLockedState(true, func(state *model.State) error {
		index := findHostedZoneByName(state, zoneName)
		if index < 0 {
			return ErrNotFound
		}
		removed = normalizeHostedZoneForRead(state.HostedZones[index])
		if removed.Status == model.HostedZoneStatusDeleted {
			return ErrNotFound
		}
		state.HostedZones[index].Status = model.HostedZoneStatusDeleted
		state.HostedZones[index].UpdatedAt = time.Now().UTC()
		return nil
	})
	return removed, err
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
	if s.usingDatabase() {
		return s.pgListDNSRecords(zoneID)
	}
	records := []model.DNSRecord{}
	err := s.withLockedState(false, func(state *model.State) error {
		if findHostedZoneByID(state, zoneID) < 0 {
			return ErrNotFound
		}
		for _, record := range state.DNSRecords {
			record = normalizeDNSRecordForRead(record)
			if record.ZoneID != zoneID || record.Status == model.DNSRecordStatusDisabled {
				continue
			}
			records = append(records, record)
		}
		sortDNSRecords(records)
		return nil
	})
	return records, err
}

func (s *Store) GetDNSRecord(zoneID, recordID string) (model.DNSRecord, error) {
	zoneID = strings.TrimSpace(zoneID)
	recordID = strings.TrimSpace(recordID)
	if zoneID == "" || recordID == "" {
		return model.DNSRecord{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgGetDNSRecord(zoneID, recordID)
	}
	var record model.DNSRecord
	err := s.withLockedState(false, func(state *model.State) error {
		index := findDNSRecord(state, zoneID, recordID)
		if index < 0 {
			return ErrNotFound
		}
		record = normalizeDNSRecordForRead(state.DNSRecords[index])
		return nil
	})
	return record, err
}

func (s *Store) PutDNSRecord(zone model.HostedZone, record model.DNSRecord, overwrite bool) (model.DNSRecord, error) {
	zone = normalizeHostedZoneForRead(zone)
	record, err := normalizeDNSRecordForStore(zone, record)
	if err != nil {
		return model.DNSRecord{}, err
	}
	if s.usingDatabase() {
		return s.pgPutDNSRecord(zone, record, overwrite)
	}
	var out model.DNSRecord
	err = s.withLockedState(true, func(state *model.State) error {
		zoneIndex := findHostedZoneByID(state, zone.ID)
		if zoneIndex < 0 {
			return ErrNotFound
		}
		existingRecords := make([]model.DNSRecord, 0, len(state.DNSRecords))
		for _, existing := range state.DNSRecords {
			existing = normalizeDNSRecordForRead(existing)
			if existing.ZoneID == zone.ID && existing.Status != model.DNSRecordStatusDisabled {
				existingRecords = append(existingRecords, existing)
			}
		}
		index := findDNSRecord(state, zone.ID, record.ID)
		if index >= 0 && record.CreatedAt.IsZero() {
			record.CreatedAt = state.DNSRecords[index].CreatedAt
		}
		if err := validateDNSRecordConflicts(record, existingRecords, overwrite); err != nil {
			return err
		}
		now := time.Now().UTC()
		if record.ID == "" {
			record.ID = dnsRecordID(zone.ID, record.FQDN, record.Type, record.Source, record.SourceRefType, record.SourceRefID)
		}
		if record.CreatedAt.IsZero() {
			record.CreatedAt = now
		}
		record.UpdatedAt = now
		index = findDNSRecord(state, zone.ID, record.ID)
		if index >= 0 {
			state.DNSRecords[index] = record
		} else {
			state.DNSRecords = append(state.DNSRecords, record)
		}
		out = normalizeDNSRecordForRead(record)
		return nil
	})
	return out, err
}

func (s *Store) PatchDNSRecord(zone model.HostedZone, recordID string, patch model.DNSRecordPatch, overwrite bool) (model.DNSRecord, error) {
	current, err := s.GetDNSRecord(zone.ID, recordID)
	if err != nil {
		return model.DNSRecord{}, err
	}
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
	return s.PutDNSRecord(zone, current, overwrite)
}

func (s *Store) DeleteDNSRecord(zoneID, recordID string) (model.DNSRecord, error) {
	zoneID = strings.TrimSpace(zoneID)
	recordID = strings.TrimSpace(recordID)
	if zoneID == "" || recordID == "" {
		return model.DNSRecord{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgDeleteDNSRecord(zoneID, recordID)
	}
	var removed model.DNSRecord
	err := s.withLockedState(true, func(state *model.State) error {
		index := findDNSRecord(state, zoneID, recordID)
		if index < 0 {
			return ErrNotFound
		}
		removed = normalizeDNSRecordForRead(state.DNSRecords[index])
		state.DNSRecords = append(state.DNSRecords[:index], state.DNSRecords[index+1:]...)
		return nil
	})
	return removed, err
}

func (s *Store) DeleteDNSRecordsBySourceRef(zoneID, source, sourceRefType, sourceRefID string) ([]model.DNSRecord, error) {
	zoneID = strings.TrimSpace(zoneID)
	source = model.NormalizeDNSRecordSource(source)
	sourceRefType = strings.TrimSpace(sourceRefType)
	sourceRefID = strings.TrimSpace(sourceRefID)
	if zoneID == "" || source == "" || sourceRefID == "" {
		return nil, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgDeleteDNSRecordsBySourceRef(zoneID, source, sourceRefType, sourceRefID)
	}
	removed := []model.DNSRecord{}
	err := s.withLockedState(true, func(state *model.State) error {
		kept := state.DNSRecords[:0]
		for _, record := range state.DNSRecords {
			normalized := normalizeDNSRecordForRead(record)
			if normalized.ZoneID == zoneID &&
				normalized.Source == source &&
				normalized.SourceRefType == sourceRefType &&
				normalized.SourceRefID == sourceRefID {
				removed = append(removed, normalized)
				continue
			}
			kept = append(kept, record)
		}
		state.DNSRecords = kept
		return nil
	})
	return removed, err
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
		zone.ID = hostedZoneID(zone.ZoneName)
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

func findHostedZoneByName(state *model.State, zoneName string) int {
	if state == nil {
		return -1
	}
	zoneName = normalizeDNSZone(zoneName)
	for index, zone := range state.HostedZones {
		if normalizeDNSZone(zone.ZoneName) == zoneName {
			return index
		}
	}
	return -1
}

func findHostedZoneByID(state *model.State, id string) int {
	if state == nil {
		return -1
	}
	id = strings.TrimSpace(id)
	for index, zone := range state.HostedZones {
		if strings.TrimSpace(zone.ID) == id {
			return index
		}
	}
	return -1
}

func findDNSRecord(state *model.State, zoneID, recordID string) int {
	if state == nil {
		return -1
	}
	zoneID = strings.TrimSpace(zoneID)
	recordID = strings.TrimSpace(recordID)
	for index, record := range state.DNSRecords {
		if strings.TrimSpace(record.ZoneID) == zoneID && strings.TrimSpace(record.ID) == recordID {
			return index
		}
	}
	return -1
}

func hostedZoneID(zoneName string) string {
	return "dnszone_" + shortHostedDNSHash(normalizeDNSZone(zoneName))
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

func sortHostedZones(zones []model.HostedZone) {
	sort.Slice(zones, func(i, j int) bool {
		return zones[i].ZoneName < zones[j].ZoneName
	})
}

func sortDNSRecords(records []model.DNSRecord) {
	sort.Slice(records, func(i, j int) bool {
		if records[i].FQDN != records[j].FQDN {
			return records[i].FQDN < records[j].FQDN
		}
		if records[i].Type != records[j].Type {
			return records[i].Type < records[j].Type
		}
		return records[i].ID < records[j].ID
	})
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
