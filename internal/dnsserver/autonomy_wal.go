package dnsserver

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"time"

	"fugue/internal/localwal"
	"fugue/internal/model"
)

func (s *Service) recordDNSValueExpiryWAL(bundle *model.EdgeDNSBundle, index *dnsRecordIndex, name string, now time.Time) {
	if bundle == nil || strings.TrimSpace(s.Config.AutonomyWALPath) == "" {
		return
	}
	matching, _ := edgeDNSMatchingRecordsIndexed(bundle, name, index)
	for _, record := range matching {
		expired := 0
		for _, until := range record.ValueExpirations {
			if until.Sub(now) < time.Second {
				expired++
			}
		}
		if expired == 0 {
			continue
		}
		generation := edgeDNSCacheGeneration(*bundle)
		identity := fmt.Sprintf("%s|%s|%s|expired:%d", generation, record.Name, record.RecordGeneration, expired)
		if !s.reserveTemporaryFilterWAL(identity, now) {
			continue
		}
		sum := sha256.Sum256([]byte(identity))
		fact, err := localwal.NewRecord("dns-server", firstNonEmpty(s.Config.PhysicalNodeID, s.Config.DNSNodeID), "dns_value_expired", map[string]string{"record_name": record.Name, "record_generation": record.RecordGeneration, "expired_value_count": fmt.Sprint(expired), "evidence_digest": fmt.Sprintf("sha256:%x", sum)}, generation, nil, now)
		if err != nil {
			s.logAutonomyWALError("DNS value expiration", err)
			continue
		}
		fact.Subject = record.Name
		if err := localwal.Append(s.Config.AutonomyWALPath, fact); err != nil {
			s.logAutonomyWALError("DNS value expiration", err)
		}
	}
}

func (s *Service) recordDNSTemporaryFilterWAL(bundle *model.EdgeDNSBundle, queryName string, qtype uint16, audit edgeDNSAnswerAudit, filtered edgeDNSFilteredCandidate, now time.Time) {
	if s == nil {
		return
	}
	walPath := strings.TrimSpace(s.Config.AutonomyWALPath)
	if walPath == "" {
		return
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	candidate := filtered.Candidate
	reason := strings.TrimSpace(filtered.Reason)
	if reason == "" {
		reason = "temporary_filter"
	}
	generation := ""
	dnsNodeID := strings.TrimSpace(firstNonEmpty(s.Config.PhysicalNodeID, s.Config.DNSNodeID))
	edgeGroupID := strings.TrimSpace(s.Config.EdgeGroupID)
	zone := normalizeName(s.Config.Zone)
	if bundle != nil {
		generation = edgeDNSCacheGeneration(*bundle)
		dnsNodeID = strings.TrimSpace(firstNonEmpty(s.Config.PhysicalNodeID, bundle.DNSNodeID, s.Config.DNSNodeID))
		edgeGroupID = strings.TrimSpace(firstNonEmpty(bundle.EdgeGroupID, edgeGroupID))
		zone = normalizeName(firstNonEmpty(bundle.Zone, zone))
	}
	edgeID := strings.TrimSpace(candidate.EdgeID)
	edgeCandidateKey := firstNonEmpty(edgeID, candidate.EdgeGroupID, candidate.IP)
	if edgeCandidateKey == "" {
		edgeCandidateKey = "unknown"
	}
	dedupeKey := strings.Join([]string{
		generation,
		normalizeName(queryName),
		dnsTypeString(qtype),
		strings.TrimSpace(audit.RecordName),
		edgeCandidateKey,
		reason,
	}, "|")
	if !s.reserveTemporaryFilterWAL(dedupeKey, now) {
		return
	}
	expiresAt := now.Add(edgeHealthProbeTTL)
	record, err := localwal.NewRecord("dns-server", dnsNodeID, "temporary_filter", map[string]string{
		"zone":               zone,
		"edge_group_id":      edgeGroupID,
		"query_name":         normalizeName(queryName),
		"qtype":              dnsTypeString(qtype),
		"owner_name":         strings.TrimSpace(audit.OwnerName),
		"record_name":        strings.TrimSpace(audit.RecordName),
		"record_type":        strings.TrimSpace(audit.RecordType),
		"record_kind":        strings.TrimSpace(audit.RecordKind),
		"record_generation":  strings.TrimSpace(audit.RecordGeneration),
		"policy_kind":        strings.TrimSpace(audit.PolicyKind),
		"edge_id":            edgeID,
		"candidate_group_id": strings.TrimSpace(candidate.EdgeGroupID),
		"candidate_ip":       strings.TrimSpace(candidate.IP),
		"reason":             reason,
		"source":             "dns_answer_time_filter",
		"ttl_seconds":        fmt.Sprintf("%.0f", edgeHealthProbeTTL.Seconds()),
	}, generation, &expiresAt, now)
	if err != nil {
		s.logAutonomyWALError("dns temporary filter", err)
		return
	}
	record.Subject = firstNonEmpty(edgeID, strings.TrimSpace(candidate.EdgeGroupID), strings.TrimSpace(candidate.IP))
	record.SafetyClass = "L1_temporary_filter"
	if err := localwal.Append(walPath, record); err != nil {
		s.logAutonomyWALError("dns temporary filter", err)
	}
}

func (s *Service) reserveTemporaryFilterWAL(key string, now time.Time) bool {
	key = strings.TrimSpace(key)
	if key == "" {
		return false
	}
	s.walMu.Lock()
	defer s.walMu.Unlock()
	if s.walFilterLast == nil {
		s.walFilterLast = map[string]time.Time{}
	}
	if last, ok := s.walFilterLast[key]; ok && now.Sub(last) < dnsTemporaryFilterWALInterval {
		return false
	}
	for existing, last := range s.walFilterLast {
		if now.Sub(last) >= dnsTemporaryFilterWALInterval {
			delete(s.walFilterLast, existing)
		}
	}
	s.walFilterLast[key] = now
	return true
}

func (s *Service) logAutonomyWALError(action string, err error) {
	if s == nil || s.Logger == nil || err == nil {
		return
	}
	s.Logger.Printf("dns autonomy wal append failed; action=%s error=%s", strings.TrimSpace(action), s.redact(err.Error()))
}
