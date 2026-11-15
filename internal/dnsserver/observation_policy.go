package dnsserver

import (
	"errors"
	"net/netip"
	"reflect"
	"slices"
	"strings"
	"time"

	"fugue/internal/config"
	"fugue/internal/dnsfacts"
	"fugue/internal/dnsobservation"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"github.com/miekg/dns"
)

// VerifiedObservationPolicy exports only authenticated configured membership.
// It does not turn a candidate into a ready answer, renew a proof, select a
// release, or assert that the captured policy is still current after a cutover.
// The caller binds the checkpoint to the actual public Service/Pod via read-only
// Kubernetes observations; external observations remain independently acquired.
func VerifiedObservationPolicy(configuration config.DNSConfig, checkpoint []byte, snapshot dnsfacts.Snapshot, queries []dnsobservation.Query, now time.Time) (dnsobservation.Policy, error) {
	fail := errors.New("DNS observation policy authority or freshness mismatch")
	service := &Service{Config: configuration}
	c, p, _, err := service.decodeDNSCheckpointPayload(checkpoint)
	if err != nil {
		return dnsobservation.Policy{}, errors.New("DNS observation policy checkpoint authentication failed")
	}
	until := c.AppliedAt.Add(time.Duration(p.Policy.MaxStaleSeconds) * time.Second)
	planDigest, err := platformconfig.Digest(p.Plan)
	if err != nil || len(queries) < 1 || len(queries) > 32 || c.AppliedAt.After(now) || !until.After(now) ||
		snapshot.Schema != dnsfacts.Schema || snapshot.NodeID != c.NodeID || snapshot.EdgeGroupID != c.GroupID ||
		snapshot.ParentDigest != c.Parent.ContentHash || snapshot.PlanDigest != planDigest ||
		!reflect.DeepEqual(snapshot.Assignment, c.Candidate.Assignment) || !snapshot.CheckpointValidUntil.Equal(until) ||
		snapshot.EvaluatedAt.IsZero() || snapshot.EvaluatedAt.After(now) || now.Sub(snapshot.EvaluatedAt) > time.Duration(p.Policy.DNSReadiness.ProbeIntervalSeconds)*time.Second {
		return dnsobservation.Policy{}, fail
	}
	// This bounds transport/queue delay, not readiness leases. Readiness and
	// recovery windows remain enforced by the DNS consumer on every answer.
	if limit := now.Add(10 * time.Minute); until.After(limit) {
		until = limit
	}
	result := dnsobservation.Policy{Schema: dnsobservation.PolicySchema, NodeID: c.NodeID, Assignment: c.Candidate.Assignment,
		ParentDigest: c.Parent.ContentHash, CapturedAt: now, ValidUntil: until, Records: []dnsobservation.Record{}}
	seen := map[string]bool{}
	for _, query := range queries {
		query.Hostname = strings.TrimSuffix(strings.ToLower(query.Hostname), ".")
		identity := query.Hostname + "\x00" + query.Type
		if _, ok := dns.IsDomainName(query.Hostname); !ok || query.Hostname == "" || !slices.Contains([]string{"A", "AAAA"}, query.Type) || seen[identity] {
			return dnsobservation.Policy{}, errors.New("bounded unique address observation queries required")
		}
		seen[identity] = true
		entry := dnsobservation.Record{Query: query, Allowed: []string{}}
		found := false
		for _, view := range p.Queries {
			if view.NodeID != c.NodeID || view.EdgeGroupID != c.GroupID {
				continue
			}
			for _, record := range view.Records {
				if record.Name != query.Hostname || record.Type != query.Type {
					continue
				}
				if found || record.Status != "" && record.Status != model.EdgeRouteStatusActive || record.TTL < 1 || len(record.ScopedCandidates) != 0 || record.AnswerPolicy.ECSEnabled {
					return dnsobservation.Policy{}, errors.New("ambiguous or unsupported DNS observation policy")
				}
				found = true
				entry.MaximumTTL = uint32(max(record.TTL, record.AnswerPolicy.TTLSeconds))
				entry.Allowed, err = observationRecordAddresses(record, now)
				if err != nil {
					return dnsobservation.Policy{}, err
				}
				for value, expiry := range record.ValueExpirations {
					if len(record.Candidates) == 0 && slices.Contains(entry.Allowed, value) && expiry.Before(result.ValidUntil) {
						result.ValidUntil = expiry
					}
				}
			}
		}
		result.Records = append(result.Records, entry)
	}
	return result, nil
}

func observationRecordAddresses(record model.EdgeDNSRecord, now time.Time) ([]string, error) {
	values := []string{}
	if len(record.Candidates) == 0 {
		for _, value := range record.Values {
			if expiry, exists := record.ValueExpirations[value]; !exists || expiry.After(now) {
				values = append(values, value)
			}
		}
	} else {
		policy := record.AnswerPolicy
		var primary []string
		switch policy.PolicyKind {
		case model.DNSAnswerPolicyKindPhysicalOrder:
			if model.ValidateDNSPhysicalOrder(policy.PhysicalOrder) != nil || policy.PhysicalSelection != nil {
				return nil, errors.New("invalid DNS observation physical order")
			}
			primary = policy.PhysicalOrder.OrderedEdgeIDs
		case model.DNSAnswerPolicyKindPhysicalQuality:
			if model.ValidateDNSPhysicalSelection(policy.PhysicalSelection) != nil || policy.PhysicalOrder != nil {
				return nil, errors.New("invalid DNS observation physical selection")
			}
			primary = policy.PhysicalSelection.OrderedEdgeIDs
		default:
			return nil, errors.New("unsupported DNS observation candidate policy")
		}
		for _, candidate := range record.Candidates {
			allowed := slices.Contains(primary, candidate.EdgeID)
			if availability := policy.Availability; availability != nil {
				if model.ValidateDNSAvailabilityPolicy(availability) != nil {
					return nil, errors.New("invalid DNS observation availability policy")
				}
				fallback := slices.Contains(availability.OrderedEdgeIDs, candidate.EdgeID)
				if availability.Version == model.DNSAvailabilityRedundantVersion {
					allowed = fallback
				} else {
					allowed = allowed || fallback
				}
			}
			if allowed && (len(policy.AllowedEdgeGroups) == 0 || slices.Contains(policy.AllowedEdgeGroups, candidate.EdgeGroupID)) {
				values = append(values, candidate.IP)
			}
		}
	}
	for _, value := range values {
		ip, err := netip.ParseAddr(value)
		if err != nil || ip.String() != value || ip.Is4() != (record.Type == "A") {
			return nil, errors.New("invalid DNS observation address family")
		}
	}
	slices.Sort(values)
	return slices.Compact(values), nil
}
