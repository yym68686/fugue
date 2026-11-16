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
	service := &Service{Config: configuration}
	c, p, _, err := service.decodeDNSCheckpointPayload(checkpoint)
	if err != nil {
		return dnsobservation.Policy{}, errors.New("DNS observation policy checkpoint authentication failed")
	}
	until := c.AppliedAt.Add(time.Duration(p.Policy.MaxStaleSeconds) * time.Second)
	planDigest, err := platformconfig.Digest(p.Plan)
	// Preserve the ordered fail-closed checks. Only fixed gate names may leave
	// this verifier: never values, private input, proof data or nested errors.
	reason := ""
	switch {
	case err != nil:
		reason = "plan_encoding"
	case len(queries) < 1 || len(queries) > 32:
		reason = "query_count"
	case c.AppliedAt.After(now):
		reason = "checkpoint_future"
	case !until.After(now):
		reason = "checkpoint_expired"
	case snapshot.Schema != dnsfacts.Schema:
		reason = "snapshot_schema"
	case snapshot.NodeID != c.NodeID:
		reason = "snapshot_node"
	case snapshot.EdgeGroupID != c.GroupID:
		reason = "snapshot_group"
	case snapshot.ParentDigest != c.Parent.ContentHash:
		reason = "snapshot_parent"
	case snapshot.PlanDigest != planDigest:
		reason = "snapshot_plan"
	case !reflect.DeepEqual(snapshot.Assignment, c.Candidate.Assignment):
		reason = observationPolicyAssignmentRejection(snapshot.Assignment, c.Candidate.Assignment)
	case !snapshot.CheckpointValidUntil.Equal(until):
		reason = "checkpoint_deadline"
	case snapshot.EvaluatedAt.IsZero():
		reason = "evaluation_missing"
	case snapshot.EvaluatedAt.After(now):
		reason = "evaluation_future"
	case now.Sub(snapshot.EvaluatedAt) > time.Duration(p.Policy.DNSReadiness.ProbeIntervalSeconds)*time.Second:
		reason = "evaluation_stale"
	}
	if reason != "" {
		return dnsobservation.Policy{}, errors.New("DNS observation policy rejected: " + reason)
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

func observationPolicyAssignmentRejection(observed, expected model.PlatformConsumerAssignment) string {
	switch {
	case observed.ExpectedConsumerSetID != expected.ExpectedConsumerSetID:
		return "assignment_expected_set"
	case observed.ReleaseSetID != expected.ReleaseSetID:
		return "assignment_release_set"
	case observed.ArtifactReleaseID != expected.ArtifactReleaseID:
		return "assignment_release"
	case observed.ArtifactKind != expected.ArtifactKind:
		return "assignment_kind"
	case observed.ScopeKey != expected.ScopeKey:
		return "assignment_scope"
	case observed.ExpectedGeneration != expected.ExpectedGeneration:
		return "assignment_generation"
	case observed.Revision != expected.Revision:
		return "assignment_revision"
	case observed.ArtifactID != expected.ArtifactID:
		return "assignment_artifact"
	case observed.ContentHash != expected.ContentHash:
		return "assignment_content"
	case observed.GenerationSequence != expected.GenerationSequence:
		return "assignment_sequence"
	case observed.ReleaseChannel != expected.ReleaseChannel:
		return "assignment_channel"
	case observed.FencingToken != expected.FencingToken:
		return "assignment_fence"
	case observed.ExpectedProtocolVersion != expected.ExpectedProtocolVersion:
		return "assignment_protocol"
	case observed.ExpectedSchemaVersion != expected.ExpectedSchemaVersion:
		return "assignment_schema"
	case !reflect.DeepEqual(observed.CompatibilityCapabilities, expected.CompatibilityCapabilities):
		return "assignment_capabilities"
	case !reflect.DeepEqual(observed.HeartbeatDeadline, expected.HeartbeatDeadline):
		return "assignment_heartbeat_deadline"
	case !reflect.DeepEqual(observed.ConvergenceDeadline, expected.ConvergenceDeadline):
		return "assignment_convergence_deadline"
	default:
		// The outer DeepEqual remains authoritative if the model gains fields.
		return "assignment_other"
	}
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
