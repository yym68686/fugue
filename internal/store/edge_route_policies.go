package store

import (
	"context"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

func (s *Store) ListEdgeRoutePolicies() ([]model.EdgeRoutePolicy, error) {
	policies, err := s.pgListEdgeRoutePolicies()
	if err != nil {
		return nil, err
	}
	now, err := s.EdgeRoutePolicyTime()
	if err != nil {
		return nil, err
	}
	for index := range policies {
		policies[index].ExclusionLifecycle = model.EdgeRoutePolicyExclusionLifecycleAt(policies[index], now)
	}
	return policies, nil
}

func (s *Store) GetEdgeRoutePolicy(hostname string) (model.EdgeRoutePolicy, error) {
	hostname = normalizeEdgeRoutePolicyHostname(hostname)
	if hostname == "" {
		return model.EdgeRoutePolicy{}, ErrInvalidInput
	}
	policy, err := s.pgGetEdgeRoutePolicy(hostname)
	if err != nil {
		return model.EdgeRoutePolicy{}, err
	}
	now, err := s.EdgeRoutePolicyTime()
	if err != nil {
		return model.EdgeRoutePolicy{}, err
	}
	policy.ExclusionLifecycle = model.EdgeRoutePolicyExclusionLifecycleAt(policy, now)
	return policy, nil
}

func (s *Store) PutEdgeRoutePolicy(policy model.EdgeRoutePolicy) (model.EdgeRoutePolicy, error) {
	policy, err := normalizeEdgeRoutePolicyForStore(policy)
	if err != nil {
		return model.EdgeRoutePolicy{}, err
	}
	return s.pgPutEdgeRoutePolicy(policy)
}

// PutEdgeRoutePolicyCAS is the only mutation path for exclusion state. The
// generation and opaque fence are both required so stale writers and replays
// fail closed across file and PostgreSQL backends.
func (s *Store) PutEdgeRoutePolicyCAS(policy model.EdgeRoutePolicy, expectedGeneration uint64, expectedFence string) (model.EdgeRoutePolicy, error) {
	policy, err := normalizeEdgeRoutePolicyForStore(policy)
	if err != nil {
		return model.EdgeRoutePolicy{}, err
	}
	if model.EdgeRoutePolicyHasExclusions(policy) && (policy.ExclusionReason == "" || len(policy.ExclusionReason) > 512 || !validEdgeExclusionOwnerDigest(policy.ExclusionOwnerDigest)) {
		return model.EdgeRoutePolicy{}, ErrInvalidInput
	}
	expectedFence = strings.TrimSpace(expectedFence)
	return s.pgPutEdgeRoutePolicyCAS(policy, expectedGeneration, expectedFence)
}

func (s *Store) DeleteEdgeRoutePolicy(hostname string) (model.EdgeRoutePolicy, error) {
	hostname = normalizeEdgeRoutePolicyHostname(hostname)
	if hostname == "" {
		return model.EdgeRoutePolicy{}, ErrInvalidInput
	}
	return s.pgDeleteEdgeRoutePolicy(hostname)
}

func (s *Store) DeleteEdgeRoutePolicyCAS(hostname string, expectedGeneration uint64, expectedFence string) (model.EdgeRoutePolicy, error) {
	hostname = normalizeEdgeRoutePolicyHostname(hostname)
	expectedFence = strings.TrimSpace(expectedFence)
	if hostname == "" || expectedGeneration == 0 || expectedFence == "" {
		return model.EdgeRoutePolicy{}, ErrInvalidInput
	}
	return s.pgDeleteEdgeRoutePolicyCAS(hostname, expectedGeneration, expectedFence)
}

func normalizeEdgeRoutePolicyForStore(policy model.EdgeRoutePolicy) (model.EdgeRoutePolicy, error) {
	// Lifecycle and clear-evidence fields are read-time projections. They must
	// never become authoritative durable input when a read model is written back.
	policy.ExclusionLifecycle = ""
	policy.ExclusionEvidenceFresh = false
	policy.ExclusionEvidenceCheckedAt = nil
	policy.ExclusionEvidenceReason = ""
	policy.ID = strings.TrimSpace(policy.ID)
	policy.Hostname = normalizeEdgeRoutePolicyHostname(policy.Hostname)
	policy.AppID = strings.TrimSpace(policy.AppID)
	policy.TenantID = strings.TrimSpace(policy.TenantID)
	policy.EdgeGroupID = normalizeEdgeGroupID(policy.EdgeGroupID)
	policy.ExcludedEdgeIDs = normalizeEdgeRoutePolicyIDList(policy.ExcludedEdgeIDs, false)
	policy.ExcludedEdgeGroupIDs = normalizeEdgeRoutePolicyIDList(policy.ExcludedEdgeGroupIDs, true)
	policy.ExclusionReason = strings.TrimSpace(policy.ExclusionReason)
	policy.ExclusionScope = strings.TrimSpace(strings.ToLower(policy.ExclusionScope))
	policy.ExclusionOwnerDigest = strings.TrimSpace(strings.ToLower(policy.ExclusionOwnerDigest))
	policy.ExclusionFence = strings.TrimSpace(policy.ExclusionFence)
	if policy.ExclusionCreatedAt != nil {
		created := policy.ExclusionCreatedAt.UTC()
		policy.ExclusionCreatedAt = &created
	}
	if policy.MinHealthyEdgeNodes < 0 {
		policy.MinHealthyEdgeNodes = 0
	}
	if policy.ExclusionExpiresAt != nil {
		expiresAt := policy.ExclusionExpiresAt.UTC()
		policy.ExclusionExpiresAt = &expiresAt
		if expiresAt.IsZero() {
			policy.ExclusionExpiresAt = nil
		}
	}
	policy.RoutePolicy = model.NormalizeEdgeRoutePolicy(policy.RoutePolicy)
	if policy.Hostname == "" || policy.AppID == "" || policy.TenantID == "" || policy.RoutePolicy == "" {
		return model.EdgeRoutePolicy{}, ErrInvalidInput
	}
	policy.Enabled = model.EdgeRoutePolicyAllowsTraffic(policy.RoutePolicy)
	if policy.Enabled && policy.EdgeGroupID == "" && len(policy.ExcludedEdgeIDs) == 0 && len(policy.ExcludedEdgeGroupIDs) == 0 {
		return model.EdgeRoutePolicy{}, ErrInvalidInput
	}
	if !policy.Enabled {
		policy.EdgeGroupID = ""
		policy.ExcludedEdgeIDs = nil
		policy.ExcludedEdgeGroupIDs = nil
		policy.ExclusionReason = ""
		policy.ExclusionExpiresAt = nil
		policy.MinHealthyEdgeNodes = 0
	}
	if !model.EdgeRoutePolicyHasExclusions(policy) {
		clearEdgeRoutePolicyExclusionMetadata(&policy)
	} else {
		policy.ExclusionScope = model.EdgeRoutePolicyExclusionScope(policy)
	}
	return policy, nil
}

// EdgeRoutePolicyTime is the canonical lifecycle clock. PostgreSQL-backed API
// replicas share database server time so expiry cannot disagree across pods.
func (s *Store) EdgeRoutePolicyTime() (time.Time, error) {

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var now time.Time
	if err := s.db.QueryRowContext(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return time.Time{}, fmt.Errorf("read edge exclusion lifecycle time: %w", err)
	}
	return now.UTC(), nil
}

func validEdgeExclusionOwnerDigest(value string) bool {
	value = strings.TrimSpace(strings.ToLower(value))
	if !strings.HasPrefix(value, "sha256:") || len(value) != len("sha256:")+64 {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	return err == nil
}

func clearEdgeRoutePolicyExclusionMetadata(policy *model.EdgeRoutePolicy) {
	if policy == nil {
		return
	}
	policy.ExclusionReason = ""
	policy.ExclusionExpiresAt = nil
	policy.ExclusionScope = ""
	policy.ExclusionOwnerDigest = ""
	policy.ExclusionCreatedAt = nil
	policy.ExclusionLifecycle = ""
	policy.ExclusionEvidenceFresh = false
	policy.ExclusionEvidenceCheckedAt = nil
	policy.ExclusionEvidenceReason = ""
}

func firstNonEmptyStore(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

func sortEdgeRoutePolicies(policies []model.EdgeRoutePolicy) {
	sort.Slice(policies, func(i, j int) bool {
		if policies[i].Hostname != policies[j].Hostname {
			return policies[i].Hostname < policies[j].Hostname
		}
		return policies[i].CreatedAt.Before(policies[j].CreatedAt)
	})
}

func normalizeEdgeRoutePolicyHostname(hostname string) string {
	return strings.Trim(strings.TrimSpace(strings.ToLower(hostname)), ".")
}

func normalizeEdgeGroupID(edgeGroupID string) string {
	return strings.TrimSpace(strings.ToLower(edgeGroupID))
}

func normalizeEdgeRoutePolicyIDList(values []string, edgeGroup bool) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if edgeGroup {
			value = normalizeEdgeGroupID(value)
		} else {
			value = normalizeEdgeID(value)
		}
		if value == "" {
			continue
		}
		key := value
		if edgeGroup {
			key = strings.ToLower(value)
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	if len(out) == 0 {
		return nil
	}
	return out
}
