package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

func (s *Store) ListEdgeRoutePolicies() ([]model.EdgeRoutePolicy, error) {
	if s.usingDatabase() {
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

	policies := make([]model.EdgeRoutePolicy, 0)
	err := s.withLockedState(false, func(state *model.State) error {
		policies = append(policies, state.EdgeRoutePolicies...)
		now := time.Now().UTC()
		for index := range policies {
			policies[index].ExclusionLifecycle = model.EdgeRoutePolicyExclusionLifecycleAt(policies[index], now)
		}
		sortEdgeRoutePolicies(policies)
		return nil
	})
	return policies, err
}

func (s *Store) GetEdgeRoutePolicy(hostname string) (model.EdgeRoutePolicy, error) {
	hostname = normalizeEdgeRoutePolicyHostname(hostname)
	if hostname == "" {
		return model.EdgeRoutePolicy{}, ErrInvalidInput
	}
	if s.usingDatabase() {
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

	var policy model.EdgeRoutePolicy
	err := s.withLockedState(false, func(state *model.State) error {
		index := findEdgeRoutePolicy(state, hostname)
		if index < 0 {
			return ErrNotFound
		}
		policy = state.EdgeRoutePolicies[index]
		policy.ExclusionLifecycle = model.EdgeRoutePolicyExclusionLifecycleAt(policy, time.Now().UTC())
		return nil
	})
	return policy, err
}

func (s *Store) PutEdgeRoutePolicy(policy model.EdgeRoutePolicy) (model.EdgeRoutePolicy, error) {
	policy, err := normalizeEdgeRoutePolicyForStore(policy)
	if err != nil {
		return model.EdgeRoutePolicy{}, err
	}
	if s.usingDatabase() {
		return s.pgPutEdgeRoutePolicy(policy)
	}

	var out model.EdgeRoutePolicy
	err = s.withLockedState(true, func(state *model.State) error {
		now := time.Now().UTC()
		index := findEdgeRoutePolicy(state, policy.Hostname)
		if index >= 0 {
			existing := state.EdgeRoutePolicies[index]
			if (model.EdgeRoutePolicyHasExclusions(existing) || model.EdgeRoutePolicyHasExclusions(policy)) && !edgeRoutePolicyExclusionMaterialEqual(existing, policy) {
				return ErrConflict
			}
			if policy.ID == "" {
				policy.ID = existing.ID
			}
			if policy.CreatedAt.IsZero() {
				policy.CreatedAt = existing.CreatedAt
			}
		} else {
			if policy.ID == "" {
				policy.ID = model.NewID("edge_route_policy")
			}
			if policy.CreatedAt.IsZero() {
				policy.CreatedAt = now
			}
		}
		policy.UpdatedAt = now
		if index >= 0 {
			state.EdgeRoutePolicies[index] = policy
		} else {
			state.EdgeRoutePolicies = append(state.EdgeRoutePolicies, policy)
		}
		out = policy
		return nil
	})
	return out, err
}

func edgeRoutePolicyExclusionMaterialEqual(a, b model.EdgeRoutePolicy) bool {
	return reflect.DeepEqual(a.ExcludedEdgeIDs, b.ExcludedEdgeIDs) &&
		reflect.DeepEqual(a.ExcludedEdgeGroupIDs, b.ExcludedEdgeGroupIDs) &&
		strings.TrimSpace(a.ExclusionReason) == strings.TrimSpace(b.ExclusionReason) &&
		edgeRoutePolicyTimesEqual(a.ExclusionExpiresAt, b.ExclusionExpiresAt) &&
		strings.TrimSpace(a.ExclusionOwnerDigest) == strings.TrimSpace(b.ExclusionOwnerDigest) &&
		a.ExclusionGeneration == b.ExclusionGeneration &&
		strings.TrimSpace(a.ExclusionFence) == strings.TrimSpace(b.ExclusionFence)
}

func edgeRoutePolicyTimesEqual(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Equal(*b)
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
	if s.usingDatabase() {
		return s.pgPutEdgeRoutePolicyCAS(policy, expectedGeneration, expectedFence)
	}
	var out model.EdgeRoutePolicy
	err = s.withLockedState(true, func(state *model.State) error {
		now := time.Now().UTC()
		migrateLegacyEdgeRoutePolicyExclusionsInState(state, now)
		index := findEdgeRoutePolicy(state, policy.Hostname)
		if index < 0 {
			if expectedGeneration != 0 || expectedFence != "" {
				return ErrConflict
			}
			policy.ID = firstNonEmptyStore(policy.ID, model.NewID("edge_route_policy"))
			policy.CreatedAt = now
		} else {
			existing := state.EdgeRoutePolicies[index]
			if existing.ExclusionGeneration != expectedGeneration || strings.TrimSpace(existing.ExclusionFence) != expectedFence {
				return ErrConflict
			}
			removedEdges := removedEdgeExclusionIDs(existing.ExcludedEdgeIDs, policy.ExcludedEdgeIDs, false)
			removedGroups := removedEdgeExclusionIDs(existing.ExcludedEdgeGroupIDs, policy.ExcludedEdgeGroupIDs, true)
			if len(removedEdges) > 0 || len(removedGroups) > 0 {
				if err := validateFileEdgeExclusionClearState(state, removedEdges, removedGroups, now); err != nil {
					return err
				}
			}
			policy.ID = existing.ID
			policy.CreatedAt = existing.CreatedAt
		}
		policy.ExclusionGeneration = expectedGeneration + 1
		policy.ExclusionFence = model.NewID("edge_exclusion_fence")
		policy.UpdatedAt = now
		if model.EdgeRoutePolicyHasExclusions(policy) {
			if policy.ExclusionCreatedAt == nil {
				created := now
				policy.ExclusionCreatedAt = &created
			}
			policy.ExclusionScope = model.EdgeRoutePolicyExclusionScope(policy)
		} else {
			clearEdgeRoutePolicyExclusionMetadata(&policy)
			// Clearing is itself a fenced exclusion mutation; keep the new CAS
			// boundary even though there is no active exclusion.
			policy.ExclusionGeneration = expectedGeneration + 1
			policy.ExclusionFence = model.NewID("edge_exclusion_fence")
		}
		if index >= 0 {
			state.EdgeRoutePolicies[index] = policy
		} else {
			state.EdgeRoutePolicies = append(state.EdgeRoutePolicies, policy)
		}
		out = policy
		return nil
	})
	return out, err
}

func (s *Store) DeleteEdgeRoutePolicy(hostname string) (model.EdgeRoutePolicy, error) {
	hostname = normalizeEdgeRoutePolicyHostname(hostname)
	if hostname == "" {
		return model.EdgeRoutePolicy{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgDeleteEdgeRoutePolicy(hostname)
	}

	var removed model.EdgeRoutePolicy
	err := s.withLockedState(true, func(state *model.State) error {
		index := findEdgeRoutePolicy(state, hostname)
		if index < 0 {
			return ErrNotFound
		}
		removed = state.EdgeRoutePolicies[index]
		if model.EdgeRoutePolicyHasExclusions(removed) {
			return ErrConflict
		}
		state.EdgeRoutePolicies = append(state.EdgeRoutePolicies[:index], state.EdgeRoutePolicies[index+1:]...)
		return nil
	})
	return removed, err
}

func (s *Store) DeleteEdgeRoutePolicyCAS(hostname string, expectedGeneration uint64, expectedFence string) (model.EdgeRoutePolicy, error) {
	hostname = normalizeEdgeRoutePolicyHostname(hostname)
	expectedFence = strings.TrimSpace(expectedFence)
	if hostname == "" || expectedGeneration == 0 || expectedFence == "" {
		return model.EdgeRoutePolicy{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgDeleteEdgeRoutePolicyCAS(hostname, expectedGeneration, expectedFence)
	}
	var removed model.EdgeRoutePolicy
	err := s.withLockedState(true, func(state *model.State) error {
		migrateLegacyEdgeRoutePolicyExclusionsInState(state, time.Now().UTC())
		index := findEdgeRoutePolicy(state, hostname)
		if index < 0 {
			return ErrNotFound
		}
		removed = state.EdgeRoutePolicies[index]
		if removed.ExclusionGeneration != expectedGeneration || strings.TrimSpace(removed.ExclusionFence) != expectedFence {
			return ErrConflict
		}
		if model.EdgeRoutePolicyHasExclusions(removed) {
			if err := validateFileEdgeExclusionClearState(state, removed.ExcludedEdgeIDs, removed.ExcludedEdgeGroupIDs, time.Now().UTC()); err != nil {
				return err
			}
		}
		state.EdgeRoutePolicies = append(state.EdgeRoutePolicies[:index], state.EdgeRoutePolicies[index+1:]...)
		return nil
	})
	return removed, err
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

func migrateLegacyEdgeRoutePolicyExclusionsInState(state *model.State, now time.Time) {
	if state == nil {
		return
	}
	for index := range state.EdgeRoutePolicies {
		policy := &state.EdgeRoutePolicies[index]
		policy.ExclusionLifecycle = ""
		policy.ExclusionEvidenceFresh = false
		policy.ExclusionEvidenceCheckedAt = nil
		policy.ExclusionEvidenceReason = ""
		if !model.EdgeRoutePolicyHasExclusions(*policy) {
			clearEdgeRoutePolicyExclusionMetadata(policy)
			continue
		}
		policy.ExclusionScope = model.EdgeRoutePolicyExclusionScope(*policy)
		if policy.ExclusionCreatedAt == nil {
			created := policy.UpdatedAt.UTC()
			if created.IsZero() {
				created = policy.CreatedAt.UTC()
			}
			if created.IsZero() {
				created = now.UTC()
			}
			policy.ExclusionCreatedAt = &created
		}
		if policy.ExclusionGeneration == 0 {
			policy.ExclusionGeneration = 1
		}
		if strings.TrimSpace(policy.ExclusionFence) == "" {
			policy.ExclusionFence = legacyEdgeExclusionFence(*policy)
		}
	}
}

// EdgeRoutePolicyTime is the canonical lifecycle clock. PostgreSQL-backed API
// replicas share database server time so expiry cannot disagree across pods.
func (s *Store) EdgeRoutePolicyTime() (time.Time, error) {
	if !s.usingDatabase() {
		return time.Now().UTC(), nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var now time.Time
	if err := s.db.QueryRowContext(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return time.Time{}, fmt.Errorf("read edge exclusion lifecycle time: %w", err)
	}
	return now.UTC(), nil
}

func legacyEdgeExclusionFence(policy model.EdgeRoutePolicy) string {
	material := strings.Join([]string{
		strings.TrimSpace(strings.ToLower(policy.Hostname)),
		strings.Join(policy.ExcludedEdgeIDs, ","),
		strings.Join(policy.ExcludedEdgeGroupIDs, ","),
		policy.ExclusionReason,
		policy.CreatedAt.UTC().Format(time.RFC3339Nano),
	}, "\x00")
	digest := sha256.Sum256([]byte(material))
	return "legacy-sha256:" + hex.EncodeToString(digest[:])
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

func findEdgeRoutePolicy(state *model.State, hostname string) int {
	if state == nil {
		return -1
	}
	hostname = normalizeEdgeRoutePolicyHostname(hostname)
	for index, policy := range state.EdgeRoutePolicies {
		if strings.EqualFold(policy.Hostname, hostname) {
			return index
		}
	}
	return -1
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
