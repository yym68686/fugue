package store

import (
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

func (s *Store) ListEdgeRoutePolicies() ([]model.EdgeRoutePolicy, error) {
	if s.usingDatabase() {
		return s.pgListEdgeRoutePolicies()
	}

	policies := make([]model.EdgeRoutePolicy, 0)
	err := s.withLockedState(false, func(state *model.State) error {
		policies = append(policies, state.EdgeRoutePolicies...)
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
		return s.pgGetEdgeRoutePolicy(hostname)
	}

	var policy model.EdgeRoutePolicy
	err := s.withLockedState(false, func(state *model.State) error {
		index := findEdgeRoutePolicy(state, hostname)
		if index < 0 {
			return ErrNotFound
		}
		policy = state.EdgeRoutePolicies[index]
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
		state.EdgeRoutePolicies = append(state.EdgeRoutePolicies[:index], state.EdgeRoutePolicies[index+1:]...)
		return nil
	})
	return removed, err
}

func normalizeEdgeRoutePolicyForStore(policy model.EdgeRoutePolicy) (model.EdgeRoutePolicy, error) {
	policy.ID = strings.TrimSpace(policy.ID)
	policy.Hostname = normalizeEdgeRoutePolicyHostname(policy.Hostname)
	policy.AppID = strings.TrimSpace(policy.AppID)
	policy.TenantID = strings.TrimSpace(policy.TenantID)
	policy.EdgeGroupID = normalizeEdgeGroupID(policy.EdgeGroupID)
	policy.RoutePolicy = model.NormalizeEdgeRoutePolicy(policy.RoutePolicy)
	if policy.Hostname == "" || policy.AppID == "" || policy.TenantID == "" || policy.RoutePolicy == "" {
		return model.EdgeRoutePolicy{}, ErrInvalidInput
	}
	policy.Enabled = model.EdgeRoutePolicyAllowsTraffic(policy.RoutePolicy)
	if policy.Enabled && policy.EdgeGroupID == "" {
		return model.EdgeRoutePolicy{}, ErrInvalidInput
	}
	if !policy.Enabled {
		policy.EdgeGroupID = ""
	}
	return policy, nil
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
