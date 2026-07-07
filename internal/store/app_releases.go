package store

import (
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

func (s *Store) CreateAppRelease(release model.AppRelease) (model.AppRelease, error) {
	release, err := normalizeAppReleaseForStore(release)
	if err != nil {
		return model.AppRelease{}, err
	}
	if s.usingDatabase() {
		return s.pgCreateAppRelease(release)
	}
	var out model.AppRelease
	err = s.withLockedState(true, func(state *model.State) error {
		for _, existing := range state.AppReleases {
			if existing.ID == release.ID {
				return ErrConflict
			}
		}
		state.AppReleases = append(state.AppReleases, release)
		out = release
		return nil
	})
	return out, err
}

func (s *Store) UpdateAppRelease(release model.AppRelease) (model.AppRelease, error) {
	release, err := normalizeAppReleaseForStore(release)
	if err != nil {
		return model.AppRelease{}, err
	}
	if s.usingDatabase() {
		return s.pgUpdateAppRelease(release)
	}
	var out model.AppRelease
	err = s.withLockedState(true, func(state *model.State) error {
		index := findAppReleaseByID(state.AppReleases, release.ID)
		if index < 0 {
			return ErrNotFound
		}
		state.AppReleases[index] = release
		out = release
		return nil
	})
	return out, err
}

func (s *Store) GetAppRelease(tenantID string, platformAdmin bool, releaseID string) (model.AppRelease, error) {
	releaseID = strings.TrimSpace(releaseID)
	if releaseID == "" {
		return model.AppRelease{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgGetAppRelease(tenantID, platformAdmin, releaseID)
	}
	var out model.AppRelease
	err := s.withLockedState(false, func(state *model.State) error {
		index := findAppReleaseByID(state.AppReleases, releaseID)
		if index < 0 {
			return ErrNotFound
		}
		release := state.AppReleases[index]
		if !platformAdmin && strings.TrimSpace(release.TenantID) != strings.TrimSpace(tenantID) {
			return ErrNotFound
		}
		out = release
		return nil
	})
	return out, err
}

func (s *Store) ListAppReleases(filter model.AppReleaseFilter) ([]model.AppRelease, error) {
	filter = normalizeAppReleaseFilter(filter)
	if s.usingDatabase() {
		return s.pgListAppReleases(filter)
	}
	out := []model.AppRelease{}
	err := s.withLockedState(false, func(state *model.State) error {
		for _, release := range state.AppReleases {
			if !appReleaseMatchesFilter(release, filter) {
				continue
			}
			out = append(out, release)
		}
		sortAppReleases(out)
		return nil
	})
	return out, err
}

func (s *Store) GetAppTrafficPolicy(tenantID string, platformAdmin bool, appID string) (model.AppTrafficPolicy, error) {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return model.AppTrafficPolicy{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgGetAppTrafficPolicy(tenantID, platformAdmin, appID)
	}
	var out model.AppTrafficPolicy
	err := s.withLockedState(false, func(state *model.State) error {
		index := findAppTrafficPolicyByApp(state.AppTrafficPolicies, appID)
		if index < 0 {
			return ErrNotFound
		}
		policy := state.AppTrafficPolicies[index]
		if !platformAdmin && strings.TrimSpace(policy.TenantID) != strings.TrimSpace(tenantID) {
			return ErrNotFound
		}
		out = policy
		return nil
	})
	return out, err
}

func (s *Store) ListAppTrafficPolicies(tenantID string, platformAdmin bool) ([]model.AppTrafficPolicy, error) {
	tenantID = strings.TrimSpace(tenantID)
	if s.usingDatabase() {
		return s.pgListAppTrafficPolicies(tenantID, platformAdmin)
	}
	out := []model.AppTrafficPolicy{}
	err := s.withLockedState(false, func(state *model.State) error {
		for _, policy := range state.AppTrafficPolicies {
			if !platformAdmin && strings.TrimSpace(policy.TenantID) != tenantID {
				continue
			}
			out = append(out, policy)
		}
		sort.Slice(out, func(i, j int) bool {
			if out[i].AppID != out[j].AppID {
				return out[i].AppID < out[j].AppID
			}
			return out[i].UpdatedAt.After(out[j].UpdatedAt)
		})
		return nil
	})
	return out, err
}

func (s *Store) UpsertAppTrafficPolicy(policy model.AppTrafficPolicy) (model.AppTrafficPolicy, error) {
	policy, err := normalizeAppTrafficPolicyForStore(policy)
	if err != nil {
		return model.AppTrafficPolicy{}, err
	}
	if s.usingDatabase() {
		return s.pgUpsertAppTrafficPolicy(policy)
	}
	var out model.AppTrafficPolicy
	err = s.withLockedState(true, func(state *model.State) error {
		index := findAppTrafficPolicyByApp(state.AppTrafficPolicies, policy.AppID)
		if index >= 0 {
			current := state.AppTrafficPolicies[index]
			if policy.ID == "" {
				policy.ID = current.ID
			}
			if policy.CreatedAt.IsZero() {
				policy.CreatedAt = current.CreatedAt
			}
			state.AppTrafficPolicies[index] = policy
		} else {
			state.AppTrafficPolicies = append(state.AppTrafficPolicies, policy)
		}
		out = policy
		return nil
	})
	return out, err
}

func normalizeAppReleaseForStore(release model.AppRelease) (model.AppRelease, error) {
	now := time.Now().UTC()
	release.ID = strings.TrimSpace(release.ID)
	if release.ID == "" {
		release.ID = model.NewID("apprel")
	}
	release.TenantID = strings.TrimSpace(release.TenantID)
	release.AppID = strings.TrimSpace(release.AppID)
	release.Role = NormalizeAppReleaseRole(release.Role)
	release.Status = NormalizeAppReleaseStatus(release.Status)
	release.SourceRef = strings.TrimSpace(release.SourceRef)
	release.ResolvedImageRef = strings.TrimSpace(release.ResolvedImageRef)
	release.UpstreamURL = strings.TrimSpace(release.UpstreamURL)
	release.RuntimeID = strings.TrimSpace(release.RuntimeID)
	release.DeploymentName = strings.TrimSpace(release.DeploymentName)
	release.ServiceName = strings.TrimSpace(release.ServiceName)
	release.StatusReason = strings.TrimSpace(release.StatusReason)
	if release.TenantID == "" || release.AppID == "" || release.Role == "" || release.Status == "" {
		return model.AppRelease{}, ErrInvalidInput
	}
	if release.CreatedAt.IsZero() {
		release.CreatedAt = now
	}
	release.UpdatedAt = now
	return release, nil
}

func normalizeAppReleaseFilter(filter model.AppReleaseFilter) model.AppReleaseFilter {
	filter.TenantID = strings.TrimSpace(filter.TenantID)
	filter.AppID = strings.TrimSpace(filter.AppID)
	filter.Role = NormalizeAppReleaseRole(filter.Role)
	return filter
}

func NormalizeAppReleaseRole(role string) string {
	switch strings.TrimSpace(strings.ToLower(role)) {
	case model.AppReleaseRoleStable:
		return model.AppReleaseRoleStable
	case model.AppReleaseRoleCandidate:
		return model.AppReleaseRoleCandidate
	case model.AppReleaseRolePrevious:
		return model.AppReleaseRolePrevious
	case model.AppReleaseRoleRetired:
		return model.AppReleaseRoleRetired
	default:
		return ""
	}
}

func NormalizeAppReleaseStatus(status string) string {
	switch strings.TrimSpace(strings.ToLower(status)) {
	case model.AppReleaseStatusCreating:
		return model.AppReleaseStatusCreating
	case model.AppReleaseStatusReady:
		return model.AppReleaseStatusReady
	case model.AppReleaseStatusServing:
		return model.AppReleaseStatusServing
	case model.AppReleaseStatusDraining:
		return model.AppReleaseStatusDraining
	case model.AppReleaseStatusFailed:
		return model.AppReleaseStatusFailed
	case model.AppReleaseStatusRetired:
		return model.AppReleaseStatusRetired
	default:
		return model.AppReleaseStatusReady
	}
}

func normalizeAppTrafficPolicyForStore(policy model.AppTrafficPolicy) (model.AppTrafficPolicy, error) {
	now := time.Now().UTC()
	policy.ID = strings.TrimSpace(policy.ID)
	if policy.ID == "" {
		policy.ID = model.NewID("apptp")
	}
	policy.TenantID = strings.TrimSpace(policy.TenantID)
	policy.AppID = strings.TrimSpace(policy.AppID)
	policy.Mode = NormalizeAppTrafficMode(policy.Mode)
	policy.StableReleaseID = strings.TrimSpace(policy.StableReleaseID)
	policy.CandidateReleaseID = strings.TrimSpace(policy.CandidateReleaseID)
	policy.StickyHeader = strings.TrimSpace(policy.StickyHeader)
	policy.StickyCookie = strings.TrimSpace(policy.StickyCookie)
	policy.UpdatedByType = strings.TrimSpace(policy.UpdatedByType)
	policy.UpdatedByID = strings.TrimSpace(policy.UpdatedByID)
	if policy.TenantID == "" || policy.AppID == "" || policy.Mode == "" {
		return model.AppTrafficPolicy{}, ErrInvalidInput
	}
	if policy.StableWeight < 0 || policy.CandidateWeight < 0 || policy.StableWeight+policy.CandidateWeight != 100 {
		return model.AppTrafficPolicy{}, ErrInvalidInput
	}
	if policy.Mode != model.AppTrafficModeSingle && policy.CandidateWeight > 0 && policy.CandidateReleaseID == "" {
		return model.AppTrafficPolicy{}, ErrInvalidInput
	}
	if policy.StableWeight > 0 && policy.StableReleaseID == "" {
		return model.AppTrafficPolicy{}, ErrInvalidInput
	}
	if policy.CreatedAt.IsZero() {
		policy.CreatedAt = now
	}
	policy.UpdatedAt = now
	return policy, nil
}

func NormalizeAppTrafficMode(mode string) string {
	switch strings.TrimSpace(strings.ToLower(mode)) {
	case model.AppTrafficModeSingle:
		return model.AppTrafficModeSingle
	case model.AppTrafficModeCanary:
		return model.AppTrafficModeCanary
	case model.AppTrafficModeWeighted:
		return model.AppTrafficModeWeighted
	case model.AppTrafficModePaused:
		return model.AppTrafficModePaused
	default:
		return model.AppTrafficModeSingle
	}
}

func appReleaseMatchesFilter(release model.AppRelease, filter model.AppReleaseFilter) bool {
	if !filter.PlatformAdmin && strings.TrimSpace(release.TenantID) != filter.TenantID {
		return false
	}
	if filter.PlatformAdmin && filter.TenantID != "" && strings.TrimSpace(release.TenantID) != filter.TenantID {
		return false
	}
	if filter.AppID != "" && strings.TrimSpace(release.AppID) != filter.AppID {
		return false
	}
	if filter.Role != "" && strings.TrimSpace(release.Role) != filter.Role {
		return false
	}
	if !filter.IncludeRetired && strings.TrimSpace(release.Role) == model.AppReleaseRoleRetired {
		return false
	}
	return true
}

func sortAppReleases(releases []model.AppRelease) {
	sort.Slice(releases, func(i, j int) bool {
		if releases[i].AppID != releases[j].AppID {
			return releases[i].AppID < releases[j].AppID
		}
		if releases[i].Role != releases[j].Role {
			return releases[i].Role < releases[j].Role
		}
		return releases[i].CreatedAt.After(releases[j].CreatedAt)
	})
}

func findAppReleaseByID(releases []model.AppRelease, id string) int {
	id = strings.TrimSpace(id)
	for idx, release := range releases {
		if strings.TrimSpace(release.ID) == id {
			return idx
		}
	}
	return -1
}

func findAppTrafficPolicyByApp(policies []model.AppTrafficPolicy, appID string) int {
	appID = strings.TrimSpace(appID)
	for idx, policy := range policies {
		if strings.TrimSpace(policy.AppID) == appID {
			return idx
		}
	}
	return -1
}
