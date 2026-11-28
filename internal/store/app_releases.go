package store

import (
	"context"
	"strings"
	"time"

	"fugue/internal/model"
)

func (s *Store) CreateAppRelease(release model.AppRelease) (model.AppRelease, error) {
	if release.RevisionWorkload != nil {
		return model.AppRelease{}, ErrInvalidInput
	}
	release, err := normalizeAppReleaseForStore(release)
	if err != nil {
		return model.AppRelease{}, err
	}
	// Insertion can register an already-retired historical record. It
	// cannot replace a live release; transitions still require retirement.
	return s.pgCreateAppRelease(release)
}

func (s *Store) UpdateAppRelease(release model.AppRelease) (model.AppRelease, error) {
	release, err := normalizeAppReleaseForStore(release)
	if err != nil {
		return model.AppRelease{}, err
	}
	if AppReleaseIsRetired(release) || release.RetiredAt != nil {
		return model.AppRelease{}, ErrConflict
	}
	return s.pgUpdateAppRelease(release)
}

func (s *Store) GetAppRelease(tenantID string, platformAdmin bool, releaseID string) (model.AppRelease, error) {
	releaseID = strings.TrimSpace(releaseID)
	if releaseID == "" {
		return model.AppRelease{}, ErrInvalidInput
	}
	return s.pgGetAppRelease(tenantID, platformAdmin, releaseID)
}

func (s *Store) ListAppReleases(filter model.AppReleaseFilter) ([]model.AppRelease, error) {
	filter = normalizeAppReleaseFilter(filter)
	return s.pgListAppReleases(filter)
}

// ListAppReleaseMetadata is for runtime evidence readers that never consume
// executable config snapshots. Full release reads remain available for rollback.
func (s *Store) ListAppReleaseMetadata(filter model.AppReleaseFilter) ([]model.AppRelease, error) {
	filter = normalizeAppReleaseFilter(filter)
	return s.pgListAppReleaseView(filter, false)
}

// listAppReleaseRetirementState preserves all matching release facts and only
// includes executable configuration for releases awaiting drain. The retirement
// gate needs those complete snapshots; already-retired cleanup only uses the
// immutable workload binding. This view must not be persisted as a full release.
func (s *Store) listAppReleaseRetirementState(ctx context.Context, filter model.AppReleaseFilter) ([]model.AppRelease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	filter = normalizeAppReleaseFilter(filter)
	return s.pgListAppReleaseProjectionContext(ctx, filter, appReleaseDrainSpec)
}

func (s *Store) GetAppTrafficPolicy(tenantID string, platformAdmin bool, appID string) (model.AppTrafficPolicy, error) {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return model.AppTrafficPolicy{}, ErrInvalidInput
	}
	return s.pgGetAppTrafficPolicy(tenantID, platformAdmin, appID)
}

func (s *Store) ListAppTrafficPolicies(tenantID string, platformAdmin bool) ([]model.AppTrafficPolicy, error) {
	tenantID = strings.TrimSpace(tenantID)
	return s.pgListAppTrafficPolicies(tenantID, platformAdmin)
}

func (s *Store) UpsertAppTrafficPolicy(policy model.AppTrafficPolicy) (model.AppTrafficPolicy, error) {
	policy, err := normalizeAppTrafficPolicyForStore(policy)
	if err != nil {
		return model.AppTrafficPolicy{}, err
	}
	return s.pgUpsertAppTrafficPolicy(policy)
}

func AppReleaseIsRetired(release model.AppRelease) bool {
	return release.Status == model.AppReleaseStatusRetired || release.Role == model.AppReleaseRoleRetired
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
	release.RollbackTargetID = strings.TrimSpace(release.RollbackTargetID)
	release.ReleaseMessage = strings.TrimSpace(release.ReleaseMessage)
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

// Context variants keep runtime observation work within its caller's budget.
func (s *Store) ListAppReleaseMetadataContext(ctx context.Context, filter model.AppReleaseFilter) ([]model.AppRelease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.pgListAppReleaseViewContext(ctx, normalizeAppReleaseFilter(filter), false)
}
func (s *Store) ListAppTrafficPoliciesContext(ctx context.Context, tenantID string, platformAdmin bool) ([]model.AppTrafficPolicy, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.pgListAppTrafficPoliciesContext(ctx, strings.TrimSpace(tenantID), platformAdmin)
}
