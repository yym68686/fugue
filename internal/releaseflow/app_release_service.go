package releaseflow

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/store"
)

type AppReleaseStore interface {
	CreateAppRelease(model.AppRelease) (model.AppRelease, error)
	UpdateAppRelease(model.AppRelease) (model.AppRelease, error)
	GetAppRelease(tenantID string, platformAdmin bool, releaseID string) (model.AppRelease, error)
	ListAppReleases(model.AppReleaseFilter) ([]model.AppRelease, error)
	GetAppTrafficPolicy(tenantID string, platformAdmin bool, appID string) (model.AppTrafficPolicy, error)
	UpsertAppTrafficPolicy(model.AppTrafficPolicy) (model.AppTrafficPolicy, error)
}

type AppReleaseService struct {
	Store             AppReleaseStore
	Now               func() time.Time
	ServiceURLForApp  func(context.Context, model.App) string
	DefaultStickyName string
}

type CreateReleaseRequest struct {
	Role             string
	SourceRef        string
	ResolvedImageRef string
	UpstreamURL      string
	RuntimeID        string
	DeploymentName   string
	ServiceName      string
	Status           string
	StatusReason     string
	RollbackTargetID string
	ReleaseMessage   string
	RetentionUntil   *time.Time
	SpecSnapshot     *model.AppSpec
}

type TrafficPatch struct {
	Mode               string
	StableReleaseID    string
	CandidateReleaseID string
	StableWeight       *int
	CandidateWeight    *int
	StickyHeader       string
	StickyCookie       string
}

func (s AppReleaseService) EnsureStableTrafficPolicy(ctx context.Context, principal model.Principal, app model.App) (model.AppTrafficPolicy, error) {
	if s.Store == nil {
		return model.AppTrafficPolicy{}, fmt.Errorf("app release store is nil")
	}
	if policy, err := s.Store.GetAppTrafficPolicy(principal.TenantID, principal.IsPlatformAdmin(), app.ID); err == nil {
		return policy, nil
	} else if !errors.Is(err, store.ErrNotFound) {
		return model.AppTrafficPolicy{}, err
	}
	stable, err := s.EnsureStableRelease(ctx, app)
	if err != nil {
		return model.AppTrafficPolicy{}, err
	}
	stickyCookie := strings.TrimSpace(s.DefaultStickyName)
	if stickyCookie == "" {
		stickyCookie = "Fugue-Release-Stickiness"
	}
	return s.Store.UpsertAppTrafficPolicy(model.AppTrafficPolicy{
		TenantID:        app.TenantID,
		AppID:           app.ID,
		Mode:            model.AppTrafficModeSingle,
		StableReleaseID: stable.ID,
		StableWeight:    100,
		CandidateWeight: 0,
		StickyCookie:    stickyCookie,
		UpdatedByType:   principal.ActorType,
		UpdatedByID:     principal.ActorID,
	})
}

func (s AppReleaseService) EnsureStableRelease(ctx context.Context, app model.App) (model.AppRelease, error) {
	if s.Store == nil {
		return model.AppRelease{}, fmt.Errorf("app release store is nil")
	}
	releases, err := s.Store.ListAppReleases(model.AppReleaseFilter{
		TenantID:      app.TenantID,
		AppID:         app.ID,
		Role:          model.AppReleaseRoleStable,
		PlatformAdmin: true,
	})
	if err != nil {
		return model.AppRelease{}, err
	}
	if len(releases) > 0 {
		return releases[0], nil
	}
	status := model.AppReleaseStatusReady
	if app.Status.CurrentReplicas <= 0 {
		status = model.AppReleaseStatusCreating
	}
	now := s.now()
	spec := app.Spec
	return s.Store.CreateAppRelease(model.AppRelease{
		TenantID:         app.TenantID,
		AppID:            app.ID,
		Role:             model.AppReleaseRoleStable,
		SourceRef:        AppReleaseSourceRef(app),
		ResolvedImageRef: app.Spec.Image,
		UpstreamURL:      s.serviceURLForApp(ctx, app),
		RuntimeID:        app.Spec.RuntimeID,
		Status:           status,
		SpecSnapshot:     &spec,
		ReadyAt:          &now,
	})
}

func (s AppReleaseService) CreateRelease(ctx context.Context, app model.App, req CreateReleaseRequest) (model.AppRelease, error) {
	if s.Store == nil {
		return model.AppRelease{}, fmt.Errorf("app release store is nil")
	}
	role := store.NormalizeAppReleaseRole(req.Role)
	if role == "" {
		role = model.AppReleaseRoleCandidate
	}
	status := store.NormalizeAppReleaseStatus(req.Status)
	upstreamURL := strings.TrimSpace(req.UpstreamURL)
	if role == model.AppReleaseRoleStable && upstreamURL == "" {
		upstreamURL = s.serviceURLForApp(ctx, app)
	}
	if status == model.AppReleaseStatusReady && upstreamURL == "" && role == model.AppReleaseRoleCandidate {
		status = model.AppReleaseStatusCreating
	}
	var readyAt *time.Time
	if status == model.AppReleaseStatusReady || status == model.AppReleaseStatusServing {
		now := s.now()
		readyAt = &now
	}
	specSnapshot := req.SpecSnapshot
	if specSnapshot == nil && role == model.AppReleaseRoleStable {
		spec := app.Spec
		specSnapshot = &spec
	}
	return s.Store.CreateAppRelease(model.AppRelease{
		TenantID:         app.TenantID,
		AppID:            app.ID,
		Role:             role,
		SourceRef:        firstNonEmpty(req.SourceRef, AppReleaseSourceRef(app)),
		ResolvedImageRef: strings.TrimSpace(req.ResolvedImageRef),
		UpstreamURL:      upstreamURL,
		RuntimeID:        firstNonEmpty(req.RuntimeID, app.Spec.RuntimeID),
		DeploymentName:   strings.TrimSpace(req.DeploymentName),
		ServiceName:      strings.TrimSpace(req.ServiceName),
		Status:           status,
		StatusReason:     strings.TrimSpace(req.StatusReason),
		RollbackTargetID: strings.TrimSpace(req.RollbackTargetID),
		ReleaseMessage:   strings.TrimSpace(req.ReleaseMessage),
		RetentionUntil:   req.RetentionUntil,
		SpecSnapshot:     specSnapshot,
		ReadyAt:          readyAt,
	})
}

func (s AppReleaseService) PatchTrafficPolicy(ctx context.Context, principal model.Principal, app model.App, patch TrafficPatch) (model.AppTrafficPolicy, error) {
	current, err := s.EnsureStableTrafficPolicy(ctx, principal, app)
	if err != nil {
		return model.AppTrafficPolicy{}, err
	}
	if patch.Mode != "" {
		current.Mode = patch.Mode
	}
	if patch.StableReleaseID != "" {
		current.StableReleaseID = strings.TrimSpace(patch.StableReleaseID)
	}
	if patch.CandidateReleaseID != "" {
		current.CandidateReleaseID = strings.TrimSpace(patch.CandidateReleaseID)
	}
	if patch.StableWeight != nil {
		current.StableWeight = *patch.StableWeight
	}
	if patch.CandidateWeight != nil {
		current.CandidateWeight = *patch.CandidateWeight
	}
	if patch.StickyHeader != "" {
		current.StickyHeader = strings.TrimSpace(patch.StickyHeader)
	}
	if patch.StickyCookie != "" {
		current.StickyCookie = strings.TrimSpace(patch.StickyCookie)
	}
	current.UpdatedByType = principal.ActorType
	current.UpdatedByID = principal.ActorID
	if err := s.ValidateTrafficPolicyReferences(principal, app, current); err != nil {
		return model.AppTrafficPolicy{}, err
	}
	return s.Store.UpsertAppTrafficPolicy(current)
}

func (s AppReleaseService) PromoteRelease(ctx context.Context, principal model.Principal, app model.App, release model.AppRelease, candidateWeight int) (model.AppTrafficPolicy, error) {
	if candidateWeight < 0 || candidateWeight > 100 {
		return model.AppTrafficPolicy{}, fmt.Errorf("candidate_weight must be between 0 and 100")
	}
	policy, err := s.EnsureStableTrafficPolicy(ctx, principal, app)
	if err != nil {
		return model.AppTrafficPolicy{}, err
	}
	if candidateWeight < 100 {
		policy.Mode = model.AppTrafficModeCanary
		policy.CandidateReleaseID = release.ID
		policy.StableWeight = 100 - candidateWeight
		policy.CandidateWeight = candidateWeight
		policy.UpdatedByType = principal.ActorType
		policy.UpdatedByID = principal.ActorID
		return s.Store.UpsertAppTrafficPolicy(policy)
	}
	now := s.now()
	oldStableID := policy.StableReleaseID
	if oldStableID != "" && oldStableID != release.ID {
		if oldStable, err := s.Store.GetAppRelease(app.TenantID, true, oldStableID); err == nil {
			oldStable.Role = model.AppReleaseRolePrevious
			oldStable.Status = model.AppReleaseStatusDraining
			oldStable.StatusReason = "superseded by release " + release.ID
			oldStable.ReleaseMessage = "previous stable kept draining as rollback target"
			oldStable.RetentionUntil = releaseRetentionUntil(app, now)
			_, _ = s.Store.UpdateAppRelease(oldStable)
		}
	}
	release.Role = model.AppReleaseRoleStable
	release.Status = model.AppReleaseStatusServing
	release.StatusReason = ""
	release.RollbackTargetID = oldStableID
	release.ReleaseMessage = "promoted to stable"
	release.PromotedAt = &now
	release.ReadyAt = FirstNonNilTime(release.ReadyAt, &now)
	if _, err := s.Store.UpdateAppRelease(release); err != nil {
		return model.AppTrafficPolicy{}, err
	}
	policy.Mode = model.AppTrafficModeSingle
	policy.StableReleaseID = release.ID
	policy.CandidateReleaseID = ""
	policy.StableWeight = 100
	policy.CandidateWeight = 0
	policy.UpdatedByType = principal.ActorType
	policy.UpdatedByID = principal.ActorID
	return s.Store.UpsertAppTrafficPolicy(policy)
}

func (s AppReleaseService) AbortRelease(ctx context.Context, principal model.Principal, app model.App, release model.AppRelease, markFailed bool, reason string) (model.AppTrafficPolicy, error) {
	policy, err := s.EnsureStableTrafficPolicy(ctx, principal, app)
	if err != nil {
		return model.AppTrafficPolicy{}, err
	}
	policy.Mode = model.AppTrafficModeSingle
	policy.CandidateReleaseID = ""
	policy.StableWeight = 100
	policy.CandidateWeight = 0
	policy.UpdatedByType = principal.ActorType
	policy.UpdatedByID = principal.ActorID
	policy, err = s.Store.UpsertAppTrafficPolicy(policy)
	if err != nil {
		return model.AppTrafficPolicy{}, err
	}
	if markFailed {
		release.Status = model.AppReleaseStatusFailed
		release.StatusReason = strings.TrimSpace(reason)
		release.ReleaseMessage = "aborted: " + strings.TrimSpace(reason)
		if _, err := s.Store.UpdateAppRelease(release); err != nil {
			return model.AppTrafficPolicy{}, err
		}
	}
	return policy, nil
}

func (s AppReleaseService) RetireRelease(ctx context.Context, app model.App, release model.AppRelease, reason string) (model.AppRelease, error) {
	if s.Store == nil {
		return model.AppRelease{}, fmt.Errorf("app release store is nil")
	}
	now := s.now()
	release.Role = model.AppReleaseRoleRetired
	release.Status = model.AppReleaseStatusRetired
	release.StatusReason = strings.TrimSpace(reason)
	release.ReleaseMessage = "retired: " + strings.TrimSpace(reason)
	release.RetiredAt = &now
	return s.Store.UpdateAppRelease(release)
}

func (s AppReleaseService) RestoreStableTraffic(ctx context.Context, principal model.Principal, app model.App) (model.AppTrafficPolicy, error) {
	policy, err := s.EnsureStableTrafficPolicy(ctx, principal, app)
	if err != nil {
		return model.AppTrafficPolicy{}, err
	}
	policy.Mode = model.AppTrafficModeSingle
	policy.CandidateReleaseID = ""
	policy.StableWeight = 100
	policy.CandidateWeight = 0
	policy.UpdatedByType = principal.ActorType
	policy.UpdatedByID = principal.ActorID
	return s.Store.UpsertAppTrafficPolicy(policy)
}

func (s AppReleaseService) ValidateTrafficPolicyReferences(principal model.Principal, app model.App, policy model.AppTrafficPolicy) error {
	if s.Store == nil {
		return fmt.Errorf("app release store is nil")
	}
	if _, err := s.Store.GetAppRelease(principal.TenantID, principal.IsPlatformAdmin(), policy.StableReleaseID); err != nil {
		return fmt.Errorf("stable_release_id is invalid")
	}
	if policy.CandidateReleaseID != "" {
		release, err := s.Store.GetAppRelease(principal.TenantID, principal.IsPlatformAdmin(), policy.CandidateReleaseID)
		if err != nil {
			return fmt.Errorf("candidate_release_id is invalid")
		}
		if release.AppID != app.ID {
			return fmt.Errorf("candidate_release_id belongs to another app")
		}
		if policy.CandidateWeight > 0 && strings.TrimSpace(release.UpstreamURL) == "" {
			return fmt.Errorf("candidate release has no upstream_url")
		}
	}
	return nil
}

func AppReleaseSourceRef(app model.App) string {
	if app.Source != nil {
		return firstNonEmpty(app.Source.ImageRef, app.Source.RepoURL, app.Spec.Image)
	}
	return app.Spec.Image
}

func FirstNonNilTime(values ...*time.Time) *time.Time {
	for _, value := range values {
		if value != nil {
			return value
		}
	}
	return nil
}

func (s AppReleaseService) now() time.Time {
	if s.Now != nil {
		return s.Now().UTC()
	}
	return time.Now().UTC()
}

func (s AppReleaseService) serviceURLForApp(ctx context.Context, app model.App) string {
	if s.ServiceURLForApp != nil {
		return s.ServiceURLForApp(ctx, app)
	}
	return ""
}

func releaseRetentionUntil(app model.App, now time.Time) *time.Time {
	policy := model.NormalizeAppContinuityPolicy(app.Spec.Continuity)
	if policy == nil || policy.ZeroDowntime == nil || policy.ZeroDowntime.RollbackWindowSeconds <= 0 {
		return nil
	}
	until := now.Add(time.Duration(policy.ZeroDowntime.RollbackWindowSeconds) * time.Second)
	return &until
}
