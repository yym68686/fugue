package controller

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"

	"fugue/internal/model"
	"fugue/internal/releaseflow"
	"fugue/internal/store"
)

func safeRolloutOperationReleaseID(op model.Operation) string {
	digest := sha256.Sum256([]byte(op.TenantID + "\x00" + op.AppID + "\x00" + op.ID))
	// Release IDs also appear in Kubernetes label values (maximum 63 bytes).
	return fmt.Sprintf("apprel_op_%x", digest[:24])
}

// Recover before canonical baseline reconciliation: that reconciliation resets
// traffic and is only valid before this operation creates its first candidate.
func (s *Service) resumeSafeRollout(ctx context.Context, op model.Operation, previous, candidate model.App) (*safeRolloutState, error) {
	if op.ID == "" || op.AppID != candidate.ID || op.TenantID != candidate.TenantID {
		return nil, fmt.Errorf("rollout operation ownership differs")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	releases, err := s.Store.ListAppReleases(model.AppReleaseFilter{TenantID: candidate.TenantID, AppID: candidate.ID, IncludeRetired: true})
	if err != nil {
		return nil, err
	}
	references := map[string]string{}
	for _, r := range releases {
		if r.ID == safeRolloutOperationReleaseID(op) || (r.RevisionWorkload != nil && r.RevisionWorkload.OperationID == op.ID) {
			references[r.ID] = r.RollbackTargetID
		}
	}
	attempt, found, err := s.Store.FindReleaseAttemptForOperation(op.ID)
	if err != nil {
		return nil, err
	}
	if found {
		if attempt.AppID != candidate.ID || attempt.TenantID != candidate.TenantID {
			return nil, fmt.Errorf("rollout attempt owner differs")
		}
		steps, err := s.Store.ListReleaseSteps(candidate.TenantID, false, attempt.ID)
		if err != nil {
			return nil, err
		}
		for _, step := range steps {
			if step.OperationID != op.ID || step.Status != model.ReleaseStepStatusCompleted || step.Payload["phase"] != "candidate_create" {
				continue
			}
			id, _ := step.Payload["candidate_release_id"].(string)
			stable, _ := step.Payload["stable_release_id"].(string)
			if id == "" || stable == "" {
				return nil, fmt.Errorf("rollout creation receipt has incomplete identity")
			}
			if old := references[id]; old != "" && old != stable {
				return nil, fmt.Errorf("rollout rollback identity differs")
			}
			references[id] = stable
		}
	}
	if len(references) == 0 {
		return nil, nil
	}
	if len(references) != 1 {
		return nil, fmt.Errorf("operation has multiple candidate releases")
	}
	var id, stableID string
	for id, stableID = range references {
	}
	var release, stable model.AppRelease
	for _, r := range releases {
		if r.ID == id {
			release = r
		}
		if r.ID == stableID {
			stable = r
		}
	}
	if release.ID == "" || stable.ID == "" || release.ID == stable.ID {
		return nil, fmt.Errorf("rollout release or rollback target is unavailable")
	}
	if release.AppID != candidate.ID || release.TenantID != candidate.TenantID || release.SpecSnapshot == nil ||
		!s.safeRolloutResumeSpecMatches(candidate, *release.SpecSnapshot) || release.RuntimeID != candidate.Spec.RuntimeID ||
		!s.migrationImageRefsEquivalent(candidate, release.ResolvedImageRef, candidate.Spec.Image) || release.SourceRef != releaseflow.AppReleaseSourceRef(candidate) {
		return nil, fmt.Errorf("rollout candidate no longer matches operation executable intent")
	}
	if release.RevisionWorkload != nil && release.RevisionWorkload.OperationID != op.ID {
		return nil, fmt.Errorf("revision belongs to another operation")
	}
	switch release.Status {
	case model.AppReleaseStatusCreating, model.AppReleaseStatusReady, model.AppReleaseStatusServing:
	default:
		return nil, fmt.Errorf("rollout release cannot resume from %s", release.Status)
	}
	if release.Role != model.AppReleaseRoleCandidate && release.Role != model.AppReleaseRoleStable {
		return nil, fmt.Errorf("rollout release is no longer a candidate or stable")
	}
	if stable.AppID != candidate.ID || stable.TenantID != candidate.TenantID || stable.SpecSnapshot == nil || stable.Status == model.AppReleaseStatusRetired {
		return nil, fmt.Errorf("rollout rollback target cannot be recovered")
	}
	policy, err := s.Store.GetAppTrafficPolicy(candidate.TenantID, false, candidate.ID)
	if err != nil {
		return nil, err
	}
	state := &safeRolloutState{Enabled: true, PreviousApp: previous, CandidateApp: candidate, StableRelease: stable, Candidate: release, Resumed: true}
	state.PreviousApp.Spec = *cloneControllerAppSpec(stable.SpecSnapshot)
	if policy.AppID != candidate.ID || policy.TenantID != candidate.TenantID || policy.StableWeight+policy.CandidateWeight != 100 {
		return nil, fmt.Errorf("rollout traffic ownership or weights differ")
	}
	switch {
	case policy.Mode == model.AppTrafficModeSingle && policy.StableReleaseID == release.ID && policy.StableWeight == 100 && policy.CandidateReleaseID == "" && policy.CandidateWeight == 0:
		if release.Role != model.AppReleaseRoleStable || release.Status != model.AppReleaseStatusServing {
			return nil, fmt.Errorf("promoted traffic has no matching serving release")
		}
		state.AlreadyPromoted = true
	case policy.StableReleaseID == stable.ID && ((policy.Mode == model.AppTrafficModeSingle && policy.StableWeight == 100 && policy.CandidateWeight == 0 && policy.CandidateReleaseID == "") ||
		(policy.Mode == model.AppTrafficModeCanary && policy.CandidateReleaseID == release.ID && policy.CandidateWeight > 0 && policy.CandidateWeight < 100)):
		state.ResumeWeight = policy.CandidateWeight
		// Promotion writes the release before the policy. Preserve that phase
		// after a crash and finish the pending policy write after a fresh gate.
		state.PromotionPending = release.Role == model.AppReleaseRoleStable && release.Status == model.AppReleaseStatusServing
	default:
		return nil, fmt.Errorf("traffic no longer belongs to the resumed rollout")
	}
	if release.RollbackTargetID != "" && release.RollbackTargetID != stable.ID {
		return nil, fmt.Errorf("resumed rollback target changed")
	}
	if s.Config.KubectlApply && (release.Status != model.AppReleaseStatusCreating) && release.RevisionWorkload == nil {
		return nil, fmt.Errorf("resumed active revision has no immutable workload binding")
	}
	if state.ResumeWeight > 0 {
		allowed := false
		for _, weight := range s.safeRolloutPlanForApp(candidate).CanarySteps {
			if weight == state.ResumeWeight {
				allowed = true
			}
		}
		if !allowed {
			return nil, fmt.Errorf("resumed canary weight is not in the current rollout plan")
		}
	}
	return state, nil
}

func (s *Service) safeRolloutResumeSpecMatches(app model.App, stored model.AppSpec) bool {
	normalize := func(spec model.AppSpec) model.AppSpec {
		spec, _ = model.StripFugueInjectedAppEnvFromSpec(spec)
		spec = normalizeBuildpacksLaunchOverrideSpec(app, spec)
		spec.RolloutIntent = ""
		if s.migrationImageRefsEquivalent(app, spec.Image, app.Spec.Image) {
			spec.Image = app.Spec.Image
		}
		model.ApplyAppSpecDefaults(&spec)
		return spec
	}
	return reflect.DeepEqual(normalize(stored), normalize(app.Spec))
}

func (s *Service) createSafeRolloutCandidate(op model.Operation, app model.App, stable model.AppRelease) (model.AppRelease, error) {
	if op.ID == "" || op.AppID != app.ID || op.TenantID != app.TenantID {
		return model.AppRelease{}, fmt.Errorf("candidate operation owner is incomplete")
	}
	spec := *cloneControllerAppSpec(&app.Spec)
	r, err := s.Store.CreateAppRelease(model.AppRelease{ID: safeRolloutOperationReleaseID(op), TenantID: app.TenantID, AppID: app.ID, Role: model.AppReleaseRoleCandidate,
		SourceRef: releaseflow.AppReleaseSourceRef(app), ResolvedImageRef: app.Spec.Image, RuntimeID: app.Spec.RuntimeID, Status: model.AppReleaseStatusCreating, SpecSnapshot: &spec, RollbackTargetID: stable.ID})
	if errors.Is(err, store.ErrConflict) {
		return model.AppRelease{}, fmt.Errorf("operation candidate already exists; resume required: %w", err)
	}
	return r, err
}

func (s *Service) verifyResumedSafeRolloutWorkload(ctx context.Context, client *kubeClient, op model.Operation, state *safeRolloutState, objects []map[string]any) error {
	if state.Candidate.RevisionWorkload == nil {
		return fmt.Errorf("resumed workload is unbound")
	}
	// Use the original revision solely for verification, keeping the currently
	// serving canonical target and published weights unchanged.
	copy := *state
	copy.Candidate.DeploymentName = copy.Candidate.RevisionWorkload.DeploymentName
	copy.Candidate.ServiceName = copy.Candidate.RevisionWorkload.ServiceName
	return s.bindSafeRolloutWorkload(ctx, client, op, &copy, objects)
}
