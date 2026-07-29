// Package releasecontrol owns the durable, idempotent control-loop boundary
// for component release plans. This migration stage can reconcile only
// observation-only shadow state and has no production adapter capability.
package releasecontrol

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"fugue/internal/componentmanifest"
	"fugue/internal/model"
	"fugue/internal/platformsafety"
)

const (
	ComponentPlanStatusAPIVersion = "release-control.fugue.dev/v1"
	ComponentPlanStatusKind       = "ComponentPlanStatus"
	ComponentPlanStatusPolicy     = "artifact-ledger-shadow-v1"
	ComponentPlanStateObserved    = "observed"
	componentPlanReleaseReason    = "release-control component plan shadow observation"
)

var (
	ErrComponentPlanReconcile      = errors.New("component release plan reconciliation failed")
	componentPlanDigestPattern     = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	componentPlanGenerationPattern = regexp.MustCompile(`^git-[0-9a-f]{40}$`)
	componentPlanScopePattern      = regexp.MustCompile(`^component-release-plan:[0-9a-f]{40}\.\.[0-9a-f]{40}$`)
)

// ComponentPlanStore is the smallest context-aware durable boundary required
// by the shadow reconciler. Production implementations communicate through a
// versioned API; the package does not import the concrete control-plane store.
type ComponentPlanStore interface {
	GetPlatformArtifact(ctx context.Context, id string) (model.PlatformArtifact, error)
	ReleasePlatformArtifact(
		ctx context.Context,
		id string,
		req model.PlatformArtifactReleaseRequest,
		principal model.Principal,
	) (model.PlatformArtifact, model.PlatformArtifactRelease, model.PlatformReleaseMessage, *model.PlatformLKGSnapshot, error)
}

// ComponentPlanSpec is the immutable desired state supplied to one reconcile
// attempt. The caller must bind both identity and content to remove a mutable
// lookup from the control-loop boundary.
type ComponentPlanSpec struct {
	ArtifactID  string `json:"artifactId"`
	ContentHash string `json:"contentHash"`
	Generation  string `json:"generation"`
}

// ComponentPlanStatus is the durable observed state returned by a successful
// idempotent reconcile. It deliberately carries no production authorization.
type ComponentPlanStatus struct {
	APIVersion                string `json:"apiVersion"`
	Kind                      string `json:"kind"`
	Policy                    string `json:"policy"`
	State                     string `json:"state"`
	ArtifactID                string `json:"artifactId"`
	ContentHash               string `json:"contentHash"`
	ScopeKey                  string `json:"scopeKey"`
	Generation                string `json:"generation"`
	PlanDigest                string `json:"planDigest"`
	CoordinationDigest        string `json:"coordinationDigest"`
	ReleaseID                 string `json:"releaseId"`
	LaneKey                   string `json:"laneKey"`
	FencingToken              int64  `json:"fencingToken"`
	LaneVersion               int64  `json:"laneVersion"`
	IdempotencyKey            string `json:"idempotencyKey"`
	ObservationOnly           bool   `json:"observationOnly"`
	ProductionMutationAllowed bool   `json:"productionMutationAllowed"`
	Digest                    string `json:"digest"`
}

// ReconcileComponentPlan converges one validated component plan to exactly
// one shadow release ledger entry. The envelope supplies the idempotency key;
// the store supplies atomic lane fencing. Repeating the same spec returns the
// same release and status digest.
func ReconcileComponentPlan(
	ctx context.Context,
	store ComponentPlanStore,
	spec ComponentPlanSpec,
	principal model.Principal,
) (ComponentPlanStatus, error) {
	if ctx == nil || store == nil {
		return ComponentPlanStatus{}, ErrComponentPlanReconcile
	}
	if err := ctx.Err(); err != nil {
		return ComponentPlanStatus{}, fmt.Errorf("%w: context is canceled", ErrComponentPlanReconcile)
	}
	if !principal.IsPlatformAdmin() ||
		strings.TrimSpace(principal.ActorType) == "" ||
		strings.TrimSpace(principal.ActorID) == "" {
		return ComponentPlanStatus{}, fmt.Errorf("%w: platform release-control identity is required", ErrComponentPlanReconcile)
	}
	if strings.TrimSpace(spec.ArtifactID) == "" ||
		!componentPlanDigestPattern.MatchString(spec.ContentHash) ||
		strings.TrimSpace(spec.Generation) == "" {
		return ComponentPlanStatus{}, fmt.Errorf("%w: spec identity is incomplete", ErrComponentPlanReconcile)
	}

	artifact, err := store.GetPlatformArtifact(ctx, spec.ArtifactID)
	if err != nil {
		return ComponentPlanStatus{}, fmt.Errorf("%w: read artifact: %w", ErrComponentPlanReconcile, err)
	}
	if artifact.ID != spec.ArtifactID ||
		artifact.ContentHash != spec.ContentHash ||
		artifact.Generation != spec.Generation ||
		artifact.ArtifactKind != model.PlatformArtifactKindComponentReleasePlan ||
		artifact.Status != model.PlatformArtifactStatusValidated {
		return ComponentPlanStatus{}, fmt.Errorf("%w: artifact does not match the validated spec", ErrComponentPlanReconcile)
	}
	if err := componentmanifest.ValidateArtifactBinding(
		artifact.Content,
		artifact.Scope.ScopeType,
		artifact.Scope.Key,
		artifact.ScopeKey,
		artifact.Generation,
	); err != nil {
		return ComponentPlanStatus{}, fmt.Errorf("%w: artifact binding is invalid", ErrComponentPlanReconcile)
	}
	envelope, err := componentmanifest.DecodeShadowArtifactContent(artifact.Content)
	if err != nil {
		return ComponentPlanStatus{}, fmt.Errorf("%w: decode artifact: %v", ErrComponentPlanReconcile, err)
	}
	if err := ctx.Err(); err != nil {
		return ComponentPlanStatus{}, fmt.Errorf("%w: context was canceled before persistence", ErrComponentPlanReconcile)
	}

	returnedArtifact, release, message, _, err := store.ReleasePlatformArtifact(
		ctx,
		artifact.ID,
		model.PlatformArtifactReleaseRequest{
			ReleaseChannel: model.PlatformArtifactReleaseChannelShadow,
			Reason:         componentPlanReleaseReason,
			IdempotencyKey: envelope.CoordinationPlan.IdempotencyKey,
		},
		principal,
	)
	if err != nil {
		return ComponentPlanStatus{}, fmt.Errorf("%w: persist shadow status: %w", ErrComponentPlanReconcile, err)
	}
	if err := verifyShadowReleaseResult(artifact, returnedArtifact, release, message, envelope, principal); err != nil {
		return ComponentPlanStatus{}, fmt.Errorf("%w: %v", ErrComponentPlanReconcile, err)
	}

	status := ComponentPlanStatus{
		APIVersion:                ComponentPlanStatusAPIVersion,
		Kind:                      ComponentPlanStatusKind,
		Policy:                    ComponentPlanStatusPolicy,
		State:                     ComponentPlanStateObserved,
		ArtifactID:                artifact.ID,
		ContentHash:               artifact.ContentHash,
		ScopeKey:                  artifact.ScopeKey,
		Generation:                artifact.Generation,
		PlanDigest:                envelope.ChangePlan.PlanDigest,
		CoordinationDigest:        envelope.CoordinationPlan.CoordinationDigest,
		ReleaseID:                 release.ID,
		LaneKey:                   release.LaneKey,
		FencingToken:              release.FencingToken,
		LaneVersion:               release.Version,
		IdempotencyKey:            release.IdempotencyKey,
		ObservationOnly:           true,
		ProductionMutationAllowed: false,
	}
	status.Digest = DigestComponentPlanStatus(status)
	if err := VerifyComponentPlanStatus(status); err != nil {
		return ComponentPlanStatus{}, fmt.Errorf("%w: build status: %v", ErrComponentPlanReconcile, err)
	}
	return status, nil
}

func verifyShadowReleaseResult(
	expectedArtifact model.PlatformArtifact,
	returnedArtifact model.PlatformArtifact,
	release model.PlatformArtifactRelease,
	message model.PlatformReleaseMessage,
	envelope componentmanifest.ShadowArtifactEnvelope,
	principal model.Principal,
) error {
	expectedLaneKey := platformsafety.ReleaseLaneKey(
		model.PlatformArtifactKindComponentReleasePlan,
		expectedArtifact.ScopeKey,
		model.PlatformArtifactReleaseChannelShadow,
	)
	if !canonicalJSONEqual(returnedArtifact, expectedArtifact) ||
		returnedArtifact.ID != expectedArtifact.ID ||
		returnedArtifact.ArtifactKind != model.PlatformArtifactKindComponentReleasePlan ||
		returnedArtifact.Status != model.PlatformArtifactStatusValidated ||
		returnedArtifact.ContentHash != expectedArtifact.ContentHash ||
		returnedArtifact.Generation != expectedArtifact.Generation ||
		returnedArtifact.Scope != expectedArtifact.Scope ||
		returnedArtifact.ScopeKey != expectedArtifact.ScopeKey ||
		release.ArtifactID != expectedArtifact.ID ||
		release.ArtifactKind != model.PlatformArtifactKindComponentReleasePlan ||
		release.Scope != expectedArtifact.Scope ||
		release.ScopeKey != expectedArtifact.ScopeKey ||
		release.Generation != expectedArtifact.Generation ||
		release.ReleaseChannel != model.PlatformArtifactReleaseChannelShadow ||
		release.Status != model.PlatformArtifactReleaseStatusActive ||
		release.LaneKey != expectedLaneKey ||
		release.FencingToken <= 0 ||
		release.Version <= 0 ||
		release.IdempotencyKey != envelope.CoordinationPlan.IdempotencyKey ||
		release.CandidateGeneration != expectedArtifact.Generation ||
		release.Reason != componentPlanReleaseReason ||
		release.CanaryRuleRef != "" ||
		release.OverrideMode != model.PlatformArtifactOverrideModeNone ||
		len(release.BypassedInvariants) != 0 ||
		release.ServingUnverifiedGeneration != "" ||
		release.ReleasedByType != strings.TrimSpace(principal.ActorType) ||
		release.ReleasedByID != strings.TrimSpace(principal.ActorID) {
		return errors.New("store returned an unbound or production-capable shadow release")
	}
	if strings.TrimSpace(message.ID) == "" ||
		message.ReleaseID != release.ID ||
		message.ArtifactID != expectedArtifact.ID ||
		message.ArtifactKind != model.PlatformArtifactKindComponentReleasePlan ||
		message.Scope != expectedArtifact.Scope ||
		message.ScopeKey != expectedArtifact.ScopeKey ||
		message.Generation != expectedArtifact.Generation ||
		message.ReleaseChannel != model.PlatformArtifactReleaseChannelShadow ||
		message.MessageType != model.PlatformReleaseMessageTypeRelease {
		return errors.New("store returned an unbound shadow release message")
	}
	return nil
}

func canonicalJSONEqual(left, right any) bool {
	leftBytes, leftErr := json.Marshal(left)
	rightBytes, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftBytes, rightBytes)
}

// VerifyComponentPlanStatus validates a persisted status independently of a
// live store read. The content/release bindings are covered by its digest.
func VerifyComponentPlanStatus(status ComponentPlanStatus) error {
	if status.APIVersion != ComponentPlanStatusAPIVersion ||
		status.Kind != ComponentPlanStatusKind ||
		status.Policy != ComponentPlanStatusPolicy ||
		status.State != ComponentPlanStateObserved ||
		status.ObservationOnly != true ||
		status.ProductionMutationAllowed ||
		strings.TrimSpace(status.ArtifactID) == "" ||
		!componentPlanScopePattern.MatchString(status.ScopeKey) ||
		!componentPlanGenerationPattern.MatchString(status.Generation) ||
		strings.TrimSpace(status.ReleaseID) == "" ||
		strings.TrimSpace(status.IdempotencyKey) == "" ||
		status.FencingToken <= 0 ||
		status.LaneVersion <= 0 ||
		!componentPlanDigestPattern.MatchString(status.ContentHash) ||
		!componentPlanDigestPattern.MatchString(status.PlanDigest) ||
		!componentPlanDigestPattern.MatchString(status.CoordinationDigest) {
		return ErrComponentPlanReconcile
	}
	expectedLaneKey := platformsafety.ReleaseLaneKey(
		model.PlatformArtifactKindComponentReleasePlan,
		status.ScopeKey,
		model.PlatformArtifactReleaseChannelShadow,
	)
	if status.LaneKey != expectedLaneKey || status.Digest != DigestComponentPlanStatus(status) {
		return ErrComponentPlanReconcile
	}
	if status.IdempotencyKey != "component-shadow/"+strings.TrimPrefix(status.PlanDigest, "sha256:") {
		return ErrComponentPlanReconcile
	}
	return nil
}

// DigestComponentPlanStatus returns the canonical status digest with the
// self-referential field omitted.
func DigestComponentPlanStatus(status ComponentPlanStatus) string {
	status.Digest = ""
	encoded, err := json.Marshal(status)
	if err != nil {
		panic(fmt.Sprintf("marshal component plan status: %v", err))
	}
	digest := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(digest[:])
}
