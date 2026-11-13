package api

import (
	"context"
	"reflect"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformsafety"
	"fugue/internal/store"
)

type qualityInputSource struct {
	parent  model.PlatformArtifact
	lkg     model.PlatformLKGSnapshot
	release model.PlatformArtifactRelease
}

// Historical quality evidence is optional, but it may be installed only under
// its original positive verified baseline. A newer pending full/gray candidate
// neither lends authority to arbitrary history nor revokes that independent LKG.
func (s *Server) observeQualityInputSource(parent model.PlatformArtifact) (qualityInputSource, error) {
	var source qualityInputSource
	state, err := s.producedTrafficState(parent.ScopeKey)
	if err != nil || state.lkg == nil || !state.hasFull || parent.ArtifactKind != model.PlatformArtifactKindReleaseSet ||
		!platformsafety.EvaluatePlatformLKGSnapshot(*state.lkg, parent, s.bundleKeyring(), time.Now().UTC()).Pass {
		return source, store.ErrConflict
	}
	current, err := s.store.GetPlatformArtifact(parent.ID)
	if err != nil || !reflect.DeepEqual(parent, current) || s.store.VerifyPlatformArtifactIntegrity(current) != nil {
		return source, store.ErrConflict
	}
	release, err := s.store.GetPlatformArtifactRelease(state.lkg.VerifiedByReleaseID)
	if err != nil || release.ArtifactID != parent.ID || release.Generation != parent.Generation ||
		release.ArtifactKind != parent.ArtifactKind || release.ScopeKey != parent.ScopeKey ||
		release.ReleaseChannel != model.PlatformArtifactReleaseChannelFull || release.VerificationState != model.PlatformArtifactVerificationStateVerified ||
		release.VerifiedLKGGeneration != parent.Generation || release.VerifiedAt == nil ||
		(release.Status != model.PlatformArtifactReleaseStatusActive && release.Status != model.PlatformArtifactReleaseStatusSuperseded) {
		return source, store.ErrConflict
	}
	return qualityInputSource{parent: current, lkg: *state.lkg, release: release}, nil
}

func (s *Server) checkQualityInputSource(ctx context.Context, source qualityInputSource) error {
	if prepared, err := preparedHistoricalQualityAdmission(ctx, source, false); prepared {
		return err
	}
	return s.store.CheckPlatformInputRead(ctx, []store.PlatformInputReadSelection{{
		ArtifactID: source.parent.ID, ContentHash: source.parent.ContentHash, ReleaseID: source.release.ID,
		LKGKind: source.lkg.ArtifactKind, LKGScope: source.lkg.ScopeKey, LKG: &source.lkg,
	}})
}

func (s *Server) qualityInputSourceUnchanged(ctx context.Context, source qualityInputSource) bool {
	current, err := s.observeQualityInputSource(source.parent)
	return err == nil && ctx.Err() == nil && reflect.DeepEqual(source, current)
}
