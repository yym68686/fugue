package api

import (
	"context"
	"net/http"

	"fugue/internal/model"
	"fugue/internal/platformcontrol"
)

func (s *Server) readCheckedConsumerArtifact(ctx context.Context, claims platformcontrol.PlatformComponentIdentityClaims, artifactID, setID string, newSession func() func(string) (model.PlatformArtifact, error)) (consumerArtifactLookup, int, string) {
	for attempt := 0; ; attempt++ {
		attemptCtx := withConsumerInputReadObservation(ctx)
		load := newSession()
		read := newConsumerArtifactReader(load)
		resolved, err := s.resolvePlatformConsumerAssignmentsWithReader(claims, read)
		if err != nil {
			return consumerArtifactLookup{}, http.StatusServiceUnavailable, err.Error()
		}
		var selected *consumerArtifactLookup
		for i := range resolved {
			item := &resolved[i]
			if item.Assignment.ExpectedConsumerSetID == setID && (item.Assignment.ReleaseSetID == artifactID || item.Artifact.ID == artifactID) {
				selected = item
				break
			}
		}
		if selected == nil {
			return consumerArtifactLookup{}, http.StatusNotFound, "consumer artifact assignment not found"
		}
		response := *selected
		if selected.Assignment.ReleaseSetID == artifactID {
			parent, err := read(artifactID)
			if err != nil || parent.Status != model.PlatformArtifactStatusValidated || s.store.VerifyPlatformArtifactIntegrity(parent) != nil || !validateReleaseSetReferences(parent, read).Pass {
				return consumerArtifactLookup{}, http.StatusServiceUnavailable, "consumer parent artifact unavailable"
			}
			response.Artifact = parent
		}
		if err := s.checkConsumerInputReadsWithLoader(attemptCtx, claims, []consumerArtifactLookup{*selected}, read, false, load); err != nil {
			// A complete retry must still match the requested artifact and
			// expected set. Never substitute a newly published artifact here.
			if attempt == 0 && ctx.Err() == nil && consumerInputReadAuthorityChanged(attemptCtx, err) {
				s.operationObservations().Observe("consumer-artifact-authority-retry", 0)
				continue
			}
			return consumerArtifactLookup{}, http.StatusServiceUnavailable, "consumer input dependencies unavailable or changed"
		}
		return response, http.StatusOK, ""
	}
}
