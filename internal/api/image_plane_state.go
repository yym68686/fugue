package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"fugue/internal/auth"
	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/platformcontrol"
	"fugue/internal/platformsafety"
)

const imageReplicationPlanPollInterval = 500 * time.Millisecond

// handleGetImageReplicationPlanState is a fixed-purpose component endpoint.
// Its node, scope, artifact kind, and release channel are all server-bound to
// the verified short-lived image-cache identity. In particular, this endpoint
// cannot be used to inspect another node or to escape the shadow lane.
func (s *Server) handleGetImageReplicationPlanState(w http.ResponseWriter, r *http.Request) {
	setImagePlaneStateNoStoreHeaders(w)
	claims, ok := auth.PlatformComponentIdentityFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, http.StatusInternalServerError, "verified platform component identity missing")
		return
	}
	if !imageReplicationPlanIdentityMatches(claims) {
		httpx.WriteError(w, http.StatusForbidden, "image-cache node identity required")
		return
	}
	currentGeneration, waitSeconds, err := parseImageReplicationPlanStateQuery(r)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	artifact, release, found, waited, err := s.waitForImageReplicationPlanState(
		r.Context(),
		claims.ScopeKey,
		currentGeneration,
		time.Duration(waitSeconds)*time.Second,
	)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		s.writeStoreError(w, err)
		return
	}
	if err := r.Context().Err(); err != nil {
		return
	}
	if found {
		if err := s.validateImageReplicationPlanDesiredState(claims, artifact, release); err != nil {
			if s.log != nil {
				s.log.Printf("reject image replication desired state for %s: %v", claims.ScopeKey, err)
			}
			httpx.WriteError(w, http.StatusInternalServerError, "image replication plan desired state is unavailable")
			return
		}
	}

	lkg, lkgArtifact, err := s.loadImageReplicationPlanLKG(claims)
	if err != nil {
		if s.log != nil {
			s.log.Printf("load image replication plan LKG for %s failed: %v", claims.ScopeKey, err)
		}
		httpx.WriteError(w, http.StatusInternalServerError, "image replication plan recovery state is unavailable")
		return
	}

	response := model.ImageReplicationPlanStateResponse{
		APIVersion:     model.ImagePlaneAPIVersionV1,
		Kind:           model.ImageReplicationPlanStateKind,
		Component:      model.PlatformConsumerComponentImageCache,
		NodeID:         claims.NodeID,
		ScopeKey:       claims.ScopeKey,
		ArtifactKind:   model.PlatformArtifactKindImageReplicationPlan,
		ReleaseChannel: model.ImageReplicationPlanReleaseChannel,
		LKG:            lkg,
		LKGArtifact:    lkgArtifact,
		Waited:         waited,
		ServerTime:     time.Now().UTC(),
	}
	if found {
		response.Artifact = &artifact
		response.Release = &release
		response.Generation = artifact.Generation
		expectedSet, expectedErr := s.imageReplicationPlanExpectedConsumerSet(claims, artifact, release)
		if expectedErr != nil {
			s.writeStoreError(w, expectedErr)
			return
		}
		if expectedSet != nil {
			response.ExpectedConsumerSetID = expectedSet.ID
			response.Heartbeat, err = s.imageReplicationPlanHeartbeatContract(claims, *expectedSet, release)
		}
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
	}
	if err := r.Context().Err(); err != nil {
		return
	}
	httpx.WriteJSON(w, http.StatusOK, response)
}

func imageReplicationPlanIdentityMatches(claims platformcontrol.PlatformComponentIdentityClaims) bool {
	nodeID := strings.ToLower(strings.TrimSpace(claims.NodeID))
	return claims.Component == model.PlatformConsumerComponentImageCache &&
		nodeID != "" &&
		claims.NodeID == nodeID &&
		claims.CredentialID == model.PlatformConsumerComponentImageCache+":"+nodeID &&
		claims.ScopeKey == "node:"+nodeID &&
		len(claims.ArtifactKinds) == 1 &&
		claims.ArtifactKinds[0] == model.PlatformArtifactKindImageReplicationPlan
}

func parseImageReplicationPlanStateQuery(r *http.Request) (string, int, error) {
	query := r.URL.Query()
	for key, values := range query {
		switch key {
		case "current_generation", "wait_seconds":
		default:
			return "", 0, fmt.Errorf("unsupported query parameter %q", key)
		}
		if len(values) != 1 {
			return "", 0, fmt.Errorf("query parameter %q must be provided once", key)
		}
	}
	currentGeneration := strings.TrimSpace(query.Get("current_generation"))
	if len(currentGeneration) > 256 {
		return "", 0, errors.New("current_generation is too long")
	}
	waitSeconds := 0
	if raw := strings.TrimSpace(query.Get("wait_seconds")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 || parsed > 30 {
			return "", 0, errors.New("wait_seconds must be an integer between 0 and 30")
		}
		waitSeconds = parsed
	}
	return currentGeneration, waitSeconds, nil
}

func (s *Server) waitForImageReplicationPlanState(
	ctx context.Context,
	scopeKey string,
	currentGeneration string,
	maxWait time.Duration,
) (model.PlatformArtifact, model.PlatformArtifactRelease, bool, bool, error) {
	deadline := time.Now().Add(maxWait)
	waited := false
	for {
		if err := ctx.Err(); err != nil {
			return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, false, waited, err
		}
		artifact, release, found, err := s.store.GetActivePlatformArtifact(
			model.PlatformArtifactKindImageReplicationPlan,
			scopeKey,
			model.ImageReplicationPlanReleaseChannel,
		)
		if err != nil {
			return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, false, waited, err
		}
		if currentGeneration == "" || !found || artifact.Generation != currentGeneration || maxWait <= 0 || !time.Now().Before(deadline) {
			return artifact, release, found, waited, nil
		}

		waited = true
		remaining := time.Until(deadline)
		interval := imageReplicationPlanPollInterval
		if remaining < interval {
			interval = remaining
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, false, waited, ctx.Err()
		case <-timer.C:
		}
	}
}

func (s *Server) validateImageReplicationPlanDesiredState(
	claims platformcontrol.PlatformComponentIdentityClaims,
	artifact model.PlatformArtifact,
	release model.PlatformArtifactRelease,
) error {
	if err := s.validateImageReplicationPlanArtifact(claims, artifact); err != nil {
		return err
	}
	if release.ID == "" ||
		release.ArtifactID != artifact.ID ||
		release.ArtifactKind != artifact.ArtifactKind ||
		release.Scope != artifact.Scope ||
		release.ScopeKey != artifact.ScopeKey ||
		release.Generation != artifact.Generation ||
		release.ReleaseChannel != model.ImageReplicationPlanReleaseChannel ||
		release.Status != model.PlatformArtifactReleaseStatusActive {
		return errors.New("active image replication release does not match its artifact and shadow lane")
	}
	switch release.VerificationState {
	case model.PlatformArtifactVerificationStateServingUnverified, model.PlatformArtifactVerificationStateVerified:
		return nil
	default:
		return errors.New("active image replication release is not in a servable verification state")
	}
}

func (s *Server) validateImageReplicationPlanArtifact(
	claims platformcontrol.PlatformComponentIdentityClaims,
	artifact model.PlatformArtifact,
) error {
	if artifact.ArtifactKind != model.PlatformArtifactKindImageReplicationPlan ||
		artifact.Status != model.PlatformArtifactStatusValidated ||
		artifact.Scope.ScopeType != "node" ||
		artifact.Scope.Key != claims.ScopeKey ||
		artifact.ScopeKey != claims.ScopeKey ||
		!strings.EqualFold(strings.TrimSpace(artifact.Scope.NodeID), claims.NodeID) {
		return errors.New("image replication artifact is not bound to the authenticated node scope")
	}
	if validation := platformArtifactInvariantValidation(artifact); !validation.Pass {
		return errors.New("image replication artifact versioned envelope is invalid")
	}
	if integrity := platformsafety.EvaluateArtifactIntegrity(artifact, s.bundleKeyring()); !integrity.Pass {
		return errors.New("image replication artifact signature or content integrity is invalid")
	}
	return nil
}

func (s *Server) loadImageReplicationPlanLKG(
	claims platformcontrol.PlatformComponentIdentityClaims,
) (*model.PlatformLKGSnapshot, *model.PlatformArtifact, error) {
	lkg, err := s.store.GetPlatformLKG(model.PlatformArtifactKindImageReplicationPlan, claims.ScopeKey)
	if err != nil || lkg == nil {
		return lkg, nil, err
	}
	artifact, err := s.store.GetPlatformArtifact(lkg.ArtifactID)
	if err != nil {
		return nil, nil, err
	}
	if err := s.validateImageReplicationPlanArtifact(claims, artifact); err != nil {
		return nil, nil, err
	}
	if integrity := platformsafety.EvaluatePlatformLKGSnapshot(*lkg, artifact, s.bundleKeyring(), time.Now().UTC()); !integrity.Pass {
		return nil, nil, errors.New("image replication plan LKG signature, expiry, or content integrity is invalid")
	}
	return lkg, &artifact, nil
}

func (s *Server) imageReplicationPlanExpectedConsumerSet(
	claims platformcontrol.PlatformComponentIdentityClaims,
	artifact model.PlatformArtifact,
	release model.PlatformArtifactRelease,
) (*model.PlatformExpectedConsumerSet, error) {
	sets, err := s.store.ListPlatformExpectedConsumerSets(model.PlatformExpectedConsumerSetFilter{
		ArtifactReleaseID: release.ID,
		ArtifactKind:      model.PlatformArtifactKindImageReplicationPlan,
		ScopeKey:          claims.ScopeKey,
		Limit:             1,
	})
	if err != nil || len(sets) == 0 {
		return nil, err
	}
	set := sets[0]
	if set.ExpectedGeneration != artifact.Generation {
		return nil, nil
	}
	expectedConsumerID := model.PlatformConsumerComponentImageCache + ":" + claims.NodeID
	for _, consumer := range set.Consumers {
		if consumer.ConsumerID == expectedConsumerID &&
			consumer.Component == model.PlatformConsumerComponentImageCache &&
			strings.EqualFold(consumer.NodeID, claims.NodeID) &&
			consumer.ArtifactKind == model.PlatformArtifactKindImageReplicationPlan &&
			consumer.ScopeKey == claims.ScopeKey &&
			consumer.ExpectedGeneration == artifact.Generation {
			return &set, nil
		}
	}
	return nil, nil
}

func (s *Server) imageReplicationPlanHeartbeatContract(
	claims platformcontrol.PlatformComponentIdentityClaims,
	set model.PlatformExpectedConsumerSet,
	release model.PlatformArtifactRelease,
) (*model.ImageReplicationPlanHeartbeatContract, error) {
	contract := &model.ImageReplicationPlanHeartbeatContract{
		ExpectedConsumerSetID: set.ID,
		ReleaseSetID:          set.ReleaseSetID,
		ArtifactReleaseID:     release.ID,
		FencingToken:          release.FencingToken,
		ProtocolVersion:       model.PlatformConsumerProtocolVersionV1,
		SchemaVersion:         model.PlatformConsumerSchemaVersionV1,
	}
	consumers, err := s.store.ListPlatformConsumers(model.PlatformArtifactKindImageReplicationPlan, claims.ScopeKey)
	if err != nil {
		return nil, err
	}
	expectedConsumerID := model.PlatformConsumerComponentImageCache + ":" + claims.NodeID
	matches := 0
	for _, consumer := range consumers {
		if consumer.ConsumerID != expectedConsumerID ||
			consumer.Component != model.PlatformConsumerComponentImageCache ||
			!strings.EqualFold(consumer.NodeID, claims.NodeID) {
			continue
		}
		matches++
		cursor, err := platformcontrol.PlatformConsumerHeartbeatCursorFromInstance(consumer)
		if err != nil {
			return nil, err
		}
		if cursor == nil {
			continue
		}
		if cursor.FencingToken > release.FencingToken ||
			(strings.TrimSpace(cursor.DesiredGeneration) != "" &&
				strings.TrimSpace(cursor.DesiredGeneration) != strings.TrimSpace(set.ExpectedGeneration) &&
				release.FencingToken <= cursor.FencingToken) {
			return nil, errors.New("image-cache heartbeat cursor is ahead of the active release fence")
		}
		contract.SequenceFloor = cursor.Sequence
		issuedAt := cursor.IssuedAt.UTC()
		contract.IssuedAtFloor = &issuedAt
	}
	if matches > 1 {
		return nil, errors.New("multiple image-cache heartbeat cursors exist for one node scope")
	}
	return contract, nil
}

func setImagePlaneStateNoStoreHeaders(w http.ResponseWriter) {
	w.Header().Set("Cache-Control", "private, no-store, max-age=0")
	w.Header().Set("Pragma", "no-cache")
}
