package store

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformsafety"
)

const platformLKGDefaultTTL = 7 * 24 * time.Hour

var validPlatformArtifactKinds = map[string]struct{}{
	model.PlatformArtifactKindEdgeRouteBundle:           {},
	model.PlatformArtifactKindDNSAnswerBundle:           {},
	model.PlatformArtifactKindCaddyRouteConfig:          {},
	model.PlatformArtifactKindDiscoveryBundle:           {},
	model.PlatformArtifactKindNodeDesiredState:          {},
	model.PlatformArtifactKindRuntimePlacementPlan:      {},
	model.PlatformArtifactKindRuntimeContinuityPlan:     {},
	model.PlatformArtifactKindNodeGuardianPolicy:        {},
	model.PlatformArtifactKindReleaseGuardPolicy:        {},
	model.PlatformArtifactKindEdgeRankingPolicy:         {},
	model.PlatformArtifactKindTrafficSafetyPolicy:       {},
	model.PlatformArtifactKindSubsystemFailureContracts: {},
	model.PlatformArtifactKindGatePolicyRegistry:        {},
	model.PlatformArtifactKindAutomaticActionContracts:  {},
}

func NormalizePlatformArtifactKind(raw string) string {
	kind := strings.TrimSpace(strings.ToLower(raw))
	if _, ok := validPlatformArtifactKinds[kind]; ok {
		return kind
	}
	return ""
}

func NormalizePlatformReleaseChannel(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case model.PlatformArtifactReleaseChannelShadow:
		return model.PlatformArtifactReleaseChannelShadow
	case model.PlatformArtifactReleaseChannelGray:
		return model.PlatformArtifactReleaseChannelGray
	case "", model.PlatformArtifactReleaseChannelFull:
		return model.PlatformArtifactReleaseChannelFull
	default:
		return ""
	}
}

func NormalizePlatformArtifactScope(scope model.PlatformArtifactScope) (model.PlatformArtifactScope, string) {
	scope.ScopeType = strings.TrimSpace(strings.ToLower(scope.ScopeType))
	scope.Key = strings.TrimSpace(strings.ToLower(scope.Key))
	scope.TenantID = strings.TrimSpace(scope.TenantID)
	scope.ProjectID = strings.TrimSpace(scope.ProjectID)
	scope.AppID = strings.TrimSpace(scope.AppID)
	scope.Hostname = strings.TrimSpace(strings.ToLower(scope.Hostname))
	scope.EdgeGroupID = strings.TrimSpace(scope.EdgeGroupID)
	scope.EdgeID = strings.TrimSpace(scope.EdgeID)
	scope.NodeID = strings.TrimSpace(scope.NodeID)
	scope.Region = strings.TrimSpace(strings.ToLower(scope.Region))
	scope.Country = strings.TrimSpace(strings.ToUpper(scope.Country))
	scope.TrafficClass = strings.TrimSpace(strings.ToLower(scope.TrafficClass))
	if scope.Key != "" {
		return scope, scope.Key
	}
	parts := []string{}
	add := func(key, value string) {
		if strings.TrimSpace(value) != "" {
			parts = append(parts, key+"="+strings.TrimSpace(value))
		}
	}
	add("tenant", scope.TenantID)
	add("project", scope.ProjectID)
	add("app", scope.AppID)
	add("host", scope.Hostname)
	add("edge_group", scope.EdgeGroupID)
	add("edge", scope.EdgeID)
	add("node", scope.NodeID)
	add("region", scope.Region)
	add("country", scope.Country)
	add("traffic_class", scope.TrafficClass)
	if len(parts) == 0 {
		if scope.ScopeType == "" {
			scope.ScopeType = "global"
		}
		scope.Key = scope.ScopeType
		return scope, scope.Key
	}
	if scope.ScopeType == "" {
		scope.ScopeType = "scoped"
	}
	scope.Key = scope.ScopeType + ":" + strings.Join(parts, ",")
	return scope, scope.Key
}

func (s *Store) CreatePlatformArtifact(in model.PlatformArtifact) (model.PlatformArtifact, error) {
	artifact, err := normalizePlatformArtifactForStore(in)
	if err != nil {
		return model.PlatformArtifact{}, err
	}
	if s.usingDatabase() {
		return s.pgCreatePlatformArtifact(artifact)
	}
	err = s.withLockedState(true, func(state *model.State) error {
		artifact.GenerationSequence = nextPlatformArtifactGenerationSequence(
			state.PlatformArtifacts,
			artifact.ArtifactKind,
			artifact.ScopeKey,
		)
		artifact, err = platformsafety.SignPlatformArtifact(artifact, s.platformArtifactSigningKeyring())
		if err != nil {
			return err
		}
		for _, existing := range state.PlatformArtifacts {
			if existing.ID == artifact.ID {
				return ErrConflict
			}
		}
		state.PlatformArtifactContents = upsertPlatformArtifactContent(state.PlatformArtifactContents, buildPlatformArtifactContent(artifact))
		state.PlatformArtifacts = append(state.PlatformArtifacts, artifact)
		return nil
	})
	if err != nil {
		return model.PlatformArtifact{}, err
	}
	return artifact, nil
}

func (s *Store) ListPlatformArtifacts(filter model.PlatformArtifactFilter) ([]model.PlatformArtifact, error) {
	filter.ArtifactKind = NormalizePlatformArtifactKind(filter.ArtifactKind)
	filter.ScopeKey = strings.TrimSpace(strings.ToLower(filter.ScopeKey))
	filter.Status = strings.TrimSpace(strings.ToLower(filter.Status))
	if s.usingDatabase() {
		return s.pgListPlatformArtifacts(filter)
	}
	artifacts := []model.PlatformArtifact{}
	err := s.withLockedState(false, func(state *model.State) error {
		for _, artifact := range state.PlatformArtifacts {
			if !platformArtifactMatchesFilter(artifact, filter) {
				continue
			}
			artifacts = append(artifacts, artifact)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sortPlatformArtifacts(artifacts)
	if filter.Limit > 0 && len(artifacts) > filter.Limit {
		artifacts = artifacts[:filter.Limit]
	}
	return artifacts, nil
}

func (s *Store) GetPlatformArtifact(idOrGeneration string) (model.PlatformArtifact, error) {
	idOrGeneration = strings.TrimSpace(idOrGeneration)
	if idOrGeneration == "" {
		return model.PlatformArtifact{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgGetPlatformArtifact(idOrGeneration)
	}
	var out model.PlatformArtifact
	err := s.withLockedState(false, func(state *model.State) error {
		for _, artifact := range state.PlatformArtifacts {
			if artifact.ID == idOrGeneration || artifact.Generation == idOrGeneration {
				out = artifact
				return nil
			}
		}
		return ErrNotFound
	})
	return out, err
}

func (s *Store) GetPlatformArtifactContent(contentHash string) (model.PlatformArtifactContent, error) {
	contentHash = strings.TrimSpace(contentHash)
	if contentHash == "" {
		return model.PlatformArtifactContent{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgGetPlatformArtifactContent(contentHash)
	}
	var out model.PlatformArtifactContent
	err := s.withLockedState(false, func(state *model.State) error {
		for _, content := range state.PlatformArtifactContents {
			if content.ContentHash == contentHash {
				out = content
				return nil
			}
		}
		return ErrNotFound
	})
	return out, err
}

func (s *Store) ValidatePlatformArtifact(id string, results []model.PlatformArtifactValidationResult) (model.PlatformArtifact, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.PlatformArtifact{}, ErrInvalidInput
	}
	results = normalizePlatformArtifactValidationResults(results)
	if s.usingDatabase() {
		return s.pgValidatePlatformArtifact(id, results)
	}
	var out model.PlatformArtifact
	err := s.withLockedState(true, func(state *model.State) error {
		for index := range state.PlatformArtifacts {
			if state.PlatformArtifacts[index].ID != id && state.PlatformArtifacts[index].Generation != id {
				continue
			}
			state.PlatformArtifacts[index].ValidationResults = results
			state.PlatformArtifacts[index].Status = platformArtifactStatusFromValidation(results)
			state.PlatformArtifacts[index].UpdatedAt = time.Now().UTC()
			out = state.PlatformArtifacts[index]
			return nil
		}
		return ErrNotFound
	})
	return out, err
}

func (s *Store) ReleasePlatformArtifact(id string, req model.PlatformArtifactReleaseRequest, principal model.Principal) (model.PlatformArtifact, model.PlatformArtifactRelease, model.PlatformReleaseMessage, *model.PlatformLKGSnapshot, error) {
	channel := NormalizePlatformReleaseChannel(req.ReleaseChannel)
	if channel == "" {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgReleasePlatformArtifact(id, req, principal)
	}
	var artifact model.PlatformArtifact
	var release model.PlatformArtifactRelease
	var message model.PlatformReleaseMessage
	var lkg *model.PlatformLKGSnapshot
	err := s.withLockedState(true, func(state *model.State) error {
		index := platformArtifactIndex(state.PlatformArtifacts, id)
		if index < 0 {
			return ErrNotFound
		}
		artifact = state.PlatformArtifacts[index]
		now := time.Now().UTC()
		laneKey := platformsafety.ReleaseLaneKey(artifact.ArtifactKind, artifact.ScopeKey, channel)
		if existingRelease, ok := platformReleaseByIdempotencyKey(state.PlatformArtifactReleases, laneKey, req.IdempotencyKey); ok {
			existingArtifactIndex := platformArtifactIndex(state.PlatformArtifacts, existingRelease.ArtifactID)
			if existingArtifactIndex < 0 {
				return ErrNotFound
			}
			existingMessage, found := platformReleaseMessageForRelease(state.PlatformReleaseMessages, existingRelease.ID)
			if !found {
				return ErrConflict
			}
			artifact = state.PlatformArtifacts[existingArtifactIndex]
			release = existingRelease
			message = existingMessage
			return nil
		}
		if artifact.Status != model.PlatformArtifactStatusValidated {
			return ErrConflict
		}
		lkg = verifiedPlatformLKGSnapshotFromState(
			state,
			artifact.ArtifactKind,
			artifact.ScopeKey,
			now,
			s.platformArtifactSigningKeyring(),
		)
		pinnedRollbackGeneration := ""
		if lkg != nil {
			pinnedRollbackGeneration = lkg.Generation
		}
		previousGenerationSequence, err := activePlatformReleaseGenerationSequence(
			state.PlatformArtifactReleases,
			state.PlatformArtifacts,
			artifact.ArtifactKind,
			artifact.ScopeKey,
			channel,
		)
		if err != nil {
			return err
		}
		if decision := platformsafety.EvaluateArtifactRelease(
			artifact,
			channel,
			pinnedRollbackGeneration,
			req.CanaryRuleRef,
			previousGenerationSequence,
			s.platformArtifactSigningKeyring(),
		); !decision.Pass {
			return ErrConflict
		}
		lane, err := nextPlatformReleaseLane(state.PlatformReleaseLanes, artifact.ArtifactKind, artifact.ScopeKey, channel, now)
		if err != nil {
			return err
		}
		entry := buildPlatformArtifactReleaseLedgerEntry(
			artifact,
			channel,
			pinnedRollbackGeneration,
			req.CanaryRuleRef,
			req.Reason,
			req.IdempotencyKey,
			model.PlatformReleaseMessageTypeRelease,
			principal,
			lane,
			now,
		)
		release = entry.Release
		state.PlatformArtifactReleases = supersedePlatformReleases(state.PlatformArtifactReleases, artifact.ArtifactKind, artifact.ScopeKey, channel, release.ID, now)
		state.PlatformArtifactReleases = append(state.PlatformArtifactReleases, release)
		lane.ActiveReleaseID = release.ID
		state.PlatformReleaseLanes = upsertPlatformReleaseLane(state.PlatformReleaseLanes, lane)
		message = entry.Message
		state.PlatformReleaseMessages = append(state.PlatformReleaseMessages, message)
		return nil
	})
	return artifact, release, message, lkg, err
}

func (s *Store) RollbackPlatformArtifact(id string, req model.PlatformArtifactRollbackRequest, principal model.Principal) (model.PlatformArtifact, model.PlatformArtifactRelease, model.PlatformReleaseMessage, *model.PlatformLKGSnapshot, error) {
	if strings.TrimSpace(req.ToGeneration) == "" || strings.TrimSpace(req.Reason) == "" {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, ErrInvalidInput
	}
	channel := NormalizePlatformReleaseChannel(req.ReleaseChannel)
	if channel == "" {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgRollbackPlatformArtifact(id, req, principal)
	}
	var target model.PlatformArtifact
	var release model.PlatformArtifactRelease
	var message model.PlatformReleaseMessage
	var lkg *model.PlatformLKGSnapshot
	err := s.withLockedState(true, func(state *model.State) error {
		currentIndex := platformArtifactIndex(state.PlatformArtifacts, id)
		if currentIndex < 0 {
			return ErrNotFound
		}
		current := state.PlatformArtifacts[currentIndex]
		targetIndex := platformArtifactGenerationIndex(state.PlatformArtifacts, current.ArtifactKind, current.ScopeKey, strings.TrimSpace(req.ToGeneration))
		if targetIndex < 0 {
			return ErrNotFound
		}
		target = state.PlatformArtifacts[targetIndex]
		if target.Status != model.PlatformArtifactStatusValidated {
			return ErrConflict
		}
		now := time.Now().UTC()
		lkg = verifiedPlatformLKGSnapshotFromState(
			state,
			target.ArtifactKind,
			target.ScopeKey,
			now,
			s.platformArtifactSigningKeyring(),
		)
		pinnedRollbackGeneration := ""
		if lkg != nil {
			pinnedRollbackGeneration = lkg.Generation
		}
		canaryRuleRef := strings.TrimSpace(req.CanaryRuleRef)
		if channel == model.PlatformArtifactReleaseChannelGray && canaryRuleRef == "" {
			if activeRelease, ok := activePlatformReleaseForScope(state.PlatformArtifactReleases, current.ArtifactKind, current.ScopeKey, channel); ok {
				canaryRuleRef = activeRelease.CanaryRuleRef
			}
		}
		if decision := platformsafety.EvaluateArtifactRollback(
			target,
			channel,
			pinnedRollbackGeneration,
			canaryRuleRef,
			s.platformArtifactSigningKeyring(),
		); !decision.Pass {
			return ErrConflict
		}
		lane, err := nextPlatformReleaseLane(state.PlatformReleaseLanes, target.ArtifactKind, target.ScopeKey, channel, now)
		if err != nil {
			return err
		}
		entry := buildPlatformArtifactReleaseLedgerEntry(
			target,
			channel,
			pinnedRollbackGeneration,
			canaryRuleRef,
			req.Reason,
			"",
			model.PlatformReleaseMessageTypeRollback,
			principal,
			lane,
			now,
		)
		release = entry.Release
		state.PlatformArtifactReleases = supersedePlatformReleases(state.PlatformArtifactReleases, target.ArtifactKind, target.ScopeKey, channel, release.ID, now)
		state.PlatformArtifactReleases = append(state.PlatformArtifactReleases, release)
		lane.ActiveReleaseID = release.ID
		state.PlatformReleaseLanes = upsertPlatformReleaseLane(state.PlatformReleaseLanes, lane)
		message = entry.Message
		state.PlatformReleaseMessages = append(state.PlatformReleaseMessages, message)
		return nil
	})
	return target, release, message, lkg, err
}

func (s *Store) VerifyPlatformArtifactReleaseLKG(releaseID string, req model.PlatformArtifactVerifyLKGRequest, principal model.Principal) (model.PlatformArtifact, model.PlatformArtifactRelease, model.PlatformReleaseMessage, *model.PlatformLKGSnapshot, error) {
	releaseID = strings.TrimSpace(releaseID)
	if releaseID == "" {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgVerifyPlatformArtifactReleaseLKG(releaseID, req, principal)
	}
	var artifact model.PlatformArtifact
	var release model.PlatformArtifactRelease
	var message model.PlatformReleaseMessage
	var lkg *model.PlatformLKGSnapshot
	err := s.withLockedState(true, func(state *model.State) error {
		releaseIndex := platformArtifactReleaseIndex(state.PlatformArtifactReleases, releaseID)
		if releaseIndex < 0 {
			return ErrNotFound
		}
		release = state.PlatformArtifactReleases[releaseIndex]
		if release.Status != model.PlatformArtifactReleaseStatusActive {
			return ErrConflict
		}
		currentLKG := verifiedPlatformLKGSnapshotFromState(
			state,
			release.ArtifactKind,
			release.ScopeKey,
			time.Now().UTC(),
			s.platformArtifactSigningKeyring(),
		)
		requestEvidenceHash := platformsafety.VerificationEvidenceHash(req)
		if release.VerificationState == model.PlatformArtifactVerificationStateVerified {
			if currentLKG != nil &&
				currentLKG.Generation == release.Generation &&
				currentLKG.VerifiedByReleaseID == release.ID &&
				currentLKG.VerificationEvidenceHash == requestEvidenceHash {
				artifactIndex := platformArtifactIndex(state.PlatformArtifacts, release.ArtifactID)
				if artifactIndex < 0 {
					return ErrNotFound
				}
				artifact = state.PlatformArtifacts[artifactIndex]
				message, _ = platformReleaseMessageForRelease(state.PlatformReleaseMessages, release.ID)
				lkg = currentLKG
				return nil
			}
			return ErrConflict
		}
		lane, ok := platformReleaseLaneByKey(state.PlatformReleaseLanes, release.LaneKey)
		if !ok || lane.Frozen || lane.ActiveReleaseID != release.ID || lane.FencingToken != release.FencingToken {
			return ErrConflict
		}
		artifactIndex := platformArtifactIndex(state.PlatformArtifacts, release.ArtifactID)
		if artifactIndex < 0 {
			return ErrNotFound
		}
		artifact = state.PlatformArtifacts[artifactIndex]
		if decision := platformsafety.EvaluateArtifactIntegrity(artifact, s.platformArtifactSigningKeyring()); !decision.Pass {
			return ErrConflict
		}
		if decision := platformsafety.EvaluateLKGPromotion(release, req, currentLKG != nil); !decision.Pass {
			return ErrConflict
		}
		now := time.Now().UTC()
		snapshot, err := buildPlatformLKGSnapshot(
			artifact,
			release.ID,
			requestEvidenceHash,
			now,
			s.platformArtifactSigningKeyring(),
		)
		if err != nil {
			return err
		}
		state.PlatformLKGSnapshots = upsertPlatformLKGSnapshot(state.PlatformLKGSnapshots, snapshot)
		lkg = &snapshot
		release.VerificationState = model.PlatformArtifactVerificationStateVerified
		release.VerificationEvidence = platformsafety.VerificationEvidenceMap(req)
		release.VerifiedLKGGeneration = artifact.Generation
		release.ServingUnverifiedGeneration = ""
		release.VerifiedAt = &now
		release.Version++
		release.UpdatedAt = now
		state.PlatformArtifactReleases[releaseIndex] = release
		message = buildPlatformReleaseMessage(artifact, release, model.PlatformReleaseMessageTypeVerifiedLKG, now)
		state.PlatformReleaseMessages = append(state.PlatformReleaseMessages, message)
		return nil
	})
	return artifact, release, message, lkg, err
}

func (s *Store) GetActivePlatformArtifact(kind, scopeKey, channel string) (model.PlatformArtifact, model.PlatformArtifactRelease, bool, error) {
	kind = NormalizePlatformArtifactKind(kind)
	scopeKey = strings.TrimSpace(strings.ToLower(scopeKey))
	if scopeKey == "" {
		scopeKey = "global"
	}
	channel = NormalizePlatformReleaseChannel(channel)
	if kind == "" || channel == "" {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, false, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgGetActivePlatformArtifact(kind, scopeKey, channel)
	}
	var artifact model.PlatformArtifact
	var release model.PlatformArtifactRelease
	found := false
	err := s.withLockedState(false, func(state *model.State) error {
		for _, candidate := range state.PlatformArtifactReleases {
			if candidate.ArtifactKind == kind && candidate.ScopeKey == scopeKey && candidate.ReleaseChannel == channel && candidate.Status == model.PlatformArtifactReleaseStatusActive {
				if !found || candidate.ReleasedAt.After(release.ReleasedAt) {
					release = candidate
					found = true
				}
			}
		}
		if !found {
			return nil
		}
		index := platformArtifactIndex(state.PlatformArtifacts, release.ArtifactID)
		if index < 0 {
			return ErrNotFound
		}
		artifact = state.PlatformArtifacts[index]
		return nil
	})
	return artifact, release, found, err
}

func (s *Store) ListPlatformReleaseMessages(kind, scopeKey string, since time.Time, limit int) ([]model.PlatformReleaseMessage, error) {
	kind = NormalizePlatformArtifactKind(kind)
	scopeKey = strings.TrimSpace(strings.ToLower(scopeKey))
	if scopeKey == "" {
		scopeKey = "global"
	}
	if kind == "" {
		return nil, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgListPlatformReleaseMessages(kind, scopeKey, since, limit)
	}
	messages := []model.PlatformReleaseMessage{}
	err := s.withLockedState(false, func(state *model.State) error {
		for _, message := range state.PlatformReleaseMessages {
			if message.ArtifactKind != kind || message.ScopeKey != scopeKey {
				continue
			}
			if !since.IsZero() && !message.CreatedAt.After(since) {
				continue
			}
			messages = append(messages, message)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sortPlatformReleaseMessages(messages)
	if limit > 0 && len(messages) > limit {
		messages = messages[:limit]
	}
	return messages, nil
}

func (s *Store) UpsertPlatformConsumerHeartbeat(req model.PlatformConsumerHeartbeatRequest) (model.PlatformConsumerInstance, error) {
	consumer := normalizePlatformConsumerHeartbeat(req)
	if consumer.ConsumerID == "" || consumer.ArtifactKind == "" {
		return model.PlatformConsumerInstance{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgUpsertPlatformConsumerHeartbeat(consumer)
	}
	var out model.PlatformConsumerInstance
	err := s.withLockedState(true, func(state *model.State) error {
		for index := range state.PlatformConsumerInstances {
			if state.PlatformConsumerInstances[index].ConsumerID == consumer.ConsumerID &&
				state.PlatformConsumerInstances[index].ArtifactKind == consumer.ArtifactKind &&
				state.PlatformConsumerInstances[index].ScopeKey == consumer.ScopeKey {
				consumer.ID = state.PlatformConsumerInstances[index].ID
				state.PlatformConsumerInstances[index] = consumer
				out = consumer
				return nil
			}
		}
		state.PlatformConsumerInstances = append(state.PlatformConsumerInstances, consumer)
		out = consumer
		return nil
	})
	return out, err
}

func (s *Store) ListPlatformConsumers(kind, scopeKey string) ([]model.PlatformConsumerInstance, error) {
	kind = NormalizePlatformArtifactKind(kind)
	scopeKey = strings.TrimSpace(strings.ToLower(scopeKey))
	if scopeKey == "" {
		scopeKey = "global"
	}
	if kind == "" {
		return nil, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgListPlatformConsumers(kind, scopeKey)
	}
	consumers := []model.PlatformConsumerInstance{}
	err := s.withLockedState(false, func(state *model.State) error {
		for _, consumer := range state.PlatformConsumerInstances {
			if consumer.ArtifactKind == kind && consumer.ScopeKey == scopeKey {
				consumers = append(consumers, consumer)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sortPlatformConsumers(consumers)
	return consumers, nil
}

func (s *Store) GetPlatformLKG(kind, scopeKey string) (*model.PlatformLKGSnapshot, error) {
	kind = NormalizePlatformArtifactKind(kind)
	scopeKey = strings.TrimSpace(strings.ToLower(scopeKey))
	if scopeKey == "" {
		scopeKey = "global"
	}
	if kind == "" {
		return nil, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgGetPlatformLKG(kind, scopeKey)
	}
	var out *model.PlatformLKGSnapshot
	err := s.withLockedState(false, func(state *model.State) error {
		for index := range state.PlatformLKGSnapshots {
			snapshot := state.PlatformLKGSnapshots[index]
			if snapshot.ArtifactKind != kind || snapshot.ScopeKey != scopeKey {
				continue
			}
			if out == nil || snapshot.UpdatedAt.After(out.UpdatedAt) {
				copy := snapshot
				out = &copy
			}
		}
		return nil
	})
	return out, err
}

func normalizePlatformArtifactForStore(artifact model.PlatformArtifact) (model.PlatformArtifact, error) {
	now := time.Now().UTC()
	artifact.ID = strings.TrimSpace(artifact.ID)
	if artifact.ID == "" {
		artifact.ID = model.NewID("artifact")
	}
	artifact.ArtifactKind = NormalizePlatformArtifactKind(artifact.ArtifactKind)
	if artifact.ArtifactKind == "" || artifact.Content == nil {
		return model.PlatformArtifact{}, ErrInvalidInput
	}
	artifact.Scope, artifact.ScopeKey = NormalizePlatformArtifactScope(artifact.Scope)
	artifact.SchemaVersion = strings.TrimSpace(artifact.SchemaVersion)
	if artifact.SchemaVersion == "" {
		artifact.SchemaVersion = model.PlatformArtifactSchemaVersionV1
	}
	if artifact.SchemaVersion != model.PlatformArtifactSchemaVersionV1 {
		return model.PlatformArtifact{}, ErrInvalidInput
	}
	artifact.GenerationSequence = 0
	artifact.Provenance = model.PlatformArtifactProvenance{}
	canonicalContent, err := canonicalPlatformArtifactContent(artifact.Content)
	if err != nil {
		return model.PlatformArtifact{}, err
	}
	artifact.Content = canonicalContent
	contentHash, err := platformArtifactContentHash(artifact.Content)
	if err != nil {
		return model.PlatformArtifact{}, err
	}
	artifact.ContentHash = contentHash
	artifact.Generation = strings.TrimSpace(artifact.Generation)
	if artifact.Generation == "" {
		artifact.Generation = "gen_" + contentHash[:16]
	}
	artifact.Status = strings.TrimSpace(strings.ToLower(artifact.Status))
	if artifact.Status == "" {
		artifact.Status = model.PlatformArtifactStatusDraft
	}
	artifact.CompatibilityFloor = strings.TrimSpace(artifact.CompatibilityFloor)
	artifact.CreatedByType = strings.TrimSpace(artifact.CreatedByType)
	artifact.CreatedByID = strings.TrimSpace(artifact.CreatedByID)
	if artifact.Metadata == nil {
		artifact.Metadata = map[string]string{}
	}
	if artifact.CreatedAt.IsZero() {
		artifact.CreatedAt = now
	}
	artifact.UpdatedAt = now
	return artifact, nil
}

func nextPlatformArtifactGenerationSequence(artifacts []model.PlatformArtifact, kind, scopeKey string) int64 {
	var sequence int64
	for _, artifact := range artifacts {
		if artifact.ArtifactKind == kind &&
			artifact.ScopeKey == scopeKey &&
			artifact.GenerationSequence > sequence {
			sequence = artifact.GenerationSequence
		}
	}
	return sequence + 1
}

func platformArtifactContentHash(content map[string]any) (string, error) {
	raw, err := platformArtifactContentBytes(content)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func canonicalPlatformArtifactContent(content map[string]any) (map[string]any, error) {
	raw, err := json.Marshal(content)
	if err != nil {
		return nil, fmt.Errorf("marshal platform artifact content: %w", err)
	}
	var canonical map[string]any
	if err := json.Unmarshal(raw, &canonical); err != nil {
		return nil, fmt.Errorf("canonicalize platform artifact content: %w", err)
	}
	if canonical == nil {
		return nil, ErrInvalidInput
	}
	return canonical, nil
}

func platformArtifactContentBytes(content map[string]any) ([]byte, error) {
	raw, err := json.Marshal(content)
	if err != nil {
		return nil, fmt.Errorf("marshal platform artifact content: %w", err)
	}
	return raw, nil
}

func buildPlatformArtifactContent(artifact model.PlatformArtifact) model.PlatformArtifactContent {
	raw, _ := platformArtifactContentBytes(artifact.Content)
	return model.PlatformArtifactContent{
		ContentHash: artifact.ContentHash,
		Content:     artifact.Content,
		SizeBytes:   int64(len(raw)),
		CreatedAt:   artifact.CreatedAt,
		UpdatedAt:   artifact.UpdatedAt,
	}
}

func upsertPlatformArtifactContent(contents []model.PlatformArtifactContent, content model.PlatformArtifactContent) []model.PlatformArtifactContent {
	for index := range contents {
		if contents[index].ContentHash == content.ContentHash {
			if content.CreatedAt.IsZero() {
				content.CreatedAt = contents[index].CreatedAt
			}
			contents[index] = content
			return contents
		}
	}
	return append(contents, content)
}

func platformArtifactMatchesFilter(artifact model.PlatformArtifact, filter model.PlatformArtifactFilter) bool {
	if filter.ArtifactKind != "" && artifact.ArtifactKind != filter.ArtifactKind {
		return false
	}
	if filter.ScopeKey != "" && artifact.ScopeKey != filter.ScopeKey {
		return false
	}
	if filter.Status != "" && artifact.Status != filter.Status {
		return false
	}
	return true
}

func sortPlatformArtifacts(artifacts []model.PlatformArtifact) {
	sort.Slice(artifacts, func(i, j int) bool {
		if !artifacts[i].UpdatedAt.Equal(artifacts[j].UpdatedAt) {
			return artifacts[i].UpdatedAt.After(artifacts[j].UpdatedAt)
		}
		return artifacts[i].ID < artifacts[j].ID
	})
}

func normalizePlatformArtifactValidationResults(results []model.PlatformArtifactValidationResult) []model.PlatformArtifactValidationResult {
	out := make([]model.PlatformArtifactValidationResult, 0, len(results))
	for _, result := range results {
		result.Name = strings.TrimSpace(result.Name)
		if result.Name == "" {
			continue
		}
		result.Severity = strings.TrimSpace(strings.ToLower(result.Severity))
		if result.Severity == "" {
			result.Severity = model.RobustnessSeverityInfo
		}
		if result.Evidence == nil {
			result.Evidence = map[string]string{}
		}
		out = append(out, result)
	}
	return out
}

func platformArtifactStatusFromValidation(results []model.PlatformArtifactValidationResult) string {
	for _, result := range results {
		if !result.Pass {
			return model.PlatformArtifactStatusRejected
		}
	}
	return model.PlatformArtifactStatusValidated
}

func platformArtifactIndex(artifacts []model.PlatformArtifact, idOrGeneration string) int {
	idOrGeneration = strings.TrimSpace(idOrGeneration)
	for index, artifact := range artifacts {
		if artifact.ID == idOrGeneration || artifact.Generation == idOrGeneration {
			return index
		}
	}
	return -1
}

func platformArtifactGenerationIndex(artifacts []model.PlatformArtifact, kind, scopeKey, generation string) int {
	for index, artifact := range artifacts {
		if artifact.ArtifactKind == kind && artifact.ScopeKey == scopeKey && artifact.Generation == generation {
			return index
		}
	}
	return -1
}

type platformArtifactReleaseLedgerEntry struct {
	Artifact model.PlatformArtifact
	Release  model.PlatformArtifactRelease
	Message  model.PlatformReleaseMessage
}

func buildPlatformArtifactReleaseLedgerEntry(
	artifact model.PlatformArtifact,
	channel string,
	rollbackTargetGeneration string,
	canaryRuleRef string,
	reason string,
	idempotencyKey string,
	messageType string,
	principal model.Principal,
	lane model.PlatformReleaseLane,
	now time.Time,
) platformArtifactReleaseLedgerEntry {
	servingUnverifiedGeneration := ""
	if channel == model.PlatformArtifactReleaseChannelFull {
		servingUnverifiedGeneration = artifact.Generation
	}
	release := model.PlatformArtifactRelease{
		ID:                          model.NewID("artifactrel"),
		ArtifactID:                  artifact.ID,
		ArtifactKind:                artifact.ArtifactKind,
		Scope:                       artifact.Scope,
		ScopeKey:                    artifact.ScopeKey,
		Generation:                  artifact.Generation,
		ReleaseChannel:              channel,
		Status:                      model.PlatformArtifactReleaseStatusActive,
		LaneKey:                     lane.LaneKey,
		FencingToken:                lane.FencingToken,
		Version:                     1,
		IdempotencyKey:              strings.TrimSpace(idempotencyKey),
		CandidateGeneration:         artifact.Generation,
		ServingUnverifiedGeneration: servingUnverifiedGeneration,
		VerifiedLKGGeneration:       strings.TrimSpace(rollbackTargetGeneration),
		PinnedRollbackGeneration:    strings.TrimSpace(rollbackTargetGeneration),
		VerificationState:           model.PlatformArtifactVerificationStateServingUnverified,
		RollbackTargetGeneration:    strings.TrimSpace(rollbackTargetGeneration),
		CanaryRuleRef:               strings.TrimSpace(canaryRuleRef),
		Reason:                      strings.TrimSpace(reason),
		ReleasedByType:              strings.TrimSpace(principal.ActorType),
		ReleasedByID:                strings.TrimSpace(principal.ActorID),
		ReleasedAt:                  now,
		CreatedAt:                   now,
		UpdatedAt:                   now,
	}
	entry := platformArtifactReleaseLedgerEntry{
		Artifact: artifact,
		Release:  release,
		Message:  buildPlatformReleaseMessage(artifact, release, messageType, now),
	}
	return entry
}

func platformArtifactReleaseIndex(releases []model.PlatformArtifactRelease, releaseID string) int {
	releaseID = strings.TrimSpace(releaseID)
	for index, release := range releases {
		if release.ID == releaseID {
			return index
		}
	}
	return -1
}

func platformReleaseByIdempotencyKey(releases []model.PlatformArtifactRelease, laneKey, idempotencyKey string) (model.PlatformArtifactRelease, bool) {
	idempotencyKey = strings.TrimSpace(idempotencyKey)
	if idempotencyKey == "" {
		return model.PlatformArtifactRelease{}, false
	}
	for _, release := range releases {
		if release.LaneKey == laneKey && release.IdempotencyKey == idempotencyKey {
			return release, true
		}
	}
	return model.PlatformArtifactRelease{}, false
}

func activePlatformReleaseForScope(releases []model.PlatformArtifactRelease, kind, scopeKey, channel string) (model.PlatformArtifactRelease, bool) {
	var out model.PlatformArtifactRelease
	found := false
	for _, release := range releases {
		if release.ArtifactKind != kind ||
			release.ScopeKey != scopeKey ||
			release.ReleaseChannel != channel ||
			release.Status != model.PlatformArtifactReleaseStatusActive {
			continue
		}
		if !found || release.ReleasedAt.After(out.ReleasedAt) {
			out = release
			found = true
		}
	}
	return out, found
}

func activePlatformReleaseGenerationSequence(
	releases []model.PlatformArtifactRelease,
	artifacts []model.PlatformArtifact,
	kind string,
	scopeKey string,
	channel string,
) (int64, error) {
	release, found := activePlatformReleaseForScope(releases, kind, scopeKey, channel)
	if !found {
		return 0, nil
	}
	index := platformArtifactIndex(artifacts, release.ArtifactID)
	if index < 0 {
		return 0, ErrConflict
	}
	return artifacts[index].GenerationSequence, nil
}

func platformReleaseMessageForRelease(messages []model.PlatformReleaseMessage, releaseID string) (model.PlatformReleaseMessage, bool) {
	var out model.PlatformReleaseMessage
	found := false
	for _, message := range messages {
		if message.ReleaseID != releaseID {
			continue
		}
		if !found || message.CreatedAt.After(out.CreatedAt) {
			out = message
			found = true
		}
	}
	return out, found
}

func nextPlatformReleaseLane(lanes []model.PlatformReleaseLane, kind, scopeKey, channel string, now time.Time) (model.PlatformReleaseLane, error) {
	laneKey := platformsafety.ReleaseLaneKey(kind, scopeKey, channel)
	lane, ok := platformReleaseLaneByKey(lanes, laneKey)
	if !ok {
		return model.PlatformReleaseLane{
			LaneKey:        laneKey,
			ArtifactKind:   kind,
			ScopeKey:       scopeKey,
			ReleaseChannel: channel,
			FencingToken:   1,
			Version:        1,
			UpdatedAt:      now,
		}, nil
	}
	if lane.Frozen {
		return model.PlatformReleaseLane{}, ErrConflict
	}
	lane.FencingToken++
	lane.Version++
	lane.UpdatedAt = now
	return lane, nil
}

func platformReleaseLaneByKey(lanes []model.PlatformReleaseLane, laneKey string) (model.PlatformReleaseLane, bool) {
	for _, lane := range lanes {
		if lane.LaneKey == laneKey {
			return lane, true
		}
	}
	return model.PlatformReleaseLane{}, false
}

func upsertPlatformReleaseLane(lanes []model.PlatformReleaseLane, lane model.PlatformReleaseLane) []model.PlatformReleaseLane {
	out := make([]model.PlatformReleaseLane, 0, len(lanes)+1)
	for _, existing := range lanes {
		if existing.LaneKey == lane.LaneKey {
			continue
		}
		out = append(out, existing)
	}
	out = append(out, lane)
	return out
}

func supersedePlatformReleases(releases []model.PlatformArtifactRelease, kind, scopeKey, channel, keepID string, now time.Time) []model.PlatformArtifactRelease {
	out := make([]model.PlatformArtifactRelease, 0, len(releases))
	for _, release := range releases {
		if release.ID != keepID && release.ArtifactKind == kind && release.ScopeKey == scopeKey && release.ReleaseChannel == channel && release.Status == model.PlatformArtifactReleaseStatusActive {
			release.Status = model.PlatformArtifactReleaseStatusSuperseded
			release.UpdatedAt = now
		}
		out = append(out, release)
	}
	return out
}

func buildPlatformReleaseMessage(artifact model.PlatformArtifact, release model.PlatformArtifactRelease, messageType string, now time.Time) model.PlatformReleaseMessage {
	expiresAt := now.Add(7 * 24 * time.Hour)
	return model.PlatformReleaseMessage{
		ID:             model.NewID("artifactmsg"),
		ReleaseID:      release.ID,
		ArtifactID:     artifact.ID,
		ArtifactKind:   artifact.ArtifactKind,
		Scope:          artifact.Scope,
		ScopeKey:       artifact.ScopeKey,
		Generation:     artifact.Generation,
		ReleaseChannel: release.ReleaseChannel,
		MessageType:    messageType,
		CreatedAt:      now,
		ExpiresAt:      &expiresAt,
	}
}

func sortPlatformReleaseMessages(messages []model.PlatformReleaseMessage) {
	sort.Slice(messages, func(i, j int) bool {
		if !messages[i].CreatedAt.Equal(messages[j].CreatedAt) {
			return messages[i].CreatedAt.After(messages[j].CreatedAt)
		}
		return messages[i].ID < messages[j].ID
	})
}

func buildPlatformLKGSnapshot(
	artifact model.PlatformArtifact,
	releaseID string,
	evidenceHash string,
	now time.Time,
	keyring bundleauth.Keyring,
) (model.PlatformLKGSnapshot, error) {
	snapshot := model.PlatformLKGSnapshot{
		ID:                       model.NewID("artifactlkg"),
		ArtifactID:               artifact.ID,
		ArtifactKind:             artifact.ArtifactKind,
		Scope:                    artifact.Scope,
		ScopeKey:                 artifact.ScopeKey,
		SchemaVersion:            artifact.SchemaVersion,
		Generation:               artifact.Generation,
		GenerationSequence:       artifact.GenerationSequence,
		ContentHash:              artifact.ContentHash,
		ArtifactProvenance:       artifact.Provenance,
		VerifiedByReleaseID:      strings.TrimSpace(releaseID),
		VerificationEvidenceHash: strings.TrimSpace(evidenceHash),
		ExpiresAt:                now.Add(platformLKGDefaultTTL),
		CreatedAt:                now,
		UpdatedAt:                now,
	}
	return platformsafety.SignPlatformLKGSnapshot(snapshot, keyring)
}

func platformLKGSnapshotForScope(snapshots []model.PlatformLKGSnapshot, kind, scopeKey string) *model.PlatformLKGSnapshot {
	for index := range snapshots {
		snapshot := snapshots[index]
		if snapshot.ArtifactKind == kind && snapshot.ScopeKey == scopeKey {
			copy := snapshot
			return &copy
		}
	}
	return nil
}

func verifiedPlatformLKGSnapshotFromState(
	state *model.State,
	kind string,
	scopeKey string,
	now time.Time,
	keyring bundleauth.Keyring,
) *model.PlatformLKGSnapshot {
	if state == nil {
		return nil
	}
	snapshot := platformLKGSnapshotForScope(state.PlatformLKGSnapshots, kind, scopeKey)
	if snapshot == nil {
		return nil
	}
	artifactIndex := platformArtifactIndex(state.PlatformArtifacts, snapshot.ArtifactID)
	if artifactIndex < 0 || !platformLKGSnapshotMatchesArtifact(*snapshot, state.PlatformArtifacts[artifactIndex], now, keyring) {
		return nil
	}
	return snapshot
}

func platformLKGSnapshotMatchesArtifact(
	snapshot model.PlatformLKGSnapshot,
	artifact model.PlatformArtifact,
	now time.Time,
	keyring bundleauth.Keyring,
) bool {
	if artifact.Status != model.PlatformArtifactStatusValidated {
		return false
	}
	return platformsafety.EvaluatePlatformLKGSnapshot(snapshot, artifact, keyring, now).Pass
}

func upsertPlatformLKGSnapshot(snapshots []model.PlatformLKGSnapshot, snapshot model.PlatformLKGSnapshot) []model.PlatformLKGSnapshot {
	out := make([]model.PlatformLKGSnapshot, 0, len(snapshots)+1)
	for _, existing := range snapshots {
		if existing.ArtifactKind == snapshot.ArtifactKind && existing.ScopeKey == snapshot.ScopeKey {
			continue
		}
		out = append(out, existing)
	}
	out = append(out, snapshot)
	return out
}

func normalizePlatformConsumerHeartbeat(req model.PlatformConsumerHeartbeatRequest) model.PlatformConsumerInstance {
	now := time.Now().UTC()
	kind := NormalizePlatformArtifactKind(req.ArtifactKind)
	scopeKey := strings.TrimSpace(strings.ToLower(req.ScopeKey))
	if scopeKey == "" {
		scopeKey = "global"
	}
	consumerID := strings.TrimSpace(req.ConsumerID)
	id := model.NewID("artifactconsumer")
	if consumerID != "" {
		sum := sha256.Sum256([]byte(consumerID + "|" + kind + "|" + scopeKey))
		id = "artifactconsumer_" + hex.EncodeToString(sum[:8])
	}
	return model.PlatformConsumerInstance{
		ID:                id,
		ConsumerID:        consumerID,
		Component:         strings.TrimSpace(req.Component),
		NodeID:            strings.TrimSpace(req.NodeID),
		ArtifactKind:      kind,
		ScopeKey:          scopeKey,
		SupportedKinds:    normalizeStringList(req.SupportedKinds),
		DesiredGeneration: strings.TrimSpace(req.DesiredGeneration),
		ActualGeneration:  strings.TrimSpace(req.ActualGeneration),
		LKGGeneration:     strings.TrimSpace(req.LKGGeneration),
		ApplyStatus:       strings.TrimSpace(strings.ToLower(req.ApplyStatus)),
		ProbeStatus:       strings.TrimSpace(strings.ToLower(req.ProbeStatus)),
		ServingLKG:        req.ServingLKG,
		LKGExpired:        req.LKGExpired,
		LastError:         strings.TrimSpace(req.LastError),
		LastHeartbeatAt:   now,
		UpdatedAt:         now,
	}
}

func sortPlatformConsumers(consumers []model.PlatformConsumerInstance) {
	sort.Slice(consumers, func(i, j int) bool {
		if !consumers[i].UpdatedAt.Equal(consumers[j].UpdatedAt) {
			return consumers[i].UpdatedAt.After(consumers[j].UpdatedAt)
		}
		return consumers[i].ConsumerID < consumers[j].ConsumerID
	})
}
