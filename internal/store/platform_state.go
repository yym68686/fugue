package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformcontrol"
	"fugue/internal/platformsafety"
)

const (
	platformLKGDefaultTTL          = 7 * 24 * time.Hour
	platformLKGHistoryDefaultLimit = 3
)

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
	model.PlatformArtifactKindPlatformIntent:            {},
	model.PlatformArtifactKindPolicySnapshot:            {},
	model.PlatformArtifactKindReleaseSet:                {},
	model.PlatformArtifactKindHostedPathProposal:        {},
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
	if NormalizePlatformArtifactKind(in.ArtifactKind) == model.PlatformArtifactKindHostedPathProposal {
		return model.PlatformArtifact{}, ErrInvalidInput
	}
	artifact, err := normalizePlatformArtifactForStore(in)
	if err != nil {
		return model.PlatformArtifact{}, err
	}
	return s.pgCreatePlatformArtifact(artifact)
}

// EnsurePlatformArtifact creates an immutable generation once and otherwise
// returns the existing equivalent artifact. A reused generation with different
// signed inputs is a conflict rather than an update.
func (s *Store) EnsurePlatformArtifact(in model.PlatformArtifact) (model.PlatformArtifact, bool, error) {
	if NormalizePlatformArtifactKind(in.ArtifactKind) == model.PlatformArtifactKindHostedPathProposal {
		return model.PlatformArtifact{}, false, ErrInvalidInput
	}
	return s.ensurePlatformArtifact(in)
}

func (s *Store) ensurePlatformArtifact(in model.PlatformArtifact) (model.PlatformArtifact, bool, error) {
	artifact, err := normalizePlatformArtifactForStore(in)
	if err != nil {
		return model.PlatformArtifact{}, false, err
	}
	return s.pgEnsurePlatformArtifact(artifact)
}

func (s *Store) GetPlatformArtifactByIdentity(kind, scope, generation string) (model.PlatformArtifact, error) {
	kind = NormalizePlatformArtifactKind(kind)
	scope = strings.ToLower(strings.TrimSpace(scope))
	generation = strings.TrimSpace(generation)
	if kind == "" || scope == "" || generation == "" {
		return model.PlatformArtifact{}, ErrInvalidInput
	}
	return s.pgGetPlatformArtifactByIdentity(kind, scope, generation)
}

func ensurePlatformArtifactEquivalent(existing, candidate model.PlatformArtifact, keyring bundleauth.Keyring) error {
	decision := platformsafety.EvaluateArtifactIntegrity(existing, keyring)
	if !decision.Pass {
		return fmt.Errorf("%w: existing platform artifact integrity verification failed", ErrConflict)
	}
	if existing.Scope != candidate.Scope ||
		strings.TrimSpace(existing.SchemaVersion) != strings.TrimSpace(candidate.SchemaVersion) ||
		!strings.EqualFold(strings.TrimSpace(existing.ContentHash), strings.TrimSpace(candidate.ContentHash)) ||
		strings.TrimSpace(existing.CompatibilityFloor) != strings.TrimSpace(candidate.CompatibilityFloor) ||
		strings.TrimSpace(existing.CreatedByType) != strings.TrimSpace(candidate.CreatedByType) ||
		strings.TrimSpace(existing.CreatedByID) != strings.TrimSpace(candidate.CreatedByID) ||
		!equalPlatformArtifactMetadata(existing.Metadata, candidate.Metadata) {
		return fmt.Errorf("%w: platform artifact generation is already bound to different immutable content or metadata", ErrConflict)
	}
	return nil
}

func equalPlatformArtifactMetadata(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if rightValue, ok := right[key]; !ok || rightValue != value {
			return false
		}
	}
	return true
}

func (s *Store) ListPlatformArtifacts(filter model.PlatformArtifactFilter) ([]model.PlatformArtifact, error) {
	return s.listPlatformArtifacts(filter, true)
}

// ListPlatformArtifactMetadata preserves listing filters, order, and metadata
// without loading executable artifact content. It must not be used for content
// validation, signature verification, publication, or application.
func (s *Store) ListPlatformArtifactMetadata(filter model.PlatformArtifactFilter) ([]model.PlatformArtifact, error) {
	return s.listPlatformArtifacts(filter, false)
}

func (s *Store) listPlatformArtifacts(filter model.PlatformArtifactFilter, includeContent bool) ([]model.PlatformArtifact, error) {
	filter.ArtifactKind = NormalizePlatformArtifactKind(filter.ArtifactKind)
	filter.ScopeKey = strings.TrimSpace(strings.ToLower(filter.ScopeKey))
	filter.Status = strings.TrimSpace(strings.ToLower(filter.Status))
	filter.Generation = strings.TrimSpace(filter.Generation)
	return s.pgListPlatformArtifactsView(filter, includeContent)
}

func (s *Store) GetPlatformArtifact(idOrGeneration string) (model.PlatformArtifact, error) {
	return s.GetPlatformArtifactContext(context.Background(), idOrGeneration)
}

func (s *Store) GetPlatformArtifactContext(ctx context.Context, idOrGeneration string) (model.PlatformArtifact, error) {
	idOrGeneration = strings.TrimSpace(idOrGeneration)
	if idOrGeneration == "" {
		return model.PlatformArtifact{}, ErrInvalidInput
	}
	if err := ctx.Err(); err != nil {
		return model.PlatformArtifact{}, err
	}
	return s.pgGetPlatformArtifact(ctx, idOrGeneration)
}

func (s *Store) GetPlatformArtifactContent(contentHash string) (model.PlatformArtifactContent, error) {
	contentHash = strings.TrimSpace(contentHash)
	if contentHash == "" {
		return model.PlatformArtifactContent{}, ErrInvalidInput
	}
	return s.pgGetPlatformArtifactContent(contentHash)
}

func (s *Store) ValidatePlatformArtifact(id string, results []model.PlatformArtifactValidationResult) (model.PlatformArtifact, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.PlatformArtifact{}, ErrInvalidInput
	}
	results = normalizePlatformArtifactValidationResults(results)
	return s.pgValidatePlatformArtifact(id, results)
}

func (s *Store) ReleasePlatformArtifact(id string, req model.PlatformArtifactReleaseRequest, principal model.Principal) (model.PlatformArtifact, model.PlatformArtifactRelease, model.PlatformReleaseMessage, *model.PlatformLKGSnapshot, error) {
	return s.releasePlatformArtifact(id, req, principal, nil)
}

func (s *Store) releasePlatformArtifact(id string, req model.PlatformArtifactReleaseRequest, principal model.Principal, guard *platformProducerReleaseGuard) (model.PlatformArtifact, model.PlatformArtifactRelease, model.PlatformReleaseMessage, *model.PlatformLKGSnapshot, error) {
	channel := NormalizePlatformReleaseChannel(req.ReleaseChannel)
	if channel == "" {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, ErrInvalidInput
	}
	return s.pgReleasePlatformArtifact(id, req, principal, guard)
}

func (s *Store) RollbackPlatformArtifact(id string, req model.PlatformArtifactRollbackRequest, principal model.Principal) (model.PlatformArtifact, model.PlatformArtifactRelease, model.PlatformReleaseMessage, *model.PlatformLKGSnapshot, error) {
	return s.rollbackPlatformArtifact(id, req, principal, nil)
}

func (s *Store) rollbackPlatformArtifact(id string, req model.PlatformArtifactRollbackRequest, principal model.Principal, guard *platformProducerReleaseGuard) (model.PlatformArtifact, model.PlatformArtifactRelease, model.PlatformReleaseMessage, *model.PlatformLKGSnapshot, error) {
	if strings.TrimSpace(req.ToGeneration) == "" || strings.TrimSpace(req.Reason) == "" {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, ErrInvalidInput
	}
	channel := NormalizePlatformReleaseChannel(req.ReleaseChannel)
	if channel == "" {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, ErrInvalidInput
	}
	return s.pgRollbackPlatformArtifact(id, req, principal, guard)
}

func (s *Store) VerifyPlatformArtifactReleaseLKG(releaseID string, req model.PlatformArtifactVerifyLKGRequest, principal model.Principal) (model.PlatformArtifact, model.PlatformArtifactRelease, model.PlatformReleaseMessage, *model.PlatformLKGSnapshot, error) {
	return s.verifyPlatformArtifactReleaseLKG(releaseID, req, principal, nil)
}

func (s *Store) verifyPlatformArtifactReleaseLKG(releaseID string, req model.PlatformArtifactVerifyLKGRequest, principal model.Principal, guard *platformProducerReleaseGuard) (model.PlatformArtifact, model.PlatformArtifactRelease, model.PlatformReleaseMessage, *model.PlatformLKGSnapshot, error) {
	releaseID = strings.TrimSpace(releaseID)
	if releaseID == "" {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, ErrInvalidInput
	}
	return s.pgVerifyPlatformArtifactReleaseLKG(releaseID, req, principal, guard)
}

func (s *Store) GetActivePlatformArtifact(kind, scopeKey, channel string) (model.PlatformArtifact, model.PlatformArtifactRelease, bool, error) {
	return s.GetActivePlatformArtifactWithReader(kind, scopeKey, channel, nil)
}

// A caller-owned reader can reuse immutable artifacts within one request. The
// active release row is always fetched afresh; this never caches a lane/fence.
func (s *Store) GetActivePlatformArtifactWithReader(kind, scopeKey, channel string, read func(string) (model.PlatformArtifact, error)) (model.PlatformArtifact, model.PlatformArtifactRelease, bool, error) {
	return s.GetActivePlatformArtifactWithReaderContext(context.Background(), kind, scopeKey, channel, read)
}

func (s *Store) GetActivePlatformArtifactWithReaderContext(ctx context.Context, kind, scopeKey, channel string, read func(string) (model.PlatformArtifact, error)) (model.PlatformArtifact, model.PlatformArtifactRelease, bool, error) {
	kind = NormalizePlatformArtifactKind(kind)
	scopeKey = strings.TrimSpace(strings.ToLower(scopeKey))
	if scopeKey == "" {
		scopeKey = "global"
	}
	channel = NormalizePlatformReleaseChannel(channel)
	if kind == "" || channel == "" {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, false, ErrInvalidInput
	}
	if err := ctx.Err(); err != nil {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, false, err
	}
	return s.pgGetActivePlatformArtifactWithReader(ctx, kind, scopeKey, channel, read)
}

// CurrentFullRollbackMessage returns historical identity, never serving authority.
// Uniqueness and the identity of the selected row must hold in the same snapshot.
func (s *Store) CurrentFullRollbackMessage(ctx context.Context, expected model.PlatformArtifactRelease) (*model.PlatformReleaseMessage, error) {
	if s == nil || expected.ID == "" || expected.Status != model.PlatformArtifactReleaseStatusActive ||
		expected.ReleaseChannel != model.PlatformArtifactReleaseChannelFull {
		return nil, ErrInvalidInput
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.pgCurrentFullRollbackMessage(ctx, expected)
}

// GetPlatformArtifactRelease returns an immutable release ledger entry by ID.
// Consumers use this read-only lookup to bind runtime evidence to the exact
// fencing token selected by the release controller.
func (s *Store) GetPlatformArtifactRelease(id string) (model.PlatformArtifactRelease, error) {
	return s.GetPlatformArtifactReleaseContext(context.Background(), id)
}

func (s *Store) GetPlatformArtifactReleaseContext(ctx context.Context, id string) (model.PlatformArtifactRelease, error) {
	id = strings.TrimSpace(id)
	if s == nil || id == "" {
		return model.PlatformArtifactRelease{}, ErrInvalidInput
	}
	if err := ctx.Err(); err != nil {
		return model.PlatformArtifactRelease{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return pgGetPlatformArtifactRelease(ctx, s.db, id, false)
}

func (s *Store) VerifyPlatformArtifactIntegrity(artifact model.PlatformArtifact) error {
	if s == nil {
		return ErrInvalidInput
	}
	if decision := platformsafety.EvaluateArtifactIntegrity(artifact, s.platformArtifactSigningKeyring()); !decision.Pass {
		return ErrConflict
	}
	return nil
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
	return s.pgListPlatformReleaseMessages(kind, scopeKey, since, limit)
}

func (s *Store) UpsertPlatformConsumerHeartbeat(req model.PlatformConsumerHeartbeatRequest) (model.PlatformConsumerInstance, error) {
	consumer := normalizePlatformConsumerHeartbeat(req)
	if consumer.ConsumerID == "" || consumer.ArtifactKind == "" {
		return model.PlatformConsumerInstance{}, ErrInvalidInput
	}
	return s.pgUpsertPlatformConsumerHeartbeat(consumer)
}

func (s *Store) AcceptTrustedPlatformConsumerHeartbeat(
	claims platformcontrol.PlatformComponentIdentityClaims,
	expectedSetID string,
	heartbeat platformcontrol.PlatformConsumerHeartbeatEnvelope,
	receivedAt time.Time,
	policy platformcontrol.PlatformConsumerHeartbeatValidationPolicy,
) (model.PlatformConsumerInstance, error) {
	return s.acceptTrustedPlatformConsumerHeartbeat(claims, expectedSetID, heartbeat, receivedAt, policy, nil)
}

func (s *Store) AcceptTrustedPlatformConsumerHeartbeatWithAudit(
	claims platformcontrol.PlatformComponentIdentityClaims,
	expectedSetID string,
	heartbeat platformcontrol.PlatformConsumerHeartbeatEnvelope,
	receivedAt time.Time,
	policy platformcontrol.PlatformConsumerHeartbeatValidationPolicy,
	auditKeyring bundleauth.Keyring,
) (model.PlatformConsumerInstance, error) {
	return s.acceptTrustedPlatformConsumerHeartbeat(claims, expectedSetID, heartbeat, receivedAt, policy, &auditKeyring)
}

// Only diagnostic lineage is copied. Store-owned cancellation and deadlines
// remain independent of the HTTP request, including a disconnected caller.
func (s *Store) AcceptTrustedPlatformConsumerHeartbeatWithAuditObserved(ctx context.Context, claims platformcontrol.PlatformComponentIdentityClaims, expectedSetID string, heartbeat platformcontrol.PlatformConsumerHeartbeatEnvelope, receivedAt time.Time, policy platformcontrol.PlatformConsumerHeartbeatValidationPolicy, auditKeyring bundleauth.Keyring) (model.PlatformConsumerInstance, error) {
	return s.acceptTrustedPlatformConsumerHeartbeat(claims, expectedSetID, heartbeat, receivedAt, policy, &auditKeyring, ctx)
}

func (s *Store) acceptTrustedPlatformConsumerHeartbeat(
	claims platformcontrol.PlatformComponentIdentityClaims,
	expectedSetID string,
	heartbeat platformcontrol.PlatformConsumerHeartbeatEnvelope,
	receivedAt time.Time,
	policy platformcontrol.PlatformConsumerHeartbeatValidationPolicy,
	auditKeyring *bundleauth.Keyring,
	observation ...context.Context,
) (model.PlatformConsumerInstance, error) {
	expectedSetID = strings.TrimSpace(expectedSetID)
	if expectedSetID == "" {
		return model.PlatformConsumerInstance{}, ErrInvalidInput
	}
	return s.pgAcceptTrustedPlatformConsumerHeartbeat(claims, expectedSetID, heartbeat, receivedAt, policy, auditKeyring, observation...)
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
	return s.pgListPlatformConsumers(kind, scopeKey)
}

// ListPlatformAuthorityConsumers reads retained global/cell identities for
// transport-authority selection. A node read is capped at 64 receipts and an
// inventory read at 4096. Overflow fails instead of silently selecting from a
// truncated set. The result grants no authority without exact member and
// selected-backend verification by the caller.
func (s *Store) ListPlatformAuthorityConsumers(kind, nodeID string) ([]model.PlatformConsumerInstance, error) {
	return s.ListPlatformAuthorityConsumersContext(context.Background(), kind, nodeID)
}

func (s *Store) ListPlatformAuthorityConsumersContext(ctx context.Context, kind, nodeID string) ([]model.PlatformConsumerInstance, error) {
	kind = NormalizePlatformArtifactKind(kind)
	if kind == "" || strings.TrimSpace(nodeID) != nodeID || len(nodeID) > 253 {
		return nil, ErrInvalidInput
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	limit := 4096
	if nodeID != "" {
		limit = 64
	}
	return s.pgListPlatformAuthorityConsumers(ctx, kind, nodeID, limit)
}

func (s *Store) CreatePlatformExpectedConsumerSet(in model.PlatformExpectedConsumerSet) (model.PlatformExpectedConsumerSet, error) {
	set, err := normalizePlatformExpectedConsumerSetForStore(in)
	if err != nil {
		return model.PlatformExpectedConsumerSet{}, err
	}
	return s.pgCreatePlatformExpectedConsumerSet(set)
}

func (s *Store) GetPlatformExpectedConsumerSet(id string) (model.PlatformExpectedConsumerSet, error) {
	return s.GetPlatformExpectedConsumerSetContext(context.Background(), id)
}

func (s *Store) GetPlatformExpectedConsumerSetContext(ctx context.Context, id string) (model.PlatformExpectedConsumerSet, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.PlatformExpectedConsumerSet{}, ErrInvalidInput
	}
	if err := ctx.Err(); err != nil {
		return model.PlatformExpectedConsumerSet{}, err
	}
	return s.pgGetPlatformExpectedConsumerSet(ctx, id)
}

func (s *Store) ListPlatformExpectedConsumerSets(filter model.PlatformExpectedConsumerSetFilter) ([]model.PlatformExpectedConsumerSet, error) {
	return s.ListPlatformExpectedConsumerSetsContext(context.Background(), filter)
}

func (s *Store) ListPlatformExpectedConsumerSetsContext(ctx context.Context, filter model.PlatformExpectedConsumerSetFilter) ([]model.PlatformExpectedConsumerSet, error) {
	filter, err := normalizePlatformExpectedConsumerSetFilter(filter)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.pgListPlatformExpectedConsumerSets(ctx, filter)
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
	return s.pgGetPlatformLKG(kind, scopeKey)
}

func (s *Store) ListPlatformLKGHistory(kind, scopeKey string, limit int) ([]model.PlatformLKGSnapshot, error) {
	kind = NormalizePlatformArtifactKind(kind)
	scopeKey = strings.TrimSpace(strings.ToLower(scopeKey))
	if scopeKey == "" {
		scopeKey = "global"
	}
	if kind == "" || limit < 0 {
		return nil, ErrInvalidInput
	}
	if limit == 0 {
		limit = platformLKGHistoryDefaultLimit
	}
	return s.pgListPlatformLKGHistory(kind, scopeKey, limit)
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

func verifyReusedPlatformArtifactContent(existing, candidate model.PlatformArtifactContent) error {
	existingBytes, err := platformArtifactContentBytes(existing.Content)
	if err != nil {
		return err
	}
	candidateBytes, err := platformArtifactContentBytes(candidate.Content)
	if err != nil {
		return err
	}
	if existing.ContentHash != candidate.ContentHash || existing.SizeBytes != candidate.SizeBytes || !bytes.Equal(existingBytes, candidateBytes) {
		return fmt.Errorf("%w: existing platform artifact content differs", ErrConflict)
	}
	return nil
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
	overrideMode string,
	overrideExpiresAt *time.Time,
	bypassedInvariants []string,
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
		OverrideMode:                strings.TrimSpace(overrideMode),
		OverrideExpiresAt:           clonePlatformTimePointer(overrideExpiresAt),
		BypassedInvariants:          append([]string(nil), bypassedInvariants...),
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

func platformArtifactOverrideMode(
	softOverride bool,
	forcePublish bool,
	kernelBreakGlass *model.PlatformKernelBreakGlassRequest,
) (string, error) {
	softOverride = softOverride || forcePublish
	if softOverride && kernelBreakGlass != nil {
		return "", ErrInvalidInput
	}
	if kernelBreakGlass != nil {
		return model.PlatformArtifactOverrideModeKernelBreakGlass, nil
	}
	if softOverride {
		return model.PlatformArtifactOverrideModeSoft, nil
	}
	return model.PlatformArtifactOverrideModeNone, nil
}

func validatePlatformArtifactOverrideAuthorization(
	overrideMode string,
	reason string,
	kernelBreakGlass *model.PlatformKernelBreakGlassRequest,
	artifact model.PlatformArtifact,
	principal model.Principal,
	now time.Time,
) error {
	switch overrideMode {
	case model.PlatformArtifactOverrideModeNone:
		return nil
	case model.PlatformArtifactOverrideModeSoft:
		if strings.TrimSpace(reason) == "" ||
			(!principal.HasScope("artifact.soft_override") &&
				!principal.HasScope("artifact.force_publish")) {
			return ErrInvalidInput
		}
		return nil
	case model.PlatformArtifactOverrideModeKernelBreakGlass:
		if strings.TrimSpace(reason) == "" ||
			!principal.IsPlatformAdmin() ||
			!principal.HasExplicitScope("artifact.kernel_break_glass") {
			return ErrInvalidInput
		}
		if err := platformsafety.ValidateKernelBreakGlassAuthorization(kernelBreakGlass, artifact, now); err != nil {
			return ErrInvalidInput
		}
		return nil
	default:
		return ErrInvalidInput
	}
}

func platformKernelBreakGlassExpiry(authorization *model.PlatformKernelBreakGlassRequest) *time.Time {
	if authorization == nil || authorization.ExpiresAt.IsZero() {
		return nil
	}
	expiresAt := authorization.ExpiresAt.UTC()
	return &expiresAt
}

func clonePlatformTimePointer(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := value.UTC()
	return &copy
}

func buildPlatformSafetyOverrideAuditEvent(
	release model.PlatformArtifactRelease,
	messageType string,
	sequence int64,
	previousHash string,
	keyring bundleauth.Keyring,
) (model.AuditEvent, error) {
	action := "platform_artifact." + release.OverrideMode + "_" + strings.TrimSpace(messageType)
	metadata := map[string]string{
		"artifact_id":          release.ArtifactID,
		"artifact_kind":        release.ArtifactKind,
		"scope_key":            release.ScopeKey,
		"generation":           release.Generation,
		"release_channel":      release.ReleaseChannel,
		"override_mode":        release.OverrideMode,
		"bypassed_invariants":  strings.Join(release.BypassedInvariants, ","),
		"reason":               release.Reason,
		"fencing_token":        fmt.Sprintf("%d", release.FencingToken),
		"automatic_expiration": "operation_scoped",
	}
	if release.OverrideExpiresAt != nil {
		metadata["override_expires_at"] = release.OverrideExpiresAt.UTC().Format(time.RFC3339Nano)
	}
	return platformsafety.SignTamperEvidentAuditEvent(model.AuditEvent{
		ID:            model.NewID("audit"),
		ActorType:     release.ReleasedByType,
		ActorID:       release.ReleasedByID,
		Action:        action,
		TargetType:    "platform_artifact_release",
		TargetID:      release.ID,
		Metadata:      metadata,
		ChainID:       platformsafety.PlatformSafetyAuditChainID,
		ChainSequence: sequence,
		PreviousHash:  previousHash,
		CreatedAt:     release.CreatedAt,
	}, keyring)
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
	var out *model.PlatformLKGSnapshot
	for index := range snapshots {
		snapshot := snapshots[index]
		if snapshot.ArtifactKind == kind && snapshot.ScopeKey == scopeKey &&
			(out == nil || platformLKGSnapshotIsNewer(snapshot, *out)) {
			copy := snapshot
			out = &copy
		}
	}
	return out
}

func platformLKGSnapshotIsNewer(candidate, current model.PlatformLKGSnapshot) bool {
	if !candidate.UpdatedAt.Equal(current.UpdatedAt) {
		return candidate.UpdatedAt.After(current.UpdatedAt)
	}
	if !candidate.CreatedAt.Equal(current.CreatedAt) {
		return candidate.CreatedAt.After(current.CreatedAt)
	}
	if candidate.GenerationSequence != current.GenerationSequence {
		return candidate.GenerationSequence > current.GenerationSequence
	}
	return candidate.ID > current.ID
}

func verifiedPlatformLKGSnapshotFromState(
	state *storeProjection, kind string,
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

func normalizePlatformConsumerHeartbeat(req model.PlatformConsumerHeartbeatRequest) model.PlatformConsumerInstance {
	now := time.Now().UTC()
	kind := NormalizePlatformArtifactKind(req.ArtifactKind)
	scopeKey := strings.TrimSpace(strings.ToLower(req.ScopeKey))
	if scopeKey == "" {
		scopeKey = "global"
	}
	consumerID := strings.TrimSpace(req.ConsumerID)
	id := platformConsumerInstanceID(consumerID, kind, scopeKey)
	var issuedAt *time.Time
	if req.IssuedAt != nil && !req.IssuedAt.IsZero() {
		value := req.IssuedAt.UTC()
		issuedAt = &value
	}
	return model.PlatformConsumerInstance{
		ID:                        id,
		ConsumerID:                consumerID,
		Component:                 strings.TrimSpace(req.Component),
		NodeID:                    strings.TrimSpace(req.NodeID),
		ArtifactKind:              kind,
		ScopeKey:                  scopeKey,
		ReleaseSetID:              strings.TrimSpace(req.ReleaseSetID),
		ExpectedConsumerSetID:     strings.TrimSpace(req.ExpectedConsumerSetID),
		FencingToken:              req.FencingToken,
		SupportedKinds:            normalizeStringList(req.SupportedKinds),
		ProtocolVersion:           strings.TrimSpace(strings.ToLower(req.ProtocolVersion)),
		SchemaVersion:             strings.TrimSpace(strings.ToLower(req.SchemaVersion)),
		CompatibilityCapabilities: normalizeStringList(req.CompatibilityCapabilities),
		Sequence:                  req.Sequence,
		IssuedAt:                  issuedAt,
		Nonce:                     strings.TrimSpace(req.Nonce),
		GenerationSequence:        req.GenerationSequence,
		EvidenceHash:              strings.TrimSpace(strings.ToLower(req.EvidenceHash)),
		DesiredGeneration:         strings.TrimSpace(req.DesiredGeneration),
		ActualGeneration:          strings.TrimSpace(req.ActualGeneration),
		CandidateGeneration:       strings.TrimSpace(req.CandidateGeneration),
		LKGGeneration:             strings.TrimSpace(req.LKGGeneration),
		ApplyStatus:               strings.TrimSpace(strings.ToLower(req.ApplyStatus)),
		ProbeStatus:               strings.TrimSpace(strings.ToLower(req.ProbeStatus)),
		ServingLKG:                req.ServingLKG,
		LKGExpired:                req.LKGExpired,
		LastError:                 strings.TrimSpace(req.LastError),
		LastHeartbeatAt:           now,
		UpdatedAt:                 now,
	}
}

func buildPlatformConsumerHeartbeatAuditEvent(
	consumer model.PlatformConsumerInstance,
	sequence int64,
	previousHash string,
	keyring bundleauth.Keyring,
) (model.AuditEvent, error) {
	chainID := platformConsumerHeartbeatAuditChainID(consumer)
	if chainID == "" || sequence <= 0 || !consumer.IdentityVerified ||
		strings.TrimSpace(consumer.CredentialID) == "" || strings.TrimSpace(consumer.EvidenceHash) == "" ||
		consumer.Sequence <= 0 || consumer.IssuedAt == nil || consumer.IssuedAt.IsZero() ||
		consumer.LastHeartbeatAt.IsZero() {
		return model.AuditEvent{}, ErrInvalidInput
	}
	metadata := map[string]string{
		"consumer_id":              strings.TrimSpace(consumer.ConsumerID),
		"credential_id":            strings.TrimSpace(consumer.CredentialID),
		"token_id":                 strings.TrimSpace(consumer.TokenID),
		"component":                strings.TrimSpace(consumer.Component),
		"node_id":                  strings.TrimSpace(consumer.NodeID),
		"artifact_kind":            strings.TrimSpace(consumer.ArtifactKind),
		"scope_key":                strings.TrimSpace(consumer.ScopeKey),
		"release_set_id":           strings.TrimSpace(consumer.ReleaseSetID),
		"expected_consumer_set_id": strings.TrimSpace(consumer.ExpectedConsumerSetID),
		"fencing_token":            fmt.Sprintf("%d", consumer.FencingToken),
		"heartbeat_sequence":       fmt.Sprintf("%d", consumer.Sequence),
		"issued_at":                consumer.IssuedAt.UTC().Format(time.RFC3339Nano),
		"generation_sequence":      fmt.Sprintf("%d", consumer.GenerationSequence),
		"desired_generation":       strings.TrimSpace(consumer.DesiredGeneration),
		"actual_generation":        strings.TrimSpace(consumer.ActualGeneration),
		"candidate_generation":     strings.TrimSpace(consumer.CandidateGeneration),
		"lkg_generation":           strings.TrimSpace(consumer.LKGGeneration),
		"apply_status":             strings.TrimSpace(consumer.ApplyStatus),
		"probe_status":             strings.TrimSpace(consumer.ProbeStatus),
		"serving_lkg":              fmt.Sprintf("%t", consumer.ServingLKG),
		"lkg_expired":              fmt.Sprintf("%t", consumer.LKGExpired),
		"last_error_present":       fmt.Sprintf("%t", strings.TrimSpace(consumer.LastError) != ""),
		"evidence_hash":            strings.TrimSpace(strings.ToLower(consumer.EvidenceHash)),
	}
	action, targetType := "platform_consumer.heartbeat_accepted", "platform_consumer"
	if strings.HasPrefix(consumer.ID, "shadow_") {
		action, targetType = "platform_consumer.shadow_observed", "platform_shadow_consumer"
	}
	return platformsafety.SignTamperEvidentAuditEvent(model.AuditEvent{
		ID:            model.NewID("audit"),
		ActorType:     "platform_component",
		ActorID:       strings.TrimSpace(consumer.CredentialID),
		Action:        action,
		TargetType:    targetType,
		TargetID:      strings.TrimSpace(consumer.ID),
		Metadata:      metadata,
		ChainID:       chainID,
		ChainSequence: sequence,
		PreviousHash:  strings.TrimSpace(previousHash),
		CreatedAt:     consumer.LastHeartbeatAt.UTC(),
	}, keyring)
}

func platformConsumerHeartbeatAuditChainID(consumer model.PlatformConsumerInstance) string {
	consumerID := strings.TrimSpace(consumer.ID)
	if consumerID == "" {
		return ""
	}
	return "platform-consumer-heartbeat:" + consumerID
}

func platformConsumerInstanceID(consumerID, kind, scopeKey string) string {
	consumerID = strings.TrimSpace(consumerID)
	kind = NormalizePlatformArtifactKind(kind)
	scopeKey = strings.TrimSpace(strings.ToLower(scopeKey))
	if consumerID == "" || kind == "" || scopeKey == "" {
		return model.NewID("artifactconsumer")
	}
	sum := sha256.Sum256([]byte(consumerID + "|" + kind + "|" + scopeKey))
	return "artifactconsumer_" + hex.EncodeToString(sum[:8])
}

func firstNonEmptyStoreValue(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

func normalizePlatformExpectedConsumerSetForStore(in model.PlatformExpectedConsumerSet) (model.PlatformExpectedConsumerSet, error) {
	now := time.Now().UTC()
	in.ID = strings.TrimSpace(in.ID)
	in.ReleaseSetID = strings.TrimSpace(in.ReleaseSetID)
	in.ArtifactReleaseID = strings.TrimSpace(in.ArtifactReleaseID)
	in.ArtifactKind = NormalizePlatformArtifactKind(in.ArtifactKind)
	in.ExpectedGeneration = strings.TrimSpace(in.ExpectedGeneration)
	in.TopologyRevision = strings.TrimSpace(in.TopologyRevision)
	if in.ID == "" || in.ArtifactKind == "" || in.ExpectedGeneration == "" || in.TopologyRevision == "" || in.Revision <= 0 {
		return model.PlatformExpectedConsumerSet{}, ErrInvalidInput
	}
	if scopeKey := strings.TrimSpace(strings.ToLower(in.ScopeKey)); scopeKey != "" {
		in.Scope.Key = scopeKey
	}
	in.Scope, in.ScopeKey = NormalizePlatformArtifactScope(in.Scope)
	if in.HeartbeatDeadline.IsZero() || in.ConvergenceDeadline.IsZero() {
		return model.PlatformExpectedConsumerSet{}, ErrInvalidInput
	}
	in.HeartbeatDeadline = in.HeartbeatDeadline.UTC()
	in.ConvergenceDeadline = in.ConvergenceDeadline.UTC()
	if in.ConvergenceDeadline.Before(in.HeartbeatDeadline) {
		return model.PlatformExpectedConsumerSet{}, ErrInvalidInput
	}

	consumers := make([]model.PlatformExpectedConsumer, 0, len(in.Consumers))
	consumerIDs := make(map[string]struct{}, len(in.Consumers))
	required, optional := 0, 0
	for _, consumer := range in.Consumers {
		consumer.ConsumerID = strings.TrimSpace(consumer.ConsumerID)
		consumer.Component = strings.TrimSpace(consumer.Component)
		consumer.NodeID = strings.TrimSpace(consumer.NodeID)
		consumer.ArtifactKind = NormalizePlatformArtifactKind(consumer.ArtifactKind)
		consumer.ScopeKey = strings.TrimSpace(strings.ToLower(consumer.ScopeKey))
		consumer.FailureDomain = strings.TrimSpace(consumer.FailureDomain)
		consumer.Cohort = strings.TrimSpace(consumer.Cohort)
		if platformcontrol.ConsumerAuthorityID(consumer.Cohort) != "" && (consumer.Component == model.PlatformConsumerComponentEdgeWorker || consumer.Component == model.PlatformConsumerComponentDNSServer) && consumer.AuthorityID != consumer.Cohort {
			return model.PlatformExpectedConsumerSet{}, ErrInvalidInput
		}
		if consumer.AuthorityID != "" {
			identity, err := platformcontrol.PlatformConsumerID(consumer.Component, consumer.NodeID, consumer.AuthorityID)
			if err != nil || consumer.ConsumerID != identity || consumer.Cohort != consumer.AuthorityID {
				return model.PlatformExpectedConsumerSet{}, ErrInvalidInput
			}
		}
		if strings.Contains(consumer.ConsumerID, ":enrollment:") {
			if enrollment, err := platformcontrol.ConsumerEnrollmentFromID(consumer.Component, consumer.NodeID, consumer.AuthorityID, consumer.ScopeKey, consumer.ConsumerID); err != nil || enrollment == "" {
				return model.PlatformExpectedConsumerSet{}, ErrInvalidInput
			}
		}
		consumer.ExpectedProtocolVersion = strings.TrimSpace(consumer.ExpectedProtocolVersion)
		consumer.AcceptedProtocolVersions = normalizeStringList(consumer.AcceptedProtocolVersions)
		consumer.ExpectedSchemaVersion = strings.TrimSpace(consumer.ExpectedSchemaVersion)
		consumer.AcceptedSchemaVersions = normalizeStringList(consumer.AcceptedSchemaVersions)
		consumer.CompatibilityCapabilities = normalizeStringList(consumer.CompatibilityCapabilities)
		consumer.ExpectedGeneration = strings.TrimSpace(consumer.ExpectedGeneration)
		if consumer.ConsumerID == "" || consumer.Component == "" || consumer.NodeID == "" ||
			consumer.FailureDomain == "" || consumer.Cohort == "" || consumer.ExpectedProtocolVersion == "" ||
			consumer.ExpectedSchemaVersion == "" || consumer.HeartbeatFreshnessSeconds <= 0 ||
			consumer.HeartbeatDeadline.IsZero() || consumer.ConvergenceDeadline.IsZero() {
			return model.PlatformExpectedConsumerSet{}, ErrInvalidInput
		}
		if consumer.ArtifactKind != in.ArtifactKind || consumer.ScopeKey != in.ScopeKey || consumer.ExpectedGeneration != in.ExpectedGeneration {
			return model.PlatformExpectedConsumerSet{}, ErrInvalidInput
		}
		if consumer.HostedRouteTLSOnly && !platformcontrol.AllowsHostedRouteTLSOnly(consumer) {
			return model.PlatformExpectedConsumerSet{}, ErrInvalidInput
		}
		if _, exists := consumerIDs[consumer.ConsumerID]; exists {
			return model.PlatformExpectedConsumerSet{}, ErrInvalidInput
		}
		consumerIDs[consumer.ConsumerID] = struct{}{}
		if !platformStringListContains(consumer.AcceptedProtocolVersions, consumer.ExpectedProtocolVersion) ||
			!platformStringListContains(consumer.AcceptedSchemaVersions, consumer.ExpectedSchemaVersion) {
			return model.PlatformExpectedConsumerSet{}, ErrInvalidInput
		}
		consumer.HeartbeatDeadline = consumer.HeartbeatDeadline.UTC()
		consumer.ConvergenceDeadline = consumer.ConvergenceDeadline.UTC()
		if consumer.ConvergenceDeadline.Before(consumer.HeartbeatDeadline) ||
			consumer.HeartbeatDeadline.After(in.HeartbeatDeadline) ||
			consumer.ConvergenceDeadline.After(in.ConvergenceDeadline) {
			return model.PlatformExpectedConsumerSet{}, ErrInvalidInput
		}
		if consumer.Required {
			required++
		} else {
			optional++
		}
		consumers = append(consumers, consumer)
	}
	if in.RequiredCardinality != required || in.OptionalCardinality != optional || (!in.RequiresConsumers && len(consumers) > 0) {
		return model.PlatformExpectedConsumerSet{}, ErrInvalidInput
	}
	sort.Slice(consumers, func(i, j int) bool {
		return consumers[i].ConsumerID < consumers[j].ConsumerID
	})
	in.Consumers = consumers
	if in.CreatedAt.IsZero() {
		in.CreatedAt = now
	} else {
		in.CreatedAt = in.CreatedAt.UTC()
	}
	if in.UpdatedAt.IsZero() {
		in.UpdatedAt = in.CreatedAt
	} else {
		in.UpdatedAt = in.UpdatedAt.UTC()
	}
	if in.UpdatedAt.Before(in.CreatedAt) {
		return model.PlatformExpectedConsumerSet{}, ErrInvalidInput
	}
	return in, nil
}

func normalizePlatformExpectedConsumerSetFilter(filter model.PlatformExpectedConsumerSetFilter) (model.PlatformExpectedConsumerSetFilter, error) {
	filter.ReleaseSetID = strings.TrimSpace(filter.ReleaseSetID)
	filter.ArtifactReleaseID = strings.TrimSpace(filter.ArtifactReleaseID)
	if strings.TrimSpace(filter.ArtifactKind) != "" {
		filter.ArtifactKind = NormalizePlatformArtifactKind(filter.ArtifactKind)
		if filter.ArtifactKind == "" {
			return model.PlatformExpectedConsumerSetFilter{}, ErrInvalidInput
		}
	}
	filter.ScopeKey = strings.TrimSpace(strings.ToLower(filter.ScopeKey))
	if filter.Limit < 0 {
		return model.PlatformExpectedConsumerSetFilter{}, ErrInvalidInput
	}
	return filter, nil
}

func platformStringListContains(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}
