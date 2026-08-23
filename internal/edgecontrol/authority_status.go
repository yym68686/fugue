package edgecontrol

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	AuthorityStatusPathV1       = "/v1/authority/status"
	AuthorityStatusSchemaV1     = "edge-control-group-authority-status/v1"
	AuthorityGroupReadyPrefixV1 = "/v1/authority/groups/"
	EdgeControlReadyPathV1      = "/readyz"

	GroupAuthorityHealthReady       = "ready"
	GroupAuthorityHealthServingLKG  = "serving_lkg"
	GroupAuthorityHealthPublication = "publication_ready"
	GroupAuthorityHealthUnavailable = "unavailable"

	GroupAuthorityLKGCurrent   = "current"
	GroupAuthorityLKGPreserved = "preserved"
	GroupAuthorityLKGMissing   = "missing"
)

type AuthorityStatusStore interface {
	ReadGroupAuthorityStatus(context.Context, string) (AuthorityGroupStoreSnapshot, error)
}

// AuthorityGroupStoreSnapshot is the group-local publication state needed by
// the readiness endpoint. Persistent stores return all three projections from
// one exact state revision so a status read neither mixes revisions nor
// repeatedly decodes a large append-only group ledger.
type AuthorityGroupStoreSnapshot struct {
	Inventory       GroupInventorySnapshot
	InventoryExists bool
	Producer        GroupInventoryProducerState
	ProducerExists  bool
	Authority       GroupAuthorityState
	Candidate       GroupCandidateBundle
	CandidateExists bool
}

type AuthorityGroupStatus struct {
	GroupID                     string     `json:"edge_group_id"`
	Status                      string     `json:"status"`
	Ready                       bool       `json:"ready"`
	ServingHealthy              bool       `json:"serving_healthy"`
	BootstrapEligible           bool       `json:"bootstrap_eligible"`
	BootstrapValidUntil         *time.Time `json:"bootstrap_valid_until,omitempty"`
	InventorySequence           uint64     `json:"inventory_sequence,omitempty"`
	InventoryGeneration         string     `json:"inventory_generation,omitempty"`
	InventoryProducerGeneration uint64     `json:"inventory_producer_generation,omitempty"`
	InventoryProducerNodes      int        `json:"inventory_producer_nodes,omitempty"`
	InventoryHeartbeatAt        *time.Time `json:"inventory_heartbeat_at,omitempty"`
	AuthoritySequence           uint64     `json:"authority_sequence,omitempty"`
	PublicationSequence         uint64     `json:"publication_sequence,omitempty"`
	CurrentPublicationSequence  uint64     `json:"current_publication_sequence,omitempty"`
	CandidateEpoch              uint64     `json:"candidate_epoch,omitempty"`
	CandidateWorkerSourceSHA    string     `json:"candidate_worker_source_sha,omitempty"`
	PublicationDecision         string     `json:"publication_decision,omitempty"`
	BundleGeneration            string     `json:"bundle_generation,omitempty"`
	PublishedBundleDigest       string     `json:"published_bundle_digest,omitempty"`
	RecoveryEpoch               uint64     `json:"recovery_epoch"`
	BundleValidUntil            *time.Time `json:"bundle_valid_until,omitempty"`
	LKGState                    string     `json:"lkg_state"`
	FailureCode                 string     `json:"failure_code,omitempty"`
}

// authorityInventoryCursorStatus preserves the original group readiness wire
// shape used by deployed inventory producers that reject unknown JSON fields.
// Rich control-plane readers omit Accept and continue to receive the complete
// AuthorityGroupStatus projection.
type authorityInventoryCursorStatus struct {
	GroupID                     string     `json:"edge_group_id"`
	Status                      string     `json:"status"`
	Ready                       bool       `json:"ready"`
	InventorySequence           uint64     `json:"inventory_sequence,omitempty"`
	InventoryGeneration         string     `json:"inventory_generation,omitempty"`
	InventoryProducerGeneration uint64     `json:"inventory_producer_generation,omitempty"`
	InventoryProducerNodes      int        `json:"inventory_producer_nodes,omitempty"`
	InventoryHeartbeatAt        *time.Time `json:"inventory_heartbeat_at,omitempty"`
	PublicationSequence         uint64     `json:"publication_sequence,omitempty"`
	PublicationDecision         string     `json:"publication_decision,omitempty"`
	BundleGeneration            string     `json:"bundle_generation,omitempty"`
	PublishedBundleDigest       string     `json:"published_bundle_digest,omitempty"`
	RecoveryEpoch               uint64     `json:"recovery_epoch"`
	BundleValidUntil            *time.Time `json:"bundle_valid_until,omitempty"`
	LKGState                    string     `json:"lkg_state"`
	FailureCode                 string     `json:"failure_code,omitempty"`
}

type AuthorityStatusSnapshot struct {
	Schema                    string                 `json:"schema"`
	Status                    string                 `json:"status"`
	Ready                     bool                   `json:"ready"`
	ServingReady              bool                   `json:"serving_ready"`
	ReadyGroups               int                    `json:"ready_groups"`
	UnavailableGroups         int                    `json:"unavailable_groups"`
	Mode                      string                 `json:"mode"`
	Authority                 string                 `json:"authority"`
	PublicationEnabled        bool                   `json:"publication_enabled"`
	CrossGroupTransaction     bool                   `json:"cross_group_transaction"`
	Groups                    []AuthorityGroupStatus `json:"groups"`
	ProcessStartedAt          *time.Time             `json:"process_started_at,omitempty"`
	LastReconciledAt          *time.Time             `json:"last_reconciled_at,omitempty"`
	LastRouteIntentGeneration string                 `json:"last_route_intent_generation,omitempty"`
	LastPublished             int                    `json:"last_published,omitempty"`
	LastCandidatePublished    int                    `json:"last_candidate_published,omitempty"`
	LastFailed                int                    `json:"last_failed,omitempty"`
	RuntimeFailureCode        string                 `json:"runtime_failure_code,omitempty"`
	CanonicalDigest           string                 `json:"canonical_digest"`
}

type AuthorityRuntimeState struct {
	mu          sync.RWMutex
	now         func() time.Time
	startedAt   time.Time
	lastAt      time.Time
	last        AuthorityRuntimeObservation
	hasObserved bool
}

func NewAuthorityRuntimeState(now func() time.Time) *AuthorityRuntimeState {
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &AuthorityRuntimeState{now: now, startedAt: now().UTC()}
}

func (state *AuthorityRuntimeState) Observe(observation AuthorityRuntimeObservation) {
	if state == nil {
		return
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	state.lastAt = state.now().UTC()
	state.last = observation
	state.hasObserved = true
}

func (state *AuthorityRuntimeState) snapshot() (time.Time, time.Time, AuthorityRuntimeObservation, bool) {
	if state == nil {
		return time.Time{}, time.Time{}, AuthorityRuntimeObservation{}, false
	}
	state.mu.RLock()
	defer state.mu.RUnlock()
	return state.startedAt, state.lastAt, state.last, state.hasObserved
}

type authorityStatusHandler struct {
	store  AuthorityStatusStore
	groups []string
	state  *AuthorityRuntimeState
	now    func() time.Time
}

func NewAuthorityStatusHandler(store AuthorityStatusStore, groupIDs []string, state *AuthorityRuntimeState, now func() time.Time) (http.Handler, error) {
	if store == nil {
		return nil, errors.New("edge-control authority status store is nil")
	}
	groups, err := normalizeGroupIDs(groupIDs)
	if err != nil {
		return nil, err
	}
	if state == nil {
		return nil, errors.New("edge-control authority runtime state is nil")
	}
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &authorityStatusHandler{store: store, groups: groups, state: state, now: func() time.Time { return now().UTC() }}, nil
}

func (handler *authorityStatusHandler) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet || request.URL.RawQuery != "" {
		http.NotFound(w, request)
		return
	}
	if request.URL.Path == EdgeControlReadyPathV1 {
		writeJSON(w, http.StatusOK, handler.processReadySnapshot())
		return
	}
	if strings.HasPrefix(request.URL.Path, AuthorityGroupReadyPrefixV1) {
		parts := strings.Split(strings.TrimPrefix(request.URL.Path, AuthorityGroupReadyPrefixV1), "/")
		if len(parts) != 2 || parts[1] != "readyz" || parts[0] != normalizeGroupID(parts[0]) || !edgeGroupIDPattern.MatchString(parts[0]) {
			http.NotFound(w, request)
			return
		}
		groupID := parts[0]
		allowed := false
		for _, configured := range handler.groups {
			if configured == groupID {
				allowed = true
				break
			}
		}
		if !allowed {
			http.NotFound(w, request)
			return
		}
		group := handler.snapshotGroup(request.Context(), groupID, handler.now())
		status := http.StatusOK
		if !group.Ready {
			status = http.StatusServiceUnavailable
		}
		if strings.TrimSpace(request.Header.Get("Accept")) == "application/json" {
			writeJSON(w, status, authorityInventoryCursorProjection(group))
			return
		}
		writeJSON(w, status, group)
		return
	}
	if request.URL.Path != AuthorityStatusPathV1 {
		http.NotFound(w, request)
		return
	}
	writeJSON(w, http.StatusOK, handler.snapshot(request.Context()))
}

func authorityInventoryCursorProjection(group AuthorityGroupStatus) authorityInventoryCursorStatus {
	return authorityInventoryCursorStatus{
		GroupID: group.GroupID, Status: group.Status, Ready: group.Ready,
		InventorySequence: group.InventorySequence, InventoryGeneration: group.InventoryGeneration,
		InventoryProducerGeneration: group.InventoryProducerGeneration, InventoryProducerNodes: group.InventoryProducerNodes,
		InventoryHeartbeatAt: group.InventoryHeartbeatAt, PublicationSequence: group.PublicationSequence,
		PublicationDecision: group.PublicationDecision, BundleGeneration: group.BundleGeneration,
		PublishedBundleDigest: group.PublishedBundleDigest, RecoveryEpoch: group.RecoveryEpoch,
		BundleValidUntil: group.BundleValidUntil, LKGState: group.LKGState, FailureCode: group.FailureCode,
	}
}

func (handler *authorityStatusHandler) snapshot(ctx context.Context) AuthorityStatusSnapshot {
	now := handler.now()
	out := AuthorityStatusSnapshot{
		Schema: AuthorityStatusSchemaV1, Status: "unavailable", Ready: true, Mode: "group-authority",
		Authority: "edge-control", PublicationEnabled: true, CrossGroupTransaction: false,
		Groups: make([]AuthorityGroupStatus, 0, len(handler.groups)),
	}
	readyGroups := 0
	degradedGroups := 0
	for _, groupID := range handler.groups {
		group := handler.snapshotGroup(ctx, groupID, now)
		if group.Ready {
			readyGroups++
			if group.Status == GroupAuthorityHealthServingLKG {
				degradedGroups++
			}
		}
		out.Groups = append(out.Groups, group)
	}
	if readyGroups > 0 {
		out.ServingReady = true
		out.Status = "healthy"
		if readyGroups != len(handler.groups) || degradedGroups > 0 {
			out.Status = "degraded"
		}
	}
	out.ReadyGroups = readyGroups
	out.UnavailableGroups = len(handler.groups) - readyGroups
	startedAt, lastAt, observation, observed := handler.state.snapshot()
	out.ProcessStartedAt = &startedAt
	if observed {
		out.LastReconciledAt = &lastAt
		out.LastRouteIntentGeneration = observation.RouteIntentGeneration
		out.LastPublished = observation.Published
		out.LastCandidatePublished = observation.CandidatePublished
		out.LastFailed = observation.Failed
		out.RuntimeFailureCode = observation.FailureCode
		if observation.FailureCode != "" && out.Status == "healthy" {
			out.Status = "degraded"
		}
	}
	digestMaterial := out
	digestMaterial.CanonicalDigest = ""
	out.CanonicalDigest = digestJSON(digestMaterial)
	return out
}

func (handler *authorityStatusHandler) snapshotGroup(ctx context.Context, groupID string, now time.Time) AuthorityGroupStatus {
	group := AuthorityGroupStatus{GroupID: groupID, Status: GroupAuthorityHealthUnavailable, LKGState: GroupAuthorityLKGMissing}
	servingHealthy := false
	stored, storeErr := handler.store.ReadGroupAuthorityStatus(ctx, groupID)
	inventoryErr := storeErr
	if storeErr == nil && !stored.InventoryExists {
		inventoryErr = ErrGroupInventoryNotFound
	}
	if inventoryErr == nil {
		inventory := stored.Inventory
		group.InventorySequence = inventory.Sequence
		group.InventoryGeneration = inventory.Generation
		if view, err := validateGroupInventory(groupID, inventory, true, now); err == nil {
			servingHealthy = len(view.servingEdgeIDs) >= inventory.ActiveEpoch.MinHealthyInstances
		}
	}
	producerErr := storeErr
	if producerErr == nil && stored.ProducerExists {
		producer := stored.Producer
		group.InventoryProducerGeneration = producer.Generation
		nodes := make(map[string]struct{}, len(producer.Observations))
		var latest time.Time
		for _, observation := range producer.Observations {
			if observation.ObservedAt.After(latest) {
				latest = observation.ObservedAt
			}
			if !observation.ObservedAt.After(now.Add(maxInventoryHeartbeatClockSkew)) && now.Sub(observation.ObservedAt) <= GroupInventoryHeartbeatMaxAge {
				nodes[observation.NodeID] = struct{}{}
			}
		}
		group.InventoryProducerNodes = len(nodes)
		if !latest.IsZero() {
			group.InventoryHeartbeatAt = &latest
		}
	}
	authority := stored.Authority
	authorityErr := storeErr
	if authorityErr == nil && authority.LedgerExists {
		group.AuthoritySequence = authority.LedgerHead.Sequence
		group.PublicationSequence = authority.LedgerHead.Sequence
		group.PublicationDecision = authority.LedgerHead.Status
		group.FailureCode = authority.LedgerHead.FailureCode
	}
	if authorityErr == nil && authority.PublishedExists && validateGroupPublishedBundle(groupID, authority.Published) == nil {
		validUntil := authority.Published.Bundle.ValidUntil
		group.BundleGeneration = authority.Published.Bundle.Generation
		group.CurrentPublicationSequence = authority.Published.PublicationSequence
		group.PublishedBundleDigest = authority.Published.Digest
		group.RecoveryEpoch = authority.Published.RecoveryEpoch
		group.BundleValidUntil = &validUntil
		group.LKGState = GroupAuthorityLKGPreserved
		if eligible, bootstrapUntil := groupPublishedBootstrapEligibility(authority.Published, now); eligible {
			group.BootstrapEligible = true
			group.BootstrapValidUntil = &bootstrapUntil
		}
		publicationCurrent := authority.LedgerExists && authority.LedgerHead.Status == GroupAuthorityStatusPublished &&
			authority.LedgerHead.BundleGeneration == authority.Published.Bundle.Generation
		if publicationCurrent {
			group.LKGState = GroupAuthorityLKGCurrent
		}
		if authority.Published.Bundle.ValidUntil.After(now) {
			group.Status = GroupAuthorityHealthPublication
		}
		if servingHealthy && authority.Published.Bundle.ValidUntil.After(now) {
			group.Ready = true
			group.ServingHealthy = true
			group.Status = GroupAuthorityHealthReady
			if group.LKGState == GroupAuthorityLKGPreserved {
				group.Status = GroupAuthorityHealthServingLKG
			}
		}
	}
	if stored.CandidateExists {
		group.CandidateEpoch = stored.Candidate.Epoch
		group.CandidateWorkerSourceSHA = stored.Candidate.WorkerSourceSHA
	}
	if inventoryErr != nil && !errors.Is(inventoryErr, ErrGroupInventoryNotFound) && group.FailureCode == "" {
		group.FailureCode = GroupShadowFailureInventoryRead
	}
	if producerErr != nil && group.FailureCode == "" {
		group.FailureCode = GroupShadowFailureInventoryRead
	}
	if inventoryErr != nil && group.Ready {
		group.Status = GroupAuthorityHealthServingLKG
		group.LKGState = GroupAuthorityLKGPreserved
	}
	if authorityErr != nil && group.FailureCode == "" {
		group.FailureCode = GroupAuthorityFailurePublicationCAS
	}
	return group
}

func groupPublishedBootstrapEligibility(published GroupPublishedBundle, now time.Time) (bool, time.Time) {
	if published.PublishedAt.IsZero() || published.PublishedAt.After(now.Add(maxInventoryHeartbeatClockSkew)) {
		return false, time.Time{}
	}
	validUntil := published.PublishedAt.Add(maxGroupBundleValidity)
	return now.Before(validUntil), validUntil
}

func (handler *authorityStatusHandler) processReadySnapshot() AuthorityStatusSnapshot {
	startedAt, lastAt, observation, observed := handler.state.snapshot()
	out := AuthorityStatusSnapshot{
		Schema: AuthorityStatusSchemaV1, Status: "ready", Ready: true, Mode: "group-authority", Authority: "edge-control",
		PublicationEnabled: true, CrossGroupTransaction: false, ProcessStartedAt: &startedAt,
	}
	if observed {
		out.LastReconciledAt = &lastAt
		out.LastRouteIntentGeneration = observation.RouteIntentGeneration
		out.LastPublished = observation.Published
		out.LastCandidatePublished = observation.CandidatePublished
		out.LastFailed = observation.Failed
		out.RuntimeFailureCode = observation.FailureCode
	}
	digestMaterial := out
	digestMaterial.CanonicalDigest = ""
	out.CanonicalDigest = digestJSON(digestMaterial)
	return out
}

func authorityGroupReadyPath(groupID string) string {
	return AuthorityGroupReadyPrefixV1 + normalizeGroupID(groupID) + "/readyz"
}

func NewAuthorityControlHandler(boundary, heartbeat, status, bundles, recovery http.Handler, mutations ...http.Handler) (http.Handler, error) {
	if boundary == nil || heartbeat == nil || status == nil || bundles == nil || recovery == nil {
		return nil, errors.New("edge-control authority HTTP handler dependency is nil")
	}
	if len(mutations) > 3 {
		return nil, errors.New("edge-control authority mutation HTTP handlers are invalid")
	}
	for _, handler := range mutations {
		if handler == nil {
			return nil, errors.New("edge-control authority mutation HTTP handler is nil")
		}
	}
	mux := http.NewServeMux()
	mux.Handle("POST "+GroupAuthorityInventoryHeartbeatPathV1, heartbeat)
	mux.Handle("GET "+EdgeControlReadyPathV1, status)
	mux.Handle("GET "+AuthorityStatusPathV1, status)
	mux.Handle("GET "+AuthorityGroupReadyPrefixV1, status)
	mux.Handle("GET "+GroupBundleReadPathV1, bundles)
	mux.Handle("GET "+GroupCandidateBundleReadPathV1, bundles)
	mux.Handle("GET "+GroupCandidateEnvelopeReadPathV1, bundles)
	mux.Handle("POST "+GroupRecoveryPathV1, recovery)
	if len(mutations) >= 1 {
		mux.Handle("POST "+GroupPromotionPathV1, mutations[0])
	}
	if len(mutations) >= 2 {
		mux.Handle("POST "+GroupCandidateStagePathV1, mutations[1])
	}
	if len(mutations) == 3 {
		mux.Handle("POST "+GroupCandidateRecoveryPathV1, mutations[2])
	}
	mux.Handle("/", boundary)
	return mux, nil
}
