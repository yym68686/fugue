package edgecontrol

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"
)

const (
	ShadowStatusPathV1   = "/v1/shadow/status"
	ShadowReadyPathV1    = "/readyz"
	ShadowStatusSchemaV1 = "edge-control-shadow-status/v1"

	GroupLKGStateMissing   = "missing"
	GroupLKGStateCandidate = "candidate"
	GroupLKGStatePreserved = "preserved"
)

type ShadowStatusStore interface {
	ReadGroupInventory(context.Context, string) (GroupInventorySnapshot, error)
	Head(context.Context, string) (GroupShadowLedgerEntry, bool, error)
}

type ShadowGroupStatus struct {
	GroupID                        string `json:"edge_group_id"`
	Status                         string `json:"status"`
	InventorySequence              uint64 `json:"inventory_sequence,omitempty"`
	InventoryGeneration            string `json:"inventory_generation,omitempty"`
	InventoryDigest                string `json:"inventory_digest,omitempty"`
	LedgerSequence                 uint64 `json:"ledger_sequence,omitempty"`
	RouteIntentGeneration          string `json:"route_intent_generation,omitempty"`
	BundleGeneration               string `json:"bundle_generation,omitempty"`
	LastSuccessfulBundleGeneration string `json:"last_successful_bundle_generation,omitempty"`
	LKGState                       string `json:"lkg_state"`
	FailureCode                    string `json:"failure_code,omitempty"`
}

type ShadowStatusSnapshot struct {
	Schema                    string              `json:"schema"`
	Status                    string              `json:"status"`
	Mode                      string              `json:"mode"`
	Authority                 string              `json:"authority"`
	PublicationEnabled        bool                `json:"publication_enabled"`
	Groups                    []ShadowGroupStatus `json:"groups"`
	ProcessStartedAt          *time.Time          `json:"process_started_at,omitempty"`
	LastReconciledAt          *time.Time          `json:"last_reconciled_at,omitempty"`
	LastRouteIntentGeneration string              `json:"last_route_intent_generation,omitempty"`
	LastSucceeded             int                 `json:"last_succeeded,omitempty"`
	LastFailed                int                 `json:"last_failed,omitempty"`
	RuntimeFailureCode        string              `json:"runtime_failure_code,omitempty"`
	CanonicalDigest           string              `json:"canonical_digest"`
}

type ShadowRuntimeState struct {
	mu          sync.RWMutex
	now         func() time.Time
	startedAt   time.Time
	lastAt      time.Time
	last        ShadowRuntimeObservation
	hasObserved bool
}

func NewShadowRuntimeState(now func() time.Time) *ShadowRuntimeState {
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	startedAt := now().UTC()
	return &ShadowRuntimeState{now: now, startedAt: startedAt}
}

func (state *ShadowRuntimeState) Observe(observation ShadowRuntimeObservation) {
	if state == nil {
		return
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	state.lastAt = state.now().UTC()
	state.last = observation
	state.hasObserved = true
}

func (state *ShadowRuntimeState) snapshot() (time.Time, time.Time, ShadowRuntimeObservation, bool) {
	if state == nil {
		return time.Time{}, time.Time{}, ShadowRuntimeObservation{}, false
	}
	state.mu.RLock()
	defer state.mu.RUnlock()
	return state.startedAt, state.lastAt, state.last, state.hasObserved
}

type shadowStatusHandler struct {
	store        ShadowStatusStore
	groups       []string
	runtimeState *ShadowRuntimeState
}

func NewShadowStatusHandler(store ShadowStatusStore, groupIDs []string, runtimeState ...*ShadowRuntimeState) (http.Handler, error) {
	if store == nil {
		return nil, errors.New("edge-control shadow status store is nil")
	}
	groups, err := normalizeGroupIDs(groupIDs)
	if err != nil {
		return nil, err
	}
	if len(runtimeState) > 1 {
		return nil, errors.New("edge-control shadow status runtime state is ambiguous")
	}
	var state *ShadowRuntimeState
	if len(runtimeState) == 1 {
		state = runtimeState[0]
		if state == nil {
			return nil, errors.New("edge-control shadow status runtime state is nil")
		}
	}
	return shadowStatusHandler{store: store, groups: groups, runtimeState: state}, nil
}

func (handler shadowStatusHandler) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet || (request.URL.Path != ShadowStatusPathV1 && request.URL.Path != ShadowReadyPathV1) || request.URL.RawQuery != "" {
		http.NotFound(w, request)
		return
	}
	status := handler.snapshot(request.Context())
	statusCode := http.StatusOK
	if request.URL.Path == ShadowReadyPathV1 && status.Status != "healthy" {
		statusCode = http.StatusServiceUnavailable
	}
	writeJSON(w, statusCode, status)
}

func (handler shadowStatusHandler) snapshot(ctx context.Context) ShadowStatusSnapshot {
	status := ShadowStatusSnapshot{Schema: ShadowStatusSchemaV1, Status: "healthy", Mode: "shadow-only", Authority: "none", PublicationEnabled: false}
	status.Groups = make([]ShadowGroupStatus, 0, len(handler.groups))
	for _, groupID := range handler.groups {
		group := ShadowGroupStatus{GroupID: groupID, Status: "pending", LKGState: GroupLKGStateMissing}
		inventory, inventoryErr := handler.store.ReadGroupInventory(ctx, groupID)
		if inventoryErr == nil {
			group.InventorySequence = inventory.Sequence
			group.InventoryGeneration = inventory.Generation
			group.InventoryDigest = groupInventorySemanticDigest(inventory)
		} else if !errors.Is(inventoryErr, ErrGroupInventoryNotFound) {
			group.Status = "failed"
			group.FailureCode = GroupShadowFailureInventoryRead
		}
		head, exists, ledgerErr := handler.store.Head(ctx, groupID)
		if ledgerErr != nil {
			group.Status = "failed"
			group.FailureCode = GroupShadowFailureLedgerRead
		} else if exists {
			group.Status = head.Status
			group.LedgerSequence = head.Sequence
			group.RouteIntentGeneration = head.RouteIntentGeneration
			group.BundleGeneration = head.BundleGeneration
			group.LastSuccessfulBundleGeneration = head.LastSuccessfulBundleGeneration
			group.FailureCode = head.FailureCode
			if head.LastSuccessfulBundleGeneration != "" {
				group.LKGState = GroupLKGStatePreserved
				if head.Status == GroupShadowStatusCompiled && head.BundleGeneration == head.LastSuccessfulBundleGeneration {
					group.LKGState = GroupLKGStateCandidate
				}
			}
		}
		if inventoryErr != nil || ledgerErr != nil || !exists || group.Status != GroupShadowStatusCompiled || group.LKGState != GroupLKGStateCandidate {
			status.Status = "degraded"
		}
		status.Groups = append(status.Groups, group)
	}
	if handler.runtimeState != nil {
		startedAt, lastAt, observation, observed := handler.runtimeState.snapshot()
		status.ProcessStartedAt = &startedAt
		if observed {
			status.LastReconciledAt = &lastAt
			status.LastRouteIntentGeneration = observation.RouteIntentGeneration
			status.LastSucceeded = observation.Succeeded
			status.LastFailed = observation.Failed
			status.RuntimeFailureCode = observation.FailureCode
		}
		if !observed || observation.FailureCode != "" || observation.Succeeded != len(handler.groups) || observation.Failed != 0 {
			status.Status = "degraded"
		}
	}
	digestMaterial := status
	digestMaterial.CanonicalDigest = ""
	status.CanonicalDigest = digestJSON(digestMaterial)
	return status
}

func NewShadowControlHandler(boundary, heartbeat, status http.Handler) (http.Handler, error) {
	if boundary == nil || heartbeat == nil || status == nil {
		return nil, errors.New("edge-control shadow HTTP handler dependency is nil")
	}
	mux := http.NewServeMux()
	mux.Handle("POST "+GroupInventoryHeartbeatPathV1, heartbeat)
	mux.Handle("GET "+ShadowReadyPathV1, status)
	mux.Handle("GET "+ShadowStatusPathV1, status)
	mux.Handle("/", boundary)
	return mux, nil
}
