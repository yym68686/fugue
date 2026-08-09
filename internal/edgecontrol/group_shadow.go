package edgecontrol

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"fugue/internal/model"
)

const (
	GroupInventorySchemaV1    = "edge-control-group-inventory/v1"
	GroupShadowBatchSchemaV1  = "edge-control-group-shadow-batch/v1"
	GroupShadowLedgerSchemaV1 = "edge-control-group-shadow-ledger/v1"

	GroupShadowStatusCompiled = "compiled"
	GroupShadowStatusFailed   = "failed"

	GroupShadowFailureInventoryRead    = "inventory_read_failed"
	GroupShadowFailureInventoryInvalid = "inventory_invalid"
	GroupShadowFailureNoHealthyActive  = "no_healthy_active_instances"
	GroupShadowFailureNoRoutableRoutes = "no_routable_routes"
	GroupShadowFailureCompile          = "compile_failed"
	GroupShadowFailureLedgerRead       = "ledger_read_failed"
	GroupShadowFailureLedgerCAS        = "ledger_cas_failed"

	groupShadowIssuer = "fugue-edge-control-shadow"

	// One bounded retry lets a group compiler refresh an inventory heartbeat or
	// ledger head that changed between its read and exact CAS append. Persistent
	// conflicts remain visible instead of weakening CAS or spinning forever.
	groupShadowCASAttempts = 2
)

var (
	ErrGroupShadowLedgerConflict = errors.New("edge-control group shadow ledger CAS conflict")
	errGroupInventoryInvalid     = errors.New("edge-control group inventory is invalid")
	errNoHealthyActiveInstances  = errors.New("edge-control group has no healthy active instances")
	errNoRoutableRoutes          = errors.New("edge-control group has no routable routes")
	edgeGroupIDPattern           = regexp.MustCompile(`^edge-group-[a-z0-9]+(?:-[a-z0-9]+)*$`)
)

// GroupInventorySnapshot is the Edge Control-owned, group-scoped inventory
// input. An active epoch fences blue/green identities before they can
// participate in compilation; inactive instances remain observable but never
// contribute health to the active group candidate.
type GroupInventorySnapshot struct {
	Schema      string           `json:"schema"`
	GroupID     string           `json:"edge_group_id"`
	Sequence    uint64           `json:"sequence"`
	Generation  string           `json:"generation"`
	ActiveEpoch GroupActiveEpoch `json:"active_epoch"`
	Instances   []GroupInstance  `json:"instances"`
	ObservedAt  time.Time        `json:"observed_at"`
}

type GroupActiveEpoch struct {
	GroupID             string `json:"edge_group_id"`
	Slot                string `json:"slot"`
	ReleaseEpoch        string `json:"release_epoch"`
	FenceSequence       uint64 `json:"fence_sequence"`
	MinHealthyInstances int    `json:"min_healthy_instances"`
}

// GroupInstance is the minimum health and fencing projection needed by Edge
// Control. Core node credentials, addresses, logs, and other mutable fields
// are deliberately outside the durable Edge Control inventory.
type GroupInstance struct {
	EdgeID           string `json:"edge_id"`
	GroupID          string `json:"edge_group_id"`
	Slot             string `json:"slot"`
	InstanceUID      string `json:"instance_uid"`
	ReleaseEpoch     string `json:"release_epoch"`
	EffectiveHealthy bool   `json:"effective_healthy"`
	NodeHealthy      bool   `json:"node_healthy"`
	NodeStatus       string `json:"node_status"`
	Draining         bool   `json:"draining"`
	FailureClass     string `json:"failure_class,omitempty"`
}

// GroupInventoryReader deliberately has no global inventory method. A caller
// chooses the group set and each read can fail without contaminating any other
// group's compilation transaction. Implementations must permit concurrent
// reads for different canonical group IDs.
type GroupInventoryReader interface {
	ReadGroupInventory(context.Context, string) (GroupInventorySnapshot, error)
}

// GroupShadowLedgerEntry is one append-only, group-local shadow decision.
// Bundle is unsigned and non-authoritative by construction. Failed entries
// retain only the last-success generation, leaving the prior candidate intact.
type GroupShadowLedgerEntry struct {
	Schema                         string                 `json:"schema"`
	GroupID                        string                 `json:"edge_group_id"`
	Sequence                       uint64                 `json:"sequence"`
	Status                         string                 `json:"status"`
	RouteIntentGeneration          string                 `json:"route_intent_generation"`
	InventoryGeneration            string                 `json:"inventory_generation,omitempty"`
	InventoryDigest                string                 `json:"inventory_digest,omitempty"`
	InputDigest                    string                 `json:"input_digest"`
	ActiveSlot                     string                 `json:"active_slot,omitempty"`
	ActiveReleaseEpoch             string                 `json:"active_release_epoch,omitempty"`
	ActiveFenceSequence            uint64                 `json:"active_fence_sequence,omitempty"`
	ActiveHealthyInstances         int                    `json:"active_healthy_instances,omitempty"`
	BundleGeneration               string                 `json:"bundle_generation,omitempty"`
	LastSuccessfulBundleGeneration string                 `json:"last_successful_bundle_generation,omitempty"`
	FailureCode                    string                 `json:"failure_code,omitempty"`
	Authority                      string                 `json:"authority"`
	PublicationEnabled             bool                   `json:"publication_enabled"`
	RecordedAt                     time.Time              `json:"recorded_at"`
	Bundle                         *model.EdgeRouteBundle `json:"bundle,omitempty"`
	BundleArchived                 bool                   `json:"bundle_archived,omitempty"`
}

// GroupShadowLedger provides one CAS sequence per edge group. Implementations
// may persist each group independently; there is intentionally no cross-group
// transaction or global head. Implementations must serialize within a group
// while permitting different groups to advance concurrently.
type GroupShadowLedger interface {
	Head(context.Context, string) (GroupShadowLedgerEntry, bool, error)
	AppendCAS(context.Context, string, uint64, GroupShadowLedgerEntry) (GroupShadowLedgerEntry, error)
}

type GroupShadowResult struct {
	GroupID                        string `json:"edge_group_id"`
	Status                         string `json:"status"`
	LedgerSequence                 uint64 `json:"ledger_sequence,omitempty"`
	InputDigest                    string `json:"input_digest,omitempty"`
	BundleGeneration               string `json:"bundle_generation,omitempty"`
	LastSuccessfulBundleGeneration string `json:"last_successful_bundle_generation,omitempty"`
	FailureCode                    string `json:"failure_code,omitempty"`
}

type GroupShadowBatch struct {
	Schema                string              `json:"schema"`
	RouteIntentGeneration string              `json:"route_intent_generation"`
	Results               []GroupShadowResult `json:"results"`
	Succeeded             int                 `json:"succeeded"`
	Failed                int                 `json:"failed"`
}

// GroupShadowCompiler compiles and records unsigned candidates only. It does
// not sign, publish, mutate worker state, or claim bundle authority.
type GroupShadowCompiler struct {
	Inventory       GroupInventoryReader
	Ledger          GroupShadowLedger
	Now             func() time.Time
	InventoryMaxAge time.Duration
}

func (compiler GroupShadowCompiler) Reconcile(ctx context.Context, snapshot model.EdgeRouteIntentSnapshot, groupIDs []string) (GroupShadowBatch, error) {
	if compiler.Inventory == nil {
		return GroupShadowBatch{}, errors.New("edge-control group inventory reader is nil")
	}
	if compiler.Ledger == nil {
		return GroupShadowBatch{}, errors.New("edge-control group shadow ledger is nil")
	}
	if err := validateRouteIntentSnapshot(snapshot); err != nil {
		return GroupShadowBatch{}, err
	}
	groups, err := normalizeGroupIDs(groupIDs)
	if err != nil {
		return GroupShadowBatch{}, err
	}
	now := time.Now().UTC()
	if compiler.Now != nil {
		now = compiler.Now().UTC()
	}
	intentDigest := routeIntentSemanticDigest(snapshot)
	batch := GroupShadowBatch{
		Schema:                GroupShadowBatchSchemaV1,
		RouteIntentGeneration: strings.TrimSpace(snapshot.Generation),
		Results:               make([]GroupShadowResult, len(groups)),
	}
	var wait sync.WaitGroup
	for index, groupID := range groups {
		wait.Add(1)
		go func() {
			defer wait.Done()
			batch.Results[index] = compiler.reconcileGroup(ctx, snapshot, intentDigest, groupID, now)
		}()
	}
	wait.Wait()
	for _, result := range batch.Results {
		if result.Status == GroupShadowStatusCompiled {
			batch.Succeeded++
		} else {
			batch.Failed++
		}
	}
	return batch, nil
}

func (compiler GroupShadowCompiler) reconcileGroup(ctx context.Context, snapshot model.EdgeRouteIntentSnapshot, intentDigest, groupID string, now time.Time) GroupShadowResult {
	var result GroupShadowResult
	for attempt := 0; attempt < groupShadowCASAttempts; attempt++ {
		var appendErr error
		result, appendErr = compiler.reconcileGroupAttempt(ctx, snapshot, intentDigest, groupID, now)
		if appendErr == nil || !groupShadowCASConflict(appendErr) {
			return result
		}
	}
	return result
}

func (compiler GroupShadowCompiler) reconcileGroupAttempt(ctx context.Context, snapshot model.EdgeRouteIntentSnapshot, intentDigest, groupID string, now time.Time) (GroupShadowResult, error) {
	head, exists, err := compiler.Ledger.Head(ctx, groupID)
	if err != nil {
		return GroupShadowResult{GroupID: groupID, Status: GroupShadowStatusFailed, FailureCode: GroupShadowFailureLedgerRead}, nil
	}
	expectedSequence := uint64(0)
	lastSuccessful := ""
	if exists {
		expectedSequence = head.Sequence
		lastSuccessful = head.LastSuccessfulBundleGeneration
		if head.Status == GroupShadowStatusCompiled && head.Bundle != nil {
			lastSuccessful = head.BundleGeneration
		}
	}

	entry := GroupShadowLedgerEntry{
		Schema:                         GroupShadowLedgerSchemaV1,
		GroupID:                        groupID,
		RouteIntentGeneration:          strings.TrimSpace(snapshot.Generation),
		LastSuccessfulBundleGeneration: lastSuccessful,
		Authority:                      "none",
		PublicationEnabled:             false,
		RecordedAt:                     now,
	}
	inventory, readErr := compiler.Inventory.ReadGroupInventory(ctx, groupID)
	if readErr != nil {
		entry.Status = GroupShadowStatusFailed
		entry.FailureCode = GroupShadowFailureInventoryRead
		entry.InputDigest = groupShadowInputDigest(intentDigest, groupID, "inventory-read-failed")
		if sameGroupShadowFailure(head, exists, entry) {
			return groupShadowResultFromEntry(head), nil
		}
		return compiler.appendResult(ctx, groupID, expectedSequence, entry)
	}
	entry.InventoryGeneration = strings.TrimSpace(inventory.Generation)
	entry.InventoryDigest = groupInventorySemanticDigest(inventory)
	entry.InputDigest = groupShadowInputDigest(intentDigest, groupID, entry.InventoryDigest)

	var view groupInventoryView
	var inventoryErr error
	if compiler.InventoryMaxAge > 0 && (inventory.ObservedAt.IsZero() || inventory.ObservedAt.After(now.Add(maxInventoryHeartbeatClockSkew)) || now.Sub(inventory.ObservedAt) > compiler.InventoryMaxAge) {
		inventoryErr = errGroupInventoryInvalid
	} else {
		view, inventoryErr = validateGroupInventory(groupID, inventory)
	}
	if inventoryErr != nil {
		entry.Status = GroupShadowStatusFailed
		entry.FailureCode = GroupShadowFailureInventoryInvalid
		if errors.Is(inventoryErr, errNoHealthyActiveInstances) {
			entry.FailureCode = GroupShadowFailureNoHealthyActive
		}
		if sameGroupShadowFailure(head, exists, entry) {
			return groupShadowResultFromEntry(head), nil
		}
		return compiler.appendResult(ctx, groupID, expectedSequence, entry)
	}
	entry.ActiveSlot = view.activeEpoch.Slot
	entry.ActiveReleaseEpoch = view.activeEpoch.ReleaseEpoch
	entry.ActiveFenceSequence = view.activeEpoch.FenceSequence
	entry.ActiveHealthyInstances = len(view.healthyEdgeIDs)

	bundle, compileErr := compileGroupShadowCandidate(snapshot, inventory, view, lastSuccessful, now)
	if compileErr != nil {
		entry.Status = GroupShadowStatusFailed
		entry.FailureCode = GroupShadowFailureCompile
		if errors.Is(compileErr, errNoRoutableRoutes) {
			entry.FailureCode = GroupShadowFailureNoRoutableRoutes
		}
		if sameGroupShadowFailure(head, exists, entry) {
			return groupShadowResultFromEntry(head), nil
		}
		return compiler.appendResult(ctx, groupID, expectedSequence, entry)
	}
	entry.Status = GroupShadowStatusCompiled
	entry.BundleGeneration = bundle.Generation
	entry.LastSuccessfulBundleGeneration = bundle.Generation
	entry.Bundle = &bundle
	if exists && head.Status == GroupShadowStatusCompiled && head.Bundle != nil && head.InputDigest == entry.InputDigest &&
		head.RouteIntentGeneration == entry.RouteIntentGeneration && head.BundleGeneration == entry.BundleGeneration {
		return groupShadowResultFromEntry(head), nil
	}
	return compiler.appendResult(ctx, groupID, expectedSequence, entry)
}

func (compiler GroupShadowCompiler) appendResult(ctx context.Context, groupID string, expectedSequence uint64, entry GroupShadowLedgerEntry) (GroupShadowResult, error) {
	appended, err := compiler.Ledger.AppendCAS(ctx, groupID, expectedSequence, entry)
	if err != nil {
		return GroupShadowResult{
			GroupID:                        groupID,
			Status:                         GroupShadowStatusFailed,
			LedgerSequence:                 expectedSequence,
			InputDigest:                    entry.InputDigest,
			LastSuccessfulBundleGeneration: entry.LastSuccessfulBundleGeneration,
			FailureCode:                    GroupShadowFailureLedgerCAS,
		}, err
	}
	return groupShadowResultFromEntry(appended), nil
}

func groupShadowCASConflict(err error) bool {
	return errors.Is(err, ErrGroupShadowInputCAS) || errors.Is(err, ErrGroupShadowLedgerConflict)
}

func groupShadowResultFromEntry(entry GroupShadowLedgerEntry) GroupShadowResult {
	return GroupShadowResult{
		GroupID:                        entry.GroupID,
		Status:                         entry.Status,
		LedgerSequence:                 entry.Sequence,
		InputDigest:                    entry.InputDigest,
		BundleGeneration:               entry.BundleGeneration,
		LastSuccessfulBundleGeneration: entry.LastSuccessfulBundleGeneration,
		FailureCode:                    entry.FailureCode,
	}
}

func sameGroupShadowFailure(head GroupShadowLedgerEntry, exists bool, candidate GroupShadowLedgerEntry) bool {
	return exists && head.Status == GroupShadowStatusFailed && candidate.Status == GroupShadowStatusFailed &&
		head.RouteIntentGeneration == candidate.RouteIntentGeneration && head.InputDigest == candidate.InputDigest &&
		head.FailureCode == candidate.FailureCode && head.LastSuccessfulBundleGeneration == candidate.LastSuccessfulBundleGeneration
}

type groupInventoryView struct {
	activeEpoch    GroupActiveEpoch
	healthyEdgeIDs []string
}

func validateGroupInventory(groupID string, snapshot GroupInventorySnapshot) (groupInventoryView, error) {
	groupID = normalizeGroupID(groupID)
	if snapshot.Schema != GroupInventorySchemaV1 || normalizeGroupID(snapshot.GroupID) != groupID || snapshot.Sequence == 0 || strings.TrimSpace(snapshot.Generation) == "" {
		return groupInventoryView{}, errGroupInventoryInvalid
	}
	epoch := snapshot.ActiveEpoch
	epoch.GroupID = normalizeGroupID(epoch.GroupID)
	epoch.Slot = normalizeSlot(epoch.Slot)
	epoch.ReleaseEpoch = strings.TrimSpace(epoch.ReleaseEpoch)
	if epoch.GroupID != groupID || !validEdgeSlot(epoch.Slot) || epoch.ReleaseEpoch == "" || epoch.FenceSequence == 0 || epoch.MinHealthyInstances <= 0 {
		return groupInventoryView{}, errGroupInventoryInvalid
	}

	activeSeen := make(map[string]struct{})
	healthySeen := make(map[string]struct{})
	healthy := make([]string, 0, len(snapshot.Instances))
	for _, instance := range snapshot.Instances {
		instanceGroup := normalizeGroupID(instance.GroupID)
		edgeID := normalizeEdgeIdentity(instance.EdgeID)
		slot := normalizeSlot(instance.Slot)
		releaseEpoch := strings.TrimSpace(instance.ReleaseEpoch)
		if slot != epoch.Slot || releaseEpoch != epoch.ReleaseEpoch {
			// Inactive and legacy identities remain observable in the inventory
			// snapshot, but cannot invalidate or contribute health to the exact
			// active epoch for this group.
			continue
		}
		if instanceGroup != groupID || edgeID == "" || strings.TrimSpace(instance.InstanceUID) == "" {
			return groupInventoryView{}, errGroupInventoryInvalid
		}
		if _, duplicate := activeSeen[edgeID]; duplicate {
			return groupInventoryView{}, errGroupInventoryInvalid
		}
		activeSeen[edgeID] = struct{}{}
		if !instance.EffectiveHealthy || !instance.NodeHealthy || instance.Draining ||
			model.NormalizeEdgeHealthStatus(instance.NodeStatus) != model.EdgeHealthHealthy || strings.TrimSpace(instance.FailureClass) != "" {
			continue
		}
		if _, duplicate := healthySeen[edgeID]; duplicate {
			continue
		}
		healthySeen[edgeID] = struct{}{}
		healthy = append(healthy, edgeID)
	}
	if len(activeSeen) == 0 {
		return groupInventoryView{}, errGroupInventoryInvalid
	}
	if len(healthy) < epoch.MinHealthyInstances {
		return groupInventoryView{}, errNoHealthyActiveInstances
	}
	sort.Strings(healthy)
	return groupInventoryView{activeEpoch: epoch, healthyEdgeIDs: healthy}, nil
}

func compileGroupShadowCandidate(snapshot model.EdgeRouteIntentSnapshot, inventory GroupInventorySnapshot, view groupInventoryView, previousGeneration string, now time.Time) (model.EdgeRouteBundle, error) {
	groupID := normalizeGroupID(inventory.GroupID)
	routes := append([]model.EdgeRouteIntent(nil), snapshot.Routes...)
	sort.Slice(routes, func(i, j int) bool {
		left, right := routes[i], routes[j]
		if normalizeHostname(left.Hostname) != normalizeHostname(right.Hostname) {
			return normalizeHostname(left.Hostname) < normalizeHostname(right.Hostname)
		}
		if model.NormalizeAppRoutePathPrefix(left.PathPrefix) != model.NormalizeAppRoutePathPrefix(right.PathPrefix) {
			return model.NormalizeAppRoutePathPrefix(left.PathPrefix) < model.NormalizeAppRoutePathPrefix(right.PathPrefix)
		}
		if left.RouteKind != right.RouteKind {
			return left.RouteKind < right.RouteKind
		}
		if left.Generation != right.Generation {
			return left.Generation < right.Generation
		}
		return canonicalJSON(left) < canonicalJSON(right)
	})

	bindings := make([]model.EdgeRouteBinding, 0, len(routes))
	trafficRoutes := 0
	routeHostnames := make(map[string]struct{})
	routeKeys := make(map[string]struct{})
	usedCachePolicies := make(map[string]struct{})
	for _, intent := range routes {
		applies, err := routeIntentAppliesToGroup(intent, groupID)
		if err != nil {
			return model.EdgeRouteBundle{}, err
		}
		if !applies {
			continue
		}
		binding, included, err := compileGroupRouteBinding(intent, snapshot.Generation, inventory.Generation, groupID, view.healthyEdgeIDs)
		if err != nil {
			return model.EdgeRouteBundle{}, err
		}
		if !included {
			continue
		}
		routeKey := binding.Hostname + "\x00" + binding.PathPrefix
		if _, duplicate := routeKeys[routeKey]; duplicate {
			return model.EdgeRouteBundle{}, errors.New("duplicate route intent for group")
		}
		routeKeys[routeKey] = struct{}{}
		bindings = append(bindings, binding)
		routeHostnames[normalizeHostname(binding.Hostname)] = struct{}{}
		if binding.CachePolicyID != "" {
			usedCachePolicies[binding.CachePolicyID] = struct{}{}
		}
		if binding.Status == model.EdgeRouteStatusActive && model.EdgeRoutePolicyAllowsTraffic(binding.RoutePolicy) && routeHasUpstream(binding) {
			trafficRoutes++
		}
	}
	if len(bindings) == 0 || trafficRoutes == 0 {
		return model.EdgeRouteBundle{}, errNoRoutableRoutes
	}

	tlsAllowlist := filterTLSAllowlist(snapshot.TLSAllowlist, routeHostnames)
	cachePolicies := filterCachePolicies(snapshot.CachePolicies, usedCachePolicies)
	bundle := model.EdgeRouteBundle{
		SchemaVersion: model.BundleSchemaVersionV1,
		GeneratedAt:   now,
		Issuer:        groupShadowIssuer,
		EdgeGroupID:   groupID,
		Routes:        bindings,
		TLSAllowlist:  tlsAllowlist,
		CachePolicies: cachePolicies,
	}
	bundle.Version = groupShadowBundleGeneration(bundle, snapshot.Generation, inventory)
	bundle.Generation = bundle.Version
	if previousGeneration = strings.TrimSpace(previousGeneration); previousGeneration != "" && previousGeneration != bundle.Generation {
		bundle.PreviousGeneration = previousGeneration
	}
	return bundle, nil
}

func compileGroupRouteBinding(intent model.EdgeRouteIntent, routeIntentGeneration, inventoryGeneration, groupID string, healthyEdgeIDs []string) (model.EdgeRouteBinding, bool, error) {
	hostname := normalizeHostname(intent.Hostname)
	policy := model.NormalizeEdgeRoutePolicy(intent.RoutePolicy)
	if strings.TrimSpace(intent.Generation) == "" || hostname == "" || strings.TrimSpace(intent.RouteKind) == "" || policy == "" {
		return model.EdgeRouteBinding{}, false, fmt.Errorf("%w: invalid route intent identity", errGroupInventoryInvalid)
	}
	excludedIDs := normalizeIdentitySet(intent.ExcludedEdgeIDs)
	allowedHealthy := 0
	for _, edgeID := range healthyEdgeIDs {
		if _, excluded := excludedIDs[normalizeEdgeIdentity(edgeID)]; !excluded {
			allowedHealthy++
		}
	}
	if allowedHealthy == 0 {
		return model.EdgeRouteBinding{}, false, nil
	}
	status := strings.TrimSpace(intent.OriginStatus)
	if status == "" {
		status = model.EdgeRouteStatusActive
	}
	binding := model.EdgeRouteBinding{
		Hostname: hostname, PathPrefix: model.NormalizeAppRoutePathPrefix(intent.PathPrefix), RouteKind: strings.TrimSpace(intent.RouteKind),
		AppID: strings.TrimSpace(intent.AppID), TenantID: strings.TrimSpace(intent.TenantID), RuntimeID: strings.TrimSpace(intent.RuntimeID),
		RuntimeType: strings.TrimSpace(intent.RuntimeType), RuntimeEdgeGroup: strings.TrimSpace(intent.RuntimeEdgeGroupID),
		RuntimeEdgeGroupID: strings.TrimSpace(intent.RuntimeEdgeGroupID), RuntimeClusterNode: strings.TrimSpace(intent.RuntimeClusterNode),
		SelectedEdgeGroup: groupID, EdgeGroupID: groupID,
		ExcludedEdgeIDs: normalizeIdentitySlice(intent.ExcludedEdgeIDs), ExcludedEdgeGroupIDs: normalizeGroupIDSlice(intent.ExcludedEdgeGroupIDs),
		ExclusionReason: strings.TrimSpace(intent.ExclusionReason), ExclusionExpiresAt: intent.ExclusionExpiresAt,
		MinHealthyEdgeNodes: intent.MinHealthyEdgeNodes, HealthyEdgeNodeCount: allowedHealthy,
		RoutePolicy: policy, SelectionReason: "active epoch inventory is healthy",
		UpstreamKind: strings.TrimSpace(intent.UpstreamKind), UpstreamScope: strings.TrimSpace(intent.UpstreamScope),
		UpstreamURL: strings.TrimSpace(intent.UpstreamURL), Upstreams: cloneRouteUpstreams(intent.Upstreams), ServicePort: intent.ServicePort,
		TLSPolicy: strings.TrimSpace(intent.TLSPolicy), CachePolicyID: strings.TrimSpace(intent.CachePolicyID),
		CacheNamespace: strings.TrimSpace(intent.CacheNamespace), DeploymentGeneration: strings.TrimSpace(intent.DeploymentGeneration),
		RequestBodyPolicies: model.CloneEdgeRequestBodyPolicies(intent.RequestBodyPolicies), Streaming: intent.Streaming,
		Status: status, StatusReason: strings.TrimSpace(intent.OriginStatusReason), CreatedAt: intent.CreatedAt, UpdatedAt: intent.UpdatedAt,
	}
	binding.EdgeRedundancyStatus = "ok"
	if binding.MinHealthyEdgeNodes > 0 && allowedHealthy < binding.MinHealthyEdgeNodes {
		binding.EdgeRedundancyStatus = "at_risk"
		binding.EdgeRedundancyReason = fmt.Sprintf("healthy active edge instances %d below minimum %d", allowedHealthy, binding.MinHealthyEdgeNodes)
	}
	if binding.Status != model.EdgeRouteStatusActive || !model.EdgeRoutePolicyAllowsTraffic(binding.RoutePolicy) {
		binding.UpstreamURL = ""
		binding.Upstreams = nil
	}
	if binding.Status == model.EdgeRouteStatusActive && model.EdgeRoutePolicyAllowsTraffic(binding.RoutePolicy) && !routeHasUpstream(binding) {
		return model.EdgeRouteBinding{}, false, errors.New("active route intent has no upstream")
	}
	binding.RouteGeneration = shadowRouteGeneration(binding, intent.Generation, groupID)
	binding.DecisionID = shadowDecisionID(routeIntentGeneration, inventoryGeneration, groupID, binding.RouteGeneration)
	return binding, true, nil
}

func routeIntentAppliesToGroup(intent model.EdgeRouteIntent, groupID string) (bool, error) {
	for _, excluded := range normalizeGroupIDSlice(intent.ExcludedEdgeGroupIDs) {
		if excluded == groupID {
			return false, nil
		}
	}
	switch strings.TrimSpace(intent.TargetGroupMode) {
	case model.EdgeRouteIntentGroupModeAllGroups:
		return true, nil
	case model.EdgeRouteIntentGroupModePinnedGroup:
		pinned := normalizeGroupID(intent.PinnedEdgeGroupID)
		if pinned == "" {
			return false, errors.New("pinned route intent is missing edge group")
		}
		return pinned == groupID, nil
	default:
		return false, errors.New("route intent has invalid target group mode")
	}
}

func validateRouteIntentSnapshot(snapshot model.EdgeRouteIntentSnapshot) error {
	if snapshot.SchemaVersion != model.EdgeRouteIntentSchemaVersionV1 {
		return fmt.Errorf("edge-control requires RouteIntent schema %q", model.EdgeRouteIntentSchemaVersionV1)
	}
	if strings.TrimSpace(snapshot.Generation) == "" {
		return errors.New("edge-control RouteIntent generation is required")
	}
	return nil
}

func normalizeGroupIDs(values []string) ([]string, error) {
	set := make(map[string]struct{}, len(values))
	for _, raw := range values {
		value := normalizeGroupID(raw)
		if value == "" || raw != value || !edgeGroupIDPattern.MatchString(value) {
			return nil, errors.New("edge-control group id must be canonical")
		}
		set[value] = struct{}{}
	}
	if len(set) == 0 {
		return nil, errors.New("edge-control group shadow reconcile requires at least one group")
	}
	if len(set) > 64 {
		return nil, errors.New("edge-control group reconcile exceeds the bounded group limit")
	}
	out := make([]string, 0, len(set))
	for value := range set {
		out = append(out, value)
	}
	sort.Strings(out)
	return out, nil
}

func normalizeGroupID(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func normalizeEdgeIdentity(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func normalizeSlot(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func validEdgeSlot(value string) bool {
	switch value {
	case model.EdgeSlotA, model.EdgeSlotB, model.EdgeSlotDirect:
		return true
	default:
		return false
	}
}

func normalizeHostname(value string) string {
	return strings.TrimSuffix(strings.ToLower(strings.TrimSpace(value)), ".")
}

func normalizeIdentitySet(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value = normalizeEdgeIdentity(value); value != "" {
			out[value] = struct{}{}
		}
	}
	return out
}

func normalizeIdentitySlice(values []string) []string {
	set := normalizeIdentitySet(values)
	out := make([]string, 0, len(set))
	for value := range set {
		out = append(out, value)
	}
	sort.Strings(out)
	if len(out) == 0 {
		return nil
	}
	return out
}

func normalizeGroupIDSlice(values []string) []string {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value = normalizeGroupID(value); value != "" {
			set[value] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for value := range set {
		out = append(out, value)
	}
	sort.Strings(out)
	if len(out) == 0 {
		return nil
	}
	return out
}

func routeHasUpstream(binding model.EdgeRouteBinding) bool {
	if strings.TrimSpace(binding.UpstreamURL) != "" {
		return true
	}
	for _, upstream := range binding.Upstreams {
		if strings.TrimSpace(upstream.UpstreamURL) != "" {
			return true
		}
	}
	return false
}

func cloneRouteUpstreams(values []model.EdgeRouteUpstream) []model.EdgeRouteUpstream {
	if len(values) == 0 {
		return nil
	}
	return append([]model.EdgeRouteUpstream(nil), values...)
}

func filterTLSAllowlist(values []model.EdgeTLSAllowlistEntry, hostnames map[string]struct{}) []model.EdgeTLSAllowlistEntry {
	byHostname := make(map[string]model.EdgeTLSAllowlistEntry)
	for _, value := range values {
		hostname := normalizeHostname(value.Hostname)
		if _, ok := hostnames[hostname]; !ok || hostname == "" {
			continue
		}
		value.Hostname = hostname
		previous, exists := byHostname[hostname]
		if !exists || canonicalJSON(value) < canonicalJSON(previous) {
			byHostname[hostname] = value
		}
	}
	out := make([]model.EdgeTLSAllowlistEntry, 0, len(byHostname))
	for _, value := range byHostname {
		out = append(out, value)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Hostname < out[j].Hostname })
	if len(out) == 0 {
		return nil
	}
	return out
}

func filterCachePolicies(values []model.CachePolicy, used map[string]struct{}) []model.CachePolicy {
	byID := make(map[string]model.CachePolicy, len(used))
	for _, value := range values {
		id := strings.TrimSpace(value.ID)
		if _, ok := used[id]; !ok {
			continue
		}
		value.ID = id
		value.PathPatterns = append([]string(nil), value.PathPatterns...)
		value.MethodAllowlist = append([]string(nil), value.MethodAllowlist...)
		value.StatusAllowlist = append([]int(nil), value.StatusAllowlist...)
		value.VaryAllowlist = append([]string(nil), value.VaryAllowlist...)
		previous, exists := byID[id]
		if !exists || canonicalJSON(value) < canonicalJSON(previous) {
			byID[id] = value
		}
	}
	out := make([]model.CachePolicy, 0, len(byID))
	for _, value := range byID {
		out = append(out, value)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	if len(out) == 0 {
		return nil
	}
	return out
}

func routeIntentSemanticDigest(snapshot model.EdgeRouteIntentSnapshot) string {
	routes := append([]model.EdgeRouteIntent(nil), snapshot.Routes...)
	sort.Slice(routes, func(i, j int) bool {
		left, right := routes[i], routes[j]
		if normalizeHostname(left.Hostname) != normalizeHostname(right.Hostname) {
			return normalizeHostname(left.Hostname) < normalizeHostname(right.Hostname)
		}
		if model.NormalizeAppRoutePathPrefix(left.PathPrefix) != model.NormalizeAppRoutePathPrefix(right.PathPrefix) {
			return model.NormalizeAppRoutePathPrefix(left.PathPrefix) < model.NormalizeAppRoutePathPrefix(right.PathPrefix)
		}
		if left.RouteKind != right.RouteKind {
			return left.RouteKind < right.RouteKind
		}
		if left.Generation != right.Generation {
			return left.Generation < right.Generation
		}
		return canonicalJSON(left) < canonicalJSON(right)
	})
	tls := append([]model.EdgeTLSAllowlistEntry(nil), snapshot.TLSAllowlist...)
	sort.Slice(tls, func(i, j int) bool {
		if normalizeHostname(tls[i].Hostname) != normalizeHostname(tls[j].Hostname) {
			return normalizeHostname(tls[i].Hostname) < normalizeHostname(tls[j].Hostname)
		}
		return canonicalJSON(tls[i]) < canonicalJSON(tls[j])
	})
	cache := filterCachePolicies(snapshot.CachePolicies, cachePolicyIDSet(snapshot.CachePolicies))
	return digestJSON(struct {
		SchemaVersion string                        `json:"schema_version"`
		Generation    string                        `json:"generation"`
		Routes        []model.EdgeRouteIntent       `json:"routes"`
		TLSAllowlist  []model.EdgeTLSAllowlistEntry `json:"tls_allowlist"`
		CachePolicies []model.CachePolicy           `json:"cache_policies,omitempty"`
	}{snapshot.SchemaVersion, strings.TrimSpace(snapshot.Generation), routes, tls, cache})
}

func cachePolicyIDSet(values []model.CachePolicy) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		if id := strings.TrimSpace(value.ID); id != "" {
			out[id] = struct{}{}
		}
	}
	return out
}

func groupInventorySemanticDigest(snapshot GroupInventorySnapshot) string {
	type instanceIdentity struct {
		EdgeID           string `json:"edge_id"`
		GroupID          string `json:"edge_group_id"`
		Slot             string `json:"slot"`
		InstanceUID      string `json:"instance_uid"`
		ReleaseEpoch     string `json:"release_epoch"`
		EffectiveHealthy bool   `json:"effective_healthy"`
		NodeHealthy      bool   `json:"node_healthy"`
		NodeStatus       string `json:"node_status"`
		Draining         bool   `json:"draining"`
		FailureClass     string `json:"failure_class,omitempty"`
	}
	instances := make([]instanceIdentity, 0, len(snapshot.Instances))
	activeSlot := normalizeSlot(snapshot.ActiveEpoch.Slot)
	activeReleaseEpoch := strings.TrimSpace(snapshot.ActiveEpoch.ReleaseEpoch)
	for _, instance := range snapshot.Instances {
		if normalizeSlot(instance.Slot) != activeSlot || strings.TrimSpace(instance.ReleaseEpoch) != activeReleaseEpoch {
			continue
		}
		instances = append(instances, instanceIdentity{
			EdgeID: normalizeEdgeIdentity(instance.EdgeID), GroupID: normalizeGroupID(instance.GroupID),
			Slot: normalizeSlot(instance.Slot), InstanceUID: strings.TrimSpace(instance.InstanceUID), ReleaseEpoch: strings.TrimSpace(instance.ReleaseEpoch),
			EffectiveHealthy: instance.EffectiveHealthy, NodeHealthy: instance.NodeHealthy,
			NodeStatus: model.NormalizeEdgeHealthStatus(instance.NodeStatus), Draining: instance.Draining,
			FailureClass: strings.TrimSpace(instance.FailureClass),
		})
	}
	sort.Slice(instances, func(i, j int) bool {
		left, right := instances[i], instances[j]
		if left.EdgeID != right.EdgeID {
			return left.EdgeID < right.EdgeID
		}
		if left.Slot != right.Slot {
			return left.Slot < right.Slot
		}
		return left.InstanceUID < right.InstanceUID
	})
	epoch := struct {
		GroupID            string `json:"edge_group_id"`
		Slot               string `json:"slot"`
		ReleaseEpoch       string `json:"release_epoch"`
		FenceSequence      uint64 `json:"fence_sequence"`
		MinHealthyInstance int    `json:"min_healthy_instances"`
	}{normalizeGroupID(snapshot.ActiveEpoch.GroupID), normalizeSlot(snapshot.ActiveEpoch.Slot), strings.TrimSpace(snapshot.ActiveEpoch.ReleaseEpoch), snapshot.ActiveEpoch.FenceSequence, snapshot.ActiveEpoch.MinHealthyInstances}
	return digestJSON(struct {
		Schema     string             `json:"schema"`
		GroupID    string             `json:"edge_group_id"`
		Sequence   uint64             `json:"sequence"`
		Generation string             `json:"generation"`
		Epoch      any                `json:"active_epoch"`
		Instances  []instanceIdentity `json:"instances"`
	}{snapshot.Schema, normalizeGroupID(snapshot.GroupID), snapshot.Sequence, strings.TrimSpace(snapshot.Generation), epoch, instances})
}

func groupShadowInputDigest(intentDigest, groupID, inventoryDigest string) string {
	return digestJSON([]string{intentDigest, normalizeGroupID(groupID), inventoryDigest})
}

func shadowRouteGeneration(binding model.EdgeRouteBinding, intentGeneration, groupID string) string {
	binding.RouteGeneration = ""
	binding.DecisionID = ""
	return "edgegrouproute_" + rawDigestHex(struct {
		IntentGeneration string                 `json:"intent_generation"`
		GroupID          string                 `json:"edge_group_id"`
		Binding          model.EdgeRouteBinding `json:"binding"`
	}{strings.TrimSpace(intentGeneration), normalizeGroupID(groupID), binding})
}

func shadowDecisionID(routeIntentGeneration, inventoryGeneration, groupID, routeGeneration string) string {
	return "edgegroupdecision_" + rawDigestHex([]string{
		strings.TrimSpace(routeIntentGeneration), strings.TrimSpace(inventoryGeneration), normalizeGroupID(groupID), strings.TrimSpace(routeGeneration),
	})
}

func groupShadowBundleGeneration(bundle model.EdgeRouteBundle, routeIntentGeneration string, inventory GroupInventorySnapshot) string {
	bundle.Version = ""
	bundle.Generation = ""
	bundle.PreviousGeneration = ""
	bundle.GeneratedAt = time.Time{}
	return "edgegroupbundle_" + rawDigestHex(struct {
		RouteIntentGeneration string                `json:"route_intent_generation"`
		InventoryGeneration   string                `json:"inventory_generation"`
		InventoryDigest       string                `json:"inventory_digest"`
		Bundle                model.EdgeRouteBundle `json:"bundle"`
	}{strings.TrimSpace(routeIntentGeneration), strings.TrimSpace(inventory.Generation), groupInventorySemanticDigest(inventory), bundle})
}

func digestJSON(value any) string {
	return "sha256:" + rawDigestHex(value)
}

func rawDigestHex(value any) string {
	payload, err := json.Marshal(value)
	if err != nil {
		panic(fmt.Sprintf("marshal edge-control canonical material: %v", err))
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func canonicalJSON(value any) string {
	payload, err := json.Marshal(value)
	if err != nil {
		panic(fmt.Sprintf("marshal edge-control canonical value: %v", err))
	}
	return string(payload)
}

// MemoryGroupShadowLedger is a race-safe reference implementation for the
// domain contract and tests. Runtime authority must replace it with an Edge
// Control-owned durable implementation before any group cutover.
type MemoryGroupShadowLedger struct {
	mu      sync.RWMutex
	entries map[string][]GroupShadowLedgerEntry
}

func NewMemoryGroupShadowLedger() *MemoryGroupShadowLedger {
	return &MemoryGroupShadowLedger{entries: make(map[string][]GroupShadowLedgerEntry)}
}

func (ledger *MemoryGroupShadowLedger) Head(ctx context.Context, groupID string) (GroupShadowLedgerEntry, bool, error) {
	if err := ctx.Err(); err != nil {
		return GroupShadowLedgerEntry{}, false, err
	}
	groupID = normalizeGroupID(groupID)
	ledger.mu.RLock()
	defer ledger.mu.RUnlock()
	entries := ledger.entries[groupID]
	if len(entries) == 0 {
		return GroupShadowLedgerEntry{}, false, nil
	}
	return cloneGroupShadowLedgerEntry(entries[len(entries)-1]), true, nil
}

func (ledger *MemoryGroupShadowLedger) AppendCAS(ctx context.Context, groupID string, expectedSequence uint64, entry GroupShadowLedgerEntry) (GroupShadowLedgerEntry, error) {
	if err := ctx.Err(); err != nil {
		return GroupShadowLedgerEntry{}, err
	}
	groupID = normalizeGroupID(groupID)
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	entry, err := prepareGroupShadowLedgerAppend(groupID, expectedSequence, ledger.entries[groupID], entry)
	if err != nil {
		return GroupShadowLedgerEntry{}, err
	}
	ledger.entries[groupID] = append(ledger.entries[groupID], entry)
	return cloneGroupShadowLedgerEntry(entry), nil
}

func prepareGroupShadowLedgerAppend(groupID string, expectedSequence uint64, entries []GroupShadowLedgerEntry, entry GroupShadowLedgerEntry) (GroupShadowLedgerEntry, error) {
	groupID = normalizeGroupID(groupID)
	if groupID == "" || normalizeGroupID(entry.GroupID) != groupID || entry.Schema != GroupShadowLedgerSchemaV1 || entry.Sequence != 0 {
		return GroupShadowLedgerEntry{}, errors.New("invalid edge-control group shadow ledger entry")
	}
	if entry.Authority != "none" || entry.PublicationEnabled || entry.BundleArchived || (entry.Status != GroupShadowStatusCompiled && entry.Status != GroupShadowStatusFailed) {
		return GroupShadowLedgerEntry{}, errors.New("unsafe edge-control group shadow ledger entry")
	}
	if strings.TrimSpace(entry.RouteIntentGeneration) == "" || !strings.HasPrefix(entry.InputDigest, "sha256:") || len(entry.InputDigest) != len("sha256:")+sha256.Size*2 || entry.RecordedAt.IsZero() {
		return GroupShadowLedgerEntry{}, errors.New("incomplete edge-control group shadow ledger entry")
	}
	if entry.Status == GroupShadowStatusCompiled {
		if entry.Bundle == nil || entry.BundleGeneration == "" || entry.FailureCode != "" || entry.Bundle.Generation != entry.BundleGeneration || normalizeGroupID(entry.Bundle.EdgeGroupID) != groupID {
			return GroupShadowLedgerEntry{}, errors.New("invalid compiled edge-control group shadow ledger entry")
		}
		if entry.Bundle.Issuer != groupShadowIssuer || entry.Bundle.KeyID != "" || entry.Bundle.Signature != "" || len(entry.Bundle.Signatures) != 0 || !entry.Bundle.ValidUntil.IsZero() {
			return GroupShadowLedgerEntry{}, errors.New("edge-control group compiler candidate cannot carry publication authority")
		}
	} else if entry.Bundle != nil || entry.FailureCode == "" {
		return GroupShadowLedgerEntry{}, errors.New("invalid failed edge-control group shadow ledger entry")
	}

	currentSequence := uint64(len(entries))
	if currentSequence != expectedSequence {
		return GroupShadowLedgerEntry{}, ErrGroupShadowLedgerConflict
	}
	previousSuccessful := ""
	if len(entries) > 0 {
		previous := entries[len(entries)-1]
		previousSuccessful = previous.LastSuccessfulBundleGeneration
		if previous.Status == GroupShadowStatusCompiled && previous.Bundle != nil {
			previousSuccessful = previous.BundleGeneration
		}
	}
	if entry.Status == GroupShadowStatusCompiled {
		if entry.LastSuccessfulBundleGeneration != entry.BundleGeneration {
			return GroupShadowLedgerEntry{}, errors.New("compiled edge-control group shadow entry did not advance last success")
		}
	} else if entry.LastSuccessfulBundleGeneration != previousSuccessful {
		return GroupShadowLedgerEntry{}, errors.New("failed edge-control group shadow entry changed last success")
	}
	entry.GroupID = groupID
	entry.Sequence = currentSequence + 1
	entry = cloneGroupShadowLedgerEntry(entry)
	return entry, nil
}

func (ledger *MemoryGroupShadowLedger) History(groupID string) []GroupShadowLedgerEntry {
	groupID = normalizeGroupID(groupID)
	ledger.mu.RLock()
	defer ledger.mu.RUnlock()
	entries := ledger.entries[groupID]
	out := make([]GroupShadowLedgerEntry, len(entries))
	for index := range entries {
		out[index] = cloneGroupShadowLedgerEntry(entries[index])
	}
	return out
}

func cloneGroupShadowLedgerEntry(entry GroupShadowLedgerEntry) GroupShadowLedgerEntry {
	if entry.Bundle != nil {
		bundle := cloneEdgeRouteBundle(*entry.Bundle)
		entry.Bundle = &bundle
	}
	return entry
}

func cloneEdgeRouteBundle(bundle model.EdgeRouteBundle) model.EdgeRouteBundle {
	bundle.Signatures = append([]model.BundleSignature(nil), bundle.Signatures...)
	bundle.Routes = append([]model.EdgeRouteBinding(nil), bundle.Routes...)
	for index := range bundle.Routes {
		route := &bundle.Routes[index]
		route.ExcludedEdgeIDs = append([]string(nil), route.ExcludedEdgeIDs...)
		route.ExcludedEdgeGroupIDs = append([]string(nil), route.ExcludedEdgeGroupIDs...)
		route.Upstreams = cloneRouteUpstreams(route.Upstreams)
		route.RequestBodyPolicies = model.CloneEdgeRequestBodyPolicies(route.RequestBodyPolicies)
	}
	bundle.TLSAllowlist = append([]model.EdgeTLSAllowlistEntry(nil), bundle.TLSAllowlist...)
	bundle.CachePolicies = filterCachePolicies(bundle.CachePolicies, cachePolicyIDSet(bundle.CachePolicies))
	return bundle
}
