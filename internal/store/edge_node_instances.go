package store

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

var ErrEdgeInstanceFencingNotReady = errors.New("edge instance fencing is not ready")

const (
	edgeInstanceHealthyObservationsRequired   = 2
	edgeInstanceUnhealthyObservationsRequired = 2
	edgeLegacyMigrationSlot                   = "legacy"
	edgeLegacyMigrationEpoch                  = "legacy-flat-v0"
)

func (s *Store) PutEdgeActiveEpoch(model.EdgeActiveEpoch) (model.EdgeActiveEpoch, error) {
	// Active epochs can move only through the signed activation transaction.
	return model.EdgeActiveEpoch{}, ErrConflict
}

func (s *Store) ListEdgeNodeInstances(edgeGroupID string) ([]model.EdgeNodeInstance, []model.EdgeActiveEpoch, error) {
	edgeGroupID = normalizeEdgeGroupID(edgeGroupID)
	if s.usingDatabase() {
		return s.pgListEdgeNodeInstances(edgeGroupID)
	}
	var instances []model.EdgeNodeInstance
	var epochs []model.EdgeActiveEpoch
	err := s.withLockedState(false, func(state *model.State) error {
		if err := verifyEdgeInstanceState(state); err != nil {
			return err
		}
		for _, instance := range state.EdgeNodeInstances {
			if edgeGroupID == "" || instance.EdgeGroupID == edgeGroupID {
				instance.Node = redactEdgeNode(instance.Node)
				instances = append(instances, instance)
			}
		}
		for _, epoch := range state.EdgeActiveEpochs {
			if edgeGroupID == "" || epoch.EdgeGroupID == edgeGroupID {
				epochs = append(epochs, epoch)
			}
		}
		sortEdgeNodeInstances(instances)
		sort.Slice(epochs, func(i, j int) bool { return epochs[i].EdgeGroupID < epochs[j].EdgeGroupID })
		return nil
	})
	return instances, epochs, err
}

func (s *Store) GetLatestEdgeNodeInstance(edgeID string) (model.EdgeNodeInstance, error) {
	edgeID = normalizeEdgeID(edgeID)
	if edgeID == "" {
		return model.EdgeNodeInstance{}, ErrInvalidInput
	}
	instances, _, err := s.ListEdgeNodeInstances("")
	if err != nil {
		return model.EdgeNodeInstance{}, err
	}
	var latest model.EdgeNodeInstance
	found := false
	for _, instance := range instances {
		if instance.EdgeID != edgeID || instance.Slot == edgeLegacyMigrationSlot {
			continue
		}
		if !found || instance.LastHeartbeatAt.After(latest.LastHeartbeatAt) ||
			(instance.LastHeartbeatAt.Equal(latest.LastHeartbeatAt) && edgeNodeInstanceKey(instance) > edgeNodeInstanceKey(latest)) {
			latest = instance
			found = true
		}
	}
	if !found {
		return model.EdgeNodeInstance{}, ErrNotFound
	}
	latest.Node = redactEdgeNode(latest.Node)
	return latest, nil
}

func (s *Store) UpdateEdgeInstanceHeartbeat(instance model.EdgeNodeInstance) (model.EdgeNodeInstance, error) {
	return s.updateEdgeInstanceHeartbeatAt(instance, time.Now().UTC())
}

func (s *Store) updateEdgeInstanceHeartbeatAt(instance model.EdgeNodeInstance, now time.Time) (model.EdgeNodeInstance, error) {
	instance, err := normalizeEdgeNodeInstance(instance)
	if err != nil {
		return model.EdgeNodeInstance{}, err
	}
	now = now.UTC()
	if now.IsZero() {
		return model.EdgeNodeInstance{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgUpdateEdgeInstanceHeartbeatAt(instance, now)
	}
	var out model.EdgeNodeInstance
	err = s.withLockedState(true, func(state *model.State) error {
		if strings.TrimSpace(state.EdgeInstanceFencingSchema) != model.EdgeInstanceFencingSchemaV1 {
			return ErrEdgeInstanceFencingNotReady
		}
		controlIndex := findEdgeNode(state, instance.EdgeID)
		if controlIndex < 0 || normalizeEdgeGroupID(state.EdgeNodes[controlIndex].EdgeGroupID) != instance.EdgeGroupID {
			return fmt.Errorf("%w: edge instance has no exact control identity", ErrEdgeInstanceFencingNotReady)
		}
		index := findEdgeNodeInstance(state.EdgeNodeInstances, instance)
		if index >= 0 {
			applyEdgeInstanceHealthTransition(&instance, state.EdgeNodeInstances[index], now)
			instance.CreatedAt = state.EdgeNodeInstances[index].CreatedAt
		} else {
			applyEdgeInstanceHealthTransition(&instance, model.EdgeNodeInstance{}, now)
			instance.CreatedAt = now
		}
		instance.LastHeartbeatAt = now
		instance.UpdatedAt = now
		instance.Node.LastSeenAt = edgeInstanceTimePtr(now)
		instance.Node.LastHeartbeatAt = edgeInstanceTimePtr(now)
		instance.Node.UpdatedAt = now
		if instance.Node.CreatedAt.IsZero() {
			instance.Node.CreatedAt = instance.CreatedAt
		}
		if index >= 0 {
			state.EdgeNodeInstances[index] = instance
		} else {
			state.EdgeNodeInstances = append(state.EdgeNodeInstances, instance)
		}
		out = instance
		return nil
	})
	return out, err
}

// ListActiveEdgeNodes is the only health inventory suitable for route and DNS
// selection. It never falls back to the legacy edge_nodes heartbeat fields.
func (s *Store) ListActiveEdgeNodes(edgeGroupID string) ([]model.EdgeNode, []model.EdgeGroup, error) {
	edgeGroupID = normalizeEdgeGroupID(edgeGroupID)
	if s.usingDatabase() {
		return s.pgListRouteEdgeNodes(edgeGroupID)
	}
	var nodes []model.EdgeNode
	var groups []model.EdgeGroup
	err := s.withLockedState(false, func(state *model.State) error {
		if err := verifyEdgeActivationReadiness(state); err != nil {
			return err
		}
		activation, err := normalizeStoredEdgeActivation(*state.EdgeActivation)
		if err != nil {
			return err
		}
		if !edgeActivationUsesInstanceRoutes(activation) {
			for _, node := range state.EdgeNodes {
				normalizeEdgeNodeForRead(&node)
				if edgeGroupID == "" || node.EdgeGroupID == edgeGroupID {
					nodes = append(nodes, redactEdgeNode(node))
				}
			}
			groups = edgeGroupSummaries(state.EdgeGroups, state.EdgeNodes, edgeGroupID)
			sortEdgeNodes(nodes)
			return nil
		}
		var aggregateErr error
		nodes, groups, aggregateErr = aggregateActiveEdgeNodes(state.EdgeNodeInstances, state.EdgeActiveEpochs, state.EdgeGroups, edgeGroupID)
		return aggregateErr
	})
	return nodes, groups, err
}

func migrateLegacyEdgeInstancesInState(state *model.State, now time.Time) error {
	if state == nil {
		return ErrInvalidInput
	}
	schema := strings.TrimSpace(state.EdgeInstanceFencingSchema)
	if schema == model.EdgeInstanceFencingSchemaV1 {
		return verifyEdgeInstanceState(state)
	}
	if schema != "" || len(state.EdgeNodeInstances) != 0 || len(state.EdgeActiveEpochs) != 0 {
		return fmt.Errorf("%w: partial file-store edge instance schema", ErrEdgeInstanceFencingNotReady)
	}
	now = now.UTC()
	for index := range state.EdgeNodes {
		node := state.EdgeNodes[index]
		normalizeEdgeNodeForRead(&node)
		if node.ID == "" || node.EdgeGroupID == "" {
			continue
		}
		node.TokenHash = ""
		instance := model.EdgeNodeInstance{
			EdgeID:           node.ID,
			EdgeGroupID:      node.EdgeGroupID,
			Slot:             edgeLegacyMigrationSlot,
			InstanceUID:      legacyEdgeInstanceUID(node.ID),
			ReleaseEpoch:     edgeLegacyMigrationEpoch,
			Node:             node,
			EffectiveHealthy: false,
			CreatedAt:        now,
			UpdatedAt:        now,
		}
		if node.LastHeartbeatAt != nil {
			instance.LastHeartbeatAt = node.LastHeartbeatAt.UTC()
		}
		state.EdgeNodeInstances = append(state.EdgeNodeInstances, instance)
	}
	state.EdgeInstanceFencingSchema = model.EdgeInstanceFencingSchemaV1
	return verifyEdgeInstanceMaterialForPhase(state, model.EdgeActivationPhaseLegacyAuthoritative)
}

func verifyEdgeInstanceState(state *model.State) error {
	return verifyEdgeInstanceMaterialForPhase(state, model.EdgeActivationPhaseActive)
}

func verifyEdgeActivationReadiness(state *model.State) error {
	if state == nil || state.EdgeActivation == nil {
		return fmt.Errorf("%w: activation state missing", ErrEdgeInstanceFencingNotReady)
	}
	activation, err := normalizeStoredEdgeActivation(*state.EdgeActivation)
	if err != nil {
		return err
	}
	if err := verifyEdgeInstanceMaterialForPhase(state, activation.Phase); err != nil {
		return err
	}
	if activation.Phase == model.EdgeActivationPhaseFenced {
		return verifyEdgeFenceCandidates(activation.ExpectedInstances, activation.CandidateEpochs, state.EdgeNodeInstances)
	}
	return nil
}

func verifyEdgeInstanceMaterialForPhase(state *model.State, phase string) error {
	if state == nil || strings.TrimSpace(state.EdgeInstanceFencingSchema) != model.EdgeInstanceFencingSchemaV1 {
		return fmt.Errorf("%w: schema marker missing", ErrEdgeInstanceFencingNotReady)
	}
	epochs := make(map[string]model.EdgeActiveEpoch, len(state.EdgeActiveEpochs))
	for _, epoch := range state.EdgeActiveEpochs {
		normalized, err := normalizeEdgeActiveEpoch(epoch)
		if err != nil {
			return fmt.Errorf("%w: invalid active epoch", ErrEdgeInstanceFencingNotReady)
		}
		if _, exists := epochs[normalized.EdgeGroupID]; exists {
			return fmt.Errorf("%w: duplicate active epoch for %s", ErrEdgeInstanceFencingNotReady, normalized.EdgeGroupID)
		}
		epochs[normalized.EdgeGroupID] = normalized
	}
	instanceKeys := make(map[string]struct{}, len(state.EdgeNodeInstances))
	controlGroups := make(map[string]string, len(state.EdgeNodes))
	for _, node := range state.EdgeNodes {
		edgeID := normalizeEdgeID(node.ID)
		groupID := normalizeEdgeGroupID(node.EdgeGroupID)
		if edgeID == "" || groupID == "" {
			return fmt.Errorf("%w: invalid edge control identity", ErrEdgeInstanceFencingNotReady)
		}
		if existing, ok := controlGroups[edgeID]; ok && existing != groupID {
			return fmt.Errorf("%w: ambiguous edge control identity", ErrEdgeInstanceFencingNotReady)
		}
		controlGroups[edgeID] = groupID
	}
	activeMatches := make(map[string]int, len(epochs))
	instanceGroups := make(map[string]struct{})
	for _, instance := range state.EdgeNodeInstances {
		normalized, err := normalizeStoredEdgeNodeInstance(instance)
		if err != nil {
			return fmt.Errorf("%w: invalid edge instance", ErrEdgeInstanceFencingNotReady)
		}
		key := edgeNodeInstanceKey(normalized)
		if _, exists := instanceKeys[key]; exists {
			return fmt.Errorf("%w: duplicate edge instance", ErrEdgeInstanceFencingNotReady)
		}
		if controlGroups[normalized.EdgeID] != normalized.EdgeGroupID {
			return fmt.Errorf("%w: edge instance control identity mismatch", ErrEdgeInstanceFencingNotReady)
		}
		if normalized.Slot != edgeLegacyMigrationSlot {
			instanceGroups[normalized.EdgeGroupID] = struct{}{}
		}
		instanceKeys[key] = struct{}{}
		if epoch, ok := epochs[normalized.EdgeGroupID]; ok && edgeInstanceMatchesActiveEpoch(normalized, epoch) {
			activeMatches[normalized.EdgeGroupID]++
		}
	}
	activation := model.EdgeActivationState{RouteAuthority: model.EdgeRouteAuthorityLegacy}
	if state.EdgeActivation != nil {
		activation = *state.EdgeActivation
	} else if phase == model.EdgeActivationPhaseActive || phase == model.EdgeActivationPhaseEnforced {
		activation.RouteAuthority = model.EdgeRouteAuthorityActiveEpoch
	}
	if edgeActivationUsesInstanceRoutes(activation) {
		for edgeGroupID := range epochs {
			if activeMatches[edgeGroupID] == 0 {
				return fmt.Errorf("%w: active epoch %s has no exact instance", ErrEdgeInstanceFencingNotReady, edgeGroupID)
			}
		}
		for edgeGroupID := range instanceGroups {
			if _, ok := epochs[edgeGroupID]; !ok {
				return fmt.Errorf("%w: edge instance group %s has no active epoch", ErrEdgeInstanceFencingNotReady, edgeGroupID)
			}
		}
	}
	return nil
}

func aggregateActiveEdgeNodes(instances []model.EdgeNodeInstance, epochs []model.EdgeActiveEpoch, groups []model.EdgeGroup, edgeGroupID string) ([]model.EdgeNode, []model.EdgeGroup, error) {
	epochByGroup := make(map[string]model.EdgeActiveEpoch, len(epochs))
	for _, raw := range epochs {
		epoch, err := normalizeEdgeActiveEpoch(raw)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: invalid active epoch", ErrEdgeInstanceFencingNotReady)
		}
		if _, exists := epochByGroup[epoch.EdgeGroupID]; exists {
			return nil, nil, fmt.Errorf("%w: ambiguous active epoch", ErrEdgeInstanceFencingNotReady)
		}
		epochByGroup[epoch.EdgeGroupID] = epoch
	}
	byEdgeID := make(map[string][]model.EdgeNodeInstance)
	instanceGroups := make(map[string]struct{})
	for _, raw := range instances {
		instance, err := normalizeStoredEdgeNodeInstance(raw)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: invalid edge instance", ErrEdgeInstanceFencingNotReady)
		}
		if edgeGroupID != "" && instance.EdgeGroupID != edgeGroupID {
			continue
		}
		instanceGroups[instance.EdgeGroupID] = struct{}{}
		epoch, ok := epochByGroup[instance.EdgeGroupID]
		if !ok || !edgeInstanceMatchesActiveEpoch(instance, epoch) {
			continue
		}
		byEdgeID[instance.EdgeID] = append(byEdgeID[instance.EdgeID], instance)
	}
	for instanceGroupID := range instanceGroups {
		if _, ok := epochByGroup[instanceGroupID]; !ok {
			return nil, nil, fmt.Errorf("%w: active epoch missing for %s", ErrEdgeInstanceFencingNotReady, instanceGroupID)
		}
	}
	nodes := make([]model.EdgeNode, 0, len(byEdgeID))
	for _, candidates := range byEdgeID {
		sort.Slice(candidates, func(i, j int) bool {
			iMature := candidates[i].HealthStateSince.After(candidates[i].CreatedAt)
			jMature := candidates[j].HealthStateSince.After(candidates[j].CreatedAt)
			if iMature != jMature {
				return iMature
			}
			if iMature && !candidates[i].CreatedAt.Equal(candidates[j].CreatedAt) {
				// Once a replacement has completed the required health transition,
				// its physical UID fences older same-epoch processes. A late old
				// heartbeat can be audited but can never retake route authority.
				return candidates[i].CreatedAt.After(candidates[j].CreatedAt)
			}
			if candidates[i].LastHeartbeatAt.Equal(candidates[j].LastHeartbeatAt) {
				return edgeNodeInstanceKey(candidates[i]) > edgeNodeInstanceKey(candidates[j])
			}
			return candidates[i].LastHeartbeatAt.After(candidates[j].LastHeartbeatAt)
		})
		selected := candidates[0]
		node := selected.Node
		node.ID = selected.EdgeID
		node.EdgeGroupID = selected.EdgeGroupID
		node.Healthy = selected.EffectiveHealthy && !selected.Node.Draining
		if node.Healthy && model.NormalizeEdgeHealthStatus(node.Status) != model.EdgeHealthDegraded {
			node.Status = model.EdgeHealthHealthy
		} else if model.NormalizeEdgeHealthStatus(node.Status) == model.EdgeHealthHealthy {
			node.Status = model.EdgeHealthUnhealthy
		}
		nodes = append(nodes, redactEdgeNode(node))
	}

	healthyByGroup := make(map[string]int)
	for _, node := range nodes {
		if node.Healthy && !node.Draining {
			healthyByGroup[node.EdgeGroupID]++
		}
	}
	for index := range nodes {
		epoch := epochByGroup[nodes[index].EdgeGroupID]
		if healthyByGroup[nodes[index].EdgeGroupID] >= epoch.MinHealthyInstances {
			continue
		}
		nodes[index].Healthy = false
		nodes[index].Status = model.EdgeHealthUnhealthy
		nodes[index].LastError = fmt.Sprintf("active epoch has %d/%d stable healthy instances", healthyByGroup[nodes[index].EdgeGroupID], epoch.MinHealthyInstances)
	}
	sortEdgeNodes(nodes)
	return nodes, edgeGroupSummaries(groups, nodes, edgeGroupID), nil
}

func applyEdgeInstanceHealthTransition(next *model.EdgeNodeInstance, previous model.EdgeNodeInstance, now time.Time) {
	stableHealthy := edgeInstanceReportsHealthy(*next) && !edgeInstanceHardFailure(*next)
	if stableHealthy {
		next.ConsecutiveHealthy = previous.ConsecutiveHealthy + 1
		next.ConsecutiveUnhealthy = 0
		next.EffectiveHealthy = previous.EffectiveHealthy || next.ConsecutiveHealthy >= edgeInstanceHealthyObservationsRequired
	} else {
		next.ConsecutiveHealthy = 0
		next.ConsecutiveUnhealthy = previous.ConsecutiveUnhealthy + 1
		next.EffectiveHealthy = previous.EffectiveHealthy && next.ConsecutiveUnhealthy < edgeInstanceUnhealthyObservationsRequired && !edgeInstanceHardFailure(*next)
	}
	if previous.EffectiveHealthy != next.EffectiveHealthy || previous.HealthStateSince.IsZero() {
		next.HealthStateSince = now
	} else {
		next.HealthStateSince = previous.HealthStateSince
	}
}

func edgeInstanceReportsHealthy(instance model.EdgeNodeInstance) bool {
	if !instance.Node.Healthy || instance.Node.Draining {
		return false
	}
	switch model.NormalizeEdgeHealthStatus(instance.Node.Status) {
	case model.EdgeHealthHealthy:
		return true
	case model.EdgeHealthDegraded:
		return strings.TrimSpace(instance.Node.CaddyLastError) == "" && instance.Node.CaddyRouteCount > 0
	default:
		return false
	}
}

func edgeInstanceHardFailure(instance model.EdgeNodeInstance) bool {
	switch instance.FailureClass {
	case model.EdgeInstanceFailureSignatureInvalid, model.EdgeInstanceFailureMaxStaleExceeded, model.EdgeInstanceFailureIdentityDrift:
		return true
	default:
		return false
	}
}

func normalizeEdgeActiveEpoch(epoch model.EdgeActiveEpoch) (model.EdgeActiveEpoch, error) {
	epoch.EdgeGroupID = normalizeEdgeGroupID(epoch.EdgeGroupID)
	epoch.Slot = normalizeEdgeSlot(epoch.Slot, false)
	epoch.ReleaseEpoch = normalizeEdgeInstanceToken(epoch.ReleaseEpoch, 256)
	if epoch.EdgeGroupID == "" || epoch.Slot == "" || epoch.ReleaseEpoch == "" || epoch.FenceSequence == 0 {
		return model.EdgeActiveEpoch{}, ErrInvalidInput
	}
	if epoch.MinHealthyInstances == 0 {
		epoch.MinHealthyInstances = 1
	}
	if epoch.MinHealthyInstances < 1 || epoch.MinHealthyInstances > 64 {
		return model.EdgeActiveEpoch{}, ErrInvalidInput
	}
	epoch.ActivatedAt = epoch.ActivatedAt.UTC()
	epoch.CreatedAt = epoch.CreatedAt.UTC()
	epoch.UpdatedAt = epoch.UpdatedAt.UTC()
	return epoch, nil
}

func normalizeEdgeNodeInstance(instance model.EdgeNodeInstance) (model.EdgeNodeInstance, error) {
	instance.EdgeID = normalizeEdgeID(instance.EdgeID)
	instance.EdgeGroupID = normalizeEdgeGroupID(instance.EdgeGroupID)
	instance.Slot = normalizeEdgeSlot(instance.Slot, false)
	instance.InstanceUID = normalizeEdgeInstanceToken(instance.InstanceUID, 128)
	instance.ReleaseEpoch = normalizeEdgeInstanceToken(instance.ReleaseEpoch, 256)
	instance.FailureClass = strings.TrimSpace(strings.ToLower(instance.FailureClass))
	if instance.EdgeID == "" || instance.EdgeGroupID == "" || instance.Slot == "" || instance.InstanceUID == "" || instance.ReleaseEpoch == "" {
		return model.EdgeNodeInstance{}, ErrInvalidInput
	}
	switch instance.FailureClass {
	case model.EdgeInstanceFailureNone, model.EdgeInstanceFailureSignatureInvalid, model.EdgeInstanceFailureMaxStaleExceeded, model.EdgeInstanceFailureIdentityDrift:
	default:
		return model.EdgeNodeInstance{}, ErrInvalidInput
	}
	instance.Node.ID = firstNonEmpty(instance.Node.ID, instance.EdgeID)
	instance.Node.EdgeGroupID = firstNonEmpty(instance.Node.EdgeGroupID, instance.EdgeGroupID)
	instance.Node.TokenHash = ""
	if normalizeEdgeID(instance.Node.ID) != instance.EdgeID || normalizeEdgeGroupID(instance.Node.EdgeGroupID) != instance.EdgeGroupID {
		return model.EdgeNodeInstance{}, ErrInvalidInput
	}
	node, err := normalizeEdgeNodeForStore(instance.Node)
	if err != nil {
		return model.EdgeNodeInstance{}, err
	}
	instance.Node = node
	return instance, nil
}

func normalizeStoredEdgeNodeInstance(instance model.EdgeNodeInstance) (model.EdgeNodeInstance, error) {
	if instance.Slot == edgeLegacyMigrationSlot {
		instance.EdgeID = normalizeEdgeID(instance.EdgeID)
		instance.EdgeGroupID = normalizeEdgeGroupID(instance.EdgeGroupID)
		instance.InstanceUID = normalizeEdgeInstanceToken(instance.InstanceUID, 128)
		instance.ReleaseEpoch = normalizeEdgeInstanceToken(instance.ReleaseEpoch, 256)
		if instance.EdgeID == "" || instance.EdgeGroupID == "" || instance.InstanceUID == "" || instance.ReleaseEpoch != edgeLegacyMigrationEpoch {
			return model.EdgeNodeInstance{}, ErrInvalidInput
		}
		return instance, nil
	}
	return normalizeEdgeNodeInstance(instance)
}

func normalizeEdgeSlot(value string, allowLegacy bool) string {
	value = strings.TrimSpace(strings.ToLower(value))
	switch value {
	case model.EdgeSlotA, model.EdgeSlotB, model.EdgeSlotDirect:
		return value
	case edgeLegacyMigrationSlot:
		if allowLegacy {
			return value
		}
	}
	return ""
}

func normalizeEdgeInstanceToken(value string, maxLength int) string {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" || len(value) > maxLength {
		return ""
	}
	for _, character := range value {
		switch {
		case character >= 'a' && character <= 'z':
		case character >= '0' && character <= '9':
		case strings.ContainsRune("-._:+/@", character):
		default:
			return ""
		}
	}
	return value
}

func sameEdgeActiveEpochIdentity(left, right model.EdgeActiveEpoch) bool {
	return left.EdgeGroupID == right.EdgeGroupID && left.Slot == right.Slot && left.ReleaseEpoch == right.ReleaseEpoch && left.FenceSequence == right.FenceSequence && left.MinHealthyInstances == right.MinHealthyInstances
}

func edgeInstanceMatchesActiveEpoch(instance model.EdgeNodeInstance, epoch model.EdgeActiveEpoch) bool {
	return instance.EdgeGroupID == epoch.EdgeGroupID && instance.Slot == epoch.Slot && instance.ReleaseEpoch == epoch.ReleaseEpoch
}

func edgeNodeInstanceKey(instance model.EdgeNodeInstance) string {
	return strings.Join([]string{instance.EdgeID, instance.EdgeGroupID, instance.Slot, instance.InstanceUID, instance.ReleaseEpoch}, "\x00")
}

func findEdgeNodeInstance(instances []model.EdgeNodeInstance, needle model.EdgeNodeInstance) int {
	key := edgeNodeInstanceKey(needle)
	for index := range instances {
		if edgeNodeInstanceKey(instances[index]) == key {
			return index
		}
	}
	return -1
}

func findEdgeActiveEpoch(epochs []model.EdgeActiveEpoch, edgeGroupID string) int {
	for index := range epochs {
		if normalizeEdgeGroupID(epochs[index].EdgeGroupID) == edgeGroupID {
			return index
		}
	}
	return -1
}

func sortEdgeNodeInstances(instances []model.EdgeNodeInstance) {
	sort.Slice(instances, func(i, j int) bool { return edgeNodeInstanceKey(instances[i]) < edgeNodeInstanceKey(instances[j]) })
}

func legacyEdgeInstanceUID(edgeID string) string {
	digest := sha256.Sum256([]byte(normalizeEdgeID(edgeID)))
	return "legacy-" + hex.EncodeToString(digest[:12])
}

func edgeInstanceTimePtr(value time.Time) *time.Time {
	copy := value
	return &copy
}
