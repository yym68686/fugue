package edgecontrol

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"
)

const (
	maxGroupInventoryProducerObservations = 256
	maxGroupInventoryProducerNonces       = 32
)

type GroupInventoryProducerState struct {
	Generation   uint64                              `json:"generation"`
	ActiveEpoch  GroupActiveEpoch                    `json:"active_epoch"`
	RecentNonces []string                            `json:"recent_nonces"`
	Observations []GroupInventoryProducerObservation `json:"observations"`
}

type GroupInventoryProducerObservation struct {
	CredentialID       string        `json:"credential_id"`
	TokenID            string        `json:"token_id"`
	NodeID             string        `json:"node_id"`
	Slot               string        `json:"slot"`
	ProducerGeneration uint64        `json:"producer_generation"`
	ObservedAt         time.Time     `json:"observed_at"`
	Instance           GroupInstance `json:"instance"`
}

func (store *PersistentGroupStore) StoreGroupInventoryProducerHeartbeat(
	ctx context.Context,
	identity GroupInventoryProducerIdentity,
	heartbeat GroupInventoryHeartbeat,
	now time.Time,
) (GroupInventorySnapshot, error) {
	var stored GroupInventorySnapshot
	now = now.UTC()
	if err := validateAuthorityInventoryProducerHeartbeat(heartbeat, identity, now); err != nil {
		return GroupInventorySnapshot{}, err
	}
	groupID := normalizeGroupID(identity.GroupID)
	err := store.withGroupState(ctx, groupID, true, func(state *persistentGroupState) error {
		currentSequence := uint64(0)
		if state.Inventory != nil {
			currentSequence = state.Inventory.Sequence
		}
		if heartbeat.ExpectedSequence != currentSequence {
			return ErrGroupInventoryCASConflict
		}
		producer := state.InventoryProducer
		if producer == nil {
			producer = &GroupInventoryProducerState{}
		}
		if heartbeat.ProducerGeneration != producer.Generation+1 {
			return ErrGroupInventoryProducerGeneration
		}
		for _, nonce := range producer.RecentNonces {
			if nonce == heartbeat.Nonce {
				return ErrGroupInventoryProducerReplay
			}
		}
		epoch := heartbeat.Inventory.ActiveEpoch
		if producer.Generation > 0 {
			switch {
			case epoch.FenceSequence < producer.ActiveEpoch.FenceSequence:
				return ErrGroupInventoryProducerEpoch
			case epoch.FenceSequence == producer.ActiveEpoch.FenceSequence && !equalGroupServingEpoch(epoch, producer.ActiveEpoch):
				return ErrGroupInventoryProducerEpoch
			}
		}
		producer.Generation = heartbeat.ProducerGeneration
		producer.ActiveEpoch = epoch
		producer.RecentNonces = append(producer.RecentNonces, heartbeat.Nonce)
		if len(producer.RecentNonces) > maxGroupInventoryProducerNonces {
			producer.RecentNonces = append([]string(nil), producer.RecentNonces[len(producer.RecentNonces)-maxGroupInventoryProducerNonces:]...)
		}

		instance := heartbeat.Inventory.Instances[0]
		observation := GroupInventoryProducerObservation{
			CredentialID: identity.CredentialID, TokenID: identity.TokenID, NodeID: identity.NodeID, Slot: instance.Slot,
			ProducerGeneration: heartbeat.ProducerGeneration, ObservedAt: now, Instance: instance,
		}
		replaced := false
		for index := range producer.Observations {
			if producer.Observations[index].NodeID == observation.NodeID && producer.Observations[index].Slot == observation.Slot {
				producer.Observations[index] = observation
				replaced = true
				break
			}
		}
		if !replaced {
			if len(producer.Observations) >= maxGroupInventoryProducerObservations {
				return errGroupInventoryInvalid
			}
			producer.Observations = append(producer.Observations, observation)
		}
		sort.Slice(producer.Observations, func(i, j int) bool {
			left := producer.Observations[i].NodeID + "\x00" + producer.Observations[i].Slot
			right := producer.Observations[j].NodeID + "\x00" + producer.Observations[j].Slot
			return left < right
		})

		instances := make([]GroupInstance, 0, len(producer.Observations))
		for _, current := range producer.Observations {
			if current.ObservedAt.After(now.Add(maxInventoryHeartbeatClockSkew)) || now.Sub(current.ObservedAt) > maxInventoryHeartbeatTTL {
				continue
			}
			instances = append(instances, current.Instance)
		}
		sort.Slice(instances, func(i, j int) bool {
			left := instances[i].EdgeID + "\x00" + instances[i].Slot + "\x00" + instances[i].InstanceUID
			right := instances[j].EdgeID + "\x00" + instances[j].Slot + "\x00" + instances[j].InstanceUID
			return left < right
		})
		stored = GroupInventorySnapshot{
			Schema: GroupInventorySchemaV1, GroupID: groupID, FaultDomainID: producer.ActiveEpoch.FaultDomainID, EdgePoolID: producer.ActiveEpoch.EdgePoolID, Sequence: currentSequence + 1,
			ActiveEpoch: producer.ActiveEpoch, Instances: instances, ObservedAt: now,
		}
		stored.Generation = groupInventoryProducerGeneration(producer.Generation, stored)
		state.InventoryProducer = producer
		state.Inventory = &stored
		return nil
	})
	return cloneGroupInventorySnapshot(stored), err
}

func (store *PersistentGroupStore) ReadGroupInventoryProducerState(ctx context.Context, groupID string) (GroupInventoryProducerState, bool, error) {
	var out GroupInventoryProducerState
	exists := false
	err := store.withGroupState(ctx, groupID, false, func(state *persistentGroupState) error {
		if state.InventoryProducer == nil {
			return nil
		}
		out = cloneGroupInventoryProducerState(*state.InventoryProducer)
		exists = true
		return nil
	})
	return out, exists, err
}

func validateGroupInventoryProducerState(value GroupInventoryProducerState, groupID string) error {
	if value.Generation == 0 || value.ActiveEpoch.GroupID != groupID || value.ActiveEpoch.Slot != normalizeSlot(value.ActiveEpoch.Slot) ||
		!validEdgeSlot(value.ActiveEpoch.Slot) || strings.TrimSpace(value.ActiveEpoch.ReleaseEpoch) == "" ||
		value.ActiveEpoch.FenceSequence == 0 || value.ActiveEpoch.MinHealthyInstances <= 0 ||
		len(value.RecentNonces) == 0 || len(value.RecentNonces) > maxGroupInventoryProducerNonces ||
		len(value.Observations) == 0 || len(value.Observations) > maxGroupInventoryProducerObservations {
		return errors.New("edge-control persistent inventory producer state is invalid")
	}
	seenNonces := make(map[string]struct{}, len(value.RecentNonces))
	for _, nonce := range value.RecentNonces {
		if !inventoryNoncePattern.MatchString(nonce) {
			return errors.New("edge-control persistent inventory producer nonce is invalid")
		}
		if _, duplicate := seenNonces[nonce]; duplicate {
			return errors.New("edge-control persistent inventory producer nonce is duplicated")
		}
		seenNonces[nonce] = struct{}{}
	}
	previous := ""
	for _, observation := range value.Observations {
		identity := observation.NodeID + "\x00" + observation.Slot
		if identity <= previous || strings.TrimSpace(observation.CredentialID) == "" || strings.TrimSpace(observation.TokenID) == "" ||
			strings.TrimSpace(observation.NodeID) == "" || observation.NodeID != observation.Instance.EdgeID ||
			observation.Slot != normalizeSlot(observation.Slot) || !validEdgeSlot(observation.Slot) || observation.Slot != observation.Instance.Slot ||
			observation.ProducerGeneration == 0 || observation.ProducerGeneration > value.Generation || observation.ObservedAt.IsZero() ||
			observation.Instance.GroupID != groupID || strings.TrimSpace(observation.Instance.InstanceUID) == "" || strings.TrimSpace(observation.Instance.ReleaseEpoch) == "" {
			return errors.New("edge-control persistent inventory producer observation is invalid")
		}
		previous = identity
	}
	return nil
}

func cloneGroupInventoryProducerState(value GroupInventoryProducerState) GroupInventoryProducerState {
	value.RecentNonces = append([]string(nil), value.RecentNonces...)
	value.Observations = append([]GroupInventoryProducerObservation(nil), value.Observations...)
	return value
}

func equalGroupServingEpoch(left, right GroupActiveEpoch) bool {
	return left.GroupID == right.GroupID && left.Slot == right.Slot &&
		left.FenceSequence == right.FenceSequence && left.MinHealthyInstances == right.MinHealthyInstances
}

func groupInventoryProducerGeneration(producerGeneration uint64, snapshot GroupInventorySnapshot) string {
	material := struct {
		ProducerGeneration uint64           `json:"producer_generation"`
		GroupID            string           `json:"edge_group_id"`
		ActiveEpoch        GroupActiveEpoch `json:"active_epoch"`
		Instances          []GroupInstance  `json:"instances"`
	}{producerGeneration, snapshot.GroupID, snapshot.ActiveEpoch, snapshot.Instances}
	raw, err := json.Marshal(material)
	if err != nil {
		panic(err)
	}
	digest := sha256.Sum256(raw)
	return "inventory-" + hex.EncodeToString(digest[:])
}
