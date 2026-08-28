package edgecontrol

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformcontrol"
)

const InventoryPlatformIdentityKeyringSchemaV1 = "edge-inventory-platform-identity-keyring/v1"

var (
	ErrGroupInventoryProducerIdentity   = errors.New("edge-control inventory producer identity mismatch")
	ErrGroupInventoryProducerReplay     = errors.New("edge-control inventory producer replay")
	ErrGroupInventoryProducerGeneration = errors.New("edge-control inventory producer generation conflict")
	ErrGroupInventoryProducerEpoch      = errors.New("edge-control inventory producer epoch conflict")
)

type inventoryPlatformIdentityKeyringFile struct {
	Schema        string   `json:"schema"`
	Generation    uint64   `json:"generation"`
	GroupID       string   `json:"edge_group_id"`
	ActiveKeyID   string   `json:"active_key_id"`
	ActiveKey     string   `json:"active_key"`
	PreviousKeyID string   `json:"previous_key_id,omitempty"`
	PreviousKey   string   `json:"previous_key,omitempty"`
	RevokedKeyIDs []string `json:"revoked_key_ids,omitempty"`
}

func (handler *groupInventoryHeartbeatHandler) serveAuthorityHeartbeat(
	w http.ResponseWriter,
	request *http.Request,
	heartbeat GroupInventoryHeartbeat,
	groupID string,
	now time.Time,
) {
	identity, err := authenticateInventoryProducerPlatformIdentity(
		filepath.Join(handler.keyringDir, groupID+".json"),
		groupID,
		request.Header.Get("Authorization"),
		now,
	)
	if err != nil {
		writeInventoryHeartbeatError(w, http.StatusUnauthorized, "credential_rejected")
		return
	}
	if err := validateAuthorityInventoryProducerHeartbeat(heartbeat, identity, now); err != nil {
		status := http.StatusBadRequest
		code := "inventory_rejected"
		if errors.Is(err, ErrGroupInventoryProducerIdentity) {
			status = http.StatusForbidden
			code = "identity_rejected"
		}
		writeInventoryHeartbeatError(w, status, code)
		return
	}
	stored, err := handler.store.StoreGroupInventoryProducerHeartbeat(request.Context(), identity, heartbeat, now)
	if err != nil {
		switch {
		case errors.Is(err, ErrGroupInventoryCASConflict), errors.Is(err, ErrGroupInventorySequence),
			errors.Is(err, ErrGroupInventoryProducerReplay), errors.Is(err, ErrGroupInventoryProducerGeneration), errors.Is(err, ErrGroupInventoryProducerEpoch):
			writeInventoryHeartbeatError(w, http.StatusConflict, "sequence_conflict")
		case errors.Is(err, ErrGroupInventoryProducerIdentity), errors.Is(err, errGroupInventoryInvalid):
			writeInventoryHeartbeatError(w, http.StatusBadRequest, "inventory_rejected")
		default:
			writeInventoryHeartbeatError(w, http.StatusServiceUnavailable, "store_unavailable")
		}
		return
	}
	writeJSON(w, http.StatusCreated, GroupInventoryHeartbeatReceipt{
		Schema: GroupInventoryHeartbeatReceiptSchemaV1, GroupID: groupID,
		Sequence: stored.Sequence, Generation: stored.Generation,
		InventoryDigest: groupInventorySemanticDigest(stored), Authority: handler.authority, Publication: handler.publication,
		ProducerNodeID: identity.NodeID, ProducerGeneration: heartbeat.ProducerGeneration,
	})
}

func authenticateInventoryProducerPlatformIdentity(path, groupID, authorization string, now time.Time) (GroupInventoryProducerIdentity, error) {
	const prefix = "Bearer "
	if !strings.HasPrefix(authorization, prefix) || strings.TrimSpace(authorization) != authorization {
		return GroupInventoryProducerIdentity{}, ErrGroupInventoryProducerIdentity
	}
	token := strings.TrimPrefix(authorization, prefix)
	if token == "" || strings.ContainsAny(token, "\r\n\t ") {
		return GroupInventoryProducerIdentity{}, ErrGroupInventoryProducerIdentity
	}
	keyring, err := loadInventoryPlatformIdentityKeyring(path, groupID)
	if err != nil {
		return GroupInventoryProducerIdentity{}, err
	}
	claims, err := platformcontrol.ParsePlatformComponentIdentity(keyring, token, now.UTC())
	if err != nil {
		return GroupInventoryProducerIdentity{}, ErrGroupInventoryProducerIdentity
	}
	if claims.Component != model.PlatformConsumerComponentEdgeWorker || claims.ScopeKey != groupID ||
		len(claims.ArtifactKinds) != 1 || claims.ArtifactKinds[0] != model.PlatformArtifactKindEdgeRouteBundle ||
		strings.TrimSpace(claims.NodeID) == "" || claims.NodeID != strings.TrimSpace(claims.NodeID) {
		return GroupInventoryProducerIdentity{}, ErrGroupInventoryProducerIdentity
	}
	return GroupInventoryProducerIdentity{
		CredentialID: claims.CredentialID,
		TokenID:      claims.TokenID,
		NodeID:       claims.NodeID,
		GroupID:      groupID,
	}, nil
}

func loadInventoryPlatformIdentityKeyring(path, expectedGroupID string) (platformcontrol.PlatformComponentIdentityKeyring, error) {
	raw, err := readPrivateProjectedFile(path, maxInventoryKeyringBytes)
	if err != nil {
		return platformcontrol.PlatformComponentIdentityKeyring{}, ErrGroupInventoryProducerIdentity
	}
	defer zeroBytes(raw)
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var file inventoryPlatformIdentityKeyringFile
	if err := decoder.Decode(&file); err != nil {
		return platformcontrol.PlatformComponentIdentityKeyring{}, ErrGroupInventoryProducerIdentity
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) ||
		file.Schema != InventoryPlatformIdentityKeyringSchemaV1 || file.Generation == 0 ||
		file.GroupID != normalizeGroupID(expectedGroupID) || file.GroupID != expectedGroupID ||
		strings.TrimSpace(file.ActiveKeyID) == "" || len(strings.TrimSpace(file.ActiveKey)) < 32 ||
		(file.PreviousKeyID == "") != (file.PreviousKey == "") {
		return platformcontrol.PlatformComponentIdentityKeyring{}, ErrGroupInventoryProducerIdentity
	}
	keyring := platformcontrol.DerivePlatformComponentIdentityKeyring(
		file.ActiveKey, file.ActiveKeyID, file.PreviousKey, file.PreviousKeyID, file.RevokedKeyIDs,
	)
	file.ActiveKey = ""
	file.PreviousKey = ""
	if keyring.ActiveKeyID == "" || len(keyring.Keys) == 0 {
		return platformcontrol.PlatformComponentIdentityKeyring{}, ErrGroupInventoryProducerIdentity
	}
	return keyring, nil
}

func validateAuthorityInventoryProducerHeartbeat(value GroupInventoryHeartbeat, identity GroupInventoryProducerIdentity, now time.Time) error {
	groupID := normalizeGroupID(value.GroupID)
	if identity.GroupID != groupID || value.GroupID != groupID || identity.NodeID != value.ProducerNodeID ||
		value.ProducerNodeID != strings.TrimSpace(value.ProducerNodeID) || value.ProducerNodeID == "" {
		return ErrGroupInventoryProducerIdentity
	}
	if value.Schema != GroupInventoryHeartbeatSchemaV1 || value.KeyID != "" || value.Signature != "" ||
		value.ProducerGeneration == 0 || !inventoryNoncePattern.MatchString(value.Nonce) ||
		value.IssuedAtUnix <= 0 || value.ExpiresAtUnix <= 0 {
		return errGroupInventoryInvalid
	}
	issuedAt := time.Unix(value.IssuedAtUnix, 0).UTC()
	expiresAt := time.Unix(value.ExpiresAtUnix, 0).UTC()
	if issuedAt.After(now.Add(maxInventoryHeartbeatClockSkew)) || !expiresAt.After(now) || !expiresAt.After(issuedAt) ||
		expiresAt.Sub(issuedAt) > maxInventoryHeartbeatTTL || now.Sub(issuedAt) > maxInventoryHeartbeatTTL {
		return errGroupInventoryInvalid
	}
	inventory := value.Inventory
	if inventory.Schema != GroupInventorySchemaV1 || inventory.GroupID != groupID ||
		inventory.Sequence != value.ExpectedSequence+1 || inventory.Generation != ProducerInventoryEnvelopeGeneration(value.ProducerGeneration) ||
		inventory.ObservedAt.IsZero() || inventory.ObservedAt.Before(issuedAt.Add(-maxInventoryHeartbeatClockSkew)) ||
		inventory.ObservedAt.After(now.Add(maxInventoryHeartbeatClockSkew)) || len(inventory.Instances) != 1 {
		return errGroupInventoryInvalid
	}
	epoch := inventory.ActiveEpoch
	if epoch.GroupID != groupID || epoch.Slot != normalizeSlot(epoch.Slot) || !validEdgeSlot(epoch.Slot) ||
		strings.TrimSpace(epoch.ReleaseEpoch) == "" || epoch.ReleaseEpoch != strings.TrimSpace(epoch.ReleaseEpoch) ||
		epoch.FenceSequence == 0 || epoch.MinHealthyInstances <= 0 {
		return errGroupInventoryInvalid
	}
	instance := inventory.Instances[0]
	if instance.EdgeID != identity.NodeID || instance.GroupID != groupID || instance.Slot != normalizeSlot(instance.Slot) ||
		!validEdgeSlot(instance.Slot) || strings.TrimSpace(instance.InstanceUID) == "" || instance.InstanceUID != strings.TrimSpace(instance.InstanceUID) ||
		strings.TrimSpace(instance.ReleaseEpoch) == "" || instance.ReleaseEpoch != strings.TrimSpace(instance.ReleaseEpoch) ||
		model.NormalizeEdgeHealthStatus(instance.NodeStatus) == "" {
		return ErrGroupInventoryProducerIdentity
	}
	if instance.Slot != epoch.Slot || instance.ReleaseEpoch != epoch.ReleaseEpoch {
		return ErrGroupInventoryProducerIdentity
	}
	if err := validateInventoryTopology(value.FaultDomainID, value.EdgePoolID); err != nil ||
		inventory.FaultDomainID != value.FaultDomainID || inventory.EdgePoolID != value.EdgePoolID ||
		epoch.FaultDomainID != value.FaultDomainID || epoch.EdgePoolID != value.EdgePoolID ||
		instance.FaultDomainID != value.FaultDomainID || instance.EdgePoolID != value.EdgePoolID {
		return errGroupInventoryInvalid
	}
	if instance.EffectiveHealthy && (!instance.NodeHealthy || instance.Draining || instance.FailureClass != "" ||
		model.NormalizeEdgeHealthStatus(instance.NodeStatus) != model.EdgeHealthHealthy) {
		return errGroupInventoryInvalid
	}
	if instance.ServingHealthy != nil && *instance.ServingHealthy != instance.EffectiveHealthy {
		return errGroupInventoryInvalid
	}
	if eligibility := instance.BootstrapEligibility; eligibility != nil {
		if instance.ServingHealthy == nil || *instance.ServingHealthy || instance.EffectiveHealthy || !instance.NodeHealthy || instance.Draining ||
			instance.FailureClass != "" || model.NormalizeEdgeHealthStatus(instance.NodeStatus) != model.EdgeHealthHealthy ||
			eligibility.GroupID != groupID || eligibility.ReleaseEpoch != instance.ReleaseEpoch ||
			eligibility.ProducerGeneration != value.ProducerGeneration || !eligibility.ValidUntil.Equal(expiresAt) {
			return errGroupInventoryInvalid
		}
	}
	return nil
}

func ProducerInventoryEnvelopeGeneration(generation uint64) string {
	return "producer-" + strconv.FormatUint(generation, 10)
}
