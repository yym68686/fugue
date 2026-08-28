package edgecontrol

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	GroupInventoryHeartbeatPathV1          = "/v1/shadow/group-inventory-heartbeats"
	GroupAuthorityInventoryHeartbeatPathV1 = "/v1/authority/group-inventory-heartbeats"
	GroupInventoryHeartbeatSchemaV1        = "edge-control-group-inventory-heartbeat/v1"
	GroupInventoryHeartbeatReceiptSchemaV1 = "edge-control-group-inventory-heartbeat-receipt/v1"
	InventoryWriterKeyringSchemaV1         = "edge-control-inventory-writer-keyring/v1"
	GroupInventoryHeartbeatMaxAge          = 2 * time.Minute

	maxInventoryHeartbeatBodyBytes = 2 << 20
	maxInventoryKeyringBytes       = 64 << 10
	maxInventoryHeartbeatTTL       = GroupInventoryHeartbeatMaxAge
	maxInventoryHeartbeatClockSkew = 30 * time.Second
)

var (
	inventoryCredentialIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{2,63}$`)
	inventoryNoncePattern        = regexp.MustCompile(`^[A-Za-z0-9_-]{16,128}$`)
)

type GroupInventoryHeartbeat struct {
	Schema             string                 `json:"schema"`
	KeyID              string                 `json:"key_id"`
	GroupID            string                 `json:"edge_group_id"`
	FaultDomainID      string                 `json:"fault_domain_id,omitempty"`
	EdgePoolID         string                 `json:"edge_pool_id,omitempty"`
	ProducerNodeID     string                 `json:"producer_node_id,omitempty"`
	ProducerGeneration uint64                 `json:"producer_generation,omitempty"`
	ExpectedSequence   uint64                 `json:"expected_sequence"`
	IssuedAtUnix       int64                  `json:"issued_at_unix"`
	ExpiresAtUnix      int64                  `json:"expires_at_unix"`
	Nonce              string                 `json:"nonce"`
	Inventory          GroupInventorySnapshot `json:"inventory"`
	Signature          string                 `json:"signature"`
}

type GroupInventoryHeartbeatReceipt struct {
	Schema             string `json:"schema"`
	GroupID            string `json:"edge_group_id"`
	Sequence           uint64 `json:"sequence"`
	Generation         string `json:"generation"`
	InventoryDigest    string `json:"inventory_digest"`
	Authority          string `json:"authority"`
	Publication        bool   `json:"publication_enabled"`
	ProducerNodeID     string `json:"producer_node_id,omitempty"`
	ProducerGeneration uint64 `json:"producer_generation,omitempty"`
}

type GroupInventoryProducerIdentity struct {
	CredentialID string
	TokenID      string
	NodeID       string
	GroupID      string
}

type GroupInventoryHeartbeatStore interface {
	StoreGroupInventoryCAS(context.Context, string, uint64, GroupInventorySnapshot) error
	StoreGroupInventoryProducerHeartbeat(context.Context, GroupInventoryProducerIdentity, GroupInventoryHeartbeat, time.Time) (GroupInventorySnapshot, error)
}

type GroupInventoryHeartbeatHandlerConfig struct {
	Store              GroupInventoryHeartbeatStore
	GroupIDs           []string
	KeyringFile        string
	KeyringDir         string
	Authority          string
	PublicationEnabled bool
	Path               string
	Now                func() time.Time
}

type inventoryWriterKeyring struct {
	Schema     string               `json:"schema"`
	Generation uint64               `json:"generation"`
	GroupID    string               `json:"edge_group_id,omitempty"`
	Keys       []inventoryWriterKey `json:"keys"`
}

type inventoryWriterKey struct {
	KeyID         string `json:"key_id"`
	Secret        string `json:"secret"`
	NotBeforeUnix int64  `json:"not_before_unix"`
	NotAfterUnix  int64  `json:"not_after_unix"`
	Revoked       bool   `json:"revoked"`
}

type groupInventoryHeartbeatHandler struct {
	store       GroupInventoryHeartbeatStore
	groups      map[string]struct{}
	keyringFile string
	keyringDir  string
	authority   string
	publication bool
	path        string
	now         func() time.Time
}

func NewGroupInventoryHeartbeatHandler(config GroupInventoryHeartbeatHandlerConfig) (http.Handler, error) {
	if config.Store == nil {
		return nil, errors.New("edge-control inventory heartbeat store is nil")
	}
	groups, err := normalizeGroupIDs(config.GroupIDs)
	if err != nil {
		return nil, err
	}
	keyringFile := strings.TrimSpace(config.KeyringFile)
	keyringDir := strings.TrimSpace(config.KeyringDir)
	allowed := make(map[string]struct{}, len(groups))
	for _, groupID := range groups {
		allowed[groupID] = struct{}{}
	}
	now := func() time.Time { return time.Now().UTC() }
	if config.Now != nil {
		now = func() time.Time { return config.Now().UTC() }
	}
	authority := strings.TrimSpace(config.Authority)
	if authority == "" {
		authority = "none"
	}
	if (authority == "none" && config.PublicationEnabled) || (authority != "none" && authority != "edge-control") || (authority == "edge-control" && !config.PublicationEnabled) {
		return nil, errors.New("edge-control inventory writer authority mode is invalid")
	}
	if authority == "none" {
		if keyringFile == "" || !filepath.IsAbs(keyringFile) || filepath.Clean(keyringFile) != keyringFile || keyringDir != "" {
			return nil, errors.New("edge-control shadow inventory writer requires one absolute keyring file")
		}
	} else if keyringDir == "" || !filepath.IsAbs(keyringDir) || filepath.Clean(keyringDir) != keyringDir || keyringFile != "" {
		return nil, errors.New("edge-control authority inventory writer requires one absolute group keyring directory")
	}
	path := strings.TrimSpace(config.Path)
	if path == "" {
		path = GroupInventoryHeartbeatPathV1
	}
	if (authority == "none" && path != GroupInventoryHeartbeatPathV1) || (authority == "edge-control" && path != GroupAuthorityInventoryHeartbeatPathV1) {
		return nil, errors.New("edge-control inventory writer path does not match authority mode")
	}
	return &groupInventoryHeartbeatHandler{store: config.Store, groups: allowed, keyringFile: keyringFile, keyringDir: keyringDir, authority: authority, publication: config.PublicationEnabled, path: path, now: now}, nil
}

func (handler *groupInventoryHeartbeatHandler) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost || request.URL.Path != handler.path || request.URL.RawQuery != "" {
		http.NotFound(w, request)
		return
	}
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeInventoryHeartbeatError(w, http.StatusUnsupportedMediaType, "content_type_rejected")
		return
	}
	body, err := io.ReadAll(io.LimitReader(request.Body, maxInventoryHeartbeatBodyBytes+1))
	if err != nil || len(body) == 0 || len(body) > maxInventoryHeartbeatBodyBytes {
		writeInventoryHeartbeatError(w, http.StatusBadRequest, "body_rejected")
		return
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var heartbeat GroupInventoryHeartbeat
	if err := decoder.Decode(&heartbeat); err != nil {
		writeInventoryHeartbeatError(w, http.StatusBadRequest, "body_rejected")
		return
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeInventoryHeartbeatError(w, http.StatusBadRequest, "body_rejected")
		return
	}
	groupID := normalizeGroupID(heartbeat.GroupID)
	if _, allowed := handler.groups[groupID]; !allowed || heartbeat.GroupID != groupID {
		writeInventoryHeartbeatError(w, http.StatusForbidden, "group_rejected")
		return
	}
	now := handler.now()
	if handler.authority == "edge-control" {
		handler.serveAuthorityHeartbeat(w, request, heartbeat, groupID, now)
		return
	}
	secret, err := authenticateGroupInventoryHeartbeat(handler.keyringFile, "", heartbeat, now)
	if err != nil {
		writeInventoryHeartbeatError(w, http.StatusUnauthorized, "credential_rejected")
		return
	}
	for index := range secret {
		secret[index] = 0
	}
	if err := validateGroupInventoryHeartbeat(heartbeat, groupID); err != nil {
		writeInventoryHeartbeatError(w, http.StatusBadRequest, "inventory_rejected")
		return
	}
	if err := handler.store.StoreGroupInventoryCAS(request.Context(), groupID, heartbeat.ExpectedSequence, heartbeat.Inventory); err != nil {
		if errors.Is(err, ErrGroupInventoryCASConflict) || errors.Is(err, ErrGroupInventorySequence) {
			writeInventoryHeartbeatError(w, http.StatusConflict, "sequence_conflict")
			return
		}
		writeInventoryHeartbeatError(w, http.StatusServiceUnavailable, "store_unavailable")
		return
	}
	writeJSON(w, http.StatusCreated, GroupInventoryHeartbeatReceipt{
		Schema: GroupInventoryHeartbeatReceiptSchemaV1, GroupID: groupID,
		Sequence: heartbeat.Inventory.Sequence, Generation: strings.TrimSpace(heartbeat.Inventory.Generation),
		InventoryDigest: groupInventorySemanticDigest(heartbeat.Inventory), Authority: handler.authority, Publication: handler.publication,
	})
}

func validateGroupInventoryHeartbeat(value GroupInventoryHeartbeat, groupID string) error {
	if value.Schema != GroupInventoryHeartbeatSchemaV1 || value.GroupID != groupID ||
		value.Inventory.Schema != GroupInventorySchemaV1 || normalizeGroupID(value.Inventory.GroupID) != groupID || value.Inventory.GroupID != groupID ||
		value.Inventory.Sequence == 0 || value.Inventory.Sequence != value.ExpectedSequence+1 || strings.TrimSpace(value.Inventory.Generation) == "" ||
		value.Inventory.Generation != strings.TrimSpace(value.Inventory.Generation) {
		return errors.New("edge-control inventory heartbeat identity is invalid")
	}
	if err := validateInventoryTopology(value.FaultDomainID, value.EdgePoolID); err != nil ||
		value.Inventory.FaultDomainID != value.FaultDomainID || value.Inventory.EdgePoolID != value.EdgePoolID {
		return errors.New("edge-control inventory heartbeat topology is invalid")
	}
	epoch := value.Inventory.ActiveEpoch
	if epoch.GroupID != groupID || epoch.Slot != normalizeSlot(epoch.Slot) || !validEdgeSlot(epoch.Slot) ||
		strings.TrimSpace(epoch.ReleaseEpoch) == "" || epoch.ReleaseEpoch != strings.TrimSpace(epoch.ReleaseEpoch) || epoch.FenceSequence == 0 || epoch.MinHealthyInstances <= 0 {
		return errors.New("edge-control inventory heartbeat active epoch is invalid")
	}
	if epoch.FaultDomainID != value.FaultDomainID || epoch.EdgePoolID != value.EdgePoolID {
		return errors.New("edge-control inventory heartbeat active epoch topology is invalid")
	}
	for _, instance := range value.Inventory.Instances {
		if instance.FaultDomainID != value.FaultDomainID || instance.EdgePoolID != value.EdgePoolID {
			return errors.New("edge-control inventory heartbeat instance topology is invalid")
		}
	}
	return nil
}

func validateInventoryTopology(faultDomainID, edgePoolID string) error {
	if !topologyIdentityPattern.MatchString(faultDomainID) || !topologyIdentityPattern.MatchString(edgePoolID) {
		return errors.New("inventory topology identities are invalid")
	}
	return nil
}

func authenticateGroupInventoryHeartbeat(keyringFile, expectedGroupID string, value GroupInventoryHeartbeat, now time.Time) ([]byte, error) {
	if value.Schema != GroupInventoryHeartbeatSchemaV1 || !inventoryCredentialIDPattern.MatchString(value.KeyID) ||
		!inventoryNoncePattern.MatchString(value.Nonce) || value.IssuedAtUnix <= 0 || value.ExpiresAtUnix <= 0 {
		return nil, errors.New("edge-control inventory heartbeat credential envelope is invalid")
	}
	issuedAt := time.Unix(value.IssuedAtUnix, 0).UTC()
	expiresAt := time.Unix(value.ExpiresAtUnix, 0).UTC()
	now = now.UTC()
	if !expiresAt.After(now) || !expiresAt.After(issuedAt) || expiresAt.Sub(issuedAt) > maxInventoryHeartbeatTTL || issuedAt.After(now.Add(maxInventoryHeartbeatClockSkew)) {
		return nil, errors.New("edge-control inventory heartbeat lifetime is invalid")
	}
	keyring, err := loadInventoryWriterKeyring(keyringFile, expectedGroupID)
	if err != nil {
		return nil, err
	}
	for _, key := range keyring.Keys {
		if key.KeyID != value.KeyID {
			continue
		}
		notBefore := time.Unix(key.NotBeforeUnix, 0).UTC()
		notAfter := time.Unix(key.NotAfterUnix, 0).UTC()
		if key.Revoked || now.Before(notBefore) || !now.Before(notAfter) || issuedAt.Before(notBefore) || expiresAt.After(notAfter) {
			return nil, errors.New("edge-control inventory writer key is inactive")
		}
		secret, err := base64.RawURLEncoding.DecodeString(key.Secret)
		if err != nil || len(secret) < 32 || len(secret) > 64 {
			return nil, errors.New("edge-control inventory writer key is invalid")
		}
		if !verifyGroupInventoryHeartbeat(value, secret) {
			for index := range secret {
				secret[index] = 0
			}
			return nil, errors.New("edge-control inventory heartbeat signature is invalid")
		}
		return secret, nil
	}
	return nil, errors.New("edge-control inventory writer key is unknown")
}

func loadInventoryWriterKeyring(path, expectedGroupID string) (inventoryWriterKeyring, error) {
	data, err := readPrivateProjectedFile(path, maxInventoryKeyringBytes)
	if err != nil {
		return inventoryWriterKeyring{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var keyring inventoryWriterKeyring
	if err := decoder.Decode(&keyring); err != nil {
		return inventoryWriterKeyring{}, errors.New("edge-control inventory writer keyring is invalid")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) || keyring.Schema != InventoryWriterKeyringSchemaV1 || keyring.Generation == 0 || len(keyring.Keys) == 0 || len(keyring.Keys) > 8 {
		return inventoryWriterKeyring{}, errors.New("edge-control inventory writer keyring is invalid")
	}
	expectedGroupID = normalizeGroupID(expectedGroupID)
	if (expectedGroupID == "" && keyring.GroupID != "") || (expectedGroupID != "" && (keyring.GroupID != expectedGroupID || !edgeGroupIDPattern.MatchString(keyring.GroupID))) {
		return inventoryWriterKeyring{}, errors.New("edge-control inventory writer keyring group binding is invalid")
	}
	seen := make(map[string]struct{}, len(keyring.Keys))
	for _, key := range keyring.Keys {
		if !inventoryCredentialIDPattern.MatchString(key.KeyID) || key.NotBeforeUnix <= 0 || key.NotAfterUnix <= key.NotBeforeUnix {
			return inventoryWriterKeyring{}, errors.New("edge-control inventory writer keyring is invalid")
		}
		secret, err := base64.RawURLEncoding.DecodeString(key.Secret)
		if err != nil || len(secret) < 32 || len(secret) > 64 {
			return inventoryWriterKeyring{}, errors.New("edge-control inventory writer keyring is invalid")
		}
		for index := range secret {
			secret[index] = 0
		}
		if _, duplicate := seen[key.KeyID]; duplicate {
			return inventoryWriterKeyring{}, errors.New("edge-control inventory writer keyring is invalid")
		}
		seen[key.KeyID] = struct{}{}
	}
	return keyring, nil
}

func SignGroupInventoryHeartbeat(value *GroupInventoryHeartbeat, secret []byte) error {
	if value == nil || len(secret) < 32 || len(secret) > 64 {
		return errors.New("edge-control inventory heartbeat signing input is invalid")
	}
	value.Signature = ""
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(payload)
	value.Signature = base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return nil
}

func verifyGroupInventoryHeartbeat(value GroupInventoryHeartbeat, secret []byte) bool {
	provided, err := base64.RawURLEncoding.DecodeString(value.Signature)
	if err != nil || len(provided) != sha256.Size {
		return false
	}
	value.Signature = ""
	payload, err := json.Marshal(value)
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(payload)
	return hmac.Equal(provided, mac.Sum(nil))
}

func writeInventoryHeartbeatError(w http.ResponseWriter, status int, code string) {
	writeJSON(w, status, struct {
		Schema string `json:"schema"`
		Error  string `json:"error"`
	}{Schema: "edge-control-error/v1", Error: code})
}

// sortedInventoryGroups is shared with status/readback construction so every
// externally visible group list has one deterministic canonical order.
func sortedInventoryGroups(values map[string]struct{}) []string {
	out := make([]string, 0, len(values))
	for value := range values {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}
