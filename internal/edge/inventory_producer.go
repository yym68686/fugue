package edge

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"fugue/internal/config"
	"fugue/internal/edgegroupfront"
	"fugue/internal/model"
	"fugue/internal/platformcontrol"
)

const (
	maxInventoryProducerResponseBytes = 64 << 10
	maxInventoryProducerTokenBytes    = 64 << 10
)

type InventoryProducerConfig struct {
	URL string
	// AuthorityService is the explicit service identity projected by the
	// component manifest. It is deliberately independent of EdgeGroupID:
	// groups are publication/fencing partitions, not a naming convention for
	// Kubernetes services.
	AuthorityService    string
	IdentityKeyringFile string
	ActivationStateFile string
	Interval            time.Duration
}

type inventoryProducerIdentityKeyringFile struct {
	Schema        string   `json:"schema"`
	Generation    uint64   `json:"generation"`
	GroupID       string   `json:"edge_group_id"`
	ActiveKeyID   string   `json:"active_key_id"`
	ActiveKey     string   `json:"active_key"`
	PreviousKeyID string   `json:"previous_key_id,omitempty"`
	PreviousKey   string   `json:"previous_key,omitempty"`
	RevokedKeyIDs []string `json:"revoked_key_ids,omitempty"`
}

func InventoryProducerConfigFromEnv() InventoryProducerConfig {
	cfg := InventoryProducerConfig{
		URL:                 strings.TrimSpace(os.Getenv("FUGUE_EDGE_INVENTORY_HEARTBEAT_URL")),
		AuthorityService:    strings.TrimSpace(os.Getenv("FUGUE_EDGE_INVENTORY_AUTHORITY_SERVICE")),
		IdentityKeyringFile: strings.TrimSpace(os.Getenv("FUGUE_EDGE_INVENTORY_IDENTITY_KEYRING_FILE")),
		ActivationStateFile: strings.TrimSpace(os.Getenv("FUGUE_EDGE_INVENTORY_ACTIVATION_STATE_FILE")),
	}
	if raw := strings.TrimSpace(os.Getenv("FUGUE_EDGE_INVENTORY_HEARTBEAT_INTERVAL")); raw != "" {
		interval, err := time.ParseDuration(raw)
		if err != nil {
			cfg.Interval = -1
		} else {
			cfg.Interval = interval
		}
	} else if cfg.URL != "" || cfg.IdentityKeyringFile != "" || cfg.ActivationStateFile != "" {
		cfg.Interval = 30 * time.Second
	}
	return cfg
}

func (cfg InventoryProducerConfig) enabled() bool {
	return strings.TrimSpace(cfg.URL) != "" || strings.TrimSpace(cfg.IdentityKeyringFile) != "" || strings.TrimSpace(cfg.ActivationStateFile) != "" || cfg.Interval != 0
}

func validateInventoryProducerConfig(producer InventoryProducerConfig, edgeConfig config.EdgeConfig) error {
	if !producer.enabled() {
		return nil
	}
	rawURL := strings.TrimSpace(producer.URL)
	endpoint, err := url.Parse(rawURL)
	if err != nil || endpoint.Scheme != "http" || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" ||
		endpoint.Opaque != "" || endpoint.RawPath != "" || endpoint.Path != groupAuthorityInventoryHeartbeatPathV1 ||
		endpoint.Hostname() != strings.ToLower(endpoint.Hostname()) || endpoint.Port() != "8092" {
		return errors.New("Edge inventory producer endpoint must be exact cluster-local HTTP authority heartbeat URL")
	}
	authorityService := strings.TrimSpace(producer.AuthorityService)
	if !validAuthorityServiceName(authorityService) {
		return errors.New("Edge inventory producer authority service identity is invalid")
	}
	expectedURL := "http://" + authorityService + ".fugue-system.svc:8092" + groupAuthorityInventoryHeartbeatPathV1
	if rawURL != expectedURL {
		return errors.New("Edge inventory producer endpoint is not bound to its explicit authority Service")
	}
	for _, item := range []struct {
		name string
		path string
	}{{"identity keyring", producer.IdentityKeyringFile}, {"activation state", producer.ActivationStateFile}} {
		path := strings.TrimSpace(item.path)
		if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
			return fmt.Errorf("Edge inventory producer %s path must be absolute and normalized", item.name)
		}
	}
	if producer.IdentityKeyringFile == producer.ActivationStateFile || producer.Interval < 5*time.Second || producer.Interval > time.Minute {
		return errors.New("Edge inventory producer projection or interval is invalid")
	}
	if strings.TrimSpace(edgeConfig.EdgeID) == "" || strings.TrimSpace(edgeConfig.EdgeGroupID) == "" ||
		strings.TrimSpace(edgeConfig.EdgeSlot) == "" || strings.TrimSpace(edgeConfig.EdgeInstanceUID) == "" || strings.TrimSpace(edgeConfig.EdgeReleaseEpoch) == "" {
		return errors.New("Edge inventory producer requires the complete projected workload identity")
	}
	return nil
}

func validAuthorityServiceName(value string) bool {
	if value == "" || len(value) > 63 || value[0] == '-' || value[len(value)-1] == '-' {
		return false
	}
	for _, char := range value {
		if (char < 'a' || char > 'z') && (char < '0' || char > '9') && char != '-' {
			return false
		}
	}
	return true
}

func newInventoryProducerHTTPClient(timeout time.Duration) *http.Client {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.ForceAttemptHTTP2 = false
	return &http.Client{
		Transport: transport,
		Timeout:   timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return errors.New("Edge inventory producer redirect is forbidden")
		},
	}
}

func (s *Service) startInventoryProducerLoop(ctx context.Context) {
	if s == nil || !s.InventoryProducer.enabled() {
		return
	}
	go func() {
		run := func() {
			if err := s.InventoryHeartbeatOnce(ctx); err != nil && s.Logger != nil {
				s.Logger.Printf("edge inventory heartbeat failed; edge_id=%s edge_group_id=%s error=%s", strings.TrimSpace(s.Config.EdgeID), strings.TrimSpace(s.Config.EdgeGroupID), s.redact(err.Error()))
			}
		}
		run()
		ticker := time.NewTicker(s.InventoryProducer.Interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				run()
			}
		}
	}()
}

func (s *Service) InventoryHeartbeatOnce(ctx context.Context) (err error) {
	if s == nil || !s.InventoryProducer.enabled() {
		return nil
	}
	s.mu.Lock()
	edgeConfig := s.Config
	status := s.snapshot
	s.mu.Unlock()
	defer func() {
		s.recordInventoryHeartbeatResult(err)
	}()
	if err := validateInventoryProducerConfig(s.InventoryProducer, edgeConfig); err != nil {
		return err
	}
	activation, exists, err := edgegroupfront.ReadActivationState(s.InventoryProducer.ActivationStateFile)
	if err != nil || !exists {
		return errors.New("Edge inventory producer activation state is unavailable")
	}
	if activation.GroupID != strings.TrimSpace(edgeConfig.EdgeGroupID) {
		return errors.New("Edge inventory producer activation group is invalid")
	}
	if activation.ActiveSlot != strings.TrimSpace(edgeConfig.EdgeSlot) {
		s.recordInventoryProducerInactive()
		return nil
	}
	if activation.WorkerSourceCommit != strings.TrimSpace(edgeConfig.EdgeReleaseEpoch) {
		return errors.New("Edge inventory producer release epoch is not active")
	}
	token, err := issueBoundInventoryProducerIdentity(s.InventoryProducer.IdentityKeyringFile, edgeConfig, time.Now().UTC())
	if err != nil {
		return err
	}
	sequence, producerGeneration, err := s.readInventoryProducerCursor(ctx, edgeConfig.EdgeGroupID)
	if err != nil {
		return err
	}
	if sequence == ^uint64(0) || producerGeneration == ^uint64(0) {
		return errors.New("Edge inventory producer cursor is exhausted")
	}
	now := time.Now().UTC()
	nonce, err := newInventoryProducerNonce()
	if err != nil {
		return errors.New("Edge inventory producer nonce is unavailable")
	}
	nextProducerGeneration := producerGeneration + 1
	nodeStatus := edgeHealthStatus(status)
	effectiveHealthy := nodeStatus == model.EdgeHealthHealthy && !edgeConfig.Draining && strings.TrimSpace(status.FailureClass) == ""
	heartbeat := groupInventoryHeartbeat{
		Schema: groupInventoryHeartbeatSchemaV1, GroupID: strings.TrimSpace(edgeConfig.EdgeGroupID),
		ProducerNodeID: strings.TrimSpace(edgeConfig.EdgeID), ProducerGeneration: nextProducerGeneration,
		ExpectedSequence: sequence, IssuedAtUnix: now.Unix(), ExpiresAtUnix: now.Add(time.Minute).Unix(), Nonce: nonce,
		Inventory: groupInventorySnapshot{
			Schema: groupInventorySchemaV1, GroupID: strings.TrimSpace(edgeConfig.EdgeGroupID), Sequence: sequence + 1,
			Generation: producerInventoryEnvelopeGeneration(nextProducerGeneration), ObservedAt: now,
			ActiveEpoch: groupActiveEpoch{
				GroupID: strings.TrimSpace(edgeConfig.EdgeGroupID), Slot: activation.ActiveSlot, ReleaseEpoch: activation.WorkerSourceCommit,
				FenceSequence: activation.Generation, MinHealthyInstances: 1,
			},
			Instances: []groupInstance{{
				EdgeID: strings.TrimSpace(edgeConfig.EdgeID), GroupID: strings.TrimSpace(edgeConfig.EdgeGroupID), Slot: strings.TrimSpace(edgeConfig.EdgeSlot),
				InstanceUID: strings.TrimSpace(edgeConfig.EdgeInstanceUID), ReleaseEpoch: strings.TrimSpace(edgeConfig.EdgeReleaseEpoch),
				EffectiveHealthy: effectiveHealthy, NodeHealthy: status.Healthy, NodeStatus: nodeStatus, Draining: edgeConfig.Draining,
				FailureClass: strings.TrimSpace(status.FailureClass),
			}},
		},
	}
	payload, err := json.Marshal(heartbeat)
	if err != nil {
		return errors.New("Edge inventory producer heartbeat could not be encoded")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimSpace(s.InventoryProducer.URL), bytes.NewReader(payload))
	if err != nil {
		return errors.New("Edge inventory producer heartbeat request could not be built")
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	response, err := s.InventoryProducerHTTPClient.Do(request)
	if err != nil {
		return errors.New("Edge inventory producer heartbeat transport failed")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maxInventoryProducerResponseBytes))
		return fmt.Errorf("Edge inventory producer heartbeat returned status %d", response.StatusCode)
	}
	var receipt groupInventoryHeartbeatReceipt
	decoder := json.NewDecoder(io.LimitReader(response.Body, maxInventoryProducerResponseBytes+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&receipt); err != nil {
		return errors.New("Edge inventory producer receipt is invalid")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) || receipt.Schema != groupInventoryHeartbeatReceiptSchemaV1 ||
		receipt.GroupID != heartbeat.GroupID || receipt.Sequence != heartbeat.Inventory.Sequence || strings.TrimSpace(receipt.Generation) == "" ||
		receipt.Authority != "edge-control" || !receipt.Publication || receipt.ProducerNodeID != heartbeat.ProducerNodeID ||
		receipt.ProducerGeneration != heartbeat.ProducerGeneration {
		return errors.New("Edge inventory producer receipt binding is invalid")
	}
	s.recordInventoryHeartbeatSuccess(receipt.ProducerGeneration, now)
	return nil
}

func (s *Service) readInventoryProducerCursor(ctx context.Context, groupID string) (uint64, uint64, error) {
	endpoint, err := url.Parse(strings.TrimSpace(s.InventoryProducer.URL))
	if err != nil {
		return 0, 0, errors.New("Edge inventory producer endpoint is invalid")
	}
	endpoint.Path = authorityGroupReadyPrefixV1 + strings.TrimSpace(groupID) + "/readyz"
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return 0, 0, errors.New("Edge inventory producer cursor request could not be built")
	}
	request.Header.Set("Accept", "application/json")
	response, err := s.InventoryProducerHTTPClient.Do(request)
	if err != nil {
		return 0, 0, errors.New("Edge inventory producer cursor transport failed")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusServiceUnavailable {
		return 0, 0, fmt.Errorf("Edge inventory producer cursor returned status %d", response.StatusCode)
	}
	var status authorityGroupStatus
	decoder := json.NewDecoder(io.LimitReader(response.Body, maxInventoryProducerResponseBytes+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&status); err != nil {
		return 0, 0, errors.New("Edge inventory producer cursor response is invalid")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) || status.GroupID != strings.TrimSpace(groupID) {
		return 0, 0, errors.New("Edge inventory producer cursor binding is invalid")
	}
	return status.InventorySequence, status.InventoryProducerGeneration, nil
}

func issueBoundInventoryProducerIdentity(path string, edgeConfig config.EdgeConfig, now time.Time) (string, error) {
	raw, err := readBoundedPrivateFile(path, maxInventoryProducerTokenBytes)
	if err != nil {
		return "", errors.New("Edge inventory producer identity keyring is unavailable")
	}
	defer zeroBytes(raw)
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var file inventoryProducerIdentityKeyringFile
	if err := decoder.Decode(&file); err != nil {
		return "", errors.New("Edge inventory producer identity keyring is invalid")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) || file.Schema != inventoryPlatformIdentityKeyringSchemaV1 ||
		file.Generation == 0 || file.GroupID != strings.TrimSpace(edgeConfig.EdgeGroupID) || strings.TrimSpace(file.ActiveKeyID) == "" ||
		len(strings.TrimSpace(file.ActiveKey)) < 32 || (file.PreviousKeyID == "") != (file.PreviousKey == "") {
		return "", errors.New("Edge inventory producer identity keyring is invalid")
	}
	keyring := platformcontrol.DerivePlatformComponentIdentityKeyring(file.ActiveKey, file.ActiveKeyID, file.PreviousKey, file.PreviousKeyID, file.RevokedKeyIDs)
	file.ActiveKey = ""
	file.PreviousKey = ""
	token, err := platformcontrol.IssuePlatformComponentIdentity(keyring, platformcontrol.PlatformComponentIdentityClaims{
		CredentialID: "edge-inventory-producer", Component: model.PlatformConsumerComponentEdgeWorker,
		NodeID: strings.TrimSpace(edgeConfig.EdgeID), ScopeKey: strings.TrimSpace(edgeConfig.EdgeGroupID),
		ArtifactKinds: []string{model.PlatformArtifactKindEdgeRouteBundle},
	}, now.UTC(), 5*time.Minute)
	if err != nil {
		return "", errors.New("Edge inventory producer identity could not be issued")
	}
	return token, nil
}

func newInventoryProducerNonce() (string, error) {
	raw := make([]byte, 18)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func (s *Service) recordInventoryHeartbeatResult(err error) {
	if s == nil || err == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshot.InventoryProducerActive = true
	s.snapshot.InventoryHeartbeatError = err.Error()
}

func (s *Service) recordInventoryHeartbeatSuccess(generation uint64, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	at = at.UTC()
	s.snapshot.InventoryProducerActive = true
	s.snapshot.InventoryHeartbeatGeneration = generation
	s.snapshot.InventoryHeartbeatAt = &at
	s.snapshot.InventoryHeartbeatError = ""
}

func (s *Service) recordInventoryProducerInactive() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshot.InventoryProducerActive = false
	s.snapshot.InventoryHeartbeatError = ""
}
