package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	imageCachePlatformPlanAPIVersion         = "image-plane.fugue.dev/v1"
	imageCachePlatformPlanStateKind          = "ImageReplicationPlanState"
	imageCachePlatformPlanKind               = "ImageReplicationPlan"
	imageCachePlatformPlanComponent          = "image-cache"
	imageCachePlatformPlanArtifactKind       = "image_replication_plan"
	imageCachePlatformPlanReleaseChannel     = "shadow"
	imageCachePlatformPlanProtocolVersion    = "v1"
	imageCachePlatformPlanSchemaVersion      = "v1"
	imageCachePlatformPlanCredentialVersion  = "platform-component-identity.fugue.dev/v1"
	imageCachePlatformPlanCredentialKind     = "PlatformComponentCredential"
	imageCachePlatformPlanCredentialPrefix   = "fugue_pc_v1."
	imageCachePlatformPlanArtifactSchema     = "1.0"
	imageCachePlatformPlanArtifactStatus     = "validated"
	imageCachePlatformPlanReleaseStatus      = "active"
	imageCachePlatformPlanVerificationNew    = "serving_unverified"
	imageCachePlatformPlanVerificationPassed = "verified"
	imageCachePlatformPlanObservationVersion = "image-cache.fugue.dev/v1"
	imageCachePlatformPlanObservationKind    = "ImageReplicationPlanShadowObservation"
	imageCachePlatformPlanCapability         = "shadow-observation-v1"
	imageCachePlatformPlanApplyObserved      = "observed"
	imageCachePlatformPlanApplyFailed        = "failed"
	imageCachePlatformPlanProbePassed        = "passed"
	imageCachePlatformPlanProbeFailed        = "failed"

	imageCachePlatformPlanPath      = "/v1/image-plane/replication-plan"
	imageCachePlatformHeartbeatPath = "/v1/platform-state/consumers/trusted-heartbeat"

	imageCachePlatformCredentialMaxBytes  = 32 << 10
	imageCachePlatformPlanMaxBytes        = 4 << 20
	imageCachePlatformHeartbeatMaxBytes   = 1 << 20
	imageCachePlatformPlanErrorMaxBytes   = 512
	imageCachePlatformPlanReadinessMaxAge = 2 * time.Minute
)

var errImageCachePlatformHeartbeatConflict = errors.New("image-cache platform heartbeat conflict")

type imageCachePlatformPlanConfig struct {
	APIBaseURL        string
	NodeID            string
	CredentialPath    string
	ObservationPath   string
	AllowInsecureHTTP bool
	LongPoll          time.Duration
	RequestTimeout    time.Duration
	RetryMin          time.Duration
	RetryMax          time.Duration
	NoPlanRetry       time.Duration
	MinCredentialLife time.Duration
	ArchiveLimit      int
}

type imageCachePlatformPlanConsumer struct {
	config imageCachePlatformPlanConfig
	client *http.Client
	now    func() time.Time
	random io.Reader

	statusMu sync.RWMutex
	status   imageCachePlatformPlanStatus
}

type imageCachePlatformPlanStatus struct {
	Enabled             bool       `json:"enabled"`
	ObservationOnly     bool       `json:"observation_only"`
	State               string     `json:"state"`
	Generation          string     `json:"generation,omitempty"`
	LKGGeneration       string     `json:"lkg_generation,omitempty"`
	LastError           string     `json:"last_error,omitempty"`
	LastObservationAt   *time.Time `json:"last_observation_at,omitempty"`
	LastHeartbeatAt     *time.Time `json:"last_heartbeat_at,omitempty"`
	CredentialExpiresAt *time.Time `json:"credential_expires_at,omitempty"`
}

type imageCachePlatformCredentialEnvelope struct {
	Credential imageCachePlatformCredential `json:"credential"`
}

type imageCachePlatformCredential struct {
	APIVersion    string    `json:"api_version"`
	Kind          string    `json:"kind"`
	CredentialID  string    `json:"credential_id"`
	Token         string    `json:"token"`
	TokenID       string    `json:"token_id"`
	Component     string    `json:"component"`
	NodeID        string    `json:"node_id"`
	ScopeKey      string    `json:"scope_key"`
	ArtifactKinds []string  `json:"artifact_kinds"`
	IssuedAt      time.Time `json:"issued_at"`
	ExpiresAt     time.Time `json:"expires_at"`
	RenewAfter    time.Time `json:"renew_after"`
}

type imageCachePlatformArtifactScope struct {
	ScopeType string `json:"scope_type"`
	Key       string `json:"key"`
	NodeID    string `json:"node_id"`
}

type imageCachePlatformArtifactProvenance struct {
	Issuer    string    `json:"issuer"`
	KeyID     string    `json:"key_id"`
	Algorithm string    `json:"algorithm"`
	Signature string    `json:"signature"`
	SignedAt  time.Time `json:"signed_at"`
}

type imageCachePlatformArtifact struct {
	ID                 string                               `json:"id"`
	ArtifactKind       string                               `json:"artifact_kind"`
	Scope              imageCachePlatformArtifactScope      `json:"scope"`
	ScopeKey           string                               `json:"scope_key"`
	SchemaVersion      string                               `json:"schema_version"`
	Generation         string                               `json:"generation"`
	GenerationSequence int64                                `json:"generation_sequence"`
	Status             string                               `json:"status"`
	ContentHash        string                               `json:"content_hash"`
	Content            map[string]any                       `json:"content"`
	Provenance         imageCachePlatformArtifactProvenance `json:"provenance"`
}

type imageCachePlatformRelease struct {
	ID                string                          `json:"id"`
	ArtifactID        string                          `json:"artifact_id"`
	ArtifactKind      string                          `json:"artifact_kind"`
	Scope             imageCachePlatformArtifactScope `json:"scope"`
	ScopeKey          string                          `json:"scope_key"`
	Generation        string                          `json:"generation"`
	ReleaseChannel    string                          `json:"release_channel"`
	Status            string                          `json:"status"`
	FencingToken      int64                           `json:"fencing_token"`
	VerificationState string                          `json:"verification_state"`
}

type imageCachePlatformLKG struct {
	ID                 string                          `json:"id"`
	ArtifactID         string                          `json:"artifact_id"`
	ArtifactKind       string                          `json:"artifact_kind"`
	Scope              imageCachePlatformArtifactScope `json:"scope"`
	ScopeKey           string                          `json:"scope_key"`
	SchemaVersion      string                          `json:"schema_version"`
	Generation         string                          `json:"generation"`
	GenerationSequence int64                           `json:"generation_sequence"`
	ContentHash        string                          `json:"content_hash"`
	ExpiresAt          time.Time                       `json:"expires_at"`
}

type imageCachePlatformHeartbeatContract struct {
	ExpectedConsumerSetID string     `json:"expected_consumer_set_id"`
	ReleaseSetID          string     `json:"release_set_id"`
	ArtifactReleaseID     string     `json:"artifact_release_id"`
	FencingToken          int64      `json:"fencing_token"`
	SequenceFloor         int64      `json:"sequence_floor"`
	IssuedAtFloor         *time.Time `json:"issued_at_floor"`
	ProtocolVersion       string     `json:"protocol_version"`
	SchemaVersion         string     `json:"schema_version"`
}

type imageCachePlatformPlanResponse struct {
	APIVersion            string                               `json:"api_version"`
	Kind                  string                               `json:"kind"`
	Component             string                               `json:"component"`
	NodeID                string                               `json:"node_id"`
	ScopeKey              string                               `json:"scope_key"`
	ArtifactKind          string                               `json:"artifact_kind"`
	ReleaseChannel        string                               `json:"release_channel"`
	Artifact              *imageCachePlatformArtifact          `json:"artifact"`
	Release               *imageCachePlatformRelease           `json:"release"`
	LKG                   *imageCachePlatformLKG               `json:"lkg"`
	LKGArtifact           *imageCachePlatformArtifact          `json:"lkg_artifact"`
	ExpectedConsumerSetID string                               `json:"expected_consumer_set_id"`
	Heartbeat             *imageCachePlatformHeartbeatContract `json:"heartbeat"`
	Generation            string                               `json:"generation"`
	Waited                bool                                 `json:"waited"`
	ServerTime            time.Time                            `json:"server_time"`
}

type imageCachePlatformPlanObservation struct {
	APIVersion         string          `json:"api_version"`
	Kind               string          `json:"kind"`
	ObservationOnly    bool            `json:"observation_only"`
	NodeID             string          `json:"node_id"`
	ScopeKey           string          `json:"scope_key"`
	ArtifactKind       string          `json:"artifact_kind"`
	ReleaseChannel     string          `json:"release_channel"`
	Generation         string          `json:"generation"`
	GenerationSequence int64           `json:"generation_sequence,omitempty"`
	ContentHash        string          `json:"content_hash,omitempty"`
	LKGGeneration      string          `json:"lkg_generation,omitempty"`
	ObservedAt         time.Time       `json:"observed_at"`
	Plan               json.RawMessage `json:"plan"`
}

type imageCachePlatformHeartbeat struct {
	ConsumerID                string    `json:"consumer_id"`
	Component                 string    `json:"component"`
	NodeID                    string    `json:"node_id"`
	ArtifactKind              string    `json:"artifact_kind"`
	ScopeKey                  string    `json:"scope_key"`
	ReleaseSetID              string    `json:"release_set_id"`
	ExpectedConsumerSetID     string    `json:"expected_consumer_set_id"`
	FencingToken              int64     `json:"fencing_token"`
	ProtocolVersion           string    `json:"protocol_version"`
	SchemaVersion             string    `json:"schema_version"`
	CompatibilityCapabilities []string  `json:"compatibility_capabilities,omitempty"`
	Sequence                  int64     `json:"sequence"`
	IssuedAt                  time.Time `json:"issued_at"`
	Nonce                     string    `json:"nonce"`
	GenerationSequence        int64     `json:"generation_sequence"`
	DesiredGeneration         string    `json:"desired_generation"`
	ActualGeneration          string    `json:"actual_generation"`
	LKGGeneration             string    `json:"lkg_generation"`
	ApplyStatus               string    `json:"apply_status"`
	ProbeStatus               string    `json:"probe_status"`
	ServingLKG                bool      `json:"serving_lkg,omitempty"`
	LKGExpired                bool      `json:"lkg_expired,omitempty"`
	LastError                 string    `json:"last_error,omitempty"`
	EvidenceHash              string    `json:"evidence_hash"`
}

type imageCachePlatformHeartbeatResponse struct {
	Consumer struct {
		ConsumerID            string `json:"consumer_id"`
		Component             string `json:"component"`
		NodeID                string `json:"node_id"`
		ArtifactKind          string `json:"artifact_kind"`
		ScopeKey              string `json:"scope_key"`
		ReleaseSetID          string `json:"release_set_id"`
		ExpectedConsumerSetID string `json:"expected_consumer_set_id"`
		FencingToken          int64  `json:"fencing_token"`
		Sequence              int64  `json:"sequence"`
		DesiredGeneration     string `json:"desired_generation"`
		ActualGeneration      string `json:"actual_generation"`
		ApplyStatus           string `json:"apply_status"`
		ProbeStatus           string `json:"probe_status"`
		IdentityVerified      bool   `json:"identity_verified"`
	} `json:"consumer"`
	Drift bool `json:"drift"`
}

type imageCachePlatformPlanCycle struct {
	HasDesired    bool
	Generation    string
	LKGGeneration string
}

type imageCachePlatformHTTPError struct {
	Status  int
	Message string
}

func (e *imageCachePlatformHTTPError) Error() string {
	if e == nil {
		return "image-cache platform request failed"
	}
	if e.Message == "" {
		return fmt.Sprintf("image-cache platform request returned HTTP %d", e.Status)
	}
	return fmt.Sprintf("image-cache platform request returned HTTP %d: %s", e.Status, e.Message)
}

func newImageCachePlatformPlanConsumerFromEnvironment(apiBaseURL, nodeID string) (*imageCachePlatformPlanConsumer, error) {
	enabledRaw := strings.TrimSpace(os.Getenv("FUGUE_IMAGE_CACHE_PLATFORM_PLAN_SHADOW_ENABLED"))
	if enabledRaw == "" || enabledRaw == "false" {
		return nil, nil
	}
	if enabledRaw != "true" {
		return nil, errors.New("FUGUE_IMAGE_CACHE_PLATFORM_PLAN_SHADOW_ENABLED must be true or false")
	}
	longPoll, err := strictImageCachePlatformDuration("FUGUE_IMAGE_CACHE_PLATFORM_PLAN_LONG_POLL", 30*time.Second, time.Second, 30*time.Second)
	if err != nil {
		return nil, err
	}
	requestTimeout, err := strictImageCachePlatformDuration("FUGUE_IMAGE_CACHE_PLATFORM_PLAN_REQUEST_TIMEOUT", 40*time.Second, longPoll+time.Second, time.Minute)
	if err != nil {
		return nil, err
	}
	retryMin, err := strictImageCachePlatformDuration("FUGUE_IMAGE_CACHE_PLATFORM_PLAN_RETRY_MIN", 2*time.Second, 100*time.Millisecond, time.Minute)
	if err != nil {
		return nil, err
	}
	retryMax, err := strictImageCachePlatformDuration("FUGUE_IMAGE_CACHE_PLATFORM_PLAN_RETRY_MAX", time.Minute, retryMin, 5*time.Minute)
	if err != nil {
		return nil, err
	}
	noPlanRetry, err := strictImageCachePlatformDuration("FUGUE_IMAGE_CACHE_PLATFORM_PLAN_NO_PLAN_RETRY", 15*time.Second, time.Second, 5*time.Minute)
	if err != nil {
		return nil, err
	}
	minCredentialLife, err := strictImageCachePlatformDuration("FUGUE_IMAGE_CACHE_PLATFORM_CREDENTIAL_MIN_VALIDITY", 30*time.Second, time.Second, 10*time.Minute)
	if err != nil {
		return nil, err
	}
	archiveLimit, err := strictImageCachePlatformPositiveInt("FUGUE_IMAGE_CACHE_PLATFORM_PLAN_ARCHIVE_LIMIT", 5, 3, 32)
	if err != nil {
		return nil, err
	}
	allowInsecureHTTP, err := strictImageCachePlatformBool("FUGUE_IMAGE_CACHE_PLATFORM_PLAN_ALLOW_INSECURE_HTTP", false)
	if err != nil {
		return nil, err
	}
	config := imageCachePlatformPlanConfig{
		APIBaseURL:        apiBaseURL,
		NodeID:            nodeID,
		CredentialPath:    env("FUGUE_IMAGE_CACHE_PLATFORM_CREDENTIAL_FILE", "/run/fugue/image-cache/platform-component-credential.json"),
		ObservationPath:   env("FUGUE_IMAGE_CACHE_REPLICATION_PLAN_PATH", "/var/lib/fugue/image-cache/replication-plan.json"),
		AllowInsecureHTTP: allowInsecureHTTP,
		LongPoll:          longPoll,
		RequestTimeout:    requestTimeout,
		RetryMin:          retryMin,
		RetryMax:          retryMax,
		NoPlanRetry:       noPlanRetry,
		MinCredentialLife: minCredentialLife,
		ArchiveLimit:      archiveLimit,
	}
	return newImageCachePlatformPlanConsumer(config, nil)
}

func newImageCachePlatformPlanConsumer(config imageCachePlatformPlanConfig, client *http.Client) (*imageCachePlatformPlanConsumer, error) {
	config.APIBaseURL = strings.TrimRight(strings.TrimSpace(config.APIBaseURL), "/")
	config.NodeID = strings.ToLower(strings.TrimSpace(config.NodeID))
	config.CredentialPath = strings.TrimSpace(config.CredentialPath)
	config.ObservationPath = strings.TrimSpace(config.ObservationPath)
	if err := validateImageCachePlatformPlanConfig(config); err != nil {
		return nil, err
	}
	if client == nil {
		client = &http.Client{
			Timeout: config.RequestTimeout,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return errors.New("image-cache platform API redirects are forbidden")
			},
		}
	}
	consumer := &imageCachePlatformPlanConsumer{
		config: config,
		client: client,
		now:    func() time.Time { return time.Now().UTC() },
		random: rand.Reader,
		status: imageCachePlatformPlanStatus{
			Enabled:         true,
			ObservationOnly: true,
			State:           "starting",
		},
	}
	return consumer, nil
}

func validateImageCachePlatformPlanConfig(config imageCachePlatformPlanConfig) error {
	parsed, err := url.Parse(config.APIBaseURL)
	if err != nil || parsed == nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return errors.New("image-cache platform plan API base URL must be an absolute HTTP(S) origin")
	}
	if parsed.Scheme == "http" && !config.AllowInsecureHTTP {
		return errors.New("image-cache platform plan API requires HTTPS unless insecure HTTP is explicitly allowed")
	}
	if strings.ContainsAny(config.NodeID, " \t\r\n/") || config.NodeID == "" || len(config.NodeID) > 253 {
		return errors.New("image-cache platform plan node ID is invalid")
	}
	for name, path := range map[string]string{
		"credential":  config.CredentialPath,
		"observation": config.ObservationPath,
	} {
		if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
			return fmt.Errorf("image-cache platform plan %s path must be absolute and canonical", name)
		}
	}
	if config.LongPoll < time.Second || config.LongPoll > 30*time.Second ||
		config.RequestTimeout <= config.LongPoll || config.RequestTimeout > time.Minute ||
		config.RetryMin <= 0 || config.RetryMax < config.RetryMin ||
		config.NoPlanRetry <= 0 || config.MinCredentialLife <= 0 ||
		config.ArchiveLimit < 3 || config.ArchiveLimit > 32 {
		return errors.New("image-cache platform plan timing or archive bounds are invalid")
	}
	return nil
}

func strictImageCachePlatformDuration(key string, fallback, minimum, maximum time.Duration) (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil || value < minimum || value > maximum {
		return 0, fmt.Errorf("%s must be between %s and %s", key, minimum, maximum)
	}
	return value, nil
}

func strictImageCachePlatformPositiveInt(key string, fallback, minimum, maximum int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < minimum || value > maximum {
		return 0, fmt.Errorf("%s must be between %d and %d", key, minimum, maximum)
	}
	return value, nil
}

func strictImageCachePlatformBool(key string, fallback bool) (bool, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback, nil
	}
	if raw == "true" {
		return true, nil
	}
	if raw == "false" {
		return false, nil
	}
	return false, fmt.Errorf("%s must be true or false", key)
}

func (c *imageCachePlatformPlanConsumer) Run(ctx context.Context) {
	if c == nil || ctx == nil {
		return
	}
	failures := 0
	for ctx.Err() == nil {
		cycle, err := c.reconcileCycle(ctx)
		if err == nil {
			failures = 0
			delay := time.Duration(0)
			if !cycle.HasDesired {
				delay = c.config.NoPlanRetry
			}
			if !waitImageCachePlatformPlan(ctx, delay) {
				return
			}
			continue
		}
		if ctx.Err() != nil {
			return
		}
		failures++
		message := boundedImageCachePlatformError(err)
		c.updateFailureStatus(message)
		log.Printf("image-cache shadow plan reconcile failed: %s", message)
		if !waitImageCachePlatformPlan(ctx, imageCachePlatformBackoff(c.config.RetryMin, c.config.RetryMax, failures)) {
			return
		}
	}
}

func (c *imageCachePlatformPlanConsumer) reconcileCycle(ctx context.Context) (imageCachePlatformPlanCycle, error) {
	cycle, err := c.reconcileOnce(ctx, false)
	if errors.Is(err, errImageCachePlatformHeartbeatConflict) && ctx.Err() == nil {
		return c.reconcileOnce(ctx, true)
	}
	return cycle, err
}

func (c *imageCachePlatformPlanConsumer) reconcileOnce(ctx context.Context, forceRefresh bool) (imageCachePlatformPlanCycle, error) {
	now := c.now().UTC()
	credential, err := readImageCachePlatformCredential(c.config.CredentialPath, c.config.NodeID, now, c.config.MinCredentialLife)
	if err != nil {
		return imageCachePlatformPlanCycle{}, fmt.Errorf("load rotated platform credential: %w", err)
	}
	c.updateCredentialStatus(credential.ExpiresAt)

	current, currentErr := readImageCachePlatformPlanObservation(c.config.ObservationPath, c.config.NodeID)
	currentGeneration := current.Generation
	if forceRefresh || currentErr != nil {
		currentGeneration = ""
	}
	wait := c.config.LongPoll
	if currentGeneration == "" || forceRefresh {
		wait = 0
	}
	plan, rawPlan, err := c.fetchPlan(ctx, credential, currentGeneration, wait)
	if err != nil {
		return imageCachePlatformPlanCycle{}, err
	}
	if err := validateImageCachePlatformPlanResponse(plan, c.config.NodeID, c.now().UTC()); err != nil {
		return imageCachePlatformPlanCycle{}, fmt.Errorf("validate image replication plan: %w", err)
	}
	observation := newImageCachePlatformPlanObservation(plan, rawPlan, c.now().UTC())
	if err := writeImageCachePlatformPlanObservation(c.config.ObservationPath, observation, c.config.ArchiveLimit); err != nil {
		if plan.Artifact != nil && plan.Heartbeat != nil {
			failureHeartbeat, heartbeatErr := c.newHeartbeat(plan, current.Generation, imageCachePlatformPlanApplyFailed, imageCachePlatformPlanProbeFailed, "persist shadow observation: "+boundedImageCachePlatformError(err))
			if heartbeatErr == nil {
				_ = c.postHeartbeat(ctx, credential, failureHeartbeat)
			}
		}
		return imageCachePlatformPlanCycle{}, fmt.Errorf("persist image replication plan observation: %w", err)
	}

	cycle := imageCachePlatformPlanCycle{Generation: plan.Generation}
	if plan.LKG != nil {
		cycle.LKGGeneration = plan.LKG.Generation
	}
	if plan.Artifact == nil {
		c.updateObservationStatus(cycle, c.now().UTC(), nil)
		return cycle, nil
	}
	cycle.HasDesired = true
	heartbeat, err := c.newHeartbeat(plan, plan.Generation, imageCachePlatformPlanApplyObserved, imageCachePlatformPlanProbePassed, "")
	if err != nil {
		return cycle, err
	}
	if err := c.postHeartbeat(ctx, credential, heartbeat); err != nil {
		return cycle, err
	}
	heartbeatAt := c.now().UTC()
	c.updateObservationStatus(cycle, observation.ObservedAt, &heartbeatAt)
	return cycle, nil
}

func (c *imageCachePlatformPlanConsumer) fetchPlan(
	ctx context.Context,
	credential imageCachePlatformCredential,
	currentGeneration string,
	wait time.Duration,
) (imageCachePlatformPlanResponse, []byte, error) {
	endpoint, err := url.Parse(c.config.APIBaseURL + imageCachePlatformPlanPath)
	if err != nil {
		return imageCachePlatformPlanResponse{}, nil, err
	}
	query := endpoint.Query()
	if currentGeneration != "" {
		query.Set("current_generation", currentGeneration)
	}
	if wait > 0 {
		query.Set("wait_seconds", strconv.Itoa(int(wait/time.Second)))
	}
	endpoint.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return imageCachePlatformPlanResponse{}, nil, err
	}
	setImageCachePlatformRequestHeaders(request, credential.Token)
	response, err := c.client.Do(request)
	if err != nil {
		return imageCachePlatformPlanResponse{}, nil, fmt.Errorf("fetch image replication plan: %w", err)
	}
	body, readErr := readImageCachePlatformHTTPBody(response, imageCachePlatformPlanMaxBytes)
	if readErr != nil {
		return imageCachePlatformPlanResponse{}, nil, readErr
	}
	if response.StatusCode != http.StatusOK {
		return imageCachePlatformPlanResponse{}, nil, imageCachePlatformResponseError(response.StatusCode, body, credential.Token)
	}
	if !strings.Contains(strings.ToLower(response.Header.Get("Cache-Control")), "no-store") {
		return imageCachePlatformPlanResponse{}, nil, errors.New("image replication plan response is cacheable")
	}
	if bytes.Contains(body, []byte(credential.Token)) {
		return imageCachePlatformPlanResponse{}, nil, errors.New("image replication plan response reflected its bearer credential")
	}
	var plan imageCachePlatformPlanResponse
	if err := json.Unmarshal(body, &plan); err != nil {
		return imageCachePlatformPlanResponse{}, nil, fmt.Errorf("decode image replication plan response: %w", err)
	}
	return plan, body, nil
}

func (c *imageCachePlatformPlanConsumer) newHeartbeat(
	plan imageCachePlatformPlanResponse,
	actualGeneration string,
	applyStatus string,
	probeStatus string,
	lastError string,
) (imageCachePlatformHeartbeat, error) {
	if plan.Artifact == nil || plan.Heartbeat == nil {
		return imageCachePlatformHeartbeat{}, errors.New("image replication plan heartbeat contract is unavailable")
	}
	if plan.Heartbeat.SequenceFloor == math.MaxInt64 {
		return imageCachePlatformHeartbeat{}, errors.New("image replication plan heartbeat sequence is exhausted")
	}
	issuedAt := c.now().UTC()
	if plan.ServerTime.After(issuedAt) {
		issuedAt = plan.ServerTime.UTC()
	}
	if floor := plan.Heartbeat.IssuedAtFloor; floor != nil && !issuedAt.After(floor.UTC()) {
		issuedAt = floor.UTC().Add(time.Millisecond)
	}
	nonceBytes := make([]byte, 24)
	if _, err := io.ReadFull(c.random, nonceBytes); err != nil {
		return imageCachePlatformHeartbeat{}, fmt.Errorf("generate heartbeat nonce: %w", err)
	}
	lkgGeneration := ""
	lkgExpired := false
	if plan.LKG != nil {
		lkgGeneration = plan.LKG.Generation
		lkgExpired = !issuedAt.Before(plan.LKG.ExpiresAt.UTC())
	}
	heartbeat := imageCachePlatformHeartbeat{
		ConsumerID:                imageCachePlatformPlanComponent + ":" + c.config.NodeID,
		Component:                 imageCachePlatformPlanComponent,
		NodeID:                    c.config.NodeID,
		ArtifactKind:              imageCachePlatformPlanArtifactKind,
		ScopeKey:                  "node:" + c.config.NodeID,
		ReleaseSetID:              plan.Heartbeat.ReleaseSetID,
		ExpectedConsumerSetID:     plan.Heartbeat.ExpectedConsumerSetID,
		FencingToken:              plan.Heartbeat.FencingToken,
		ProtocolVersion:           plan.Heartbeat.ProtocolVersion,
		SchemaVersion:             plan.Heartbeat.SchemaVersion,
		CompatibilityCapabilities: []string{imageCachePlatformPlanCapability},
		Sequence:                  plan.Heartbeat.SequenceFloor + 1,
		IssuedAt:                  issuedAt,
		Nonce:                     hex.EncodeToString(nonceBytes),
		GenerationSequence:        plan.Artifact.GenerationSequence,
		DesiredGeneration:         plan.Artifact.Generation,
		ActualGeneration:          strings.TrimSpace(actualGeneration),
		LKGGeneration:             lkgGeneration,
		ApplyStatus:               applyStatus,
		ProbeStatus:               probeStatus,
		LKGExpired:                lkgExpired,
		LastError:                 truncateImageCachePlatformValue(lastError, imageCachePlatformPlanErrorMaxBytes),
	}
	evidenceHash, err := computeImageCachePlatformHeartbeatEvidenceHash(heartbeat)
	if err != nil {
		return imageCachePlatformHeartbeat{}, err
	}
	heartbeat.EvidenceHash = evidenceHash
	return heartbeat, nil
}

func (c *imageCachePlatformPlanConsumer) postHeartbeat(
	ctx context.Context,
	credential imageCachePlatformCredential,
	heartbeat imageCachePlatformHeartbeat,
) error {
	payload, err := json.Marshal(heartbeat)
	if err != nil {
		return fmt.Errorf("marshal image-cache platform heartbeat: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.config.APIBaseURL+imageCachePlatformHeartbeatPath, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	setImageCachePlatformRequestHeaders(request, credential.Token)
	request.Header.Set("Content-Type", "application/json")
	response, err := c.client.Do(request)
	if err != nil {
		return fmt.Errorf("post image-cache platform heartbeat: %w", err)
	}
	body, readErr := readImageCachePlatformHTTPBody(response, imageCachePlatformHeartbeatMaxBytes)
	if readErr != nil {
		return readErr
	}
	if response.StatusCode != http.StatusOK {
		httpErr := imageCachePlatformResponseError(response.StatusCode, body, credential.Token)
		if response.StatusCode == http.StatusConflict {
			return errors.Join(errImageCachePlatformHeartbeatConflict, httpErr)
		}
		return httpErr
	}
	if bytes.Contains(body, []byte(credential.Token)) {
		return errors.New("image-cache platform heartbeat response reflected its bearer credential")
	}
	var accepted imageCachePlatformHeartbeatResponse
	if err := json.Unmarshal(body, &accepted); err != nil {
		return fmt.Errorf("decode image-cache platform heartbeat response: %w", err)
	}
	consumer := accepted.Consumer
	if consumer.ConsumerID != heartbeat.ConsumerID ||
		consumer.Component != heartbeat.Component ||
		consumer.NodeID != heartbeat.NodeID ||
		consumer.ArtifactKind != heartbeat.ArtifactKind ||
		consumer.ScopeKey != heartbeat.ScopeKey ||
		consumer.ReleaseSetID != heartbeat.ReleaseSetID ||
		consumer.ExpectedConsumerSetID != heartbeat.ExpectedConsumerSetID ||
		consumer.FencingToken != heartbeat.FencingToken ||
		consumer.Sequence != heartbeat.Sequence ||
		consumer.DesiredGeneration != heartbeat.DesiredGeneration ||
		consumer.ActualGeneration != heartbeat.ActualGeneration ||
		consumer.ApplyStatus != heartbeat.ApplyStatus ||
		consumer.ProbeStatus != heartbeat.ProbeStatus ||
		!consumer.IdentityVerified {
		return errors.New("image-cache platform heartbeat acknowledgement drifted from the submitted identity or cursor")
	}
	return nil
}

func validateImageCachePlatformPlanResponse(plan imageCachePlatformPlanResponse, nodeID string, now time.Time) error {
	nodeID = strings.ToLower(strings.TrimSpace(nodeID))
	scopeKey := "node:" + nodeID
	if plan.APIVersion != imageCachePlatformPlanAPIVersion ||
		plan.Kind != imageCachePlatformPlanStateKind ||
		plan.Component != imageCachePlatformPlanComponent ||
		plan.NodeID != nodeID ||
		plan.ScopeKey != scopeKey ||
		plan.ArtifactKind != imageCachePlatformPlanArtifactKind ||
		plan.ReleaseChannel != imageCachePlatformPlanReleaseChannel {
		return errors.New("image replication plan identity or version envelope mismatch")
	}
	if plan.ServerTime.IsZero() || plan.ServerTime.After(now.UTC().Add(30*time.Second)) || plan.ServerTime.Before(now.UTC().Add(-2*time.Minute)) {
		return errors.New("image replication plan server time is outside the accepted freshness window")
	}
	if plan.Artifact == nil {
		if plan.Release != nil || plan.Heartbeat != nil || plan.Generation != "" || plan.ExpectedConsumerSetID != "" {
			return errors.New("empty image replication plan carries a release or heartbeat binding")
		}
	} else {
		if err := validateImageCachePlatformArtifact(*plan.Artifact, nodeID); err != nil {
			return err
		}
		if plan.Generation != plan.Artifact.Generation || plan.Release == nil || plan.Heartbeat == nil {
			return errors.New("image replication desired state is missing its exact release or heartbeat contract")
		}
		release := plan.Release
		if release.ID == "" || release.ArtifactID != plan.Artifact.ID ||
			release.ArtifactKind != plan.Artifact.ArtifactKind ||
			release.ScopeKey != scopeKey || release.Scope.Key != scopeKey || release.Scope.NodeID != nodeID ||
			release.Generation != plan.Artifact.Generation ||
			release.ReleaseChannel != imageCachePlatformPlanReleaseChannel ||
			release.Status != imageCachePlatformPlanReleaseStatus || release.FencingToken <= 0 {
			return errors.New("image replication release is not bound to the desired artifact and node")
		}
		if release.VerificationState != imageCachePlatformPlanVerificationNew && release.VerificationState != imageCachePlatformPlanVerificationPassed {
			return errors.New("image replication release verification state is not servable")
		}
		heartbeat := plan.Heartbeat
		if heartbeat.ExpectedConsumerSetID == "" || heartbeat.ExpectedConsumerSetID != plan.ExpectedConsumerSetID ||
			heartbeat.ReleaseSetID == "" || heartbeat.ArtifactReleaseID != release.ID ||
			heartbeat.FencingToken != release.FencingToken || heartbeat.SequenceFloor < 0 ||
			heartbeat.ProtocolVersion != imageCachePlatformPlanProtocolVersion ||
			heartbeat.SchemaVersion != imageCachePlatformPlanSchemaVersion {
			return errors.New("image replication heartbeat cursor is not bound to the active release")
		}
		if heartbeat.IssuedAtFloor != nil && heartbeat.IssuedAtFloor.After(plan.ServerTime.Add(30*time.Second)) {
			return errors.New("image replication heartbeat issued-at floor is ahead of the active server fence")
		}
	}
	if (plan.LKG == nil) != (plan.LKGArtifact == nil) {
		return errors.New("image replication LKG snapshot and artifact must be returned together")
	}
	if plan.LKG != nil {
		if err := validateImageCachePlatformArtifact(*plan.LKGArtifact, nodeID); err != nil {
			return fmt.Errorf("invalid image replication LKG artifact: %w", err)
		}
		if plan.LKG.ID == "" || plan.LKG.ArtifactID != plan.LKGArtifact.ID ||
			plan.LKG.ArtifactKind != imageCachePlatformPlanArtifactKind ||
			plan.LKG.ScopeKey != scopeKey || plan.LKG.Scope.Key != scopeKey || plan.LKG.Scope.NodeID != nodeID ||
			plan.LKG.SchemaVersion != imageCachePlatformPlanArtifactSchema ||
			plan.LKG.Generation != plan.LKGArtifact.Generation ||
			plan.LKG.GenerationSequence != plan.LKGArtifact.GenerationSequence ||
			plan.LKG.ContentHash != plan.LKGArtifact.ContentHash ||
			!plan.ServerTime.Before(plan.LKG.ExpiresAt.UTC()) {
			return errors.New("image replication LKG snapshot is expired or incoherent")
		}
	}
	return nil
}

func validateImageCachePlatformArtifact(artifact imageCachePlatformArtifact, nodeID string) error {
	scopeKey := "node:" + nodeID
	if artifact.ID == "" || artifact.ArtifactKind != imageCachePlatformPlanArtifactKind ||
		artifact.Scope.ScopeType != "node" || artifact.Scope.Key != scopeKey || artifact.Scope.NodeID != nodeID ||
		artifact.ScopeKey != scopeKey || artifact.SchemaVersion != imageCachePlatformPlanArtifactSchema ||
		artifact.Generation == "" || artifact.GenerationSequence <= 0 || artifact.Status != imageCachePlatformPlanArtifactStatus ||
		artifact.Provenance.KeyID == "" || artifact.Provenance.Algorithm != "hmac-sha256" || artifact.Provenance.Signature == "" || artifact.Provenance.SignedAt.IsZero() {
		return errors.New("image replication artifact identity, schema, status, or provenance is invalid")
	}
	rawContent, err := json.Marshal(artifact.Content)
	if err != nil || len(artifact.Content) == 0 {
		return errors.New("image replication artifact content is invalid")
	}
	sum := sha256.Sum256(rawContent)
	if artifact.ContentHash != "sha256:"+hex.EncodeToString(sum[:]) {
		return errors.New("image replication artifact content hash mismatch")
	}
	apiVersion, _ := artifact.Content["apiVersion"].(string)
	kind, _ := artifact.Content["kind"].(string)
	spec, _ := artifact.Content["spec"].(map[string]any)
	specNodeID, _ := spec["nodeID"].(string)
	images, imagesPresent := spec["images"]
	if apiVersion != imageCachePlatformPlanAPIVersion || kind != imageCachePlatformPlanKind || specNodeID != nodeID || !imagesPresent {
		return errors.New("image replication artifact payload envelope or node binding is invalid")
	}
	if _, ok := images.([]any); !ok {
		return errors.New("image replication artifact images must be an array")
	}
	return nil
}

func readImageCachePlatformCredential(path, nodeID string, now time.Time, minimumValidity time.Duration) (imageCachePlatformCredential, error) {
	body, err := readImageCachePlatformRegularFile(path, imageCachePlatformCredentialMaxBytes, true)
	if err != nil {
		return imageCachePlatformCredential{}, err
	}
	var envelope imageCachePlatformCredentialEnvelope
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil {
		return imageCachePlatformCredential{}, fmt.Errorf("decode image-cache platform credential: %w", err)
	}
	if err := ensureImageCachePlatformJSONEOF(decoder); err != nil {
		return imageCachePlatformCredential{}, err
	}
	credential := envelope.Credential
	nodeID = strings.ToLower(strings.TrimSpace(nodeID))
	if credential.APIVersion != imageCachePlatformPlanCredentialVersion ||
		credential.Kind != imageCachePlatformPlanCredentialKind ||
		credential.Component != imageCachePlatformPlanComponent ||
		credential.NodeID != nodeID || credential.CredentialID != imageCachePlatformPlanComponent+":"+nodeID ||
		credential.ScopeKey != "node:"+nodeID ||
		len(credential.ArtifactKinds) != 1 || credential.ArtifactKinds[0] != imageCachePlatformPlanArtifactKind ||
		credential.TokenID == "" || len(credential.TokenID) > 256 ||
		len(credential.Token) > 8192 || !strings.HasPrefix(credential.Token, imageCachePlatformPlanCredentialPrefix) || strings.ContainsAny(credential.Token, " \t\r\n") {
		return imageCachePlatformCredential{}, errors.New("image-cache platform credential identity or capability is invalid")
	}
	if credential.IssuedAt.IsZero() || credential.ExpiresAt.IsZero() || credential.RenewAfter.IsZero() ||
		!credential.IssuedAt.Before(credential.RenewAfter) || !credential.RenewAfter.Before(credential.ExpiresAt) ||
		credential.IssuedAt.After(now.UTC().Add(30*time.Second)) ||
		credential.ExpiresAt.Sub(credential.IssuedAt) < 14*time.Minute+59*time.Second ||
		credential.ExpiresAt.Sub(credential.IssuedAt) > 15*time.Minute+time.Second ||
		credential.RenewAfter.Sub(credential.IssuedAt) < 4*time.Minute+59*time.Second ||
		credential.RenewAfter.Sub(credential.IssuedAt) > 5*time.Minute+time.Second ||
		!credential.ExpiresAt.After(now.UTC().Add(minimumValidity)) {
		return imageCachePlatformCredential{}, errors.New("image-cache platform credential timestamps are invalid or expired")
	}
	return credential, nil
}

func computeImageCachePlatformHeartbeatEvidenceHash(heartbeat imageCachePlatformHeartbeat) (string, error) {
	if strings.TrimSpace(heartbeat.ConsumerID) == "" || strings.TrimSpace(heartbeat.Component) == "" ||
		strings.TrimSpace(heartbeat.NodeID) == "" || heartbeat.ArtifactKind != imageCachePlatformPlanArtifactKind ||
		strings.TrimSpace(heartbeat.ScopeKey) == "" || heartbeat.IssuedAt.IsZero() {
		return "", errors.New("image-cache platform heartbeat identity is incomplete")
	}
	capabilities := normalizedImageCachePlatformStrings(heartbeat.CompatibilityCapabilities)
	canonical := struct {
		ConsumerID                string   `json:"consumer_id"`
		Component                 string   `json:"component"`
		NodeID                    string   `json:"node_id"`
		ArtifactKind              string   `json:"artifact_kind"`
		ScopeKey                  string   `json:"scope_key"`
		ReleaseSetID              string   `json:"release_set_id"`
		ExpectedConsumerSetID     string   `json:"expected_consumer_set_id"`
		FencingToken              int64    `json:"fencing_token"`
		ProtocolVersion           string   `json:"protocol_version"`
		SchemaVersion             string   `json:"schema_version"`
		CompatibilityCapabilities []string `json:"compatibility_capabilities"`
		Sequence                  int64    `json:"sequence"`
		IssuedAt                  string   `json:"issued_at"`
		Nonce                     string   `json:"nonce"`
		GenerationSequence        int64    `json:"generation_sequence"`
		DesiredGeneration         string   `json:"desired_generation"`
		ActualGeneration          string   `json:"actual_generation"`
		LKGGeneration             string   `json:"lkg_generation"`
		ApplyStatus               string   `json:"apply_status"`
		ProbeStatus               string   `json:"probe_status"`
		ServingLKG                bool     `json:"serving_lkg"`
		LKGExpired                bool     `json:"lkg_expired"`
		LastError                 string   `json:"last_error"`
	}{
		ConsumerID:                strings.TrimSpace(heartbeat.ConsumerID),
		Component:                 strings.ToLower(strings.TrimSpace(heartbeat.Component)),
		NodeID:                    strings.TrimSpace(heartbeat.NodeID),
		ArtifactKind:              strings.ToLower(strings.TrimSpace(heartbeat.ArtifactKind)),
		ScopeKey:                  strings.ToLower(strings.TrimSpace(heartbeat.ScopeKey)),
		ReleaseSetID:              strings.TrimSpace(heartbeat.ReleaseSetID),
		ExpectedConsumerSetID:     strings.TrimSpace(heartbeat.ExpectedConsumerSetID),
		FencingToken:              heartbeat.FencingToken,
		ProtocolVersion:           strings.ToLower(strings.TrimSpace(heartbeat.ProtocolVersion)),
		SchemaVersion:             strings.ToLower(strings.TrimSpace(heartbeat.SchemaVersion)),
		CompatibilityCapabilities: capabilities,
		Sequence:                  heartbeat.Sequence,
		IssuedAt:                  heartbeat.IssuedAt.UTC().Format(time.RFC3339Nano),
		Nonce:                     strings.TrimSpace(heartbeat.Nonce),
		GenerationSequence:        heartbeat.GenerationSequence,
		DesiredGeneration:         strings.TrimSpace(heartbeat.DesiredGeneration),
		ActualGeneration:          strings.TrimSpace(heartbeat.ActualGeneration),
		LKGGeneration:             strings.TrimSpace(heartbeat.LKGGeneration),
		ApplyStatus:               strings.ToLower(strings.TrimSpace(heartbeat.ApplyStatus)),
		ProbeStatus:               strings.ToLower(strings.TrimSpace(heartbeat.ProbeStatus)),
		ServingLKG:                heartbeat.ServingLKG,
		LKGExpired:                heartbeat.LKGExpired,
		LastError:                 strings.TrimSpace(heartbeat.LastError),
	}
	payload, err := json.Marshal(canonical)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func newImageCachePlatformPlanObservation(plan imageCachePlatformPlanResponse, rawPlan []byte, observedAt time.Time) imageCachePlatformPlanObservation {
	observation := imageCachePlatformPlanObservation{
		APIVersion:      imageCachePlatformPlanObservationVersion,
		Kind:            imageCachePlatformPlanObservationKind,
		ObservationOnly: true,
		NodeID:          plan.NodeID,
		ScopeKey:        plan.ScopeKey,
		ArtifactKind:    plan.ArtifactKind,
		ReleaseChannel:  plan.ReleaseChannel,
		Generation:      plan.Generation,
		ObservedAt:      observedAt.UTC(),
		Plan:            append(json.RawMessage(nil), rawPlan...),
	}
	if plan.Artifact != nil {
		observation.GenerationSequence = plan.Artifact.GenerationSequence
		observation.ContentHash = plan.Artifact.ContentHash
	}
	if plan.LKG != nil {
		observation.LKGGeneration = plan.LKG.Generation
	}
	return observation
}

func readImageCachePlatformPlanObservation(path, nodeID string) (imageCachePlatformPlanObservation, error) {
	body, err := readImageCachePlatformRegularFile(path, imageCachePlatformPlanMaxBytes+imageCachePlatformHeartbeatMaxBytes, false)
	if errors.Is(err, os.ErrNotExist) {
		return imageCachePlatformPlanObservation{}, nil
	}
	if err != nil {
		return imageCachePlatformPlanObservation{}, err
	}
	var observation imageCachePlatformPlanObservation
	if err := json.Unmarshal(body, &observation); err != nil {
		return imageCachePlatformPlanObservation{}, err
	}
	if observation.APIVersion != imageCachePlatformPlanObservationVersion ||
		observation.Kind != imageCachePlatformPlanObservationKind || !observation.ObservationOnly ||
		observation.NodeID != strings.ToLower(strings.TrimSpace(nodeID)) ||
		observation.ScopeKey != "node:"+strings.ToLower(strings.TrimSpace(nodeID)) ||
		observation.ArtifactKind != imageCachePlatformPlanArtifactKind ||
		observation.ReleaseChannel != imageCachePlatformPlanReleaseChannel ||
		observation.ObservedAt.IsZero() || len(observation.Plan) == 0 || !json.Valid(observation.Plan) {
		return imageCachePlatformPlanObservation{}, errors.New("persisted image replication plan observation is invalid")
	}
	if bytes.Contains(body, []byte(imageCachePlatformPlanCredentialPrefix)) {
		return imageCachePlatformPlanObservation{}, errors.New("persisted image replication plan observation contains credential material")
	}
	var plan imageCachePlatformPlanResponse
	if err := json.Unmarshal(observation.Plan, &plan); err != nil ||
		plan.NodeID != observation.NodeID || plan.ScopeKey != observation.ScopeKey ||
		plan.ArtifactKind != observation.ArtifactKind || plan.ReleaseChannel != observation.ReleaseChannel ||
		plan.Generation != observation.Generation {
		return imageCachePlatformPlanObservation{}, errors.New("persisted image replication plan payload does not match its observation envelope")
	}
	if plan.Artifact == nil {
		if observation.Generation != "" || observation.GenerationSequence != 0 || observation.ContentHash != "" {
			return imageCachePlatformPlanObservation{}, errors.New("persisted empty image replication plan has desired-state metadata")
		}
	} else if plan.Artifact.Generation != observation.Generation ||
		plan.Artifact.GenerationSequence != observation.GenerationSequence ||
		plan.Artifact.ContentHash != observation.ContentHash {
		return imageCachePlatformPlanObservation{}, errors.New("persisted image replication artifact does not match its observation envelope")
	}
	planLKGGeneration := ""
	if plan.LKG != nil {
		planLKGGeneration = plan.LKG.Generation
	}
	if planLKGGeneration != observation.LKGGeneration {
		return imageCachePlatformPlanObservation{}, errors.New("persisted image replication LKG does not match its observation envelope")
	}
	return observation, nil
}

func writeImageCachePlatformPlanObservation(path string, observation imageCachePlatformPlanObservation, archiveLimit int) error {
	data, err := json.MarshalIndent(observation, "", "  ")
	if err != nil {
		return err
	}
	if bytes.Contains(data, []byte(imageCachePlatformPlanCredentialPrefix)) {
		return errors.New("refusing to persist a platform bearer credential in image-cache state")
	}
	current, currentErr := readImageCachePlatformPlanObservation(path, observation.NodeID)
	if currentErr == nil && current.APIVersion != "" && current.Generation != observation.Generation {
		currentData, err := json.MarshalIndent(current, "", "  ")
		if err != nil {
			return err
		}
		if err := atomicWriteImageCachePlatformFile(path+".previous", currentData, 0o600); err != nil {
			return fmt.Errorf("preserve previous image replication plan observation: %w", err)
		}
	}
	archiveDir := path + ".versions"
	archiveName := sanitizeImageCachePlatformArchiveName(observation.Generation)
	if archiveName == "" {
		archiveName = "no-desired-" + observation.ObservedAt.UTC().Format("20060102T150405.000000000Z")
	}
	if hash := strings.TrimPrefix(observation.ContentHash, "sha256:"); len(hash) >= 16 {
		archiveName += "-" + hash[:16]
	}
	if err := atomicWriteImageCachePlatformFile(filepath.Join(archiveDir, archiveName+".json"), data, 0o600); err != nil {
		return fmt.Errorf("archive image replication plan observation: %w", err)
	}
	if err := atomicWriteImageCachePlatformFile(path, data, 0o600); err != nil {
		return err
	}
	pruneImageCachePlatformArchives(archiveDir, archiveLimit)
	return nil
}

func readImageCachePlatformRegularFile(path string, maxBytes int64, secret bool) ([]byte, error) {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, errors.New("image-cache platform file path must be absolute and canonical")
	}
	before, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 || before.Size() < 0 || before.Size() > maxBytes {
		return nil, errors.New("image-cache platform file is not a bounded regular file")
	}
	if secret && before.Mode().Perm()&0o027 != 0 {
		return nil, fmt.Errorf("image-cache platform credential permissions %o are too broad", before.Mode().Perm())
	}
	handle, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer handle.Close()
	after, err := handle.Stat()
	if err != nil || !after.Mode().IsRegular() || !os.SameFile(before, after) {
		return nil, errors.New("image-cache platform file identity changed while opening")
	}
	if secret && after.Mode().Perm()&0o027 != 0 {
		return nil, fmt.Errorf("image-cache platform credential permissions %o changed or are too broad", after.Mode().Perm())
	}
	body, err := io.ReadAll(io.LimitReader(handle, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > maxBytes {
		return nil, errors.New("image-cache platform file exceeds its size limit")
	}
	return body, nil
}

func atomicWriteImageCachePlatformFile(path string, data []byte, mode os.FileMode) error {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return errors.New("image-cache platform state path must be absolute and canonical")
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	if info, err := os.Lstat(dir); err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("image-cache platform state directory is not a real directory")
	}
	if info, err := os.Lstat(path); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return errors.New("image-cache platform state path is a symlink")
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	temporary, err := os.CreateTemp(dir, "."+filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Chmod(mode); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return err
	}
	if directory, err := os.Open(dir); err == nil {
		_ = directory.Sync()
		_ = directory.Close()
	}
	return nil
}

func pruneImageCachePlatformArchives(dir string, limit int) {
	if limit < 1 {
		return
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	type archive struct {
		path string
		time time.Time
	}
	archives := make([]archive, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		archives = append(archives, archive{path: filepath.Join(dir, entry.Name()), time: info.ModTime()})
	}
	sort.Slice(archives, func(i, j int) bool {
		if !archives[i].time.Equal(archives[j].time) {
			return archives[i].time.After(archives[j].time)
		}
		return archives[i].path > archives[j].path
	})
	for index := limit; index < len(archives); index++ {
		_ = os.Remove(archives[index].path)
	}
}

func sanitizeImageCachePlatformArchiveName(value string) string {
	value = strings.TrimSpace(value)
	var builder strings.Builder
	for _, char := range value {
		switch {
		case char >= 'a' && char <= 'z', char >= 'A' && char <= 'Z', char >= '0' && char <= '9', char == '-', char == '_', char == '.':
			builder.WriteRune(char)
		default:
			builder.WriteByte('_')
		}
		if builder.Len() >= 128 {
			break
		}
	}
	return strings.Trim(builder.String(), "._")
}

func readImageCachePlatformHTTPBody(response *http.Response, maxBytes int64) ([]byte, error) {
	if response == nil || response.Body == nil {
		return nil, errors.New("image-cache platform API returned an empty response")
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > maxBytes {
		return nil, errors.New("image-cache platform API response exceeds its size limit")
	}
	return body, nil
}

func imageCachePlatformResponseError(status int, body []byte, token string) error {
	message := "request failed"
	var response struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(body, &response) == nil && strings.TrimSpace(response.Error) != "" {
		message = strings.TrimSpace(response.Error)
	}
	if token != "" {
		message = strings.ReplaceAll(message, token, "[redacted]")
	}
	message = truncateImageCachePlatformValue(message, imageCachePlatformPlanErrorMaxBytes)
	return &imageCachePlatformHTTPError{Status: status, Message: message}
}

func setImageCachePlatformRequestHeaders(request *http.Request, token string) {
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("User-Agent", "fugue-image-cache/image-plane-v1")
}

func ensureImageCachePlatformJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("image-cache platform JSON must contain exactly one document")
	}
	return nil
}

func normalizedImageCachePlatformStrings(values []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func imageCachePlatformBackoff(minimum, maximum time.Duration, failures int) time.Duration {
	if failures <= 1 {
		return minimum
	}
	delay := minimum
	for attempt := 1; attempt < failures && delay < maximum; attempt++ {
		if delay > maximum/2 {
			return maximum
		}
		delay *= 2
	}
	if delay > maximum {
		return maximum
	}
	return delay
}

func waitImageCachePlatformPlan(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func truncateImageCachePlatformValue(value string, maxBytes int) string {
	value = strings.TrimSpace(value)
	if maxBytes <= 0 || len(value) <= maxBytes {
		return value
	}
	return strings.TrimSpace(value[:maxBytes])
}

func boundedImageCachePlatformError(err error) string {
	if err == nil {
		return ""
	}
	return truncateImageCachePlatformValue(err.Error(), imageCachePlatformPlanErrorMaxBytes)
}

func (c *imageCachePlatformPlanConsumer) Status() imageCachePlatformPlanStatus {
	if c == nil {
		return imageCachePlatformPlanStatus{Enabled: false, ObservationOnly: true, State: "disabled"}
	}
	c.statusMu.RLock()
	defer c.statusMu.RUnlock()
	return c.status
}

func (c *imageCachePlatformPlanConsumer) updateCredentialStatus(expiresAt time.Time) {
	c.statusMu.Lock()
	defer c.statusMu.Unlock()
	expiresAt = expiresAt.UTC()
	c.status.CredentialExpiresAt = &expiresAt
	c.status.State = "credential_ready"
	c.status.LastError = ""
}

func (c *imageCachePlatformPlanConsumer) updateFailureStatus(message string) {
	c.statusMu.Lock()
	defer c.statusMu.Unlock()
	c.status.State = "degraded"
	c.status.LastError = truncateImageCachePlatformValue(message, imageCachePlatformPlanErrorMaxBytes)
}

func (c *imageCachePlatformPlanConsumer) updateObservationStatus(cycle imageCachePlatformPlanCycle, observedAt time.Time, heartbeatAt *time.Time) {
	c.statusMu.Lock()
	defer c.statusMu.Unlock()
	observedAt = observedAt.UTC()
	c.status.State = "observed"
	c.status.Generation = cycle.Generation
	c.status.LKGGeneration = cycle.LKGGeneration
	c.status.LastError = ""
	c.status.LastObservationAt = &observedAt
	if heartbeatAt != nil {
		value := heartbeatAt.UTC()
		c.status.LastHeartbeatAt = &value
	}
}
