package platformcontrol

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

const (
	platformComponentIdentityTokenPrefix = "fugue_pc_v1."
	platformComponentIdentityVersion     = "v1"
	platformComponentIdentityKeyContext  = "fugue/platform-component-identity/v1"

	PlatformComponentIdentityMaxTTL      = 15 * time.Minute
	PlatformComponentIdentityFutureSkew  = 30 * time.Second
	PlatformConsumerHeartbeatMaxAge      = 2 * time.Minute
	PlatformConsumerHeartbeatFutureSkew  = 30 * time.Second
	PlatformConsumerHeartbeatMinNonceLen = 16
)

var (
	ErrPlatformComponentIdentityInvalid        = errors.New("invalid platform component identity")
	ErrPlatformComponentIdentityExpired        = errors.New("expired platform component identity")
	ErrPlatformConsumerHeartbeatInvalid        = errors.New("invalid platform consumer heartbeat")
	ErrPlatformConsumerHeartbeatImpersonation  = errors.New("platform consumer heartbeat identity mismatch")
	ErrPlatformConsumerHeartbeatReplay         = errors.New("platform consumer heartbeat replay")
	ErrPlatformConsumerHeartbeatStale          = errors.New("stale platform consumer heartbeat")
	ErrPlatformConsumerHeartbeatFuture         = errors.New("future platform consumer heartbeat")
	ErrPlatformConsumerHeartbeatGenerationBack = errors.New("platform consumer heartbeat generation rollback")
	ErrPlatformConsumerHeartbeatFencingBack    = errors.New("platform consumer heartbeat fencing rollback")
	ErrPlatformConsumerHeartbeatEvidence       = errors.New("platform consumer heartbeat evidence hash mismatch")
	ErrPlatformConsumerHeartbeatExpectation    = errors.New("platform consumer heartbeat expected consumer mismatch")
)

type PlatformComponentIdentityKeyring struct {
	ActiveKeyID   string
	Keys          map[string]string
	RevokedKeyIDs map[string]struct{}
}

type PlatformComponentIdentityClaims struct {
	Version       string   `json:"v"`
	CredentialID  string   `json:"credential_id"`
	TokenID       string   `json:"token_id"`
	Component     string   `json:"component"`
	NodeID        string   `json:"node_id"`
	ScopeKey      string   `json:"scope_key"`
	ArtifactKinds []string `json:"artifact_kinds"`
	IssuedAtUnix  int64    `json:"issued_at"`
	ExpiresAtUnix int64    `json:"expires_at"`
}

type PlatformConsumerHeartbeatEnvelope struct {
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

type PlatformConsumerHeartbeatCursor struct {
	Sequence           int64
	IssuedAt           time.Time
	GenerationSequence int64
	FencingToken       int64
	RecentNonces       []string
}

type PlatformConsumerHeartbeatValidationPolicy struct {
	MaxAge      time.Duration
	FutureSkew  time.Duration
	MinNonceLen int
}

func DerivePlatformComponentIdentityKeyring(
	activeKey string,
	activeKeyID string,
	previousKey string,
	previousKeyID string,
	revokedKeyIDs []string,
) PlatformComponentIdentityKeyring {
	keyring := PlatformComponentIdentityKeyring{
		Keys:          map[string]string{},
		RevokedKeyIDs: map[string]struct{}{},
	}
	for _, keyID := range revokedKeyIDs {
		keyID = strings.TrimSpace(keyID)
		if keyID != "" {
			keyring.RevokedKeyIDs[keyID] = struct{}{}
		}
	}
	addKey := func(keyID, key string, active bool) {
		keyID = strings.TrimSpace(keyID)
		key = strings.TrimSpace(key)
		if keyID == "" || key == "" || keyring.isRevoked(keyID) {
			return
		}
		keyring.Keys[keyID] = derivePlatformComponentIdentityKey(key)
		if active {
			keyring.ActiveKeyID = keyID
		}
	}
	addKey(previousKeyID, previousKey, false)
	addKey(activeKeyID, activeKey, true)
	return keyring
}

func IssuePlatformComponentIdentity(
	keyring PlatformComponentIdentityKeyring,
	claims PlatformComponentIdentityClaims,
	now time.Time,
	ttl time.Duration,
) (string, error) {
	now = normalizedPlatformIdentityTime(now)
	if ttl <= 0 || ttl > PlatformComponentIdentityMaxTTL {
		return "", ErrPlatformComponentIdentityInvalid
	}
	keyID := strings.TrimSpace(keyring.ActiveKeyID)
	secret := strings.TrimSpace(keyring.Keys[keyID])
	if keyID == "" || secret == "" || keyring.isRevoked(keyID) {
		return "", ErrPlatformComponentIdentityInvalid
	}
	claims.Version = platformComponentIdentityVersion
	claims.IssuedAtUnix = now.Unix()
	claims.ExpiresAtUnix = now.Add(ttl).Unix()
	tokenID, err := randomPlatformComponentTokenID()
	if err != nil {
		return "", err
	}
	claims.TokenID = tokenID
	claims, err = normalizePlatformComponentIdentityClaims(claims)
	if err != nil {
		return "", err
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", fmt.Errorf("marshal platform component identity: %w", err)
	}
	keyIDEncoded := base64.RawURLEncoding.EncodeToString([]byte(keyID))
	payloadEncoded := base64.RawURLEncoding.EncodeToString(payload)
	signingInput := platformComponentIdentityVersion + "." + keyIDEncoded + "." + payloadEncoded
	signature := signPlatformComponentIdentity(secret, signingInput)
	return platformComponentIdentityTokenPrefix + keyIDEncoded + "." + payloadEncoded + "." + signature, nil
}

func ParsePlatformComponentIdentity(
	keyring PlatformComponentIdentityKeyring,
	token string,
	now time.Time,
) (PlatformComponentIdentityClaims, error) {
	token = strings.TrimSpace(token)
	if !strings.HasPrefix(token, platformComponentIdentityTokenPrefix) {
		return PlatformComponentIdentityClaims{}, ErrPlatformComponentIdentityInvalid
	}
	parts := strings.Split(strings.TrimPrefix(token, platformComponentIdentityTokenPrefix), ".")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return PlatformComponentIdentityClaims{}, ErrPlatformComponentIdentityInvalid
	}
	keyIDBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return PlatformComponentIdentityClaims{}, ErrPlatformComponentIdentityInvalid
	}
	keyID := strings.TrimSpace(string(keyIDBytes))
	secret := strings.TrimSpace(keyring.Keys[keyID])
	if keyID == "" || secret == "" || keyring.isRevoked(keyID) {
		return PlatformComponentIdentityClaims{}, ErrPlatformComponentIdentityInvalid
	}
	signingInput := platformComponentIdentityVersion + "." + parts[0] + "." + parts[1]
	expectedSignature := signPlatformComponentIdentity(secret, signingInput)
	if !hmac.Equal([]byte(expectedSignature), []byte(parts[2])) {
		return PlatformComponentIdentityClaims{}, ErrPlatformComponentIdentityInvalid
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return PlatformComponentIdentityClaims{}, ErrPlatformComponentIdentityInvalid
	}
	var claims PlatformComponentIdentityClaims
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&claims); err != nil {
		return PlatformComponentIdentityClaims{}, ErrPlatformComponentIdentityInvalid
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return PlatformComponentIdentityClaims{}, ErrPlatformComponentIdentityInvalid
	}
	claims, err = normalizePlatformComponentIdentityClaims(claims)
	if err != nil {
		return PlatformComponentIdentityClaims{}, err
	}
	now = normalizedPlatformIdentityTime(now)
	issuedAt := time.Unix(claims.IssuedAtUnix, 0).UTC()
	expiresAt := time.Unix(claims.ExpiresAtUnix, 0).UTC()
	if expiresAt.After(issuedAt.Add(PlatformComponentIdentityMaxTTL)) || !expiresAt.After(issuedAt) {
		return PlatformComponentIdentityClaims{}, ErrPlatformComponentIdentityInvalid
	}
	if issuedAt.After(now.Add(PlatformComponentIdentityFutureSkew)) {
		return PlatformComponentIdentityClaims{}, ErrPlatformComponentIdentityInvalid
	}
	if !expiresAt.After(now) {
		return PlatformComponentIdentityClaims{}, ErrPlatformComponentIdentityExpired
	}
	return claims, nil
}

func BindPlatformConsumerHeartbeat(
	claims PlatformComponentIdentityClaims,
	heartbeat PlatformConsumerHeartbeatEnvelope,
) (PlatformConsumerHeartbeatEnvelope, error) {
	claims, err := normalizePlatformComponentIdentityClaims(claims)
	if err != nil {
		return PlatformConsumerHeartbeatEnvelope{}, err
	}
	heartbeat.ArtifactKind = normalizeExpectedConsumerArtifactKind(heartbeat.ArtifactKind)
	heartbeat.ScopeKey = strings.TrimSpace(strings.ToLower(heartbeat.ScopeKey))
	if heartbeat.ArtifactKind == "" {
		return PlatformConsumerHeartbeatEnvelope{}, ErrPlatformConsumerHeartbeatInvalid
	}
	if !containsStringFold(claims.ArtifactKinds, heartbeat.ArtifactKind) {
		return PlatformConsumerHeartbeatEnvelope{}, ErrPlatformConsumerHeartbeatImpersonation
	}
	expectedConsumerID := claims.Component + ":" + claims.NodeID
	identityClaims := []struct {
		actual   string
		expected string
		fold     bool
	}{
		{actual: strings.TrimSpace(heartbeat.ConsumerID), expected: expectedConsumerID},
		{actual: strings.TrimSpace(heartbeat.Component), expected: claims.Component, fold: true},
		{actual: strings.TrimSpace(heartbeat.NodeID), expected: claims.NodeID},
		{actual: heartbeat.ScopeKey, expected: claims.ScopeKey, fold: true},
	}
	for _, claim := range identityClaims {
		actual, expected := claim.actual, claim.expected
		matches := actual == expected
		if claim.fold {
			matches = strings.EqualFold(actual, expected)
		}
		if actual != "" && !matches {
			return PlatformConsumerHeartbeatEnvelope{}, ErrPlatformConsumerHeartbeatImpersonation
		}
	}
	heartbeat.ConsumerID = expectedConsumerID
	heartbeat.Component = claims.Component
	heartbeat.NodeID = claims.NodeID
	heartbeat.ScopeKey = claims.ScopeKey
	heartbeat.ReleaseSetID = strings.TrimSpace(heartbeat.ReleaseSetID)
	heartbeat.ExpectedConsumerSetID = strings.TrimSpace(heartbeat.ExpectedConsumerSetID)
	heartbeat.ProtocolVersion = strings.TrimSpace(strings.ToLower(heartbeat.ProtocolVersion))
	heartbeat.SchemaVersion = strings.TrimSpace(strings.ToLower(heartbeat.SchemaVersion))
	heartbeat.CompatibilityCapabilities = normalizedPlatformIdentityStrings(heartbeat.CompatibilityCapabilities)
	heartbeat.Nonce = strings.TrimSpace(heartbeat.Nonce)
	heartbeat.DesiredGeneration = strings.TrimSpace(heartbeat.DesiredGeneration)
	heartbeat.ActualGeneration = strings.TrimSpace(heartbeat.ActualGeneration)
	heartbeat.LKGGeneration = strings.TrimSpace(heartbeat.LKGGeneration)
	heartbeat.ApplyStatus = strings.TrimSpace(strings.ToLower(heartbeat.ApplyStatus))
	heartbeat.ProbeStatus = strings.TrimSpace(strings.ToLower(heartbeat.ProbeStatus))
	heartbeat.LastError = strings.TrimSpace(heartbeat.LastError)
	heartbeat.EvidenceHash = strings.TrimSpace(strings.ToLower(heartbeat.EvidenceHash))
	if !heartbeat.IssuedAt.IsZero() {
		heartbeat.IssuedAt = heartbeat.IssuedAt.UTC()
	}
	return heartbeat, nil
}

func BindPlatformConsumerHeartbeatToExpectedSet(
	claims PlatformComponentIdentityClaims,
	set model.PlatformExpectedConsumerSet,
	heartbeat PlatformConsumerHeartbeatEnvelope,
) (PlatformConsumerHeartbeatEnvelope, error) {
	setID := strings.TrimSpace(set.ID)
	releaseSetID := strings.TrimSpace(set.ReleaseSetID)
	expectedGeneration := strings.TrimSpace(set.ExpectedGeneration)
	artifactKind := normalizeExpectedConsumerArtifactKind(set.ArtifactKind)
	scopeKey := strings.TrimSpace(strings.ToLower(set.ScopeKey))
	if setID == "" || releaseSetID == "" || expectedGeneration == "" || artifactKind == "" || scopeKey == "" {
		return PlatformConsumerHeartbeatEnvelope{}, ErrPlatformConsumerHeartbeatExpectation
	}

	if value := strings.TrimSpace(heartbeat.ExpectedConsumerSetID); value != "" && value != setID {
		return PlatformConsumerHeartbeatEnvelope{}, ErrPlatformConsumerHeartbeatExpectation
	}
	if value := strings.TrimSpace(heartbeat.ReleaseSetID); value != "" && value != releaseSetID {
		return PlatformConsumerHeartbeatEnvelope{}, ErrPlatformConsumerHeartbeatExpectation
	}
	if value := strings.TrimSpace(heartbeat.DesiredGeneration); value != "" && value != expectedGeneration {
		return PlatformConsumerHeartbeatEnvelope{}, ErrPlatformConsumerHeartbeatExpectation
	}
	if raw := strings.TrimSpace(heartbeat.ArtifactKind); raw != "" {
		if value := normalizeExpectedConsumerArtifactKind(raw); value == "" || value != artifactKind {
			return PlatformConsumerHeartbeatEnvelope{}, ErrPlatformConsumerHeartbeatExpectation
		}
	}
	if value := strings.TrimSpace(strings.ToLower(heartbeat.ScopeKey)); value != "" && value != scopeKey {
		return PlatformConsumerHeartbeatEnvelope{}, ErrPlatformConsumerHeartbeatExpectation
	}

	heartbeat.ExpectedConsumerSetID = setID
	heartbeat.ReleaseSetID = releaseSetID
	heartbeat.DesiredGeneration = expectedGeneration
	heartbeat.ArtifactKind = artifactKind
	heartbeat.ScopeKey = scopeKey
	bound, err := BindPlatformConsumerHeartbeat(claims, heartbeat)
	if err != nil {
		return PlatformConsumerHeartbeatEnvelope{}, err
	}

	matched := 0
	for _, expected := range set.Consumers {
		if strings.TrimSpace(expected.ConsumerID) != bound.ConsumerID {
			continue
		}
		if strings.TrimSpace(strings.ToLower(expected.Component)) != bound.Component ||
			strings.TrimSpace(expected.NodeID) != bound.NodeID ||
			normalizeExpectedConsumerArtifactKind(expected.ArtifactKind) != bound.ArtifactKind ||
			strings.TrimSpace(strings.ToLower(expected.ScopeKey)) != bound.ScopeKey ||
			strings.TrimSpace(expected.ExpectedGeneration) != expectedGeneration {
			return PlatformConsumerHeartbeatEnvelope{}, ErrPlatformConsumerHeartbeatExpectation
		}
		matched++
	}
	if matched != 1 {
		return PlatformConsumerHeartbeatEnvelope{}, ErrPlatformConsumerHeartbeatImpersonation
	}
	return bound, nil
}

func ComputePlatformConsumerHeartbeatEvidenceHash(heartbeat PlatformConsumerHeartbeatEnvelope) (string, error) {
	if strings.TrimSpace(heartbeat.ConsumerID) == "" ||
		strings.TrimSpace(heartbeat.Component) == "" ||
		strings.TrimSpace(heartbeat.NodeID) == "" ||
		normalizeExpectedConsumerArtifactKind(heartbeat.ArtifactKind) == "" ||
		strings.TrimSpace(heartbeat.ScopeKey) == "" {
		return "", ErrPlatformConsumerHeartbeatInvalid
	}
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
		Component:                 strings.TrimSpace(heartbeat.Component),
		NodeID:                    strings.TrimSpace(heartbeat.NodeID),
		ArtifactKind:              normalizeExpectedConsumerArtifactKind(heartbeat.ArtifactKind),
		ScopeKey:                  strings.TrimSpace(strings.ToLower(heartbeat.ScopeKey)),
		ReleaseSetID:              strings.TrimSpace(heartbeat.ReleaseSetID),
		ExpectedConsumerSetID:     strings.TrimSpace(heartbeat.ExpectedConsumerSetID),
		FencingToken:              heartbeat.FencingToken,
		ProtocolVersion:           strings.TrimSpace(strings.ToLower(heartbeat.ProtocolVersion)),
		SchemaVersion:             strings.TrimSpace(strings.ToLower(heartbeat.SchemaVersion)),
		CompatibilityCapabilities: normalizedPlatformIdentityStrings(heartbeat.CompatibilityCapabilities),
		Sequence:                  heartbeat.Sequence,
		IssuedAt:                  heartbeat.IssuedAt.UTC().Format(time.RFC3339Nano),
		Nonce:                     strings.TrimSpace(heartbeat.Nonce),
		GenerationSequence:        heartbeat.GenerationSequence,
		DesiredGeneration:         strings.TrimSpace(heartbeat.DesiredGeneration),
		ActualGeneration:          strings.TrimSpace(heartbeat.ActualGeneration),
		LKGGeneration:             strings.TrimSpace(heartbeat.LKGGeneration),
		ApplyStatus:               strings.TrimSpace(strings.ToLower(heartbeat.ApplyStatus)),
		ProbeStatus:               strings.TrimSpace(strings.ToLower(heartbeat.ProbeStatus)),
		ServingLKG:                heartbeat.ServingLKG,
		LKGExpired:                heartbeat.LKGExpired,
		LastError:                 strings.TrimSpace(heartbeat.LastError),
	}
	payload, err := json.Marshal(canonical)
	if err != nil {
		return "", fmt.Errorf("marshal platform consumer heartbeat evidence: %w", err)
	}
	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func ValidatePlatformConsumerHeartbeat(
	heartbeat PlatformConsumerHeartbeatEnvelope,
	previous *PlatformConsumerHeartbeatCursor,
	now time.Time,
	policy PlatformConsumerHeartbeatValidationPolicy,
) error {
	policy = normalizedPlatformConsumerHeartbeatPolicy(policy)
	now = normalizedPlatformIdentityTime(now)
	if strings.TrimSpace(heartbeat.ReleaseSetID) == "" ||
		heartbeat.FencingToken <= 0 ||
		heartbeat.Sequence <= 0 ||
		heartbeat.GenerationSequence <= 0 ||
		heartbeat.IssuedAt.IsZero() ||
		len([]byte(strings.TrimSpace(heartbeat.Nonce))) < policy.MinNonceLen ||
		!strings.EqualFold(strings.TrimSpace(heartbeat.ProtocolVersion), model.PlatformConsumerProtocolVersionV1) ||
		!strings.EqualFold(strings.TrimSpace(heartbeat.SchemaVersion), model.PlatformConsumerSchemaVersionV1) {
		return ErrPlatformConsumerHeartbeatInvalid
	}
	issuedAt := heartbeat.IssuedAt.UTC()
	if issuedAt.After(now.Add(policy.FutureSkew)) {
		return ErrPlatformConsumerHeartbeatFuture
	}
	if issuedAt.Before(now.Add(-policy.MaxAge)) {
		return ErrPlatformConsumerHeartbeatStale
	}
	expectedHash, err := ComputePlatformConsumerHeartbeatEvidenceHash(heartbeat)
	if err != nil {
		return err
	}
	if !hmac.Equal([]byte(expectedHash), []byte(strings.TrimSpace(strings.ToLower(heartbeat.EvidenceHash)))) {
		return ErrPlatformConsumerHeartbeatEvidence
	}
	if previous == nil {
		return nil
	}
	if heartbeat.Sequence <= previous.Sequence ||
		issuedAt.Before(previous.IssuedAt.UTC()) ||
		containsExactString(previous.RecentNonces, heartbeat.Nonce) {
		return ErrPlatformConsumerHeartbeatReplay
	}
	if heartbeat.GenerationSequence < previous.GenerationSequence {
		return ErrPlatformConsumerHeartbeatGenerationBack
	}
	if heartbeat.FencingToken < previous.FencingToken {
		return ErrPlatformConsumerHeartbeatFencingBack
	}
	return nil
}

func VerifyTrustedPlatformConsumerHeartbeat(
	claims PlatformComponentIdentityClaims,
	set model.PlatformExpectedConsumerSet,
	heartbeat PlatformConsumerHeartbeatEnvelope,
	previous *PlatformConsumerHeartbeatCursor,
	receivedAt time.Time,
	policy PlatformConsumerHeartbeatValidationPolicy,
) (model.PlatformConsumerInstance, PlatformConsumerHeartbeatCursor, error) {
	claims, err := normalizePlatformComponentIdentityClaims(claims)
	if err != nil {
		return model.PlatformConsumerInstance{}, PlatformConsumerHeartbeatCursor{}, err
	}
	bound, err := BindPlatformConsumerHeartbeatToExpectedSet(claims, set, heartbeat)
	if err != nil {
		return model.PlatformConsumerInstance{}, PlatformConsumerHeartbeatCursor{}, err
	}
	receivedAt = normalizedPlatformIdentityTime(receivedAt)
	if err := ValidatePlatformConsumerHeartbeat(bound, previous, receivedAt, policy); err != nil {
		return model.PlatformConsumerInstance{}, PlatformConsumerHeartbeatCursor{}, err
	}
	issuedAt := bound.IssuedAt.UTC()
	consumer := model.PlatformConsumerInstance{
		ConsumerID:                bound.ConsumerID,
		CredentialID:              claims.CredentialID,
		TokenID:                   claims.TokenID,
		Component:                 bound.Component,
		NodeID:                    bound.NodeID,
		ArtifactKind:              bound.ArtifactKind,
		ScopeKey:                  bound.ScopeKey,
		ReleaseSetID:              bound.ReleaseSetID,
		ExpectedConsumerSetID:     bound.ExpectedConsumerSetID,
		FencingToken:              bound.FencingToken,
		SupportedKinds:            append([]string(nil), claims.ArtifactKinds...),
		ProtocolVersion:           bound.ProtocolVersion,
		SchemaVersion:             bound.SchemaVersion,
		CompatibilityCapabilities: append([]string(nil), bound.CompatibilityCapabilities...),
		Sequence:                  bound.Sequence,
		IssuedAt:                  &issuedAt,
		Nonce:                     bound.Nonce,
		GenerationSequence:        bound.GenerationSequence,
		EvidenceHash:              bound.EvidenceHash,
		IdentityVerified:          true,
		DesiredGeneration:         bound.DesiredGeneration,
		ActualGeneration:          bound.ActualGeneration,
		LKGGeneration:             bound.LKGGeneration,
		ApplyStatus:               bound.ApplyStatus,
		ProbeStatus:               bound.ProbeStatus,
		ServingLKG:                bound.ServingLKG,
		LKGExpired:                bound.LKGExpired,
		LastError:                 bound.LastError,
		LastHeartbeatAt:           receivedAt,
		UpdatedAt:                 receivedAt,
	}
	return consumer, AdvancePlatformConsumerHeartbeatCursor(previous, bound, 32), nil
}

func AdvancePlatformConsumerHeartbeatCursor(
	previous *PlatformConsumerHeartbeatCursor,
	heartbeat PlatformConsumerHeartbeatEnvelope,
	nonceHistory int,
) PlatformConsumerHeartbeatCursor {
	if nonceHistory <= 0 {
		nonceHistory = 32
	}
	nonces := []string{}
	if previous != nil {
		nonces = append(nonces, previous.RecentNonces...)
	}
	nonce := strings.TrimSpace(heartbeat.Nonce)
	if nonce != "" && !containsExactString(nonces, nonce) {
		nonces = append(nonces, nonce)
	}
	if len(nonces) > nonceHistory {
		nonces = append([]string(nil), nonces[len(nonces)-nonceHistory:]...)
	}
	return PlatformConsumerHeartbeatCursor{
		Sequence:           heartbeat.Sequence,
		IssuedAt:           heartbeat.IssuedAt.UTC(),
		GenerationSequence: heartbeat.GenerationSequence,
		FencingToken:       heartbeat.FencingToken,
		RecentNonces:       nonces,
	}
}

func normalizePlatformComponentIdentityClaims(claims PlatformComponentIdentityClaims) (PlatformComponentIdentityClaims, error) {
	claims.Version = strings.TrimSpace(strings.ToLower(claims.Version))
	claims.CredentialID = strings.TrimSpace(claims.CredentialID)
	claims.TokenID = strings.TrimSpace(claims.TokenID)
	claims.Component = strings.TrimSpace(strings.ToLower(claims.Component))
	claims.NodeID = strings.TrimSpace(claims.NodeID)
	claims.ScopeKey = strings.TrimSpace(strings.ToLower(claims.ScopeKey))
	claims.ArtifactKinds = normalizedPlatformIdentityArtifactKinds(claims.ArtifactKinds)
	if claims.Version != platformComponentIdentityVersion ||
		claims.CredentialID == "" ||
		claims.TokenID == "" ||
		!knownPlatformConsumerComponent(claims.Component) ||
		claims.NodeID == "" ||
		claims.ScopeKey == "" ||
		len(claims.ArtifactKinds) == 0 ||
		claims.IssuedAtUnix <= 0 ||
		claims.ExpiresAtUnix <= 0 {
		return PlatformComponentIdentityClaims{}, ErrPlatformComponentIdentityInvalid
	}
	return claims, nil
}

func knownPlatformConsumerComponent(component string) bool {
	switch strings.TrimSpace(strings.ToLower(component)) {
	case model.PlatformConsumerComponentEdgeWorker,
		model.PlatformConsumerComponentDNSServer,
		model.PlatformConsumerComponentCaddyEdgeFront,
		model.PlatformConsumerComponentNodeUpdater,
		model.PlatformConsumerComponentNodeGuardian,
		model.PlatformConsumerComponentRuntimeAgent:
		return true
	default:
		return false
	}
}

func normalizedPlatformIdentityArtifactKinds(values []string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		value = normalizeExpectedConsumerArtifactKind(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func normalizedPlatformIdentityStrings(values []string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(strings.ToLower(value))
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func normalizedPlatformConsumerHeartbeatPolicy(policy PlatformConsumerHeartbeatValidationPolicy) PlatformConsumerHeartbeatValidationPolicy {
	if policy.MaxAge <= 0 {
		policy.MaxAge = PlatformConsumerHeartbeatMaxAge
	}
	if policy.FutureSkew <= 0 {
		policy.FutureSkew = PlatformConsumerHeartbeatFutureSkew
	}
	if policy.MinNonceLen <= 0 {
		policy.MinNonceLen = PlatformConsumerHeartbeatMinNonceLen
	}
	return policy
}

func containsExactString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func normalizedPlatformIdentityTime(value time.Time) time.Time {
	if value.IsZero() {
		return time.Now().UTC()
	}
	return value.UTC()
}

func randomPlatformComponentTokenID() (string, error) {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate platform component token id: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func signPlatformComponentIdentity(secret, signingInput string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(signingInput))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func derivePlatformComponentIdentityKey(source string) string {
	mac := hmac.New(sha256.New, []byte(strings.TrimSpace(source)))
	_, _ = mac.Write([]byte(platformComponentIdentityKeyContext))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func (keyring PlatformComponentIdentityKeyring) isRevoked(keyID string) bool {
	_, revoked := keyring.RevokedKeyIDs[strings.TrimSpace(keyID)]
	return revoked
}
