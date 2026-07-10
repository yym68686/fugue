package platformsafety

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
)

const (
	InvariantArtifactValidated          = "artifact.validated"
	InvariantArtifactSchema             = "artifact.schema"
	InvariantArtifactContentHash        = "artifact.content_hash"
	InvariantArtifactSignature          = "artifact.signature"
	InvariantGenerationMonotonic        = "generation.monotonic"
	InvariantShadowNoProductionImpact   = "shadow.no_production_impact"
	InvariantCanaryScopeIsolation       = "canary.scope_isolation"
	InvariantBlastRadiusHardCap         = "action.blast_radius_hard_cap"
	InvariantKillSwitchPrecedence       = "action.kill_switch_precedence"
	InvariantFullPinnedRollback         = "full.pinned_rollback"
	InvariantFencingTokenCurrent        = "release.fencing_token_current"
	InvariantVerificationEvidencePassed = "lkg.verification_evidence_passed"
	InvariantLKGNotExpired              = "lkg.not_expired"
	InvariantLKGContentIntegrity        = "lkg.content_integrity"
	InvariantLKGSignature               = "lkg.signature"

	KernelBreakGlassConfirmation = "BYPASS_PLATFORM_SAFETY_KERNEL"
	PlatformSafetyAuditChainID   = "platform-safety"
	KernelBreakGlassMaxTTL       = 15 * time.Minute
)

var immutableInvariantIDs = []string{
	InvariantArtifactValidated,
	InvariantArtifactSchema,
	InvariantArtifactContentHash,
	InvariantArtifactSignature,
	InvariantGenerationMonotonic,
	InvariantShadowNoProductionImpact,
	InvariantCanaryScopeIsolation,
	InvariantBlastRadiusHardCap,
	InvariantKillSwitchPrecedence,
	InvariantFullPinnedRollback,
	InvariantFencingTokenCurrent,
	InvariantVerificationEvidencePassed,
	InvariantLKGNotExpired,
	InvariantLKGContentIntegrity,
	InvariantLKGSignature,
}

var (
	ErrPlatformSigningKeyUnavailable = errors.New("platform signing key unavailable")
	ErrPlatformSignatureInvalid      = errors.New("platform signature invalid")
)

type Violation struct {
	Invariant string
	Message   string
}

type Decision struct {
	Pass       bool
	Violations []Violation
	Bypassed   []Violation
}

func ImmutableInvariantIDs() []string {
	return append([]string(nil), immutableInvariantIDs...)
}

func ReleaseLaneKey(kind, scopeKey, channel string) string {
	return strings.Join([]string{
		strings.TrimSpace(strings.ToLower(kind)),
		strings.TrimSpace(strings.ToLower(scopeKey)),
		strings.TrimSpace(strings.ToLower(channel)),
	}, "|")
}

func EvaluateArtifactRelease(
	artifact model.PlatformArtifact,
	channel string,
	pinnedRollbackGeneration string,
	canaryRuleRef string,
	previousGenerationSequence int64,
	keyring bundleauth.Keyring,
) Decision {
	return evaluateArtifactPublication(
		artifact,
		channel,
		pinnedRollbackGeneration,
		canaryRuleRef,
		previousGenerationSequence,
		false,
		keyring,
	)
}

func EvaluateArtifactReleaseWithOverride(
	artifact model.PlatformArtifact,
	channel string,
	pinnedRollbackGeneration string,
	canaryRuleRef string,
	previousGenerationSequence int64,
	overrideMode string,
	keyring bundleauth.Keyring,
) Decision {
	return applyPublicationOverride(
		EvaluateArtifactRelease(
			artifact,
			channel,
			pinnedRollbackGeneration,
			canaryRuleRef,
			previousGenerationSequence,
			keyring,
		),
		overrideMode,
	)
}

func EvaluateArtifactRollback(
	artifact model.PlatformArtifact,
	channel string,
	pinnedRollbackGeneration string,
	canaryRuleRef string,
	keyring bundleauth.Keyring,
) Decision {
	return evaluateArtifactPublication(
		artifact,
		channel,
		pinnedRollbackGeneration,
		canaryRuleRef,
		0,
		true,
		keyring,
	)
}

func EvaluateArtifactRollbackWithOverride(
	artifact model.PlatformArtifact,
	channel string,
	pinnedRollbackGeneration string,
	canaryRuleRef string,
	overrideMode string,
	keyring bundleauth.Keyring,
) Decision {
	return applyPublicationOverride(
		EvaluateArtifactRollback(
			artifact,
			channel,
			pinnedRollbackGeneration,
			canaryRuleRef,
			keyring,
		),
		overrideMode,
	)
}

func applyPublicationOverride(decision Decision, overrideMode string) Decision {
	overrideMode = strings.TrimSpace(strings.ToLower(overrideMode))
	if overrideMode == model.PlatformArtifactOverrideModeNone {
		return decision
	}
	remaining := make([]Violation, 0, len(decision.Violations))
	bypassed := append([]Violation(nil), decision.Bypassed...)
	for _, violation := range decision.Violations {
		if publicationOverrideAllowsInvariant(overrideMode, violation.Invariant) {
			bypassed = append(bypassed, violation)
			continue
		}
		remaining = append(remaining, violation)
	}
	decision.Violations = remaining
	decision.Bypassed = bypassed
	decision.Pass = len(remaining) == 0
	return decision
}

func publicationOverrideAllowsInvariant(overrideMode, invariant string) bool {
	switch overrideMode {
	case model.PlatformArtifactOverrideModeSoft:
		return invariant == InvariantArtifactValidated
	case model.PlatformArtifactOverrideModeKernelBreakGlass:
		switch invariant {
		case InvariantArtifactValidated, InvariantGenerationMonotonic, InvariantFullPinnedRollback:
			return true
		}
	}
	return false
}

func BypassedInvariantIDs(decision Decision) []string {
	if len(decision.Bypassed) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(decision.Bypassed))
	ids := make([]string, 0, len(decision.Bypassed))
	for _, violation := range decision.Bypassed {
		id := strings.TrimSpace(violation.Invariant)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func ValidateKernelBreakGlassAuthorization(
	authorization *model.PlatformKernelBreakGlassRequest,
	artifact model.PlatformArtifact,
	now time.Time,
) error {
	if authorization == nil {
		return errors.New("kernel break-glass authorization is required")
	}
	now = now.UTC()
	expiresAt := authorization.ExpiresAt.UTC()
	if expiresAt.IsZero() || !expiresAt.After(now) {
		return errors.New("kernel break-glass authorization is expired")
	}
	if expiresAt.After(now.Add(KernelBreakGlassMaxTTL)) {
		return fmt.Errorf("kernel break-glass TTL cannot exceed %s", KernelBreakGlassMaxTTL)
	}
	if strings.TrimSpace(authorization.Confirmation) != KernelBreakGlassConfirmation {
		return errors.New("kernel break-glass safety-kernel confirmation is invalid")
	}
	targetConfirmation := strings.TrimSpace(authorization.TargetConfirmation)
	if targetConfirmation == "" ||
		(targetConfirmation != strings.TrimSpace(artifact.ID) &&
			targetConfirmation != strings.TrimSpace(artifact.Generation)) {
		return errors.New("kernel break-glass target confirmation does not match the artifact")
	}
	return nil
}

func evaluateArtifactPublication(
	artifact model.PlatformArtifact,
	channel string,
	pinnedRollbackGeneration string,
	canaryRuleRef string,
	previousGenerationSequence int64,
	allowGenerationRollback bool,
	keyring bundleauth.Keyring,
) Decision {
	violations := append([]Violation(nil), EvaluateArtifactIntegrity(artifact, keyring).Violations...)
	if artifact.Status != model.PlatformArtifactStatusValidated {
		violations = append(violations, Violation{
			Invariant: InvariantArtifactValidated,
			Message:   "artifact must be validated before release",
		})
	}
	if !allowGenerationRollback &&
		previousGenerationSequence > 0 &&
		artifact.GenerationSequence <= previousGenerationSequence {
		violations = append(violations, Violation{
			Invariant: InvariantGenerationMonotonic,
			Message: fmt.Sprintf(
				"artifact generation sequence %d must be greater than active sequence %d",
				artifact.GenerationSequence,
				previousGenerationSequence,
			),
		})
	}
	if channel == model.PlatformArtifactReleaseChannelFull && strings.TrimSpace(pinnedRollbackGeneration) == "" {
		violations = append(violations, Violation{
			Invariant: InvariantFullPinnedRollback,
			Message:   "full release requires a pinned verified rollback generation",
		})
	}
	if channel == model.PlatformArtifactReleaseChannelGray && !CanaryScopeRefValid(canaryRuleRef) {
		violations = append(violations, Violation{
			Invariant: InvariantCanaryScopeIsolation,
			Message:   "gray release requires one bounded canary scope",
		})
	}
	return Decision{Pass: len(violations) == 0, Violations: violations}
}

func EvaluateArtifactIntegrity(artifact model.PlatformArtifact, keyring bundleauth.Keyring) Decision {
	violations := []Violation{}
	if strings.TrimSpace(artifact.SchemaVersion) != model.PlatformArtifactSchemaVersionV1 ||
		artifact.GenerationSequence <= 0 {
		violations = append(violations, Violation{
			Invariant: InvariantArtifactSchema,
			Message:   "artifact must use the supported schema and a positive generation sequence",
		})
	}
	computedHash := artifactContentHash(artifact.Content)
	if !strings.HasPrefix(strings.TrimSpace(artifact.ContentHash), "sha256:") ||
		computedHash == "" ||
		!strings.EqualFold(strings.TrimSpace(artifact.ContentHash), computedHash) {
		violations = append(violations, Violation{
			Invariant: InvariantArtifactContentHash,
			Message:   "artifact content hash must use sha256 and match canonical content",
		})
	}
	if err := VerifyPlatformArtifactSignature(artifact, keyring); err != nil {
		violations = append(violations, Violation{
			Invariant: InvariantArtifactSignature,
			Message:   "artifact provenance signature must be present and trusted: " + err.Error(),
		})
	}
	return Decision{Pass: len(violations) == 0, Violations: violations}
}

func SignPlatformArtifact(artifact model.PlatformArtifact, keyring bundleauth.Keyring) (model.PlatformArtifact, error) {
	key, keyID, err := primaryPlatformSigningKey(keyring)
	if err != nil {
		return model.PlatformArtifact{}, err
	}
	artifact.CreatedAt = canonicalPlatformSignatureTime(artifact.CreatedAt)
	artifact.UpdatedAt = canonicalPlatformSignatureTime(artifact.UpdatedAt)
	signedAt := artifact.CreatedAt
	if signedAt.IsZero() {
		signedAt = canonicalPlatformSignatureTime(time.Now())
	}
	artifact.Provenance = model.PlatformArtifactProvenance{
		Issuer:    model.PlatformArtifactIssuerFugue,
		KeyID:     keyID,
		Algorithm: model.PlatformSignatureHMACSHA256,
		SignedAt:  signedAt,
	}
	raw, err := json.Marshal(platformArtifactSigningPayloadFor(artifact))
	if err != nil {
		return model.PlatformArtifact{}, fmt.Errorf("marshal platform artifact signature payload: %w", err)
	}
	artifact.Provenance.Signature = signPlatformPayload(raw, key)
	return artifact, nil
}

func VerifyPlatformArtifactSignature(artifact model.PlatformArtifact, keyring bundleauth.Keyring) error {
	provenance := artifact.Provenance
	if strings.TrimSpace(provenance.Issuer) != model.PlatformArtifactIssuerFugue ||
		strings.TrimSpace(provenance.Algorithm) != model.PlatformSignatureHMACSHA256 ||
		strings.TrimSpace(provenance.KeyID) == "" ||
		strings.TrimSpace(provenance.Signature) == "" ||
		provenance.SignedAt.IsZero() {
		return ErrPlatformSignatureInvalid
	}
	key, err := platformVerificationKey(keyring, provenance.KeyID)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(platformArtifactSigningPayloadFor(artifact))
	if err != nil {
		return fmt.Errorf("marshal platform artifact signature payload: %w", err)
	}
	expected := signPlatformPayload(raw, key)
	if !hmac.Equal([]byte(strings.TrimSpace(provenance.Signature)), []byte(expected)) {
		return ErrPlatformSignatureInvalid
	}
	return nil
}

func SignTamperEvidentAuditEvent(event model.AuditEvent, keyring bundleauth.Keyring) (model.AuditEvent, error) {
	key, keyID, err := primaryPlatformSigningKey(keyring)
	if err != nil {
		return model.AuditEvent{}, err
	}
	event.ChainID = strings.TrimSpace(event.ChainID)
	event.PreviousHash = strings.TrimSpace(strings.ToLower(event.PreviousHash))
	if event.ChainID == "" || event.ChainSequence <= 0 {
		return model.AuditEvent{}, errors.New("tamper-evident audit chain identity and positive sequence are required")
	}
	if event.ChainSequence == 1 && event.PreviousHash != "" {
		return model.AuditEvent{}, errors.New("first tamper-evident audit event must not have a previous hash")
	}
	if event.ChainSequence > 1 &&
		(!strings.HasPrefix(event.PreviousHash, "sha256:") || len(event.PreviousHash) != len("sha256:")+sha256.Size*2) {
		return model.AuditEvent{}, errors.New("tamper-evident audit event requires a canonical previous sha256 hash")
	}
	event.CreatedAt = canonicalPlatformSignatureTime(event.CreatedAt)
	if event.CreatedAt.IsZero() {
		event.CreatedAt = canonicalPlatformSignatureTime(time.Now())
	}
	raw, err := json.Marshal(platformAuditEventHashPayloadFor(event))
	if err != nil {
		return model.AuditEvent{}, fmt.Errorf("marshal platform audit event hash payload: %w", err)
	}
	sum := sha256.Sum256(raw)
	event.EventHash = "sha256:" + hex.EncodeToString(sum[:])
	event.Provenance = model.PlatformArtifactProvenance{
		Issuer:    model.PlatformArtifactIssuerFugue,
		KeyID:     keyID,
		Algorithm: model.PlatformSignatureHMACSHA256,
		SignedAt:  event.CreatedAt,
	}
	signatureRaw, err := json.Marshal(platformAuditEventSignaturePayloadFor(event))
	if err != nil {
		return model.AuditEvent{}, fmt.Errorf("marshal platform audit event signature payload: %w", err)
	}
	event.Provenance.Signature = signPlatformPayload(signatureRaw, key)
	return event, nil
}

func VerifyTamperEvidentAuditEvent(event model.AuditEvent, keyring bundleauth.Keyring) error {
	if strings.TrimSpace(event.ChainID) == "" ||
		event.ChainSequence <= 0 ||
		!strings.HasPrefix(strings.TrimSpace(event.EventHash), "sha256:") {
		return ErrPlatformSignatureInvalid
	}
	raw, err := json.Marshal(platformAuditEventHashPayloadFor(event))
	if err != nil {
		return fmt.Errorf("marshal platform audit event hash payload: %w", err)
	}
	sum := sha256.Sum256(raw)
	expectedHash := "sha256:" + hex.EncodeToString(sum[:])
	if !strings.EqualFold(strings.TrimSpace(event.EventHash), expectedHash) {
		return ErrPlatformSignatureInvalid
	}
	provenance := event.Provenance
	if strings.TrimSpace(provenance.Issuer) != model.PlatformArtifactIssuerFugue ||
		strings.TrimSpace(provenance.Algorithm) != model.PlatformSignatureHMACSHA256 ||
		strings.TrimSpace(provenance.KeyID) == "" ||
		strings.TrimSpace(provenance.Signature) == "" ||
		provenance.SignedAt.IsZero() {
		return ErrPlatformSignatureInvalid
	}
	key, err := platformVerificationKey(keyring, provenance.KeyID)
	if err != nil {
		return err
	}
	signatureRaw, err := json.Marshal(platformAuditEventSignaturePayloadFor(event))
	if err != nil {
		return fmt.Errorf("marshal platform audit event signature payload: %w", err)
	}
	expectedSignature := signPlatformPayload(signatureRaw, key)
	if !hmac.Equal([]byte(strings.TrimSpace(provenance.Signature)), []byte(expectedSignature)) {
		return ErrPlatformSignatureInvalid
	}
	return nil
}

func VerifyTamperEvidentAuditChain(events []model.AuditEvent, chainID string, keyring bundleauth.Keyring) error {
	chainID = strings.TrimSpace(chainID)
	chain := make([]model.AuditEvent, 0, len(events))
	for _, event := range events {
		if strings.TrimSpace(event.ChainID) == chainID {
			chain = append(chain, event)
		}
	}
	sort.Slice(chain, func(i, j int) bool {
		return chain[i].ChainSequence < chain[j].ChainSequence
	})
	previousHash := ""
	for index, event := range chain {
		expectedSequence := int64(index + 1)
		if event.ChainSequence != expectedSequence ||
			!strings.EqualFold(strings.TrimSpace(event.PreviousHash), previousHash) {
			return ErrPlatformSignatureInvalid
		}
		if err := VerifyTamperEvidentAuditEvent(event, keyring); err != nil {
			return err
		}
		previousHash = strings.TrimSpace(strings.ToLower(event.EventHash))
	}
	return nil
}

func SignPlatformLKGSnapshot(snapshot model.PlatformLKGSnapshot, keyring bundleauth.Keyring) (model.PlatformLKGSnapshot, error) {
	key, keyID, err := primaryPlatformSigningKey(keyring)
	if err != nil {
		return model.PlatformLKGSnapshot{}, err
	}
	snapshot.ExpiresAt = canonicalPlatformSignatureTime(snapshot.ExpiresAt)
	snapshot.CreatedAt = canonicalPlatformSignatureTime(snapshot.CreatedAt)
	snapshot.UpdatedAt = canonicalPlatformSignatureTime(snapshot.UpdatedAt)
	signedAt := snapshot.CreatedAt
	if signedAt.IsZero() {
		signedAt = canonicalPlatformSignatureTime(time.Now())
	}
	snapshot.SnapshotProvenance = model.PlatformArtifactProvenance{
		Issuer:    model.PlatformArtifactIssuerFugue,
		KeyID:     keyID,
		Algorithm: model.PlatformSignatureHMACSHA256,
		SignedAt:  signedAt,
	}
	raw, err := json.Marshal(platformLKGSigningPayloadFor(snapshot))
	if err != nil {
		return model.PlatformLKGSnapshot{}, fmt.Errorf("marshal platform LKG signature payload: %w", err)
	}
	snapshot.SnapshotProvenance.Signature = signPlatformPayload(raw, key)
	return snapshot, nil
}

func EvaluatePlatformLKGSnapshot(
	snapshot model.PlatformLKGSnapshot,
	artifact model.PlatformArtifact,
	keyring bundleauth.Keyring,
	now time.Time,
) Decision {
	violations := append([]Violation(nil), EvaluateArtifactIntegrity(artifact, keyring).Violations...)
	if !snapshot.ExpiresAt.After(now) {
		violations = append(violations, Violation{
			Invariant: InvariantLKGNotExpired,
			Message:   "verified LKG snapshot is expired",
		})
	}
	if strings.TrimSpace(snapshot.VerifiedByReleaseID) == "" ||
		!strings.HasPrefix(strings.TrimSpace(snapshot.VerificationEvidenceHash), "sha256:") ||
		snapshot.ID == "" ||
		snapshot.ArtifactID != artifact.ID ||
		snapshot.ArtifactKind != artifact.ArtifactKind ||
		snapshot.Scope != artifact.Scope ||
		snapshot.ScopeKey != artifact.ScopeKey ||
		snapshot.SchemaVersion != artifact.SchemaVersion ||
		snapshot.Generation != artifact.Generation ||
		snapshot.GenerationSequence != artifact.GenerationSequence ||
		snapshot.ContentHash != artifact.ContentHash ||
		snapshot.ArtifactProvenance != artifact.Provenance {
		violations = append(violations, Violation{
			Invariant: InvariantLKGContentIntegrity,
			Message:   "verified LKG snapshot does not match its signed artifact",
		})
	}
	if err := verifyPlatformLKGSnapshotSignature(snapshot, keyring); err != nil {
		violations = append(violations, Violation{
			Invariant: InvariantLKGSignature,
			Message:   "verified LKG snapshot signature must be present and trusted: " + err.Error(),
		})
	}
	return Decision{Pass: len(violations) == 0, Violations: violations}
}

func verifyPlatformLKGSnapshotSignature(snapshot model.PlatformLKGSnapshot, keyring bundleauth.Keyring) error {
	provenance := snapshot.SnapshotProvenance
	if strings.TrimSpace(provenance.Issuer) != model.PlatformArtifactIssuerFugue ||
		strings.TrimSpace(provenance.Algorithm) != model.PlatformSignatureHMACSHA256 ||
		strings.TrimSpace(provenance.KeyID) == "" ||
		strings.TrimSpace(provenance.Signature) == "" ||
		provenance.SignedAt.IsZero() {
		return ErrPlatformSignatureInvalid
	}
	key, err := platformVerificationKey(keyring, provenance.KeyID)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(platformLKGSigningPayloadFor(snapshot))
	if err != nil {
		return fmt.Errorf("marshal platform LKG signature payload: %w", err)
	}
	expected := signPlatformPayload(raw, key)
	if !hmac.Equal([]byte(strings.TrimSpace(provenance.Signature)), []byte(expected)) {
		return ErrPlatformSignatureInvalid
	}
	return nil
}

type platformArtifactSigningPayload struct {
	ID                 string                      `json:"id"`
	ArtifactKind       string                      `json:"artifact_kind"`
	Scope              model.PlatformArtifactScope `json:"scope"`
	ScopeKey           string                      `json:"scope_key"`
	SchemaVersion      string                      `json:"schema_version"`
	Generation         string                      `json:"generation"`
	GenerationSequence int64                       `json:"generation_sequence"`
	ContentHash        string                      `json:"content_hash"`
	CompatibilityFloor string                      `json:"compatibility_floor,omitempty"`
	Metadata           map[string]string           `json:"metadata,omitempty"`
	CreatedByType      string                      `json:"created_by_type,omitempty"`
	CreatedByID        string                      `json:"created_by_id,omitempty"`
	Issuer             string                      `json:"issuer"`
	KeyID              string                      `json:"key_id"`
	Algorithm          string                      `json:"algorithm"`
	SignedAt           time.Time                   `json:"signed_at"`
}

type platformAuditEventHashPayload struct {
	ID            string            `json:"id"`
	TenantID      string            `json:"tenant_id,omitempty"`
	ActorType     string            `json:"actor_type"`
	ActorID       string            `json:"actor_id"`
	Action        string            `json:"action"`
	TargetType    string            `json:"target_type"`
	TargetID      string            `json:"target_id,omitempty"`
	Metadata      map[string]string `json:"metadata,omitempty"`
	ChainID       string            `json:"chain_id"`
	ChainSequence int64             `json:"chain_sequence"`
	PreviousHash  string            `json:"previous_hash,omitempty"`
	CreatedAt     time.Time         `json:"created_at"`
}

type platformAuditEventSignaturePayload struct {
	ChainID       string    `json:"chain_id"`
	ChainSequence int64     `json:"chain_sequence"`
	EventHash     string    `json:"event_hash"`
	Issuer        string    `json:"issuer"`
	KeyID         string    `json:"key_id"`
	Algorithm     string    `json:"algorithm"`
	SignedAt      time.Time `json:"signed_at"`
}

func platformAuditEventHashPayloadFor(event model.AuditEvent) platformAuditEventHashPayload {
	return platformAuditEventHashPayload{
		ID:            strings.TrimSpace(event.ID),
		TenantID:      strings.TrimSpace(event.TenantID),
		ActorType:     strings.TrimSpace(event.ActorType),
		ActorID:       strings.TrimSpace(event.ActorID),
		Action:        strings.TrimSpace(event.Action),
		TargetType:    strings.TrimSpace(event.TargetType),
		TargetID:      strings.TrimSpace(event.TargetID),
		Metadata:      event.Metadata,
		ChainID:       strings.TrimSpace(event.ChainID),
		ChainSequence: event.ChainSequence,
		PreviousHash:  strings.TrimSpace(strings.ToLower(event.PreviousHash)),
		CreatedAt:     canonicalPlatformSignatureTime(event.CreatedAt),
	}
}

func platformAuditEventSignaturePayloadFor(event model.AuditEvent) platformAuditEventSignaturePayload {
	return platformAuditEventSignaturePayload{
		ChainID:       strings.TrimSpace(event.ChainID),
		ChainSequence: event.ChainSequence,
		EventHash:     strings.TrimSpace(strings.ToLower(event.EventHash)),
		Issuer:        strings.TrimSpace(event.Provenance.Issuer),
		KeyID:         strings.TrimSpace(event.Provenance.KeyID),
		Algorithm:     strings.TrimSpace(event.Provenance.Algorithm),
		SignedAt:      canonicalPlatformSignatureTime(event.Provenance.SignedAt),
	}
}

func platformArtifactSigningPayloadFor(artifact model.PlatformArtifact) platformArtifactSigningPayload {
	return platformArtifactSigningPayload{
		ID:                 strings.TrimSpace(artifact.ID),
		ArtifactKind:       strings.TrimSpace(artifact.ArtifactKind),
		Scope:              artifact.Scope,
		ScopeKey:           strings.TrimSpace(artifact.ScopeKey),
		SchemaVersion:      strings.TrimSpace(artifact.SchemaVersion),
		Generation:         strings.TrimSpace(artifact.Generation),
		GenerationSequence: artifact.GenerationSequence,
		ContentHash:        strings.TrimSpace(artifact.ContentHash),
		CompatibilityFloor: strings.TrimSpace(artifact.CompatibilityFloor),
		Metadata:           artifact.Metadata,
		CreatedByType:      strings.TrimSpace(artifact.CreatedByType),
		CreatedByID:        strings.TrimSpace(artifact.CreatedByID),
		Issuer:             strings.TrimSpace(artifact.Provenance.Issuer),
		KeyID:              strings.TrimSpace(artifact.Provenance.KeyID),
		Algorithm:          strings.TrimSpace(artifact.Provenance.Algorithm),
		SignedAt:           artifact.Provenance.SignedAt.UTC(),
	}
}

type platformLKGSigningPayload struct {
	ID                       string                           `json:"id"`
	ArtifactID               string                           `json:"artifact_id"`
	ArtifactKind             string                           `json:"artifact_kind"`
	Scope                    model.PlatformArtifactScope      `json:"scope"`
	ScopeKey                 string                           `json:"scope_key"`
	SchemaVersion            string                           `json:"schema_version"`
	Generation               string                           `json:"generation"`
	GenerationSequence       int64                            `json:"generation_sequence"`
	ContentHash              string                           `json:"content_hash"`
	ArtifactProvenance       model.PlatformArtifactProvenance `json:"artifact_provenance"`
	VerifiedByReleaseID      string                           `json:"verified_by_release_id"`
	VerificationEvidenceHash string                           `json:"verification_evidence_hash"`
	ExpiresAt                time.Time                        `json:"expires_at"`
	CreatedAt                time.Time                        `json:"created_at"`
	Issuer                   string                           `json:"issuer"`
	KeyID                    string                           `json:"key_id"`
	Algorithm                string                           `json:"algorithm"`
	SignedAt                 time.Time                        `json:"signed_at"`
}

func platformLKGSigningPayloadFor(snapshot model.PlatformLKGSnapshot) platformLKGSigningPayload {
	return platformLKGSigningPayload{
		ID:                       strings.TrimSpace(snapshot.ID),
		ArtifactID:               strings.TrimSpace(snapshot.ArtifactID),
		ArtifactKind:             strings.TrimSpace(snapshot.ArtifactKind),
		Scope:                    snapshot.Scope,
		ScopeKey:                 strings.TrimSpace(snapshot.ScopeKey),
		SchemaVersion:            strings.TrimSpace(snapshot.SchemaVersion),
		Generation:               strings.TrimSpace(snapshot.Generation),
		GenerationSequence:       snapshot.GenerationSequence,
		ContentHash:              strings.TrimSpace(snapshot.ContentHash),
		ArtifactProvenance:       snapshot.ArtifactProvenance,
		VerifiedByReleaseID:      strings.TrimSpace(snapshot.VerifiedByReleaseID),
		VerificationEvidenceHash: strings.TrimSpace(snapshot.VerificationEvidenceHash),
		ExpiresAt:                snapshot.ExpiresAt.UTC(),
		CreatedAt:                snapshot.CreatedAt.UTC(),
		Issuer:                   strings.TrimSpace(snapshot.SnapshotProvenance.Issuer),
		KeyID:                    strings.TrimSpace(snapshot.SnapshotProvenance.KeyID),
		Algorithm:                strings.TrimSpace(snapshot.SnapshotProvenance.Algorithm),
		SignedAt:                 snapshot.SnapshotProvenance.SignedAt.UTC(),
	}
}

func primaryPlatformSigningKey(keyring bundleauth.Keyring) (string, string, error) {
	keyring = bundleauth.NewKeyring(
		keyring.PrimaryKey,
		keyring.PrimaryKeyID,
		keyring.PreviousKey,
		keyring.PreviousKeyID,
		platformRevokedKeyIDs(keyring),
	)
	key := strings.TrimSpace(keyring.PrimaryKey)
	keyID := strings.TrimSpace(keyring.PrimaryKeyID)
	if key == "" || keyID == "" || platformKeyRevoked(keyring, keyID) {
		return "", "", ErrPlatformSigningKeyUnavailable
	}
	return key, keyID, nil
}

func platformVerificationKey(keyring bundleauth.Keyring, keyID string) (string, error) {
	keyring = bundleauth.NewKeyring(
		keyring.PrimaryKey,
		keyring.PrimaryKeyID,
		keyring.PreviousKey,
		keyring.PreviousKeyID,
		platformRevokedKeyIDs(keyring),
	)
	keyID = strings.TrimSpace(keyID)
	if keyID == "" || platformKeyRevoked(keyring, keyID) {
		return "", ErrPlatformSignatureInvalid
	}
	if strings.EqualFold(keyID, keyring.PrimaryKeyID) && strings.TrimSpace(keyring.PrimaryKey) != "" {
		return strings.TrimSpace(keyring.PrimaryKey), nil
	}
	if strings.EqualFold(keyID, keyring.PreviousKeyID) && strings.TrimSpace(keyring.PreviousKey) != "" {
		return strings.TrimSpace(keyring.PreviousKey), nil
	}
	return "", ErrPlatformSignatureInvalid
}

func platformRevokedKeyIDs(keyring bundleauth.Keyring) []string {
	values := make([]string, 0, len(keyring.RevokedKeyIDs))
	for keyID := range keyring.RevokedKeyIDs {
		values = append(values, keyID)
	}
	return values
}

func platformKeyRevoked(keyring bundleauth.Keyring, keyID string) bool {
	_, revoked := keyring.RevokedKeyIDs[strings.ToLower(strings.TrimSpace(keyID))]
	return revoked
}

func signPlatformPayload(payload []byte, key string) string {
	mac := hmac.New(sha256.New, []byte(key))
	_, _ = mac.Write(payload)
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func canonicalPlatformSignatureTime(value time.Time) time.Time {
	if value.IsZero() {
		return time.Time{}
	}
	return value.UTC().Truncate(time.Microsecond)
}

func CanaryScopeRefValid(raw string) bool {
	raw = strings.TrimSpace(strings.ToLower(raw))
	switch raw {
	case "", "*", "all", "global", "scope=global", "scope:global":
		return false
	}
	var key, value string
	if left, right, ok := strings.Cut(raw, "="); ok {
		key, value = left, right
	} else if left, right, ok := strings.Cut(raw, ":"); ok {
		key, value = left, right
	} else {
		return false
	}
	switch strings.TrimSpace(key) {
	case "node", "edge", "edge_group", "failure_domain", "region", "country", "cohort", "consumer", "hostname", "service", "app":
	default:
		return false
	}
	value = strings.TrimSpace(value)
	if value == "" || value == "*" || value == "all" || value == "global" || strings.ContainsAny(value, ",;") {
		return false
	}
	return true
}

func EvaluateLKGPromotion(release model.PlatformArtifactRelease, req model.PlatformArtifactVerifyLKGRequest, hasExistingLKG bool) Decision {
	violations := []Violation{}
	initialShadowSeed := !hasExistingLKG &&
		req.AllowInitialLKG &&
		release.ReleaseChannel == model.PlatformArtifactReleaseChannelShadow
	if hasExistingLKG && release.ReleaseChannel != model.PlatformArtifactReleaseChannelFull {
		violations = append(violations, Violation{
			Invariant: InvariantFullPinnedRollback,
			Message:   "only a full release can replace an existing verified LKG",
		})
	}
	if !hasExistingLKG && !initialShadowSeed {
		violations = append(violations, Violation{
			Invariant: InvariantFullPinnedRollback,
			Message:   "initial verified LKG requires an explicit shadow seed",
		})
	}
	if req.FencingToken <= 0 || req.FencingToken != release.FencingToken {
		violations = append(violations, Violation{
			Invariant: InvariantFencingTokenCurrent,
			Message:   "verification fencing token does not match the active release",
		})
	}
	if strings.TrimSpace(req.Reason) == "" {
		violations = append(violations, Violation{
			Invariant: InvariantVerificationEvidencePassed,
			Message:   "verification reason is required",
		})
	}
	if !hasExistingLKG && !req.AllowInitialLKG {
		violations = append(violations, Violation{
			Invariant: InvariantFullPinnedRollback,
			Message:   "initial verified LKG requires explicit allow_initial_lkg",
		})
	}
	evidence := req.Evidence
	missing := []string{}
	if !evidence.ConsumerConvergence {
		missing = append(missing, "consumer_convergence")
	}
	if !evidence.LocalProbe {
		missing = append(missing, "local_probe")
	}
	if !evidence.PublicSynthetic {
		missing = append(missing, "public_synthetic")
	}
	if !evidence.WatchWindow {
		missing = append(missing, "watch_window")
	}
	if !evidence.BaselineMonotonic {
		missing = append(missing, "baseline_monotonic")
	}
	if !evidence.DatabaseRollbackCompatible {
		missing = append(missing, "database_rollback_compatible")
	}
	if len(missing) > 0 {
		violations = append(violations, Violation{
			Invariant: InvariantVerificationEvidencePassed,
			Message:   fmt.Sprintf("verification evidence did not pass: %s", strings.Join(missing, ",")),
		})
	}
	return Decision{Pass: len(violations) == 0, Violations: violations}
}

func artifactContentHash(content map[string]any) string {
	if content == nil {
		return ""
	}
	payload, err := json.Marshal(content)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func VerificationEvidenceMap(req model.PlatformArtifactVerifyLKGRequest) map[string]string {
	return map[string]string{
		"baseline_monotonic":           fmt.Sprintf("%t", req.Evidence.BaselineMonotonic),
		"consumer_convergence":         fmt.Sprintf("%t", req.Evidence.ConsumerConvergence),
		"database_rollback_compatible": fmt.Sprintf("%t", req.Evidence.DatabaseRollbackCompatible),
		"evidence_refs":                strings.Join(normalizeStrings(req.Evidence.EvidenceRefs), ","),
		"expected_consumer_set_id":     strings.TrimSpace(req.Evidence.ExpectedConsumerSetID),
		"local_probe":                  fmt.Sprintf("%t", req.Evidence.LocalProbe),
		"public_synthetic":             fmt.Sprintf("%t", req.Evidence.PublicSynthetic),
		"reason":                       strings.TrimSpace(req.Reason),
		"watch_window":                 fmt.Sprintf("%t", req.Evidence.WatchWindow),
	}
}

func VerificationEvidenceHash(req model.PlatformArtifactVerifyLKGRequest) string {
	payload, _ := json.Marshal(VerificationEvidenceMap(req))
	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func normalizeStrings(values []string) []string {
	seen := map[string]bool{}
	out := []string{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	return out
}
