// Package backupcontrol defines the versioned spec/status ownership boundary
// for extracting backup scheduling and recovery from the legacy API process.
// The v1 contract is observation-only and contains no credentials, object
// locations, database handles, or execution adapter capability.
package backupcontrol

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"
)

const (
	BackupRunSpecAPIVersion   = "backup-control.fugue.dev/v1"
	BackupRunSpecKind         = "BackupRunSpec"
	BackupRunStatusAPIVersion = "backup-control.fugue.dev/v1"
	BackupRunStatusKind       = "BackupRunStatus"
	BackupRunStatusPolicy     = "shadow-spec-status-lkg-v1"
	BackupRunModeShadow       = "shadow"

	TargetControlPlaneDatabase = "control-plane-db"
	TargetAppDatabase          = "app-database"
	TargetPersistentStorage    = "persistent-storage"
	TargetDataWorkspace        = "data-workspace"
	TargetRegistry             = "registry"
	TargetPlatformComponent    = "platform-component"

	ArtifactControlPlanePGDump = "control-plane-pg-dump"
	ArtifactAppPGDump          = "app-pg-dump"
	ArtifactFileArchive        = "file-archive"
	ArtifactDataSnapshot       = "data-snapshot"
	ArtifactRegistryArchive    = "registry-archive"

	ObservedStatePending   = "pending"
	ObservedStateRunning   = "running"
	ObservedStateSucceeded = "succeeded"
	ObservedStateFailed    = "failed"
	ObservedStateCanceled  = "canceled"
	ObservedStateBlocked   = "blocked"

	minAttemptLimit           = 1
	maxAttemptLimit           = 10
	minLeaseTTLSeconds        = 30
	maxLeaseTTLSeconds        = 600
	minOperationTimeout       = 60
	maxOperationTimeout       = 3600
	maxObservationTTL         = 5 * time.Minute
	maxObservedErrorCodeBytes = 96
	maxContractDocumentBytes  = 32 * 1024
)

var (
	ErrBackupContract      = errors.New("invalid backup-control contract")
	canonicalIDPattern     = regexp.MustCompile(`^[a-z0-9][a-z0-9._:-]{0,127}$`)
	canonicalScopePattern  = regexp.MustCompile(`^(platform|tenant|project|app)/[a-z0-9][a-z0-9._:-]{0,127}(/[a-z0-9][a-z0-9._:-]{0,127})*$`)
	canonicalDigestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	canonicalErrorCode     = regexp.MustCompile(`^[a-z0-9]+([_-][a-z0-9]+)*$`)
)

type BackupTarget struct {
	Type     string `json:"type"`
	ScopeKey string `json:"scopeKey"`
}

// BackupRunSpec is desired state without any secret or physical object-store
// address. BackendGeneration names an externally managed configuration and
// credentials remain behind that adapter boundary.
type BackupRunSpec struct {
	APIVersion             string       `json:"apiVersion"`
	Kind                   string       `json:"kind"`
	Mode                   string       `json:"mode"`
	RunID                  string       `json:"runId"`
	RequestID              string       `json:"requestId"`
	Target                 BackupTarget `json:"target"`
	CellKey                string       `json:"cellKey"`
	BackendID              string       `json:"backendId"`
	BackendGeneration      string       `json:"backendGeneration"`
	ArtifactKind           string       `json:"artifactKind"`
	AttemptLimit           int          `json:"attemptLimit"`
	LeaseTTLSeconds        int          `json:"leaseTtlSeconds"`
	OperationTimeoutSecs   int          `json:"operationTimeoutSeconds"`
	IdempotencyKey         string       `json:"idempotencyKey"`
	ObservationOnly        bool         `json:"observationOnly"`
	ProductionWriteAllowed bool         `json:"productionWriteAllowed"`
	Digest                 string       `json:"digest"`
}

// BackupArtifactRef is deliberately location-free. A worker may use the
// opaque artifact ID through a versioned adapter, but status never exposes a
// bucket, endpoint, object key, access key, token, DSN, or password.
type BackupArtifactRef struct {
	ArtifactID        string `json:"artifactId"`
	RunID             string `json:"runId"`
	Kind              string `json:"kind"`
	ContentDigest     string `json:"contentDigest"`
	ManifestDigest    string `json:"manifestDigest"`
	BackendGeneration string `json:"backendGeneration"`
}

type LegacyRunObservation struct {
	State       string
	Attempt     int
	Fence       int64
	WorkerID    string
	ErrorCode   string
	ErrorDigest string
}

// BackupRunStatus is an expiring observation of legacy state plus the
// lane-local last-known-good artifact reference. It cannot claim that the new
// component applied work while migration remains shadow-only.
type BackupRunStatus struct {
	APIVersion             string             `json:"apiVersion"`
	Kind                   string             `json:"kind"`
	Policy                 string             `json:"policy"`
	RunID                  string             `json:"runId"`
	SpecDigest             string             `json:"specDigest"`
	CellKey                string             `json:"cellKey"`
	ObservedState          string             `json:"observedState"`
	ObservedAttempt        int                `json:"observedAttempt"`
	ObservedFence          int64              `json:"observedFence"`
	ObservedWorkerID       string             `json:"observedWorkerId,omitempty"`
	ObservedErrorCode      string             `json:"observedErrorCode,omitempty"`
	ObservedErrorDigest    string             `json:"observedErrorDigest,omitempty"`
	LastKnownGood          *BackupArtifactRef `json:"lastKnownGood,omitempty"`
	ObservedAt             time.Time          `json:"observedAt"`
	ValidUntil             time.Time          `json:"validUntil"`
	ObservationOnly        bool               `json:"observationOnly"`
	ProductionWriteAllowed bool               `json:"productionWriteAllowed"`
	Digest                 string             `json:"digest"`
}

func NewShadowBackupRunSpec(
	runID string,
	requestID string,
	target BackupTarget,
	backendID string,
	backendGeneration string,
	attemptLimit int,
	leaseTTLSeconds int,
	operationTimeoutSeconds int,
) (BackupRunSpec, error) {
	target.Type = strings.TrimSpace(target.Type)
	target.ScopeKey = strings.TrimSpace(target.ScopeKey)
	spec := BackupRunSpec{
		APIVersion:             BackupRunSpecAPIVersion,
		Kind:                   BackupRunSpecKind,
		Mode:                   BackupRunModeShadow,
		RunID:                  strings.TrimSpace(runID),
		RequestID:              strings.TrimSpace(requestID),
		Target:                 target,
		CellKey:                BackupCellKey(target),
		BackendID:              strings.TrimSpace(backendID),
		BackendGeneration:      strings.TrimSpace(backendGeneration),
		ArtifactKind:           artifactKindForTarget(target.Type),
		AttemptLimit:           attemptLimit,
		LeaseTTLSeconds:        leaseTTLSeconds,
		OperationTimeoutSecs:   operationTimeoutSeconds,
		IdempotencyKey:         "backup-run/" + strings.TrimSpace(requestID),
		ObservationOnly:        true,
		ProductionWriteAllowed: false,
	}
	spec.Digest = DigestBackupRunSpec(spec)
	if err := ValidateBackupRunSpec(spec); err != nil {
		return BackupRunSpec{}, err
	}
	return spec, nil
}

func ValidateBackupRunSpec(spec BackupRunSpec) error {
	if spec.APIVersion != BackupRunSpecAPIVersion || spec.Kind != BackupRunSpecKind || spec.Mode != BackupRunModeShadow ||
		!spec.ObservationOnly || spec.ProductionWriteAllowed || !canonicalIDPattern.MatchString(spec.RunID) ||
		!canonicalIDPattern.MatchString(spec.RequestID) || !canonicalIDPattern.MatchString(spec.BackendID) ||
		!canonicalDigestPattern.MatchString(spec.BackendGeneration) ||
		spec.AttemptLimit < minAttemptLimit || spec.AttemptLimit > maxAttemptLimit ||
		spec.LeaseTTLSeconds < minLeaseTTLSeconds || spec.LeaseTTLSeconds > maxLeaseTTLSeconds ||
		spec.OperationTimeoutSecs < minOperationTimeout || spec.OperationTimeoutSecs > maxOperationTimeout {
		return ErrBackupContract
	}
	if err := validateTarget(spec.Target); err != nil || spec.ArtifactKind != artifactKindForTarget(spec.Target.Type) ||
		spec.CellKey != BackupCellKey(spec.Target) || spec.IdempotencyKey != "backup-run/"+spec.RequestID ||
		spec.Digest != DigestBackupRunSpec(spec) {
		return ErrBackupContract
	}
	return nil
}

// DecodeBackupRunSpec applies the strict v1 wire boundary before validating
// semantic and digest bindings. Additive fields require a new contract version
// rather than being silently discarded by encoding/json.
func DecodeBackupRunSpec(document []byte) (BackupRunSpec, error) {
	var spec BackupRunSpec
	if err := decodeStrictContract(document, &spec); err != nil {
		return BackupRunSpec{}, err
	}
	if err := ValidateBackupRunSpec(spec); err != nil {
		return BackupRunSpec{}, err
	}
	return spec, nil
}

func DigestBackupRunSpec(spec BackupRunSpec) string {
	spec.Digest = ""
	return canonicalDigest(spec)
}

func NewObservedBackupRunStatus(
	spec BackupRunSpec,
	observation LegacyRunObservation,
	lastKnownGood *BackupArtifactRef,
	observedAt time.Time,
	ttl time.Duration,
) (BackupRunStatus, error) {
	if err := ValidateBackupRunSpec(spec); err != nil {
		return BackupRunStatus{}, err
	}
	observedAt, err := canonicalObservationTime(observedAt)
	if err != nil || ttl <= 0 || ttl > maxObservationTTL || ttl%time.Second != 0 {
		return BackupRunStatus{}, ErrBackupContract
	}
	status := BackupRunStatus{
		APIVersion:             BackupRunStatusAPIVersion,
		Kind:                   BackupRunStatusKind,
		Policy:                 BackupRunStatusPolicy,
		RunID:                  spec.RunID,
		SpecDigest:             spec.Digest,
		CellKey:                spec.CellKey,
		ObservedState:          strings.TrimSpace(observation.State),
		ObservedAttempt:        observation.Attempt,
		ObservedFence:          observation.Fence,
		ObservedWorkerID:       strings.TrimSpace(observation.WorkerID),
		ObservedErrorCode:      strings.TrimSpace(observation.ErrorCode),
		ObservedErrorDigest:    strings.TrimSpace(observation.ErrorDigest),
		LastKnownGood:          cloneArtifactRef(lastKnownGood),
		ObservedAt:             observedAt,
		ValidUntil:             observedAt.Add(ttl),
		ObservationOnly:        true,
		ProductionWriteAllowed: false,
	}
	status.Digest = DigestBackupRunStatus(status)
	if err := ValidateBackupRunStatus(spec, status); err != nil {
		return BackupRunStatus{}, err
	}
	return status, nil
}

func ValidateBackupRunStatus(spec BackupRunSpec, status BackupRunStatus) error {
	if ValidateBackupRunSpec(spec) != nil || status.APIVersion != BackupRunStatusAPIVersion ||
		status.Kind != BackupRunStatusKind || status.Policy != BackupRunStatusPolicy ||
		status.RunID != spec.RunID || status.SpecDigest != spec.Digest || status.CellKey != spec.CellKey ||
		!status.ObservationOnly || status.ProductionWriteAllowed || status.Digest != DigestBackupRunStatus(status) ||
		!canonicalStatusTime(status.ObservedAt) || !canonicalStatusTime(status.ValidUntil) ||
		!status.ValidUntil.After(status.ObservedAt) || status.ValidUntil.Sub(status.ObservedAt) > maxObservationTTL ||
		status.ObservedAttempt < 0 || status.ObservedAttempt > spec.AttemptLimit || status.ObservedFence < 0 ||
		(status.ObservedWorkerID != "" && !canonicalIDPattern.MatchString(status.ObservedWorkerID)) {
		return ErrBackupContract
	}
	if status.LastKnownGood != nil {
		if validateArtifactRef(*status.LastKnownGood) != nil || status.LastKnownGood.Kind != spec.ArtifactKind {
			return ErrBackupContract
		}
	}
	switch status.ObservedState {
	case ObservedStatePending:
		if status.ObservedWorkerID != "" || status.ObservedErrorCode != "" || status.ObservedErrorDigest != "" {
			return ErrBackupContract
		}
	case ObservedStateRunning:
		if status.ObservedAttempt < 1 || status.ObservedFence < 1 || !canonicalIDPattern.MatchString(status.ObservedWorkerID) ||
			status.ObservedErrorCode != "" || status.ObservedErrorDigest != "" {
			return ErrBackupContract
		}
	case ObservedStateSucceeded:
		if status.ObservedAttempt < 1 || status.ObservedFence < 1 || status.LastKnownGood == nil ||
			status.LastKnownGood.RunID != status.RunID || status.LastKnownGood.BackendGeneration != spec.BackendGeneration ||
			status.ObservedErrorCode != "" || status.ObservedErrorDigest != "" {
			return ErrBackupContract
		}
	case ObservedStateFailed, ObservedStateBlocked:
		if status.ObservedAttempt < 1 || status.ObservedFence < 1 || !validObservedError(status.ObservedErrorCode, status.ObservedErrorDigest) {
			return ErrBackupContract
		}
	case ObservedStateCanceled:
		if status.ObservedAttempt < 0 || status.ObservedErrorCode != "" || status.ObservedErrorDigest != "" {
			return ErrBackupContract
		}
	default:
		return ErrBackupContract
	}
	return nil
}

// DecodeBackupRunStatus applies the same strict wire boundary and requires the
// caller to supply the exact desired spec the observation claims to satisfy.
func DecodeBackupRunStatus(spec BackupRunSpec, document []byte) (BackupRunStatus, error) {
	var status BackupRunStatus
	if err := decodeStrictContract(document, &status); err != nil {
		return BackupRunStatus{}, err
	}
	if err := ValidateBackupRunStatus(spec, status); err != nil {
		return BackupRunStatus{}, err
	}
	return status, nil
}

func DigestBackupRunStatus(status BackupRunStatus) string {
	status.Digest = ""
	return canonicalDigest(status)
}

func BackupCellKey(target BackupTarget) string {
	if validateTarget(target) != nil {
		return ""
	}
	digest := sha256.Sum256([]byte(target.Type + "\x00" + target.ScopeKey))
	return "backup/" + target.Type + "/" + hex.EncodeToString(digest[:8])
}

func artifactKindForTarget(targetType string) string {
	switch targetType {
	case TargetControlPlaneDatabase:
		return ArtifactControlPlanePGDump
	case TargetAppDatabase:
		return ArtifactAppPGDump
	case TargetPersistentStorage, TargetPlatformComponent:
		return ArtifactFileArchive
	case TargetDataWorkspace:
		return ArtifactDataSnapshot
	case TargetRegistry:
		return ArtifactRegistryArchive
	default:
		return ""
	}
}

func validateTarget(target BackupTarget) error {
	if strings.TrimSpace(target.Type) != target.Type || strings.TrimSpace(target.ScopeKey) != target.ScopeKey ||
		!canonicalScopePattern.MatchString(target.ScopeKey) || artifactKindForTarget(target.Type) == "" {
		return ErrBackupContract
	}
	prefix := strings.SplitN(target.ScopeKey, "/", 2)[0]
	switch target.Type {
	case TargetControlPlaneDatabase, TargetRegistry, TargetPlatformComponent:
		if prefix != "platform" {
			return ErrBackupContract
		}
	case TargetAppDatabase, TargetPersistentStorage:
		if prefix != "app" {
			return ErrBackupContract
		}
	case TargetDataWorkspace:
		if prefix != "project" {
			return ErrBackupContract
		}
	}
	return nil
}

func validateArtifactRef(ref BackupArtifactRef) error {
	if !canonicalIDPattern.MatchString(ref.ArtifactID) || !canonicalIDPattern.MatchString(ref.RunID) ||
		artifactKindForTargetTypeArtifact(ref.Kind) == "" || !canonicalDigestPattern.MatchString(ref.ContentDigest) ||
		!canonicalDigestPattern.MatchString(ref.ManifestDigest) || !canonicalDigestPattern.MatchString(ref.BackendGeneration) {
		return ErrBackupContract
	}
	return nil
}

func artifactKindForTargetTypeArtifact(kind string) string {
	switch kind {
	case ArtifactControlPlanePGDump, ArtifactAppPGDump, ArtifactFileArchive, ArtifactDataSnapshot, ArtifactRegistryArchive:
		return kind
	default:
		return ""
	}
}

func validObservedError(code, digest string) bool {
	return len(code) <= maxObservedErrorCodeBytes && canonicalErrorCode.MatchString(code) && canonicalDigestPattern.MatchString(digest)
}

func canonicalObservationTime(value time.Time) (time.Time, error) {
	if value.IsZero() {
		return time.Time{}, ErrBackupContract
	}
	return value.UTC().Truncate(time.Second), nil
}

func canonicalStatusTime(value time.Time) bool {
	return !value.IsZero() && value.Location() == time.UTC && value.Nanosecond() == 0
}

func cloneArtifactRef(ref *BackupArtifactRef) *BackupArtifactRef {
	if ref == nil {
		return nil
	}
	cloned := *ref
	return &cloned
}

func decodeStrictContract(document []byte, target any) error {
	if len(document) == 0 || len(document) > maxContractDocumentBytes {
		return ErrBackupContract
	}
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("%w: decode JSON", ErrBackupContract)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return fmt.Errorf("%w: trailing JSON", ErrBackupContract)
	}
	return nil
}

func canonicalDigest(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(fmt.Sprintf("marshal backup-control contract: %v", err))
	}
	digest := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(digest[:])
}
