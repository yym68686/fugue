// Package backupadapter translates the legacy Fugue backup records into the
// versioned, observation-only backup-control contract.  It deliberately has
// no store, network, credential, object-storage, or execution capability;
// callers provide already-read records and the opaque backend generation.
package backupadapter

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"fugue/internal/backupcontrol"
	"fugue/internal/model"
)

const (
	// LegacyBackupAttemptLimit covers the initial attempt plus the legacy
	// retry budget.  It is intentionally below the v1 contract ceiling.
	LegacyBackupAttemptLimit         = 4
	LegacyBackupLeaseTTLSeconds      = 120
	LegacyBackupOperationTimeoutSecs = 30 * 60
	LegacyBackupObservationTTL       = 90 * time.Second
)

var (
	legacySegmentPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._:-]{0,127}$`)
	legacyErrorPattern   = regexp.MustCompile(`^[a-z0-9]+([_-][a-z0-9]+)*$`)
	legacyWorkerPattern  = regexp.MustCompile(`^[a-z0-9][a-z0-9._:-]{0,127}$`)
)

var (
	errLegacyRunIdentity       = errors.New("legacy backup run identity is not representable")
	errLegacyBackendGeneration = errors.New("legacy backup backend generation is not representable")
	errLegacyArtifact          = errors.New("legacy backup artifact is not representable")
)

// BuildShadowSpec constructs the one canonical desired spec used by both the
// observation bridge and the future cell materializer.  backendGeneration is
// opaque by design: the adapter never sees credentials or physical storage
// locations.
func BuildShadowSpec(run model.BackupRun, backendGeneration string) (backupcontrol.BackupRunSpec, error) {
	run = model.NormalizeBackupRun(run)
	runID := strings.TrimSpace(run.ID)
	if !legacySegmentPattern.MatchString(runID) || strings.TrimSpace(run.BackendID) == "" {
		return backupcontrol.BackupRunSpec{}, errLegacyRunIdentity
	}
	target, err := TargetForRun(run)
	if err != nil {
		return backupcontrol.BackupRunSpec{}, err
	}
	backendGeneration = strings.TrimSpace(backendGeneration)
	if !strings.HasPrefix(backendGeneration, "sha256:") || len(backendGeneration) != len("sha256:")+64 {
		return backupcontrol.BackupRunSpec{}, errLegacyBackendGeneration
	}
	spec, err := backupcontrol.NewShadowBackupRunSpec(
		runID,
		runID,
		target,
		strings.TrimSpace(run.BackendID),
		backendGeneration,
		LegacyBackupAttemptLimit,
		LegacyBackupLeaseTTLSeconds,
		LegacyBackupOperationTimeoutSecs,
	)
	if err != nil {
		return backupcontrol.BackupRunSpec{}, fmt.Errorf("build legacy backup spec: %w", err)
	}
	return spec, nil
}

// TargetForRun maps only stable ownership identity into the contract scope.
// Volatile placement fields (runtime, service, and database placement) are
// intentionally excluded unless they are the only stable name available.
func TargetForRun(run model.BackupRun) (backupcontrol.BackupTarget, error) {
	target := model.NormalizeBackupTarget(run.Target)
	segment := func(value string) (string, error) {
		value = strings.ToLower(strings.TrimSpace(value))
		if !legacySegmentPattern.MatchString(value) {
			return "", errLegacyRunIdentity
		}
		return value, nil
	}
	scope := ""
	switch target.Type {
	case model.BackupTargetControlPlaneDatabase:
		scope = "platform/control-plane"
	case model.BackupTargetRegistry:
		scope = "platform/registry"
	case model.BackupTargetPlatformComponent:
		component, err := segment(firstNonEmpty(target.Component, target.Name))
		if err != nil {
			return backupcontrol.BackupTarget{}, err
		}
		scope = "platform/" + component
	case model.BackupTargetAppDatabase:
		appID, err := segment(firstNonEmpty(run.AppID, target.AppID))
		if err != nil {
			return backupcontrol.BackupTarget{}, err
		}
		scope = "app/" + appID + "/database"
	case model.BackupTargetPersistentStorage:
		appID, err := segment(firstNonEmpty(run.AppID, target.AppID))
		if err != nil {
			return backupcontrol.BackupTarget{}, err
		}
		scope = "app/" + appID + "/storage"
	case model.BackupTargetDataWorkspace:
		projectID, err := segment(firstNonEmpty(run.ProjectID, target.ProjectID))
		if err != nil {
			return backupcontrol.BackupTarget{}, err
		}
		scope = "project/" + projectID + "/workspace"
		if strings.TrimSpace(target.WorkspaceID) != "" {
			workspaceID, workspaceErr := segment(target.WorkspaceID)
			if workspaceErr != nil {
				return backupcontrol.BackupTarget{}, workspaceErr
			}
			scope += "/" + workspaceID
		}
	default:
		return backupcontrol.BackupTarget{}, errLegacyRunIdentity
	}
	return backupcontrol.BackupTarget{Type: target.Type, ScopeKey: scope}, nil
}

// BuildShadowStatus converts a read-only legacy snapshot into a strict v1
// status.  A successful run must have exactly one active, digest-complete
// artifact; all other states carry no server-invented artifact LKG. The
// isolated observer retains its own previously validated status on failures.
func BuildShadowStatus(
	spec backupcontrol.BackupRunSpec,
	run model.BackupRun,
	artifacts []model.BackupArtifact,
	now time.Time,
) (backupcontrol.BackupRunStatus, error) {
	if err := backupcontrol.ValidateBackupRunSpec(spec); err != nil {
		return backupcontrol.BackupRunStatus{}, errLegacyRunIdentity
	}
	run = model.NormalizeBackupRun(run)
	target, err := TargetForRun(run)
	if err != nil || run.ID != spec.RunID || run.BackendID != spec.BackendID || target != spec.Target {
		return backupcontrol.BackupRunStatus{}, errLegacyRunIdentity
	}
	if run.Attempt < 0 || run.Attempt > spec.AttemptLimit {
		return backupcontrol.BackupRunStatus{}, errLegacyRunIdentity
	}
	state := strings.TrimSpace(strings.ToLower(run.Status))
	observation := backupcontrol.LegacyRunObservation{
		State:   state,
		Attempt: run.Attempt,
	}
	switch state {
	case backupcontrol.ObservedStatePending:
		observation.Fence = 0
	case backupcontrol.ObservedStateRunning:
		if strings.TrimSpace(run.LeaseOwner) == "" {
			return backupcontrol.BackupRunStatus{}, errLegacyRunIdentity
		}
		observation.WorkerID = canonicalWorkerID(run.LeaseOwner)
		observation.Fence = positiveFence(run.Attempt)
	case backupcontrol.ObservedStateSucceeded:
		observation.Fence = positiveFence(run.Attempt)
	case backupcontrol.ObservedStateFailed, backupcontrol.ObservedStateBlocked:
		observation.Fence = positiveFence(run.Attempt)
		observation.ErrorCode = canonicalErrorCode(run.ErrorCode)
		observation.ErrorDigest = digestLegacyError(run.ErrorCode, run.ErrorMessage)
	case backupcontrol.ObservedStateCanceled:
		observation.Fence = positiveFence(run.Attempt)
	default:
		return backupcontrol.BackupRunStatus{}, errLegacyRunIdentity
	}
	var lkg *backupcontrol.BackupArtifactRef
	if state == backupcontrol.ObservedStateSucceeded {
		lkg, err = currentArtifactRef(spec, run, artifacts)
		if err != nil {
			return backupcontrol.BackupRunStatus{}, err
		}
	}
	status, err := backupcontrol.NewObservedBackupRunStatus(spec, observation, lkg, now, LegacyBackupObservationTTL)
	if err != nil {
		return backupcontrol.BackupRunStatus{}, fmt.Errorf("build legacy backup status: %w", err)
	}
	return status, nil
}

// BackendGeneration computes a digest from non-secret backend configuration
// and an opaque credential-generation marker supplied by the storage adapter.
// It excludes health-test timestamps and all credential material.
func BackendGeneration(backend model.BackupBackend, credentialGeneration string) (string, error) {
	backend = model.NormalizeBackupBackend(backend)
	credentialGeneration = strings.TrimSpace(credentialGeneration)
	if !legacySegmentPattern.MatchString(strings.ToLower(backend.ID)) || credentialGeneration == "" {
		return "", errLegacyBackendGeneration
	}
	payload := struct {
		Version              string                        `json:"version"`
		ID                   string                        `json:"id"`
		TenantID             string                        `json:"tenantId,omitempty"`
		Provider             string                        `json:"provider"`
		Bucket               string                        `json:"bucket,omitempty"`
		Region               string                        `json:"region,omitempty"`
		Endpoint             string                        `json:"endpoint,omitempty"`
		BaseURL              string                        `json:"baseUrl,omitempty"`
		Prefix               string                        `json:"prefix,omitempty"`
		Status               string                        `json:"status"`
		Capabilities         model.DataBackendCapabilities `json:"capabilities"`
		FugueManaged         bool                          `json:"fugueManaged"`
		Billable             bool                          `json:"billable"`
		CredentialGeneration string                        `json:"credentialGeneration"`
	}{
		Version: "backup-backend-generation-v1", ID: strings.ToLower(backend.ID), TenantID: backend.TenantID,
		Provider: backend.Provider, Bucket: backend.Bucket, Region: backend.Region, Endpoint: backend.Endpoint,
		BaseURL: backend.BaseURL, Prefix: backend.Prefix, Status: backend.Status, Capabilities: backend.Capabilities,
		FugueManaged: backend.FugueManaged, Billable: backend.Billable, CredentialGeneration: credentialGeneration,
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

func currentArtifactRef(spec backupcontrol.BackupRunSpec, run model.BackupRun, artifacts []model.BackupArtifact) (*backupcontrol.BackupArtifactRef, error) {
	if run.ArtifactCount != 1 {
		return nil, errLegacyArtifact
	}
	matching := make([]backupcontrol.BackupArtifactRef, 0, 1)
	for _, raw := range artifacts {
		artifact := model.NormalizeBackupArtifact(raw)
		if artifact.Status != model.BackupArtifactStatusActive || artifact.RunID != spec.RunID ||
			artifact.BackendID != spec.BackendID || artifact.Kind != spec.ArtifactKind {
			continue
		}
		artifactTarget, targetErr := TargetForRun(model.BackupRun{
			TenantID: artifact.TenantID, ProjectID: artifact.ProjectID, AppID: artifact.AppID,
			Target: artifact.Target,
		})
		if targetErr != nil || artifactTarget != spec.Target ||
			(strings.TrimSpace(run.TenantID) != "" && strings.TrimSpace(artifact.TenantID) != strings.TrimSpace(run.TenantID)) {
			return nil, errLegacyArtifact
		}
		contentDigest, ok := canonicalDigest(artifact.SHA256)
		if !ok {
			return nil, errLegacyArtifact
		}
		manifestDigest, ok := canonicalDigest(artifact.ManifestDigest)
		if !ok {
			return nil, errLegacyArtifact
		}
		if !legacySegmentPattern.MatchString(artifact.ID) {
			return nil, errLegacyArtifact
		}
		matching = append(matching, backupcontrol.BackupArtifactRef{
			ArtifactID: artifact.ID, RunID: artifact.RunID, Kind: artifact.Kind,
			ContentDigest: contentDigest, ManifestDigest: manifestDigest,
			BackendGeneration: spec.BackendGeneration,
		})
	}
	if len(matching) != 1 {
		return nil, errLegacyArtifact
	}
	return &matching[0], nil
}

func canonicalDigest(raw string) (string, bool) {
	raw = strings.TrimSpace(strings.ToLower(raw))
	raw = strings.TrimPrefix(raw, "sha256:")
	if len(raw) != 64 {
		return "", false
	}
	for _, char := range raw {
		if !((char >= '0' && char <= '9') || (char >= 'a' && char <= 'f')) {
			return "", false
		}
	}
	return "sha256:" + raw, true
}

func canonicalErrorCode(raw string) string {
	raw = strings.ToLower(strings.TrimSpace(raw))
	var builder strings.Builder
	lastSeparator := false
	for _, char := range raw {
		if (char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') {
			builder.WriteRune(char)
			lastSeparator = false
			continue
		}
		if !lastSeparator {
			builder.WriteByte('_')
			lastSeparator = true
		}
	}
	code := strings.Trim(builder.String(), "_-")
	if !legacyErrorPattern.MatchString(code) {
		code = "legacy_backup_failure"
	}
	if len(code) > 96 {
		code = code[:96]
		code = strings.TrimRight(code, "_-")
	}
	return code
}

func digestLegacyError(code, message string) string {
	digest := sha256.Sum256([]byte(strings.TrimSpace(code) + "\x00" + strings.TrimSpace(message)))
	return "sha256:" + hex.EncodeToString(digest[:])
}

func canonicalWorkerID(raw string) string {
	raw = strings.ToLower(strings.TrimSpace(raw))
	if legacyWorkerPattern.MatchString(raw) {
		return raw
	}
	digest := sha256.Sum256([]byte(raw))
	return "legacy-worker-" + hex.EncodeToString(digest[:])[:16]
}

func positiveFence(attempt int) int64 {
	if attempt < 1 {
		return 1
	}
	return int64(attempt)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
