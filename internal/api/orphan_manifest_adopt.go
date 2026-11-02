package api

import (
	"context"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type orphanManifestAdoptRequest struct {
	RunID              string `json:"run_id"`
	ObjectKey          string `json:"object_key"`
	ExpectedETag       string `json:"expected_etag,omitempty"`
	ExpectedConfigETag string `json:"expected_config_etag,omitempty"`
	Execute            bool   `json:"execute,omitempty"`
}

type orphanManifestAdoptResult struct {
	RunID               string   `json:"run_id"`
	ObjectKey           string   `json:"object_key"`
	RunStatus           string   `json:"run_status"`
	ETag                string   `json:"etag,omitempty"`
	ConfigETag          string   `json:"config_etag,omitempty"`
	RemoteState         string   `json:"remote_state,omitempty"`
	CanAdopt            bool     `json:"can_adopt"`
	Executed            bool     `json:"executed"`
	ArtifactID          string   `json:"artifact_id,omitempty"`
	Protected           bool     `json:"protected"`
	RestoreVerification string   `json:"restore_verification"`
	BlockMappingCount   int      `json:"block_mapping_count,omitempty"`
	Blockers            []string `json:"blockers"`
	Message             string   `json:"message,omitempty"`
	run                 model.BackupRun
	artifact            model.BackupArtifact
	configKey           string
}

func (s *Server) handleAdoptOrphanManifest(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform administrator required")
		return
	}
	var req orphanManifestAdoptRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if strings.TrimSpace(req.RunID) == "" || strings.TrimSpace(req.ObjectKey) == "" || (req.Execute && (req.ExpectedETag == "" || req.ExpectedConfigETag == "")) {
		httpx.WriteError(w, http.StatusBadRequest, "run_id and object_key are required; execution also requires both preview ETags")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 45*time.Second)
	defer cancel()
	var result orphanManifestAdoptResult
	inspect := func() error {
		var err error
		result, err = s.inspectOrphanManifestAdoption(ctx, req)
		if err != nil || !result.CanAdopt || !req.Execute {
			return err
		}
		// Recheck both source versions immediately before the atomic DB write.
		backend, err := s.store.GetBackupBackendForUse(result.run.BackendID, "", true)
		if err != nil {
			return err
		}
		objects, err := newDataObjectBackend(model.BackupBackendAsDataBackend(backend))
		if err != nil {
			return err
		}
		for key, etag := range map[string]string{result.artifact.ManifestObjectKey: result.ETag, result.configKey: result.ConfigETag} {
			info, exists, err := objects.headObjectInfo(ctx, key)
			if err != nil || !exists || info.ETag != etag {
				return errors.New("remote source changed before registration; preview again")
			}
		}
		artifact, err := s.store.AdoptBackupArtifact(result.artifact, result.run)
		if err != nil {
			return fmt.Errorf("register protected artifact: %w", err)
		}
		result.ArtifactID, result.Executed = artifact.ID, true
		result.Message = "protected artifact registered; original run failure preserved; restore verification not performed"
		return nil
	}
	var err error
	if req.Execute {
		// Serialize against exact-manifest deletion and failed-run object GC.
		acquired, lockErr := s.store.WithAdvisoryLock(ctx, orphanManifestCleanupLock, func() error {
			gcAcquired, gcErr := s.store.WithAdvisoryLock(ctx, backupFailedRunGCLock, inspect)
			if gcErr != nil {
				return gcErr
			}
			if !gcAcquired {
				return errors.New("failed-run cleanup is in progress; retry reconciliation")
			}
			return nil
		})
		err = lockErr
		if err == nil && !acquired {
			err = errors.New("another manifest reconciliation is in progress")
		}
	} else {
		err = inspect()
	}
	if err != nil {
		httpx.WriteError(w, http.StatusConflict, err.Error())
		return
	}
	s.appendAudit(principal, "backup.orphan_manifest.adopt", "backup_run", result.RunID, "", map[string]string{
		"object_key": result.ObjectKey, "artifact_id": result.ArtifactID, "executed": fmt.Sprint(result.Executed),
		"restore_verification": result.RestoreVerification, "blockers": strings.Join(result.Blockers, "; "),
	})
	code := http.StatusOK
	if req.Execute && !result.CanAdopt {
		code = http.StatusConflict
	}
	httpx.WriteJSON(w, code, map[string]any{"adoption": result})
}

func (s *Server) inspectOrphanManifestAdoption(ctx context.Context, req orphanManifestAdoptRequest) (orphanManifestAdoptResult, error) {
	base, err := s.inspectUnregisteredManifest(ctx, orphanManifestCleanupRequest{
		RunID: req.RunID, ObjectKey: req.ObjectKey, Execute: req.Execute, ExpectedETag: req.ExpectedETag,
	}, true)
	result := orphanManifestAdoptResult{RunID: base.RunID, ObjectKey: base.ObjectKey, RunStatus: base.RunStatus, ETag: base.ETag, Protected: true, RestoreVerification: "not_run", Blockers: base.Blockers, run: base.run}
	if err != nil {
		return result, err
	}
	m, run := base.manifest, base.run
	if run.Target.Type != model.BackupTargetControlPlaneDatabase || run.Target.Engine != model.BackupEngineLonghornSnapshot || m.Kind != model.BackupArtifactKindLonghornSnapshot || m.Format != "longhorn-remote-backup" || (m.PolicyID != "" && m.PolicyID != run.PolicyID) || m.ObjectKey != "" || len(m.Files) != 0 || m.LogicalBytes <= 0 || m.SizeBytes <= 0 || m.CreatedAt.IsZero() {
		result.Blockers = append(result.Blockers, "only completed unregistered control-plane Longhorn manifests are eligible")
	}
	if len(result.Blockers) != 0 {
		return result, nil
	}
	if err := s.verifyAdoptionLonghornRemote(ctx, run.ID, m); err != nil {
		result.Blockers = append(result.Blockers, err.Error())
		return result, nil
	}
	result.RemoteState = "Completed"
	backend, err := s.store.GetBackupBackendForUse(run.BackendID, "", true)
	if err != nil {
		return result, err
	}
	objects, err := newDataObjectBackend(model.BackupBackendAsDataBackend(backend))
	if err != nil {
		return result, err
	}
	configKey, err := adoptionLonghornConfigKey(objects, m)
	if err != nil {
		result.Blockers = append(result.Blockers, err.Error())
		return result, nil
	}
	result.configKey = configKey
	info, exists, err := objects.headObjectInfo(ctx, configKey)
	if err != nil || !exists || info.ETag == "" || info.Size <= 0 || info.Size > 16*1024*1024 {
		result.Blockers = append(result.Blockers, "Longhorn repository config is absent or cannot be verified")
		return result, nil
	}
	result.ConfigETag = info.ETag
	if req.Execute && req.ExpectedConfigETag != info.ETag {
		result.Blockers = append(result.Blockers, "repository config ETag changed since preview")
		return result, nil
	}
	response, err := objects.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(backend.Bucket), Key: aws.String(objects.objectKey(configKey)), IfMatch: aws.String(`"` + info.ETag + `"`)})
	if err != nil {
		return result, fmt.Errorf("read Longhorn repository config: %w", err)
	}
	data, readErr := io.ReadAll(io.LimitReader(response.Body, 16*1024*1024+1))
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil || len(data) > 16*1024*1024 {
		return result, errors.New("cannot read bounded Longhorn repository config")
	}
	count, err := validateAdoptionLonghornConfig(data, m)
	if err != nil {
		result.Blockers = append(result.Blockers, err.Error())
		return result, nil
	}
	result.BlockMappingCount = count
	referenced, err := s.store.BackupArtifactExistsForLonghornURL(m.Metadata["longhorn_backup_url"])
	if err != nil {
		return result, err
	}
	if referenced {
		result.Blockers = append(result.Blockers, "Longhorn remote backup already has an artifact record")
		return result, nil
	}
	if m.Metadata == nil {
		m.Metadata = map[string]string{}
	}
	// Historical manifests omitted policy and namespace. Ownership comes from
	// the terminal run and its exact labeled remote snapshot, never user input.
	m.Metadata["longhorn_namespace"] = adoptionLonghornNamespace(m)
	sum := sha256.Sum256(data)
	m.Metadata["reconciliation_source"] = "completed-longhorn-orphan-manifest"
	m.Metadata["reconciled_at"] = time.Now().UTC().Format(time.RFC3339Nano)
	m.Metadata["source_manifest_etag"] = result.ETag
	m.Metadata["source_config_etag"] = result.ConfigETag
	m.Metadata["source_config_sha256"] = hex.EncodeToString(sum[:])
	m.Metadata["restore_verification"] = "not_run"
	m.Metadata["repository_verification"] = "config_identity_only"
	// Platform resources have no tenant application FK, including manifests
	// produced by the historical synthetic-app implementation.
	target := model.NormalizeBackupTarget(run.Target)
	target.TenantID, target.ProjectID, target.AppID = "", "", ""
	result.artifact = model.BackupArtifact{
		RunID: run.ID, PolicyID: run.PolicyID, BackendID: run.BackendID, Target: target, Kind: m.Kind,
		Version: m.Version, ManifestObjectKey: base.logicalKey, SizeBytes: m.SizeBytes, LogicalBytes: m.LogicalBytes,
		Status: model.BackupArtifactStatusActive, Protected: true, Billable: backend.Billable, BillingClass: backupBillingClass(backend),
		Manifest: m, CreatedAt: m.CreatedAt,
	}
	result.CanAdopt = true
	result.Message = "repository identity verified; can register protected artifact; block contents and database restore are not certified"
	return result, nil
}

func (s *Server) verifyAdoptionLonghornRemote(ctx context.Context, runID string, m model.BackupManifest) error {
	ns, name, volume, targetName := adoptionLonghornNamespace(m), m.Metadata["longhorn_backup_name"], m.Metadata["volume_name"], m.Metadata["backup_target_name"]
	for _, value := range []string{ns, name, volume, targetName} {
		if !longhornResourceName.MatchString(value) {
			return errors.New("manifest has invalid Longhorn identity")
		}
	}
	client, err := s.newBackupLonghornClient("")
	if err != nil {
		return errors.New("Longhorn inventory is unavailable")
	}
	defer client.closeIdleConnections()
	remote, err := longhornGet(ctx, client, ns, "backups", name)
	if err != nil {
		return fmt.Errorf("inspect recorded Longhorn backup: %w", err)
	}
	meta, _ := remote["metadata"].(map[string]any)
	labels, _ := meta["labels"].(map[string]any)
	status, _ := remote["status"].(map[string]any)
	spec, _ := remote["spec"].(map[string]any)
	if meta["deletionTimestamp"] != nil || longhornStringValue(labels["fugue.pro/backup-run"]) != runID || longhornStringValue(status["state"]) != "Completed" || longhornStringValue(status["error"]) != "" || longhornStringValue(status["volumeName"]) != volume || longhornStringValue(status["url"]) != m.Metadata["longhorn_backup_url"] || longhornStringValue(spec["snapshotName"]) != m.Metadata["snapshot_name"] {
		return errors.New("Longhorn backup is not a matching completed remote snapshot")
	}
	target, err := longhornGet(ctx, client, ns, "backuptargets", targetName)
	if err != nil {
		return fmt.Errorf("inspect Longhorn target: %w", err)
	}
	targetSpec, _ := target["spec"].(map[string]any)
	targetStatus, _ := target["status"].(map[string]any)
	if !boolValue(targetStatus["available"]) || longhornStringValue(targetSpec["backupTargetURL"]) != m.Metadata["backup_target_url"] {
		return errors.New("Longhorn target identity or availability is unverified")
	}
	return nil
}

func adoptionLonghornNamespace(m model.BackupManifest) string {
	return firstNonEmptyString(m.Metadata["longhorn_namespace"], getenv(longhornBackupNamespaceEnv), "longhorn-system")
}

func adoptionLonghornConfigKey(objects *dataObjectBackend, m model.BackupManifest) (string, error) {
	volume, name := m.Metadata["volume_name"], m.Metadata["longhorn_backup_name"]
	if !longhornResourceName.MatchString(volume) || !longhornResourceName.MatchString(name) {
		return "", errors.New("invalid Longhorn repository identity")
	}
	repository := backupRepositoryPrefix(model.BackupArtifact{Kind: m.Kind, Manifest: m}, objects.backend)
	u, err := url.Parse(m.Metadata["longhorn_backup_url"])
	if err != nil || repository == "" || u.Query().Get("backup") != name || u.Query().Get("volume") != volume {
		return "", errors.New("Longhorn repository is outside the configured backup backend")
	}
	u.RawQuery = ""
	if u.String() != m.Metadata["backup_target_url"] {
		return "", errors.New("Longhorn backup URL repository does not match the manifest")
	}
	sum := sha512.Sum512([]byte(volume))
	hash := hex.EncodeToString(sum[:])
	key, ok := objects.logicalObjectKey(path.Join(repository, "backupstore", "volumes", hash[:2], hash[2:4], volume, "backups", "backup_"+name+".cfg"))
	if !ok {
		return "", errors.New("Longhorn repository config is outside the backup namespace")
	}
	return key, nil
}

var longhornBlockChecksum = regexp.MustCompile(`^[0-9a-f]{64}$`)

func validateAdoptionLonghornConfig(data []byte, m model.BackupManifest) (int, error) {
	var config struct {
		Name, VolumeName, SnapshotName, CreatedTime, SnapshotCreatedAt, Size, CompressionMethod string
		Parameters                                                                              map[string]string
		Blocks                                                                                  []struct {
			Offset        int64
			BlockChecksum string
		}
	}
	if err := json.Unmarshal(data, &config); err != nil {
		return 0, errors.New("invalid Longhorn repository config JSON")
	}
	if config.Name != m.Metadata["longhorn_backup_name"] || config.VolumeName != m.Metadata["volume_name"] || config.SnapshotName != m.Metadata["snapshot_name"] || (config.CompressionMethod != "lz4" && config.CompressionMethod != "gzip") || len(config.Blocks) == 0 {
		return 0, errors.New("Longhorn repository config identity or block map is invalid")
	}
	created, err := time.Parse(time.RFC3339, config.CreatedTime)
	snapshot, snapErr := time.Parse(time.RFC3339, config.SnapshotCreatedAt)
	if err != nil || snapErr != nil || snapshot.After(created) || created.After(m.CreatedAt.Add(time.Minute)) {
		return 0, errors.New("Longhorn repository timestamps are invalid")
	}
	blockSize, err := strconv.ParseInt(config.Parameters["backup-block-size"], 10, 64)
	if err != nil || blockSize < 4096 || blockSize > 64*1024*1024 || blockSize&(blockSize-1) != 0 {
		return 0, errors.New("unsupported Longhorn repository block size")
	}
	dataSize, err := strconv.ParseInt(config.Size, 10, 64)
	if err != nil || dataSize != int64(len(config.Blocks))*blockSize || dataSize > m.LogicalBytes {
		return 0, errors.New("Longhorn repository size does not match its block map")
	}
	previous := int64(-1)
	for _, block := range config.Blocks {
		if block.Offset <= previous || block.Offset < 0 || block.Offset%blockSize != 0 || block.Offset > m.LogicalBytes-blockSize || !longhornBlockChecksum.MatchString(block.BlockChecksum) {
			return 0, errors.New("Longhorn repository contains invalid or duplicate block mappings")
		}
		previous = block.Offset
	}
	return len(config.Blocks), nil
}
