package api

import (
	"context"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const orphanManifestCleanupLock = "fugue-backup-orphan-manifest-cleanup-v1"

type orphanManifestCleanupRequest struct {
	RunID        string `json:"run_id"`
	ObjectKey    string `json:"object_key"`
	ExpectedETag string `json:"expected_etag,omitempty"`
	Execute      bool   `json:"execute,omitempty"`
}

type orphanManifestCleanupResult struct {
	RunID       string   `json:"run_id"`
	ObjectKey   string   `json:"object_key"`
	BackendID   string   `json:"-"`
	SizeBytes   int64    `json:"size_bytes,omitempty"`
	ETag        string   `json:"etag,omitempty"`
	RunStatus   string   `json:"run_status"`
	RemoteState string   `json:"remote_state,omitempty"`
	CanDelete   bool     `json:"can_delete"`
	Executed    bool     `json:"executed"`
	Blockers    []string `json:"blockers"`
	Message     string   `json:"message,omitempty"`
	manifest    model.BackupManifest
	run         model.BackupRun
	logicalKey  string
}

const orphanManifestCleanupGrace = 24 * time.Hour

func (s *Server) handleCleanupOrphanManifest(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform administrator required")
		return
	}
	var req orphanManifestCleanupRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if strings.TrimSpace(req.RunID) == "" || strings.TrimSpace(req.ObjectKey) == "" {
		httpx.WriteError(w, http.StatusBadRequest, "run_id and object_key are required")
		return
	}
	if req.Execute && strings.TrimSpace(req.ExpectedETag) == "" {
		httpx.WriteError(w, http.StatusBadRequest, "expected_etag from the preview is required for execution")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	var result orphanManifestCleanupResult
	var err error
	if req.Execute {
		acquired, lockErr := s.store.WithAdvisoryLock(ctx, orphanManifestCleanupLock, func() error {
			result, err = s.inspectOrphanManifest(ctx, req)
			if err != nil || !result.CanDelete {
				return err
			}
			if err = s.deleteOrphanManifest(ctx, result.BackendID, req.ObjectKey, result.ETag); err != nil {
				return err
			}
			result.Executed = true
			result.Message = "orphan manifest deleted after immediate ETag recheck"
			return nil
		})
		if lockErr != nil {
			httpx.WriteError(w, http.StatusConflict, lockErr.Error())
			return
		}
		if !acquired {
			httpx.WriteError(w, http.StatusConflict, "another orphan cleanup is in progress")
			return
		}
		if err != nil {
			httpx.WriteError(w, http.StatusConflict, err.Error())
			return
		}
	} else {
		result, err = s.inspectOrphanManifest(ctx, req)
		if err != nil {
			httpx.WriteError(w, http.StatusConflict, err.Error())
			return
		}
	}
	s.appendAudit(principal, "backup.orphan_manifest.cleanup", "backup_run", result.RunID, "", map[string]string{
		"object_key": result.ObjectKey, "execute_requested": fmt.Sprint(req.Execute), "executed": fmt.Sprint(result.Executed), "can_delete": fmt.Sprint(result.CanDelete),
		"blockers": strings.Join(result.Blockers, "; "),
	})
	if !result.CanDelete && req.Execute {
		httpx.WriteJSON(w, http.StatusConflict, map[string]any{"cleanup": result})
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"cleanup": result})
}

func (s *Server) inspectOrphanManifest(ctx context.Context, req orphanManifestCleanupRequest) (orphanManifestCleanupResult, error) {
	return s.inspectUnregisteredManifest(ctx, req, false)
}

// Adoption shares the ownership checks, but checks remote presence separately.
// It must never reuse the cleanup eligibility result as deletion authority.
func (s *Server) inspectUnregisteredManifest(ctx context.Context, req orphanManifestCleanupRequest, adoption bool) (orphanManifestCleanupResult, error) {
	runID := strings.TrimSpace(req.RunID)
	physicalKey := req.ObjectKey
	result := orphanManifestCleanupResult{RunID: runID, ObjectKey: physicalKey, Blockers: []string{}}
	run, err := s.store.GetBackupRun(runID, "", true)
	if err != nil {
		return result, fmt.Errorf("load backup run: %w", err)
	}
	run = model.NormalizeBackupRun(run)
	result.run = run
	result.RunStatus = run.Status
	result.BackendID = run.BackendID
	if run.Status != model.BackupRunStatusFailed && run.Status != model.BackupRunStatusCanceled {
		result.Blockers = append(result.Blockers, "backup run is not failed or canceled")
	}
	if run.LockedUntil != nil && run.LockedUntil.After(time.Now()) {
		result.Blockers = append(result.Blockers, "backup run still has an active worker lease")
	}
	if exists, e := s.store.BackupArtifactExistsForRun(runID); e != nil {
		return result, fmt.Errorf("check artifact references: %w", e)
	} else if exists {
		result.Blockers = append(result.Blockers, "a backup artifact still references this run")
	}
	terminalAt := run.UpdatedAt
	if run.FinishedAt != nil {
		terminalAt = run.FinishedAt.UTC()
	}
	if terminalAt.IsZero() || time.Since(terminalAt) < orphanManifestCleanupGrace {
		result.Blockers = append(result.Blockers, "backup run has not passed the orphan-manifest safety grace period")
	}
	if run.BytesWritten != 0 || run.LogicalBytes != 0 || run.ArtifactCount != 0 {
		result.Blockers = append(result.Blockers, "backup run reports persisted bytes or artifacts")
	}
	if model.NormalizeBackupTarget(run.Target).Type != model.BackupTargetControlPlaneDatabase && model.NormalizeBackupTarget(run.Target).Type != model.BackupTargetAppDatabase {
		result.Blockers = append(result.Blockers, "backup target type is not eligible for manifest-only cleanup")
	}
	if !strings.HasSuffix(physicalKey, "/manifest.json") || !strings.Contains(physicalKey, "/"+runID+"/") || path.Clean(physicalKey) != physicalKey {
		result.Blockers = append(result.Blockers, "object key is not the canonical manifest for this run")
	}
	var manifest model.BackupManifest
	var objectBackend *dataObjectBackend
	backends, err := s.store.ListBackupBackends("", true)
	if err != nil {
		return result, fmt.Errorf("list backup backends: %w", err)
	}
	var backend model.BackupBackend
	backendByID := make(map[string]model.BackupBackend, len(backends))
	for _, candidate := range backends {
		backendByID[candidate.ID] = candidate
		if candidate.ID == run.BackendID {
			backend = candidate
		}
	}
	if backend.ID == "" {
		result.Blockers = append(result.Blockers, "backup backend is unavailable")
	} else if backend.Provider != model.DataBackendProviderCloudflareR2 {
		result.Blockers = append(result.Blockers, "backup backend is not Cloudflare R2")
	}
	if backend.ID != "" && backend.Provider == model.DataBackendProviderCloudflareR2 {
		full, e := s.store.GetBackupBackendForUse(backend.ID, "", true)
		if e != nil {
			return result, fmt.Errorf("resolve backup backend: %w", e)
		}
		objectBackend, e = newDataObjectBackend(model.BackupBackendAsDataBackend(full))
		if e != nil {
			return result, fmt.Errorf("open backup backend: %w", e)
		}
		logicalKey, ok := objectBackend.logicalObjectKey(physicalKey)
		result.logicalKey = logicalKey
		if !ok {
			result.Blockers = append(result.Blockers, "object is outside the configured backup namespace")
		} else if !orphanManifestLogicalKeyAllowed(run, logicalKey) {
			result.Blockers = append(result.Blockers, "object key does not match the failed database backup target")
		} else {
			info, exists, e := objectBackend.headObjectInfo(ctx, logicalKey)
			if e != nil {
				return result, fmt.Errorf("inspect manifest: %w", e)
			}
			if !exists {
				result.Blockers = append(result.Blockers, "manifest object is already absent")
			} else {
				result.SizeBytes, result.ETag = info.Size, info.ETag
				if req.Execute && req.ExpectedETag != info.ETag {
					result.Blockers = append(result.Blockers, "manifest ETag changed since the reviewed preview")
				}
				if info.Size <= 0 || info.Size > 1024*1024 || info.ETag == "" {
					result.Blockers = append(result.Blockers, "manifest size or version identity is invalid")
					return result, nil
				}
				response, e := objectBackend.client.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(objectBackend.backend.Bucket), Key: aws.String(physicalKey), IfMatch: aws.String(`"` + info.ETag + `"`),
				})
				if e != nil {
					return result, fmt.Errorf("read manifest: %w", e)
				}
				manifestBytes, readErr := io.ReadAll(io.LimitReader(response.Body, 1024*1024+1))
				closeErr := response.Body.Close()
				if readErr != nil {
					return result, fmt.Errorf("read manifest: %w", readErr)
				}
				if closeErr != nil {
					return result, fmt.Errorf("close manifest: %w", closeErr)
				}
				if len(manifestBytes) > 1024*1024 {
					result.Blockers = append(result.Blockers, "manifest exceeds the size limit")
				} else if e := json.Unmarshal(manifestBytes, &manifest); e != nil {
					result.Blockers = append(result.Blockers, "manifest is not valid Fugue JSON")
				} else {
					if manifest.RunID != runID {
						result.Blockers = append(result.Blockers, "manifest run_id does not match the requested run")
					}
					if strings.Trim(manifest.ManifestObjectKey, "/") != strings.Trim(physicalKey, "/") && strings.Trim(manifest.ManifestObjectKey, "/") != logicalKey {
						result.Blockers = append(result.Blockers, "manifest object key does not match the requested object")
					}
					if model.NormalizeBackupTarget(manifest.Target).Type != model.NormalizeBackupTarget(run.Target).Type {
						result.Blockers = append(result.Blockers, "manifest target does not match the failed backup run")
					}
					if model.NormalizeBackupTarget(run.Target).Engine != model.BackupEngineLonghornSnapshot &&
						(manifest.ObjectKey != "" || manifest.SizeBytes != 0 || manifest.LogicalBytes != 0 || len(manifest.Files) != 0) {
						result.Blockers = append(result.Blockers, "manifest describes persisted backup data; restore viability is not disproven")
					}
				}
				objects, e := objectBackend.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
					Bucket: aws.String(objectBackend.backend.Bucket), Prefix: aws.String(strings.TrimSuffix(physicalKey, "manifest.json")), MaxKeys: aws.Int32(2),
				})
				if e != nil {
					return result, fmt.Errorf("inspect run objects: %w", e)
				}
				if aws.ToBool(objects.IsTruncated) || len(objects.Contents) != 1 || aws.ToString(objects.Contents[0].Key) != physicalKey {
					result.Blockers = append(result.Blockers, "other objects remain in this backup run; refusing manifest-only cleanup")
				}
				references, e := s.store.ListBackupUsageArtifacts("", true)
				if e != nil {
					return result, fmt.Errorf("inspect manifest references: %w", e)
				}
				for _, reference := range references {
					a := reference.Artifact
					owner, known := backendByID[a.BackendID]
					if !known {
						result.Blockers = append(result.Blockers, "an artifact has an unresolved backend; reference inventory is incomplete")
						break
					}
					ownerBackend := model.BackupBackendAsDataBackend(owner)
					ownerKeys := &dataObjectBackend{backend: ownerBackend}
					for _, key := range []string{a.ManifestObjectKey, a.ObjectKey} {
						if key != "" && backupUsagePhysicalObjectID(ownerBackend, ownerKeys.objectKey(key)) == backupUsagePhysicalObjectID(objectBackend.backend, physicalKey) {
							result.Blockers = append(result.Blockers, "manifest key is referenced by a backup artifact")
						}
					}
				}
				if result.ETag == "" {
					result.Blockers = append(result.Blockers, "object version identity is unavailable")
				}
			}
		}
	}
	result.manifest = manifest
	if !adoption && model.NormalizeBackupTarget(run.Target).Engine == model.BackupEngineLonghornSnapshot {
		remoteState, remoteExists, remoteErr := s.inspectOrphanLonghornRemote(ctx, runID, manifest)
		result.RemoteState = remoteState
		if remoteErr != nil {
			result.Blockers = append(result.Blockers, remoteErr.Error())
		} else if remoteExists {
			result.Blockers = append(result.Blockers, "Longhorn remote backup still exists; repository data may be recoverable")
		} else if objectBackend == nil {
			result.Blockers = append(result.Blockers, "Longhorn repository backend is unavailable")
		} else if err := verifyOrphanLonghornRepositoryAbsent(ctx, objectBackend, manifest); err != nil {
			result.Blockers = append(result.Blockers, err.Error())
		}
	}
	result.CanDelete = !adoption && len(result.Blockers) == 0
	if result.CanDelete {
		result.Message = "preview passed; execution would delete exactly one unreferenced manifest"
	}
	return result, nil
}

// Longhorn Backup CR absence alone is insufficient: imported or stale CRs may
// omit repository backups. Verify the exact read-only repository config too.
// This layout follows longhorn/backupstore config.go getVolumePath and
// getBackupConfigPath (SHA-512 volume shards). A positive volume.cfg observation
// is required so an unknown/custom backupstore base can never imply absence.
func verifyOrphanLonghornRepositoryAbsent(ctx context.Context, backend *dataObjectBackend, manifest model.BackupManifest) error {
	volume, name := manifest.Metadata["volume_name"], manifest.Metadata["longhorn_backup_name"]
	if !longhornResourceName.MatchString(volume) || !longhornResourceName.MatchString(name) || manifest.Kind != model.BackupArtifactKindLonghornSnapshot {
		return errors.New("Longhorn manifest repository identity is invalid; refusing cleanup")
	}
	repository := backupRepositoryPrefix(model.BackupArtifact{Kind: manifest.Kind, Manifest: manifest}, backend.backend)
	if repository == "" {
		return errors.New("Longhorn repository does not belong to this backup backend; refusing cleanup")
	}
	backupURL, err := url.Parse(manifest.Metadata["longhorn_backup_url"])
	if err != nil || backupURL.Query().Get("backup") != name || backupURL.Query().Get("volume") != volume {
		return errors.New("Longhorn backup URL identity is invalid; refusing cleanup")
	}
	backupURL.RawQuery = ""
	if backupURL.String() != manifest.Metadata["backup_target_url"] {
		return errors.New("Longhorn backup URL repository differs; refusing cleanup")
	}
	sum := sha512.Sum512([]byte(volume))
	checksum := hex.EncodeToString(sum[:])
	base := path.Join(repository, "backupstore", "volumes", checksum[:2], checksum[2:4], volume)
	volumeKey, ok := backend.logicalObjectKey(path.Join(base, "volume.cfg"))
	if !ok {
		return errors.New("Longhorn volume configuration is outside the backup namespace")
	}
	if exists, err := backend.headObject(ctx, volumeKey); err != nil || !exists {
		return errors.New("Longhorn repository layout is unverified; refusing cleanup")
	}
	backupKey, ok := backend.logicalObjectKey(path.Join(base, "backups", "backup_"+name+".cfg"))
	if !ok {
		return errors.New("Longhorn backup configuration is outside the backup namespace")
	}
	exists, err := backend.headObject(ctx, backupKey)
	if err != nil {
		return errors.New("Longhorn repository backup cannot be inspected; refusing cleanup")
	}
	if exists {
		return errors.New("Longhorn repository backup configuration still exists; backup may be recoverable")
	}
	return nil
}

func orphanManifestLogicalKeyAllowed(run model.BackupRun, key string) bool {
	if path.Clean(key) != key || key == "" || strings.ContainsRune(key, '\x00') || strings.ContainsAny(run.ID, "/\\") || run.ID == "." || run.ID == ".." {
		return false
	}
	parts := strings.Split(key, "/")
	if len(parts) == 3 && parts[0] == "control-plane" {
		return model.NormalizeBackupTarget(run.Target).Type == model.BackupTargetControlPlaneDatabase && parts[1] == run.ID && parts[2] == "manifest.json"
	}
	// Older platform snapshots used the target component as a synthetic app ID.
	if len(parts) == 4 && parts[0] == "apps" && run.Target.Component != "" && parts[1] == run.Target.Component {
		return model.NormalizeBackupTarget(run.Target).Type == model.BackupTargetControlPlaneDatabase && parts[2] == run.ID && parts[3] == "manifest.json"
	}
	if len(parts) == 6 && parts[0] == "apps" && parts[4] == run.ID && parts[5] == "manifest.json" {
		target := model.NormalizeBackupTarget(run.Target)
		return target.Type == model.BackupTargetAppDatabase && parts[1] == firstNonEmptyString(run.TenantID, target.TenantID) && parts[2] == firstNonEmptyString(run.ProjectID, target.ProjectID) && parts[3] == firstNonEmptyString(run.AppID, target.AppID)
	}
	return false
}

func (s *Server) inspectOrphanLonghornRemote(ctx context.Context, runID string, manifest model.BackupManifest) (string, bool, error) {
	client, err := newKubeLogsClient("")
	if err != nil {
		return "", false, errors.New("Kubernetes Longhorn inventory is unavailable; refusing cleanup")
	}
	defer client.closeIdleConnections()
	namespace := firstNonEmptyString(manifest.Metadata["longhorn_namespace"], getenv(longhornBackupNamespaceEnv), "longhorn-system")
	state, exists, err := inspectOrphanLonghornRemoteWithClient(ctx, client, namespace, runID)
	if err != nil || exists {
		return state, exists, err
	}
	name := manifest.Metadata["longhorn_backup_name"]
	if !longhornResourceName.MatchString(name) || !longhornResourceName.MatchString(namespace) {
		return "", false, errors.New("manifest lacks a valid Longhorn backup identity; refusing cleanup")
	}
	// Check the recorded name too: repository synchronization can import CRs
	// without Fugue's original label.
	remote, err := longhornGet(ctx, client, namespace, "backups", name)
	if err == nil {
		status, _ := remote["status"].(map[string]any)
		return longhornStringValue(status["state"]), true, nil
	}
	if !isKubeNotFound(err) {
		return "", false, errors.New("recorded Longhorn remote backup cannot be inspected; refusing cleanup")
	}
	targetName := manifest.Metadata["backup_target_name"]
	if !longhornResourceName.MatchString(targetName) || manifest.Metadata["backup_target_url"] == "" {
		return "", false, errors.New("manifest lacks a valid Longhorn repository identity; refusing cleanup")
	}
	target, err := longhornGet(ctx, client, namespace, "backuptargets", targetName)
	if err != nil {
		return "", false, errors.New("Longhorn repository is unavailable; refusing cleanup")
	}
	spec, _ := target["spec"].(map[string]any)
	status, _ := target["status"].(map[string]any)
	if longhornStringValue(spec["backupTargetURL"]) != manifest.Metadata["backup_target_url"] || !boolValue(status["available"]) {
		return "", false, errors.New("Longhorn repository identity or availability is unverified; refusing cleanup")
	}
	return "", false, nil
}

func inspectOrphanLonghornRemoteWithClient(ctx context.Context, client *kubeLogsClient, namespace, runID string) (string, bool, error) {
	if client == nil {
		return "", false, errors.New("Longhorn remote inventory is unavailable; refusing cleanup")
	}
	continueToken := ""
	seenTokens := map[string]bool{}
	for page := 0; page < 20; page++ {
		query := "?limit=500&labelSelector=" + url.QueryEscape("fugue.pro/backup-run="+runID)
		if continueToken != "" {
			query += "&continue=" + url.QueryEscape(continueToken)
		}
		var list struct {
			Items    []map[string]any `json:"items"`
			Metadata struct {
				Continue string `json:"continue"`
			} `json:"metadata"`
		}
		apiPath := "/apis/longhorn.io/v1beta2/namespaces/" + url.PathEscape(namespace) + "/backups" + query
		if err := client.doJSON(ctx, http.MethodGet, apiPath, &list); err != nil {
			return "", false, errors.New("Longhorn remote inventory is unavailable; refusing cleanup")
		}
		for _, item := range list.Items {
			metadata, _ := item["metadata"].(map[string]any)
			labels, _ := metadata["labels"].(map[string]any)
			labelRunID := longhornStringValue(labels["fugue.pro/backup-run"])
			if labelRunID == "" {
				return "", false, errors.New("Longhorn remote inventory is ambiguous; refusing cleanup")
			}
			if labelRunID != runID {
				continue
			}
			status, _ := item["status"].(map[string]any)
			return longhornStringValue(status["state"]), true, nil
		}
		if list.Metadata.Continue == "" {
			return "", false, nil
		}
		if seenTokens[list.Metadata.Continue] {
			return "", false, errors.New("Longhorn remote inventory repeated a continuation token; refusing cleanup")
		}
		seenTokens[list.Metadata.Continue] = true
		continueToken = list.Metadata.Continue
	}
	return "", false, errors.New("Longhorn remote inventory is truncated; refusing cleanup")
}

func (s *Server) deleteOrphanManifest(ctx context.Context, backendID, physicalKey, etag string) error {
	backend, err := s.store.GetBackupBackendForUse(backendID, "", true)
	if err != nil {
		return fmt.Errorf("configured backup backend for object was not found: %w", err)
	}
	if backend.Provider != model.DataBackendProviderCloudflareR2 {
		return errors.New("configured backup backend is not Cloudflare R2")
	}
	objectBackend, err := newDataObjectBackend(model.BackupBackendAsDataBackend(backend))
	if err != nil {
		return err
	}
	if _, ok := objectBackend.logicalObjectKey(physicalKey); !ok {
		return errors.New("object is outside the configured backup namespace")
	}
	return objectBackend.deleteObjectIfMatch(ctx, physicalKey, etag)
}
