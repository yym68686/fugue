package api

import (
	"context"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"fugue/internal/model"
	"fugue/internal/store"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"
)

// Discover from the engine's paged inventory, rather than a truncated window
// of local runs. Unknown ownership is reported and never used as deletion proof.
func (s *Server) reconcileCompletedLonghornBackups(ctx context.Context, execute bool) (map[string]any, error) {
	result := map[string]any{"preview": !execute, "adopted": 0, "candidates": []string{}, "blocked": map[string]string{}, "restore_verification": "not_run"}
	if !longhornBackupEnabled() {
		result["disabled"] = true
		return result, nil
	}
	client, err := s.newBackupLonghornClient("")
	if err != nil {
		return result, err
	}
	defer client.closeIdleConnections()
	ns := firstNonEmptyString(getenv(longhornBackupNamespaceEnv), "longhorn-system")
	query := url.Values{"limit": {"500"}, "labelSelector": {"fugue.pro/backup-run"}}
	remotes := []map[string]any{}
	seen := map[string]bool{}
	for {
		var list struct {
			Items    []map[string]any `json:"items"`
			Metadata struct {
				Continue string `json:"continue"`
			} `json:"metadata"`
		}
		if err := client.doJSON(ctx, http.MethodGet, "/apis/longhorn.io/v1beta2/namespaces/"+url.PathEscape(ns)+"/backups?"+query.Encode(), &list); err != nil {
			return result, err
		}
		remotes = append(remotes, list.Items...)
		if list.Metadata.Continue == "" {
			break
		}
		if seen[list.Metadata.Continue] {
			return result, errors.New("Longhorn inventory repeated continuation token")
		}
		seen[list.Metadata.Continue] = true
		query.Set("continue", list.Metadata.Continue)
	}
	blocked := result["blocked"].(map[string]string)
	candidates := []string{}
	adopted := 0
	var retryErrors []error
	inventories := map[string]map[string]dataObjectInfo{}
	for _, remote := range remotes {
		meta, _ := remote["metadata"].(map[string]any)
		labels, _ := meta["labels"].(map[string]any)
		status, _ := remote["status"].(map[string]any)
		id := longhornStringValue(labels["fugue.pro/backup-run"])
		if longhornStringValue(status["state"]) != "Completed" || meta["deletionTimestamp"] != nil || id == "" {
			continue
		}
		exists, err := s.store.BackupArtifactExistsForRun(id)
		if err != nil {
			return result, err
		}
		if exists {
			continue
		}
		run, err := s.store.GetBackupRun(id, "", true)
		if err != nil {
			blocked[id] = "owning run unavailable"
			continue
		}
		terminalAt := run.UpdatedAt
		if run.FinishedAt != nil {
			terminalAt = *run.FinishedAt
		}
		if (run.Status != model.BackupRunStatusFailed && run.Status != model.BackupRunStatusCanceled) || time.Since(terminalAt) < 30*time.Minute || (run.LockedUntil != nil && run.LockedUntil.After(time.Now())) {
			continue
		}
		if run.Target.Engine != model.BackupEngineLonghornSnapshot {
			blocked[id] = "owning run is not a Longhorn backup"
			continue
		}
		backend, err := s.store.GetBackupBackendForUse(run.BackendID, "", true)
		if err != nil {
			blocked[id] = "backend unavailable"
			continue
		}
		objects, err := newDataObjectBackend(model.BackupBackendAsDataBackend(backend))
		if err != nil {
			return result, err
		}
		targetName := longhornStringValue(status["backupTargetName"])
		target, err := longhornGet(ctx, client, ns, "backuptargets", targetName)
		if err != nil {
			blocked[id] = "target unavailable"
			continue
		}
		targetSpec, _ := target["spec"].(map[string]any)
		m := model.BackupManifest{RunID: id, PolicyID: run.PolicyID, Target: run.Target, Kind: model.BackupArtifactKindLonghornSnapshot, Format: "longhorn-remote-backup", CreatedAt: time.Now().UTC(), LogicalBytes: longhornSizeBytes(longhornStringValue(status["volumeSize"])), SizeBytes: longhornSizeBytes(longhornStringValue(status["size"])), Metadata: map[string]string{
			"longhorn_namespace": ns, "longhorn_backup_name": longhornObjectName(remote), "volume_name": longhornStringValue(status["volumeName"]), "backup_target_name": targetName, "backup_target_url": longhornStringValue(targetSpec["backupTargetURL"]), "longhorn_backup_url": longhornStringValue(status["url"]), "snapshot_name": longhornStringValue(status["snapshotName"]),
		}}
		if m.LogicalBytes <= 0 {
			blocked[id] = "volume size unavailable"
			continue
		}
		configKey, err := adoptionLonghornConfigKey(objects, m)
		if err != nil {
			blocked[id] = err.Error()
			continue
		}
		info, present, err := objects.headObjectInfo(ctx, configKey)
		if err != nil {
			return result, err
		}
		if !present || info.ETag == "" || info.Size <= 0 || info.Size > 16*1024*1024 {
			blocked[id] = "config absent or outside supported bound"
			continue
		}
		reader, _, err := objects.getObject(ctx, configKey)
		if err != nil {
			return result, err
		}
		data, err := io.ReadAll(io.LimitReader(reader, 16*1024*1024+1))
		closeErr := reader.Close()
		if err != nil || closeErr != nil {
			return result, errors.Join(err, closeErr)
		}
		if _, err := validateAdoptionLonghornConfig(data, m); err != nil {
			blocked[id] = err.Error()
			continue
		}
		var config struct {
			CreatedTime string
			Blocks      []struct{ BlockChecksum string }
		}
		if err := json.Unmarshal(data, &config); err != nil {
			return result, err
		}
		volume := m.Metadata["volume_name"]
		sum := sha512.Sum512([]byte(volume))
		hash := hex.EncodeToString(sum[:])
		repository := backupRepositoryPrefix(model.BackupArtifact{Kind: m.Kind, Manifest: m}, objects.backend)
		blockPrefix := path.Join(repository, "backupstore", "volumes", hash[:2], hash[2:4], volume, "blocks") + "/"
		cacheKey := dataPhysicalBucket(objects.backend) + "/" + blockPrefix
		inventory, ok := inventories[cacheKey]
		if !ok {
			logical, ok := objects.logicalObjectKey(strings.TrimSuffix(blockPrefix, "/"))
			if !ok {
				return result, errors.New("block prefix outside backend")
			}
			rows, e := objects.listObjects(ctx, logical+"/")
			if e != nil {
				return result, e
			}
			inventory = map[string]dataObjectInfo{}
			for _, v := range rows {
				inventory[v.Key] = v
			}
			inventories[cacheKey] = inventory
		}
		missing := 0
		for _, b := range config.Blocks {
			key := blockPrefix + path.Join(b.BlockChecksum[:2], b.BlockChecksum[2:4], b.BlockChecksum+".blk")
			if inventory[key].Size <= 0 {
				missing++
			}
		}
		if missing > 0 {
			blocked[id] = fmt.Sprintf("%d referenced blocks absent", missing)
			continue
		}
		if err := s.verifyAdoptionLonghornRemote(ctx, id, m); err != nil {
			blocked[id] = err.Error()
			continue
		}
		candidates = append(candidates, id)
		if !execute {
			continue
		}
		// Serialize registration with manual adoption and failed-run manifest GC.
		acquired, err := s.store.WithAdvisoryLock(ctx, orphanManifestCleanupLock, func() error {
			locked, e := waitLonghornRegistrationLock(ctx, s.store, func() error {
				latest, present, e := objects.headObjectInfo(ctx, configKey)
				if e != nil {
					return e
				}
				if !present || latest.ETag != info.ETag {
					return errors.New("config changed before registration")
				}
				if e := s.verifyAdoptionLonghornRemote(ctx, id, m); e != nil {
					return e
				}
				if exists, e := s.store.BackupArtifactExistsForLonghornURL(m.Metadata["longhorn_backup_url"]); e != nil {
					return e
				} else if exists {
					return store.ErrConflict
				}
				m.CreatedAt, _ = time.Parse(time.RFC3339, config.CreatedTime)
				m.Metadata["repository_verification"] = "referenced_objects_present"
				m.Metadata["restore_verification"] = "not_run"
				m.Metadata["reconciliation_source"] = "completed-engine-reconciliation"
				m.Metadata["source_config_etag"] = info.ETag
				a := model.BackupArtifact{RunID: id, PolicyID: run.PolicyID, BackendID: run.BackendID, Target: run.Target, Kind: m.Kind, Version: run.Version, LogicalBytes: m.LogicalBytes, SizeBytes: m.SizeBytes, Manifest: m, Status: model.BackupArtifactStatusActive, Protected: time.Since(run.CreatedAt) > 7*24*time.Hour, CreatedAt: m.CreatedAt, TenantID: run.TenantID, ProjectID: run.ProjectID, AppID: run.AppID}
				a.Billable = backend.Billable
				a.BillingClass = backupBillingClass(backend)
				if a.Target.Type == model.BackupTargetControlPlaneDatabase {
					a.TenantID = ""
					a.ProjectID = ""
					a.AppID = ""
					a.Target.TenantID = ""
					a.Target.ProjectID = ""
					a.Target.AppID = ""
				}
				// Reuse an existing canonical manifest if it matches this exact backup.
				keys := []string{path.Join("control-plane", id, "manifest.json"), path.Join("apps", run.TenantID, run.ProjectID, run.AppID, id, "manifest.json")}
				if run.Target.Type == model.BackupTargetControlPlaneDatabase {
					keys = append(keys, path.Join("apps", "control-plane-postgres", id, "manifest.json"))
				}
				for _, key := range keys {
					inf, yes, e := objects.headObjectInfo(ctx, key)
					if e != nil {
						return e
					}
					if !yes {
						continue
					}
					if inf.Size > 1024*1024 {
						return errors.New("oversized source manifest")
					}
					r, _, e := objects.getObject(ctx, key)
					if e != nil {
						return e
					}
					raw, e := io.ReadAll(io.LimitReader(r, 1024*1024+1))
					r.Close()
					if e != nil {
						return e
					}
					var source model.BackupManifest
					if json.Unmarshal(raw, &source) != nil || source.RunID != id || source.Metadata["longhorn_backup_url"] != m.Metadata["longhorn_backup_url"] {
						return errors.New("existing manifest identity mismatch")
					}
					a.ManifestObjectKey = key
					break
				}
				_, e = s.store.RecoverCompletedBackupArtifact(a, run)
				return e
			})
			if e == nil && !locked {
				return store.ErrConflict
			}
			return e
		})
		if err != nil || !acquired {
			if err == nil {
				err = store.ErrConflict
			}
			blocked[id] = err.Error()
			retryErrors = append(retryErrors, fmt.Errorf("backup %s registration deferred: %w", id, err))
			continue
		}
		adopted++
	}
	result["adopted"] = adopted
	result["candidates"] = candidates
	return result, errors.Join(retryErrors...)
}

// Inventory takes time. A periodic manifest sweep may acquire its lock during
// that scan; wait for its bounded pass instead of repeatedly colliding at the
// same scheduler phase and re-listing the whole repository on every retry.
func waitLonghornRegistrationLock(ctx context.Context, db *store.Store, fn func() error) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 50*time.Second)
	defer cancel()
	for {
		acquired, err := db.WithAdvisoryLock(ctx, backupFailedRunGCLock, fn)
		if err != nil || acquired {
			return acquired, err
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false, ctx.Err()
		case <-timer.C:
		}
	}
}
