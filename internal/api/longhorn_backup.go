package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
	"fugue/internal/store"
)

const (
	longhornBackupEngineEnabledEnv = "FUGUE_BACKUP_LONGHORN_ENABLED"
	longhornBackupNamespaceEnv     = "FUGUE_BACKUP_LONGHORN_NAMESPACE"
	longhornBackupTargetEnv        = "FUGUE_BACKUP_LONGHORN_TARGET_NAME"
	longhornBackupPollEnv          = "FUGUE_BACKUP_LONGHORN_POLL_INTERVAL"
)

var errLonghornBackupUnavailable = errors.New("longhorn snapshot backup is unavailable")

type longhornBackupStatus struct {
	State            string `json:"state"`
	Error            string `json:"error"`
	URL              string `json:"url"`
	VolumeName       string `json:"volumeName"`
	VolumeSize       string `json:"volumeSize"`
	NewlyUploadBytes string `json:"newlyUploadDataSize"`
	SnapshotName     string `json:"snapshotName"`
	BackupTargetName string `json:"backupTargetName"`
}

type longhornSnapshotStatus struct {
	ReadyToUse bool   `json:"readyToUse"`
	Error      string `json:"error"`
	Size       int64  `json:"size"`
}

// runAppDatabaseLonghornBackup asks Longhorn to snapshot the managed Postgres
// volume and upload the snapshot blocks to its configured R2 backup target.
// It deliberately does not read the live PVC and never falls back to pg_dump.
func (s *Server) runAppDatabaseLonghornBackup(ctx context.Context, run model.BackupRun, app model.App, backend model.BackupBackend) ([]model.BackupArtifact, error) {
	if !longhornBackupEnabled() {
		return nil, fmt.Errorf("%w: %s is not enabled", errLonghornBackupUnavailable, longhornBackupEngineEnabledEnv)
	}
	if model.NormalizeDataBackendProvider(backend.Provider) != model.DataBackendProviderCloudflareR2 {
		return nil, fmt.Errorf("%w: Longhorn backup requires a Cloudflare R2 backend", errLonghornBackupUnavailable)
	}
	client, err := newKubeLogsClient("")
	if err != nil {
		return nil, fmt.Errorf("%w: kubernetes client: %v", errLonghornBackupUnavailable, err)
	}
	postgres := store.OwnedManagedPostgresSpec(app)
	if postgres == nil {
		return nil, fmt.Errorf("managed_postgres_missing: app has no Fugue-managed PostgreSQL database")
	}
	serviceName := firstNonEmptyString(postgres.ServiceName, run.Target.ServiceName)
	namespace := runtimeNamespaceForApp(app)
	pvc, volumeName, err := longhornPostgresVolume(ctx, client, namespace, serviceName)
	if err != nil {
		return nil, err
	}
	_ = pvc
	longhornNS := firstNonEmptyString(strings.TrimSpace(getenv(longhornBackupNamespaceEnv)), "longhorn-system")
	targetName := firstNonEmptyString(strings.TrimSpace(getenv(longhornBackupTargetEnv)), "default")
	volume, err := longhornGet(ctx, client, longhornNS, "volumes", volumeName)
	if err != nil {
		return nil, fmt.Errorf("%w: read volume %s: %v", errLonghornBackupUnavailable, volumeName, err)
	}
	volumeSpec, _ := volume["spec"].(map[string]any)
	if strings.TrimSpace(longhornStringValue(volumeSpec["backupTargetName"])) != targetName {
		return nil, fmt.Errorf("%w: volume %s backupTargetName is not %q", errLonghornBackupUnavailable, volumeName, targetName)
	}
	backupTarget, err := longhornGet(ctx, client, longhornNS, "backuptargets", targetName)
	if err != nil {
		return nil, fmt.Errorf("%w: read backup target %s: %v", errLonghornBackupUnavailable, targetName, err)
	}
	targetSpec, _ := backupTarget["spec"].(map[string]any)
	targetStatus, _ := backupTarget["status"].(map[string]any)
	if !boolValue(targetStatus["available"]) {
		return nil, fmt.Errorf("%w: backup target %s is not available", errLonghornBackupUnavailable, targetName)
	}
	backupTargetURL := longhornStringValue(targetSpec["backupTargetURL"])
	if !strings.HasPrefix(strings.ToLower(backupTargetURL), "s3://") {
		return nil, fmt.Errorf("%w: backup target %s is not an S3/R2 target", errLonghornBackupUnavailable, targetName)
	}

	namePrefix := "fugue-" + strings.Trim(strings.ReplaceAll(run.ID, "_", "-"), "-") + "-"
	snapshot := map[string]any{
		"apiVersion": "longhorn.io/v1beta2", "kind": "Snapshot",
		"metadata": map[string]any{"generateName": namePrefix, "namespace": longhornNS, "labels": map[string]string{"fugue.pro/backup-run": run.ID}},
		"spec":     map[string]any{"volume": volumeName, "createSnapshot": true},
	}
	var createdSnapshot map[string]any
	if err := longhornCreate(ctx, client, longhornNS, "snapshots", snapshot, &createdSnapshot); err != nil {
		return nil, fmt.Errorf("%w: create snapshot: %v", errLonghornBackupUnavailable, err)
	}
	snapshotName := longhornStringValue(createdSnapshot["metadata"].(map[string]any)["name"])
	defer func() {
		deleteCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		_ = longhornDelete(deleteCtx, client, longhornNS, "snapshots", snapshotName)
	}()
	if err := waitForLonghornSnapshot(ctx, client, longhornNS, snapshotName); err != nil {
		return nil, err
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	backup := map[string]any{
		"apiVersion": "longhorn.io/v1beta2", "kind": "Backup",
		"metadata": map[string]any{"generateName": namePrefix, "namespace": longhornNS, "labels": map[string]string{"fugue.pro/backup-run": run.ID}},
		"spec":     map[string]any{"snapshotName": snapshotName, "backupMode": "incremental", "syncRequestedAt": now},
	}
	var createdBackup map[string]any
	if err := longhornCreate(ctx, client, longhornNS, "backups", backup, &createdBackup); err != nil {
		return nil, fmt.Errorf("%w: create backup: %v", errLonghornBackupUnavailable, err)
	}
	backupName := longhornStringValue(createdBackup["metadata"].(map[string]any)["name"])
	status, err := waitForLonghornBackup(ctx, client, longhornNS, backupName, targetName)
	if err != nil {
		return nil, err
	}

	backendObject, err := newDataObjectBackend(model.BackupBackendAsDataBackend(backend))
	if err != nil {
		return nil, err
	}
	version := backupVersionLabel(run.Version)
	target := model.NormalizeBackupTarget(run.Target)
	target.Type, target.TenantID, target.ProjectID, target.AppID = model.BackupTargetAppDatabase, app.TenantID, app.ProjectID, app.ID
	target.Name, target.ServiceName, target.Database = app.Name, serviceName, postgres.Database
	baseKey := path.Join("apps", app.TenantID, app.ProjectID, app.ID, run.ID)
	manifestKey := baseKey + "/manifest.json"
	logicalBytes, _ := strconv.ParseInt(strings.TrimSpace(status.VolumeSize), 10, 64)
	if logicalBytes < 0 {
		logicalBytes = 0
	}
	manifest := model.NormalizeBackupManifest(model.BackupManifest{
		RunID: run.ID, PolicyID: run.PolicyID, Target: target,
		Kind: model.BackupArtifactKindLonghornSnapshot, Version: version, Format: "longhorn-remote-backup",
		ManifestObjectKey: manifestKey, SizeBytes: logicalBytes, LogicalBytes: logicalBytes,
		Metadata: map[string]string{
			"backup_engine": "longhorn-snapshot", "backup_source": "longhorn-volume", "volume_name": volumeName,
			"snapshot_name": snapshotName, "longhorn_backup_name": backupName, "longhorn_backup_url": status.URL,
			"backup_target_name": targetName, "storage_class": longhornStringValue(pvc["spec"].(map[string]any)["storageClassName"]),
			"backup_target_url": backupTargetURL,
			"local_staging":     "false", "database_replica": "false", "pg_dump": "false", "wal_required": "true",
		}, CreatedAt: time.Now().UTC(),
	})
	manifestBytes, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, err
	}
	if err := backendObject.putObject(ctx, manifestKey, strings.NewReader(string(manifestBytes)), int64(len(manifestBytes))); err != nil {
		return nil, fmt.Errorf("upload Longhorn backup manifest: %w", err)
	}
	artifact, err := s.store.CreateBackupArtifactForRun(model.BackupArtifact{
		RunID: run.ID, PolicyID: run.PolicyID, TenantID: app.TenantID, ProjectID: app.ProjectID, AppID: app.ID,
		Target: target, BackendID: backend.ID, Kind: model.BackupArtifactKindLonghornSnapshot, Version: version,
		ObjectKey: "", ManifestObjectKey: manifestKey, SizeBytes: logicalBytes, LogicalBytes: logicalBytes,
		Status: model.BackupArtifactStatusActive, Billable: backend.Billable, BillingClass: backupBillingClass(backend), Manifest: manifest,
	}, run.LeaseOwner)
	if err != nil {
		return nil, err
	}
	return []model.BackupArtifact{artifact}, nil
}

func runtimeNamespaceForApp(app model.App) string { return runtimepkg.NamespaceForTenant(app.TenantID) }

func longhornBackupEnabled() bool {
	v := strings.ToLower(strings.TrimSpace(getenv(longhornBackupEngineEnabledEnv)))
	return v == "1" || v == "true" || v == "yes"
}

func getenv(key string) string { return strings.TrimSpace(os.Getenv(key)) }

func longhornStringValue(v any) string { s, _ := v.(string); return strings.TrimSpace(s) }

func longhornPostgresVolume(ctx context.Context, client *kubeLogsClient, namespace, serviceName string) (map[string]any, string, error) {
	query := url.Values{"labelSelector": []string{"cnpg.io/cluster=" + serviceName}}
	var list map[string]any
	if err := client.doJSON(ctx, http.MethodGet, "/api/v1/namespaces/"+url.PathEscape(namespace)+"/persistentvolumeclaims?"+query.Encode(), &list); err != nil {
		return nil, "", fmt.Errorf("list postgres PVCs: %w", err)
	}
	items, _ := list["items"].([]any)
	for _, raw := range items {
		pvc, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		spec, _ := pvc["spec"].(map[string]any)
		pv := longhornStringValue(spec["volumeName"])
		if pv == "" {
			continue
		}
		var pvObj map[string]any
		if err := client.doJSON(ctx, http.MethodGet, "/api/v1/persistentvolumes/"+url.PathEscape(pv), &pvObj); err != nil {
			return nil, "", fmt.Errorf("read postgres PV %s: %w", pv, err)
		}
		pvSpec, _ := pvObj["spec"].(map[string]any)
		csi, _ := pvSpec["csi"].(map[string]any)
		handle := longhornStringValue(csi["volumeHandle"])
		if longhornStringValue(csi["driver"]) != "driver.longhorn.io" || handle == "" {
			return nil, "", fmt.Errorf("%w: postgres PVC uses CSI driver %q", errLonghornBackupUnavailable, longhornStringValue(csi["driver"]))
		}
		return pvc, handle, nil
	}
	return nil, "", fmt.Errorf("%w: no CNPG PVC for cluster %s", errLonghornBackupUnavailable, serviceName)
}

func longhornGet(ctx context.Context, client *kubeLogsClient, namespace, resource, name string) (map[string]any, error) {
	var out map[string]any
	err := client.doJSON(ctx, http.MethodGet, "/apis/longhorn.io/v1beta2/namespaces/"+url.PathEscape(namespace)+"/"+resource+"/"+url.PathEscape(name), &out)
	return out, err
}

func longhornCreate(ctx context.Context, client *kubeLogsClient, namespace, resource string, body map[string]any, out *map[string]any) error {
	return client.doJSONBody(ctx, http.MethodPost, "/apis/longhorn.io/v1beta2/namespaces/"+url.PathEscape(namespace)+"/"+resource, body, out)
}

func longhornDelete(ctx context.Context, client *kubeLogsClient, namespace, resource, name string) error {
	return client.doJSON(ctx, http.MethodDelete, "/apis/longhorn.io/v1beta2/namespaces/"+url.PathEscape(namespace)+"/"+resource+"/"+url.PathEscape(name), nil)
}

func waitForLonghornSnapshot(ctx context.Context, client *kubeLogsClient, namespace, name string) error {
	return pollLonghorn(ctx, func() (bool, error) {
		obj, err := longhornGet(ctx, client, namespace, "snapshots", name)
		if err != nil {
			return false, err
		}
		status, _ := obj["status"].(map[string]any)
		if e := longhornStringValue(status["error"]); e != "" {
			return false, fmt.Errorf("Longhorn snapshot failed: %s", e)
		}
		return boolValue(status["readyToUse"]), nil
	})
}

func waitForLonghornBackup(ctx context.Context, client *kubeLogsClient, namespace, name, target string) (longhornBackupStatus, error) {
	var latest longhornBackupStatus
	err := pollLonghorn(ctx, func() (bool, error) {
		obj, err := longhornGet(ctx, client, namespace, "backups", name)
		if err != nil {
			return false, err
		}
		statusRaw, _ := obj["status"].(map[string]any)
		b, _ := json.Marshal(statusRaw)
		_ = json.Unmarshal(b, &latest)
		if latest.BackupTargetName != "" && latest.BackupTargetName != target {
			return false, fmt.Errorf("Longhorn backup target mismatch: got %q want %q", latest.BackupTargetName, target)
		}
		switch strings.ToLower(latest.State) {
		case "completed":
			return true, nil
		case "error":
			return false, fmt.Errorf("Longhorn backup failed: %s", latest.Error)
		}
		return false, nil
	})
	if err != nil {
		return latest, err
	}
	return latest, nil
}

func pollLonghorn(ctx context.Context, check func() (bool, error)) error {
	interval := 5 * time.Second
	if d, err := time.ParseDuration(strings.TrimSpace(getenv(longhornBackupPollEnv))); err == nil && d > 0 && d < time.Minute {
		interval = d
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		ok, err := check()
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func boolValue(v any) bool { b, _ := v.(bool); return b }
