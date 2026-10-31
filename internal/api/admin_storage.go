package api

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"fugue/internal/backupusage"
	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/store"
)

type adminStorageNamespace struct {
	Kind        string     `json:"kind"`
	ResourceID  string     `json:"resource_id"`
	Name        string     `json:"name"`
	Bucket      string     `json:"bucket"`
	Prefix      string     `json:"prefix"`
	TenantID    string     `json:"tenant_id"`
	ProjectID   string     `json:"project_id"`
	Status      string     `json:"status"`
	UsedBytes   *int64     `json:"used_bytes,omitempty"`
	ObjectCount *int64     `json:"object_count,omitempty"`
	MeasuredAt  *time.Time `json:"measured_at,omitempty"`
	Stale       bool       `json:"stale"`
}
type adminStorageBucket struct {
	Name   string `json:"name"`
	Source string `json:"source"`
}
type adminStorageLonghornEntry struct {
	ArtifactID        string     `json:"artifact_id"`
	RunID             string     `json:"run_id"`
	Status            string     `json:"status"`
	BackupName        string     `json:"backup_name"`
	VolumeName        string     `json:"volume_name"`
	TargetName        string     `json:"target_name"`
	CleanupState      string     `json:"cleanup_state"`
	RemoteState       string     `json:"remote_state,omitempty"`
	Protected         bool       `json:"protected"`
	PhysicalDeletedAt *time.Time `json:"physical_deleted_at,omitempty"`
	AttemptedAt       *time.Time `json:"attempted_at,omitempty"`
	Error             string     `json:"error,omitempty"`
}
type adminStorageOverview struct {
	ObservedAt       time.Time                   `json:"observed_at"`
	Configured       bool                        `json:"configured"`
	AccountID        string                      `json:"account_id,omitempty"`
	BucketsComplete  bool                        `json:"buckets_complete"`
	LonghornComplete bool                        `json:"longhorn_complete"`
	MetricsStatus    string                      `json:"metrics_status"`
	BillingStatus    string                      `json:"billing_status"`
	Warnings         []string                    `json:"warnings"`
	Buckets          []adminStorageBucket        `json:"buckets"`
	Namespaces       []adminStorageNamespace     `json:"namespaces"`
	Longhorn         []adminStorageLonghornEntry `json:"longhorn"`
	BackupUsage      *backupusage.Usage          `json:"backup_usage,omitempty"`
}

func (s *Server) handleGetAdminStorageOverview(w http.ResponseWriter, r *http.Request) {
	if !mustPrincipal(r).IsPlatformAdmin() {
		httpx.WriteError(w, 403, "platform administrator required")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 25*time.Second)
	defer cancel()
	out := adminStorageOverview{ObservedAt: time.Now().UTC(), MetricsStatus: "unavailable: analytics integration not configured", BillingStatus: "unavailable: provider billing is not imported", Warnings: []string{}, Buckets: []adminStorageBucket{}, Namespaces: []adminStorageNamespace{}, Longhorn: []adminStorageLonghornEntry{}}
	buckets := map[string]string{}
	stores, err := s.store.ListObjectStores("", "")
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	for _, v := range stores {
		buckets[v.Bucket] = "configured"
		row := adminStorageNamespace{Kind: "project-store", ResourceID: v.ID, Name: v.Name, Bucket: v.Bucket, TenantID: v.TenantID, ProjectID: v.ProjectID, Status: v.Status, MeasuredAt: v.UsageMeasuredAt, Stale: v.UsageMeasuredAt == nil || time.Since(*v.UsageMeasuredAt) > 24*time.Hour}
		if v.UsageMeasuredAt != nil {
			row.UsedBytes, row.ObjectCount = &v.UsedBytes, &v.ObjectCount
		}
		out.Namespaces = append(out.Namespaces, row)
	}
	backends, err := s.store.ListBackupBackends("", true)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	for _, v := range backends {
		if v.Bucket != "" {
			buckets[v.Bucket] = "configured"
			out.Namespaces = append(out.Namespaces, adminStorageNamespace{Kind: "backup-backend", ResourceID: v.ID, Name: v.Name, Bucket: v.Bucket, Prefix: v.Prefix, TenantID: v.TenantID, Status: v.Status})
		}
	}
	data, err := s.store.ListDataBackends("", true)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	for _, v := range data {
		if v.Bucket != "" {
			buckets[v.Bucket] = "configured"
			out.Namespaces = append(out.Namespaces, adminStorageNamespace{Kind: "data-backend", ResourceID: v.ID, Name: v.Name, Bucket: v.Bucket, Prefix: v.Prefix, TenantID: v.TenantID, Status: v.Status})
		}
	}
	cfg, err := s.store.GetObjectStorageConfig()
	if err == nil && cfg.AccountID != "" {
		out.Configured = true
		out.AccountID = cfg.AccountID
		provider, e := s.objectStorageProvider(cfg)
		if e == nil {
			var names []string
			names, e = provider.ListBuckets(ctx)
			if e == nil {
				out.BucketsComplete = true
				for _, name := range names {
					buckets[name] = "provider"
				}
			}
		}
		if e != nil {
			out.Warnings = append(out.Warnings, "Provider bucket inventory unavailable; configured namespaces are not a complete account inventory.")
		}
	} else {
		out.Warnings = append(out.Warnings, "Provider account is not configured or its configuration could not be read.")
	}
	if usage, e := s.loadBackupUsage(ctx, "", true); e == nil {
		out.BackupUsage = &usage
	} else {
		out.Warnings = append(out.Warnings, "Cached backup usage unavailable.")
	}
	client, e := newKubeLogsClient("")
	if e == nil {
		defer client.closeIdleConnections()
		s.adminStorageCNPGNamespaces(ctx, client, &out, buckets)
		s.adminStorageLonghorn(ctx, client, &out)
	} else {
		out.Warnings = append(out.Warnings, "Kubernetes observations unavailable; CNPG and Longhorn state is unknown.")
	}
	for name, source := range buckets {
		out.Buckets = append(out.Buckets, adminStorageBucket{Name: name, Source: source})
	}
	sort.Slice(out.Buckets, func(i, j int) bool { return out.Buckets[i].Name < out.Buckets[j].Name })
	httpx.WriteJSON(w, 200, out)
}

func (s *Server) adminStorageCNPGNamespaces(ctx context.Context, client *kubeLogsClient, out *adminStorageOverview, buckets map[string]string) {
	var list struct {
		Items []struct {
			Metadata struct{ Name, Namespace string }
			Spec     struct {
				Backup struct {
					RetentionPolicy   string
					BarmanObjectStore struct{ DestinationPath string }
				}
			}
		}
		Metadata struct{ Continue string }
	}
	if err := client.doJSON(ctx, http.MethodGet, "/apis/postgresql.cnpg.io/v1/clusters?limit=500", &list); err != nil {
		out.Warnings = append(out.Warnings, "CNPG backup destinations unavailable.")
		return
	}
	if list.Metadata.Continue != "" {
		out.Warnings = append(out.Warnings, "CNPG destination inventory truncated at 500 clusters.")
	}
	for _, v := range list.Items {
		u, e := url.Parse(v.Spec.Backup.BarmanObjectStore.DestinationPath)
		if e != nil || u.Scheme != "s3" || u.Host == "" || u.User != nil {
			continue
		}
		bucket := u.Host
		if _, ok := buckets[bucket]; !ok {
			buckets[bucket] = "configured"
		}
		out.Namespaces = append(out.Namespaces, adminStorageNamespace{Kind: "cnpg-backup", ResourceID: v.Metadata.Namespace + "/" + v.Metadata.Name, Name: v.Metadata.Name, Bucket: bucket, Prefix: strings.Trim(u.Path, "/"), Status: "retention=" + v.Spec.Backup.RetentionPolicy})
	}
}

func (s *Server) adminStorageLonghorn(ctx context.Context, client *kubeLogsClient, out *adminStorageOverview) {
	items, err := s.store.ListLonghornReconciliationArtifacts(501)
	if err != nil {
		out.Warnings = append(out.Warnings, "Longhorn artifact reconciliation unavailable.")
		return
	}
	if len(items) > 500 {
		items = items[:500]
		out.Warnings = append(out.Warnings, "Longhorn reconciliation truncated at 500 newest artifacts.")
	} else {
		out.LonghornComplete = true
	}
	eligible, err := s.store.ListBackupArtifactCleanupCandidates(store.BackupArtifactCleanupFilter{Before: time.Now().UTC().Add(-backupArtifactGCGrace), Limit: 501, Kind: model.BackupArtifactKindLonghornSnapshot})
	if err != nil {
		out.LonghornComplete = false
		out.Warnings = append(out.Warnings, "Cleanup eligibility unavailable.")
		return
	}
	eligibleIDs := map[string]bool{}
	for _, a := range eligible {
		eligibleIDs[a.ID] = true
	}
	lists := map[string]map[string]map[string]any{}
	failed := map[string]bool{}
	matched := map[string]bool{}
	namespaces := map[string]bool{firstNonEmptyString(getenv(longhornBackupNamespaceEnv), "longhorn-system"): true}
	for _, v := range items {
		namespaces[firstNonEmptyString(v.Artifact.Manifest.Metadata["longhorn_namespace"], getenv(longhornBackupNamespaceEnv), "longhorn-system")] = true
	}
	for ns := range namespaces {
		if !longhornResourceName.MatchString(ns) {
			failed[ns] = true
			out.LonghornComplete = false
			continue
		}
		{
			var list struct {
				Items    []map[string]any `json:"items"`
				Metadata struct {
					Continue string `json:"continue"`
				} `json:"metadata"`
			}
			e := client.doJSON(ctx, http.MethodGet, "/apis/longhorn.io/v1beta2/namespaces/"+url.PathEscape(ns)+"/backups?limit=500", &list)
			if e != nil || list.Metadata.Continue != "" {
				failed[ns] = true
				out.LonghornComplete = false
				out.Warnings = append(out.Warnings, "Longhorn remote inventory unavailable or truncated.")
			} else {
				lists[ns] = map[string]map[string]any{}
				for _, obj := range list.Items {
					lists[ns][longhornObjectName(obj)] = obj
				}
			}
		}
	}
	for _, v := range items {
		a := v.Artifact
		m := a.Manifest.Metadata
		ns := firstNonEmptyString(m["longhorn_namespace"], getenv(longhornBackupNamespaceEnv), "longhorn-system")
		entry := adminStorageLonghornEntry{ArtifactID: a.ID, RunID: a.RunID, Status: a.Status, BackupName: m["longhorn_backup_name"], VolumeName: m["volume_name"], TargetName: m["backup_target_name"], Protected: a.Protected, PhysicalDeletedAt: v.PhysicalDeletedAt, AttemptedAt: v.PhysicalDeleteAttemptedAt, CleanupState: "active"}
		matched[ns+"/"+entry.BackupName] = true
		terminal := a.Status == model.BackupArtifactStatusDeleted || a.Status == model.BackupArtifactStatusExpired
		switch {
		case a.Protected:
			entry.CleanupState = "protected"
		case terminal && v.PhysicalDeletedAt != nil && m[model.BackupLonghornCleanupVersionKey] == model.BackupLonghornCleanupVersion:
			entry.CleanupState = "confirmed"
		case terminal && eligibleIDs[a.ID]:
			entry.CleanupState = "pending"
		case terminal && a.DeletedAt != nil && time.Since(*a.DeletedAt) < backupArtifactGCGrace:
			entry.CleanupState = "grace"
		case terminal:
			entry.CleanupState = "blocked"
		}
		if failed[ns] {
			entry.CleanupState = "unavailable"
		} else if remote, ok := lists[ns][entry.BackupName]; ok {
			metadata, _ := remote["metadata"].(map[string]any)
			labels, _ := metadata["labels"].(map[string]any)
			status, _ := remote["status"].(map[string]any)
			entry.RemoteState = longhornStringValue(status["state"])
			if terminal && v.PhysicalDeletedAt != nil {
				entry.CleanupState = "legacy_receipt"
			}
			if longhornStringValue(metadata["deletionTimestamp"]) != "" {
				entry.CleanupState = "deleting"
			}
			if longhornStringValue(labels["fugue.pro/backup-run"]) != a.RunID || longhornStringValue(status["volumeName"]) != entry.VolumeName || longhornStringValue(status["backupTargetName"]) != entry.TargetName || longhornStringValue(status["url"]) != m["longhorn_backup_url"] {
				entry.CleanupState = "blocked"
				entry.Error = "Remote backup identity does not match the artifact; automatic cleanup is blocked."
			}
		} else if entry.CleanupState != "confirmed" && !terminal {
			entry.CleanupState = "missing"
		}
		if v.PhysicalDeleteError != "" {
			entry.Error = "Cleanup pending or failed; inspect server audit/logs for details."
		}
		out.Longhorn = append(out.Longhorn, entry)
	}
	for ns, remote := range lists {
		for name, obj := range remote {
			if matched[ns+"/"+name] {
				continue
			}
			metadata, _ := obj["metadata"].(map[string]any)
			labels, _ := metadata["labels"].(map[string]any)
			status, _ := obj["status"].(map[string]any)
			out.Longhorn = append(out.Longhorn, adminStorageLonghornEntry{BackupName: name, RunID: longhornStringValue(labels["fugue.pro/backup-run"]), VolumeName: longhornStringValue(status["volumeName"]), TargetName: longhornStringValue(status["backupTargetName"]), RemoteState: longhornStringValue(status["state"]), Status: "untracked", CleanupState: "blocked", Error: "No matching artifact in bounded inventory; never automatically deleted."})
		}
	}
	if len(items) > 0 {
		out.Warnings = append(out.Warnings, fmt.Sprintf("Longhorn shared-block bytes are not attributable by snapshot size; %d artifact records inspected.", len(items)))
	}
}
