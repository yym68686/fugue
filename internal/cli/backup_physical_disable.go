package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/spf13/cobra"
)

// The disable intent shares the retention Lease. No part of this operation
// deletes a Backup, Secret, PVC, object, multipart upload or Longhorn resource.
type physicalBarmanShutdown struct {
	State                  string   `json:"state"`
	DisableRequested       bool     `json:"disable_requested"`
	WALArchivingConfigured bool     `json:"wal_archiving_configured"`
	RetentionSuspended     bool     `json:"retention_suspended"`
	SchedulesSuspended     bool     `json:"schedules_suspended"`
	ActiveBackups          []string `json:"active_backups"`
	Completed              bool     `json:"completed"`
	HistoryDeleted         bool     `json:"history_deleted"`
	RuntimeVerified        bool     `json:"runtime_verified"`
	Warning                string   `json:"warning"`
	schedules              []map[string]any
	cron                   map[string]any
}

func physicalBackupItems(ctx context.Context, kube physicalKubeFunc, resource string, allNamespaces bool) ([]map[string]any, error) {
	args := []string{"get", resource, "--chunk-size=0", "-o", "json"}
	if allNamespaces {
		args = append(args, "--all-namespaces")
	}
	list, err := kube(ctx, nil, args...)
	if err != nil {
		return nil, err
	}
	items, ok := list["items"].([]any)
	if !ok || len(items) > 500 || physicalString(physicalMap(list, "metadata"), "continue") != "" {
		return nil, fmt.Errorf("%s inventory is invalid, truncated or exceeds safety bound", resource)
	}
	result := make([]map[string]any, 0, len(items))
	for _, item := range items {
		object, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("invalid %s inventory item", resource)
		}
		result = append(result, object)
	}
	return result, nil
}

func inspectPhysicalBarmanShutdown(ctx context.Context, kube physicalKubeFunc, opts physicalBackupOptions, cm, cluster map[string]any, policy physicalBackupPolicy) (physicalBarmanShutdown, error) {
	state := physicalBarmanShutdown{State: "enabled", DisableRequested: policy.BarmanDisabled,
		WALArchivingConfigured: len(physicalMap(physicalMap(physicalMap(cluster, "spec"), "backup"), "barmanObjectStore")) > 0,
		SchedulesSuspended:     true, ActiveBackups: []string{},
		Warning: "Disabling Barman stops future base backups and continuous WAL uploads, losing future PITR coverage. Existing archive objects, PostgreSQL local WAL and Longhorn policies are unchanged. Configuration read-back does not prove runtime reload or a successful Longhorn restore."}
	owner := physicalString(physicalMap(physicalMap(cluster, "metadata"), "annotations"), physicalBarmanDisabledAnnotation)
	uid := physicalString(physicalMap(cm, "metadata"), "uid")
	if owner != "" && owner != uid {
		return state, fmt.Errorf("a different policy owns Barman shutdown")
	}
	schedules, err := physicalBackupItems(ctx, kube, "scheduledbackups.postgresql.cnpg.io", false)
	if err != nil {
		return state, err
	}
	for _, schedule := range schedules {
		spec := physicalMap(schedule, "spec")
		if physicalString(physicalMap(spec, "cluster"), "name") != policy.Cluster {
			continue
		}
		method := physicalString(spec, "method")
		if method != "" && method != "barmanObjectStore" {
			continue
		}
		meta := physicalMap(schedule, "metadata")
		if physicalString(meta, "namespace") != opts.namespace || physicalString(meta, "name") == "" || physicalString(meta, "uid") == "" || physicalString(meta, "resourceVersion") == "" {
			return state, fmt.Errorf("invalid Barman schedule identity")
		}
		state.schedules = append(state.schedules, schedule)
		if spec["suspend"] != true {
			state.SchedulesSuspended = false
		}
	}
	backups, err := physicalBackupItems(ctx, kube, "backups.postgresql.cnpg.io", false)
	if err != nil {
		return state, err
	}
	for _, backup := range backups {
		if physicalString(physicalMap(physicalMap(backup, "spec"), "cluster"), "name") == policy.Cluster {
			phase := physicalString(physicalMap(backup, "status"), "phase")
			if phase != "completed" && phase != "failed" {
				name := physicalString(physicalMap(backup, "metadata"), "name")
				if name == "" {
					return state, fmt.Errorf("active backup identity is missing")
				}
				state.ActiveBackups = append(state.ActiveBackups, name)
			}
		}
	}
	state.cron, err = kube(ctx, nil, "get", "cronjob", opts.policy, "-o", "json")
	if err != nil {
		return state, err
	}
	meta := physicalMap(state.cron, "metadata")
	if physicalString(meta, "name") != opts.policy || physicalString(meta, "namespace") != opts.namespace || physicalString(meta, "uid") == "" || physicalString(meta, "resourceVersion") == "" {
		return state, fmt.Errorf("invalid retention CronJob identity")
	}
	state.RetentionSuspended = physicalMap(state.cron, "spec")["suspend"] == true
	state.Completed = policy.BarmanDisabled && !policy.Enabled && owner == uid && !state.WALArchivingConfigured && state.RetentionSuspended && state.SchedulesSuspended && len(state.ActiveBackups) == 0
	if policy.BarmanDisabled {
		state.State = "disable_pending"
	}
	if state.Completed {
		state.State = "disabled"
	}
	return state, nil
}

func physicalIdentityPatch(object map[string]any) []map[string]any {
	meta := physicalMap(object, "metadata")
	return []map[string]any{{"op": "test", "path": "/metadata/uid", "value": physicalString(meta, "uid")}, {"op": "test", "path": "/metadata/resourceVersion", "value": physicalString(meta, "resourceVersion")}}
}

func physicalPatch(ctx context.Context, kube physicalKubeFunc, resource string, object map[string]any, changes ...map[string]any) (map[string]any, error) {
	patch := append(physicalIdentityPatch(object), changes...)
	raw, _ := json.Marshal(patch)
	return kube(ctx, nil, "patch", resource, physicalString(physicalMap(object, "metadata"), "name"), "--type=json", "-p", string(raw), "-o", "json")
}

func physicalSuspend(ctx context.Context, kube physicalKubeFunc, resource string, object map[string]any) error {
	if physicalMap(object, "spec")["suspend"] == true {
		return nil
	}
	patched, err := physicalPatch(ctx, kube, resource, object, map[string]any{"op": "add", "path": "/spec/suspend", "value": true})
	if err != nil {
		return err
	}
	if physicalMap(patched, "spec")["suspend"] != true {
		return fmt.Errorf("%s suspension did not persist", resource)
	}
	return nil
}

func physicalCheckArchiveRecoveries(ctx context.Context, kube physicalKubeFunc, policy physicalBackupPolicy) error {
	clusters, err := physicalBackupItems(ctx, kube, "clusters.postgresql.cnpg.io", true)
	if err != nil {
		return err
	}
	for _, other := range clusters {
		if physicalString(physicalMap(other, "metadata"), "uid") == policy.ClusterUID {
			continue
		}
		externals, _ := physicalMap(other, "spec")["externalClusters"].([]any)
		for _, item := range externals {
			external, _ := item.(map[string]any)
			store := physicalMap(external, "barmanObjectStore")
			server := physicalString(store, "serverName")
			if server == "" {
				server = physicalString(external, "name")
			}
			if physicalString(store, "destinationPath") == policy.Destination && server == policy.ServerName {
				if physicalMap(physicalMap(other, "spec"), "replica")["enabled"] == true {
					return fmt.Errorf("a replica cluster depends on this WAL archive; shutdown is blocked")
				}
				if physicalString(physicalMap(other, "status"), "phase") != "Cluster in healthy state" {
					return fmt.Errorf("an archive-dependent recovery is in progress; shutdown is blocked")
				}
			}
		}
	}
	return nil
}

func disablePhysicalBarman(ctx context.Context, kube physicalKubeFunc, opts physicalBackupOptions, dryRun bool) (physicalBarmanShutdown, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	var state physicalBarmanShutdown
	if !dryRun {
		release, err := physicalBackupLock(ctx, kube, opts.policy)
		if err != nil {
			return state, err
		}
		defer release()
	}
	cm, cluster, policy, err := loadPhysicalBackup(ctx, kube, opts)
	if err != nil {
		return state, err
	}
	if owner := physicalString(physicalMap(physicalMap(cluster, "metadata"), "annotations"), physicalBackupPolicyAnnotation); owner != "" && owner != physicalString(physicalMap(cm, "metadata"), "uid") {
		return state, fmt.Errorf("a different count policy owns the cluster")
	}
	plugins, _ := physicalMap(cluster, "spec")["plugins"].([]any)
	for _, item := range plugins {
		plugin, _ := item.(map[string]any)
		if plugin["isWALArchiver"] == true {
			return state, fmt.Errorf("plugin-managed WAL archiving is not supported by native Barman shutdown")
		}
	}
	state, err = inspectPhysicalBarmanShutdown(ctx, kube, opts, cm, cluster, policy)
	if err != nil {
		return state, err
	}
	if err = physicalCheckArchiveRecoveries(ctx, kube, policy); err != nil {
		return state, err
	}
	if dryRun || state.Completed {
		return state, nil
	}
	// Save a durable fail-closed intent before touching schedules or the archive.
	if !policy.BarmanDisabled {
		policy.BarmanDisabled, policy.Enabled = true, false
		raw, _ := json.Marshal(policy)
		physicalMap(cm, "data")["policy.json"] = string(raw)
		if cm, err = kube(ctx, cm, "replace", "-f", "-", "-o", "json"); err != nil {
			return state, err
		}
	}
	meta := physicalMap(cluster, "metadata")
	uid := physicalString(physicalMap(cm, "metadata"), "uid")
	if physicalString(physicalMap(meta, "annotations"), physicalBarmanDisabledAnnotation) != uid {
		changes := []map[string]any{}
		if meta["annotations"] == nil {
			changes = append(changes, map[string]any{"op": "add", "path": "/metadata/annotations", "value": map[string]any{}})
		}
		changes = append(changes, map[string]any{"op": "add", "path": "/metadata/annotations/fugue.pro~1barman-disabled", "value": uid})
		if physicalMap(physicalMap(cluster, "spec"), "backup")["retentionPolicy"] != nil {
			// Preserve history while the current upload drains, including clusters
			// that have never activated Fugue's count-retention policy.
			changes = append(changes, map[string]any{"op": "remove", "path": "/spec/backup/retentionPolicy"})
		}
		if cluster, err = physicalPatch(ctx, kube, "clusters.postgresql.cnpg.io", cluster, changes...); err != nil {
			return state, fmt.Errorf("disable intent saved; retry disable to finish activation: %w", err)
		}
	}
	if err = physicalSuspend(ctx, kube, "cronjob", state.cron); err != nil {
		return state, fmt.Errorf("disable intent saved; retention suspension failed, retry disable: %w", err)
	}
	for _, schedule := range state.schedules {
		if err = physicalSuspend(ctx, kube, "scheduledbackups.postgresql.cnpg.io", schedule); err != nil {
			return state, fmt.Errorf("disable intent saved; schedule suspension failed, retry disable: %w", err)
		}
	}
	cm, cluster, verified, err := loadPhysicalBackup(ctx, kube, opts)
	if err != nil || verified != policy {
		return state, fmt.Errorf("disable intent changed or read-back failed; inspect physical status before retrying")
	}
	state, err = inspectPhysicalBarmanShutdown(ctx, kube, opts, cm, cluster, policy)
	if err != nil {
		return state, err
	}
	if !state.SchedulesSuspended || !state.RetentionSuspended {
		return state, fmt.Errorf("a schedule changed during shutdown; retry disable")
	}
	if len(state.ActiveBackups) > 0 || !physicalBackupHealthy(cluster) {
		// Never interrupt an upload or remove archive configuration from a degraded DB.
		return state, nil
	}
	if err = physicalCheckArchiveRecoveries(ctx, kube, policy); err != nil {
		return state, err
	}
	latestCM, err := kube(ctx, nil, "get", "configmap", opts.policy, "-o", "json")
	if err != nil || physicalString(physicalMap(latestCM, "metadata"), "uid") != uid || physicalString(physicalMap(latestCM, "metadata"), "resourceVersion") != physicalString(physicalMap(cm, "metadata"), "resourceVersion") {
		return state, fmt.Errorf("disable policy changed before archive removal; retry disable")
	}
	changes := []map[string]any{}
	backup := physicalMap(physicalMap(cluster, "spec"), "backup")
	if backup["barmanObjectStore"] != nil {
		changes = append(changes, map[string]any{"op": "remove", "path": "/spec/backup/barmanObjectStore"})
	}
	if backup["retentionPolicy"] != nil {
		changes = append(changes, map[string]any{"op": "remove", "path": "/spec/backup/retentionPolicy"})
	}
	if len(changes) > 0 {
		if _, err = physicalPatch(ctx, kube, "clusters.postgresql.cnpg.io", cluster, changes...); err != nil {
			return state, fmt.Errorf("schedules paused but archive removal failed; retry disable: %w", err)
		}
	}
	cm, cluster, policy, err = loadPhysicalBackup(ctx, kube, opts)
	if err != nil {
		return state, err
	}
	state, err = inspectPhysicalBarmanShutdown(ctx, kube, opts, cm, cluster, policy)
	if err == nil && !state.Completed {
		err = fmt.Errorf("Barman shutdown configuration could not be fully verified; inspect physical status")
	}
	return state, err
}

func (c *CLI) newPhysicalBarmanDisableCommand(opts *physicalBackupOptions) *cobra.Command {
	var ack, dryRun, wait bool
	cmd := &cobra.Command{Use: "disable", Short: "Stop native Barman base backups and WAL uploads, preserving history", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		if !dryRun && !ack {
			return fmt.Errorf("--ack-loss-of-pitr is required: future continuous WAL recovery coverage will stop; historical R2 data is not deleted")
		}
		if dryRun && wait {
			return fmt.Errorf("--dry-run cannot be combined with --wait")
		}
		kube, err := physicalKubeRunner(*opts)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(cmd.Context(), 5*time.Minute)
		defer cancel()
		for {
			state, err := disablePhysicalBarman(ctx, kube, *opts, dryRun)
			if err != nil {
				return err
			}
			if dryRun || state.Completed || !wait {
				return c.renderResourceResult(map[string]any{"dry_run": dryRun, "barman": state, "next_step": "If disable_pending, rerun disable after active backups finish and the database is healthy. No automatic completion is scheduled."})
			}
			select {
			case <-ctx.Done():
				return fmt.Errorf("shutdown intent is saved but completion was not verified within five minutes; inspect physical status and rerun disable")
			case <-time.After(3 * time.Second):
			}
		}
	}}
	cmd.Flags().BoolVar(&ack, "ack-loss-of-pitr", false, "Acknowledge loss of future continuous WAL/PITR coverage")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Read shutdown state without saving intent, acquiring a Lease or changing resources")
	cmd.Flags().BoolVar(&wait, "wait", false, "Retry safe shutdown for up to five minutes; never cancel an active backup")
	return cmd
}
