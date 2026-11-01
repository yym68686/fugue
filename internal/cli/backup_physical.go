package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

const physicalBackupPolicyAnnotation = "fugue.pro/cnpg-count-policy"
const physicalBarmanDisabledAnnotation = "fugue.pro/barman-disabled"

type physicalBackupOptions struct{ namespace, policy, ssh, kubeconfig, kubeContext string }
type physicalBackupPolicy struct {
	Schema         string `json:"schema"`
	Namespace      string `json:"namespace"`
	Cluster        string `json:"cluster"`
	ClusterUID     string `json:"cluster_uid"`
	SystemID       string `json:"system_id"`
	Destination    string `json:"destination"`
	Endpoint       string `json:"endpoint"`
	ServerName     string `json:"server_name"`
	Enabled        bool   `json:"enabled"`
	RetainCount    int    `json:"retain_count"`
	BarmanDisabled bool   `json:"barman_disabled,omitempty"`
}
type physicalKubeFunc func(context.Context, any, ...string) (map[string]any, error)

func physicalKubeRunner(opts physicalBackupOptions) (physicalKubeFunc, error) {
	valid := regexp.MustCompile(`^[a-z0-9]([a-z0-9.-]*[a-z0-9])?$`)
	if !valid.MatchString(opts.namespace) || !valid.MatchString(opts.policy) || len(opts.namespace) > 63 || len(opts.policy) > 40 {
		return nil, fmt.Errorf("invalid namespace or policy name")
	}
	if opts.ssh != "" && !regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.@-]*$`).MatchString(opts.ssh) {
		return nil, fmt.Errorf("--ssh must be an SSH host alias, not a command")
	}
	return func(ctx context.Context, body any, args ...string) (map[string]any, error) {
		ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
		defer cancel()
		command := []string{"kubectl"}
		if opts.kubeconfig != "" {
			command = append(command, "--kubeconfig", opts.kubeconfig)
		}
		if opts.kubeContext != "" {
			command = append(command, "--context", opts.kubeContext)
		}
		command = append(command, "--request-timeout=30s", "-n", opts.namespace)
		command = append(command, args...)
		if opts.ssh != "" {
			quoted := make([]string, len(command))
			for i, v := range command {
				quoted[i] = shellSingleQuote(v)
			}
			command = []string{"ssh", "-T", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", "StrictHostKeyChecking=yes", "-o", "ForwardAgent=no", "-o", "ClearAllForwardings=yes", opts.ssh, strings.Join(quoted, " ")}
		}
		cmd := exec.CommandContext(ctx, command[0], command[1:]...)
		if body != nil {
			raw, err := json.Marshal(body)
			if err != nil {
				return nil, err
			}
			cmd.Stdin = bytes.NewReader(raw)
		}
		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		if err := cmd.Run(); err != nil {
			return nil, fmt.Errorf("Kubernetes backup operation failed (%v); check Kubernetes access and resource state", err)
		}
		if stdout.Len() > 8<<20 {
			return nil, fmt.Errorf("Kubernetes backup response exceeds safety bound")
		}
		var result map[string]any
		if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
			return nil, fmt.Errorf("invalid Kubernetes backup response")
		}
		return result, nil
	}, nil
}

func physicalMap(m map[string]any, key string) map[string]any {
	v, _ := m[key].(map[string]any)
	if v == nil {
		return map[string]any{}
	}
	return v
}
func physicalString(m map[string]any, key string) string { v, _ := m[key].(string); return v }

func loadPhysicalBackup(ctx context.Context, kube physicalKubeFunc, opts physicalBackupOptions) (map[string]any, map[string]any, physicalBackupPolicy, error) {
	cm, err := kube(ctx, nil, "get", "configmap", opts.policy, "-o", "json")
	if err != nil {
		return nil, nil, physicalBackupPolicy{}, err
	}
	var policy physicalBackupPolicy
	decoder := json.NewDecoder(strings.NewReader(physicalString(physicalMap(cm, "data"), "policy.json")))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&policy); err != nil {
		return nil, nil, policy, fmt.Errorf("invalid installed backup policy")
	}
	if err = decoder.Decode(new(any)); err != io.EOF {
		return nil, nil, policy, fmt.Errorf("invalid installed backup policy document")
	}
	meta := physicalMap(cm, "metadata")
	endpoint, endpointErr := url.Parse(policy.Endpoint)
	destination, destinationErr := url.Parse(policy.Destination)
	if physicalString(meta, "name") != opts.policy || physicalString(meta, "namespace") != opts.namespace || physicalString(meta, "uid") == "" || physicalString(meta, "resourceVersion") == "" || policy.ClusterUID == "" || !regexp.MustCompile(`^[0-9]+$`).MatchString(policy.SystemID) || endpointErr != nil || endpoint.Scheme != "https" || endpoint.Hostname() == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" || destinationErr != nil || destination.Scheme != "s3" || destination.Host == "" || destination.User != nil || destination.RawQuery != "" || destination.Fragment != "" {
		return nil, nil, policy, fmt.Errorf("invalid installed backup policy identity or repository")
	}
	if policy.Schema != "fugue.cnpg-count-retention/v1" || policy.Namespace != opts.namespace || policy.RetainCount < 1 || policy.RetainCount > 100 || len(policy.Cluster) > 253 || !regexp.MustCompile(`^[a-z0-9]([a-z0-9.-]*[a-z0-9])?$`).MatchString(policy.Cluster) || policy.ServerName == "" {
		return nil, nil, policy, fmt.Errorf("invalid installed backup policy identity")
	}
	cluster, err := kube(ctx, nil, "get", "clusters.postgresql.cnpg.io", policy.Cluster, "-o", "json")
	if err != nil {
		return nil, nil, policy, err
	}
	meta = physicalMap(cluster, "metadata")
	spec, status := physicalMap(cluster, "spec"), physicalMap(cluster, "status")
	store := physicalMap(physicalMap(spec, "backup"), "barmanObjectStore")
	server := physicalString(store, "serverName")
	if server == "" {
		server = physicalString(meta, "name")
	}
	if physicalString(meta, "name") != policy.Cluster || physicalString(meta, "namespace") != opts.namespace || physicalString(meta, "resourceVersion") == "" || physicalString(meta, "uid") != policy.ClusterUID || physicalString(status, "systemID") != policy.SystemID {
		return nil, nil, policy, fmt.Errorf("database or backup repository identity changed; refusing operation")
	}
	if policy.BarmanDisabled && policy.Enabled {
		return nil, nil, policy, fmt.Errorf("Barman disable intent cannot enable retention")
	}
	if len(store) == 0 {
		if !policy.BarmanDisabled || physicalString(physicalMap(meta, "annotations"), physicalBarmanDisabledAnnotation) != physicalString(physicalMap(cm, "metadata"), "uid") {
			return nil, nil, policy, fmt.Errorf("backup repository missing without owned Barman disable intent")
		}
	} else if physicalString(store, "destinationPath") != policy.Destination || physicalString(store, "endpointURL") != policy.Endpoint || server != policy.ServerName {
		return nil, nil, policy, fmt.Errorf("database or backup repository identity changed; refusing operation")
	}
	return cm, cluster, policy, nil
}

func physicalBackupHealthy(cluster map[string]any) bool {
	status := physicalMap(cluster, "status")
	ready, _ := status["readyInstances"].(float64)
	return physicalString(status, "phase") == "Cluster in healthy state" && ready >= 1 && physicalString(status, "currentPrimary") != "" && physicalString(status, "currentPrimary") == physicalString(status, "targetPrimary")
}

func physicalBackupActivated(cm, cluster map[string]any) bool {
	return physicalString(physicalMap(physicalMap(cluster, "metadata"), "annotations"), physicalBarmanDisabledAnnotation) == "" && len(physicalMap(physicalMap(physicalMap(cluster, "spec"), "backup"), "barmanObjectStore")) > 0 && physicalString(physicalMap(physicalMap(cluster, "metadata"), "annotations"), physicalBackupPolicyAnnotation) == physicalString(physicalMap(cm, "metadata"), "uid") && physicalMap(physicalMap(cluster, "spec"), "backup")["retentionPolicy"] == nil
}

func physicalBackupLock(ctx context.Context, kube physicalKubeFunc, name string) (func(), error) {
	lease, err := kube(ctx, nil, "get", "lease", name, "-o", "json")
	if err != nil {
		return nil, err
	}
	spec := physicalMap(lease, "spec")
	if physicalString(spec, "holderIdentity") != "" {
		renewed, e := time.Parse(time.RFC3339Nano, physicalString(spec, "renewTime"))
		duration, _ := spec["leaseDurationSeconds"].(float64)
		if e != nil || duration <= 0 || renewed.Add(time.Duration(duration)*time.Second).After(time.Now()) {
			return nil, fmt.Errorf("backup policy is locked by another operation")
		}
	}
	holder := "configure/" + strconv.FormatInt(time.Now().UnixNano(), 10)
	lease["spec"] = map[string]any{"holderIdentity": holder, "leaseDurationSeconds": 600, "renewTime": time.Now().UTC().Format(time.RFC3339Nano)}
	locked, err := kube(ctx, lease, "replace", "-f", "-", "-o", "json")
	if err != nil {
		return nil, err
	}
	return func() {
		physicalMap(locked, "spec")["holderIdentity"] = ""
		releaseCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_, _ = kube(releaseCtx, locked, "replace", "-f", "-", "-o", "json")
	}, nil
}

func configurePhysicalBackup(ctx context.Context, kube physicalKubeFunc, opts physicalBackupOptions, count int) (map[string]any, error) {
	if count < 1 || count > 100 {
		return nil, fmt.Errorf("--retain-count must be between 1 and 100")
	}
	release, err := physicalBackupLock(ctx, kube, opts.policy)
	if err != nil {
		return nil, err
	}
	defer release()
	cm, cluster, policy, err := loadPhysicalBackup(ctx, kube, opts)
	if err != nil {
		return nil, err
	}
	if policy.BarmanDisabled || physicalString(physicalMap(physicalMap(cluster, "metadata"), "annotations"), physicalBarmanDisabledAnnotation) != "" {
		return nil, fmt.Errorf("Barman disable intent is active; configure only changes retention and cannot re-enable backups")
	}
	meta := physicalMap(cluster, "metadata")
	annotations := physicalMap(meta, "annotations")
	if existing := physicalString(annotations, physicalBackupPolicyAnnotation); existing != "" && existing != physicalString(physicalMap(cm, "metadata"), "uid") {
		return nil, fmt.Errorf("a different count policy owns the cluster")
	}
	policy.Enabled, policy.RetainCount = true, count
	raw, _ := json.Marshal(policy)
	physicalMap(cm, "data")["policy.json"] = string(raw)
	// Store intent first. The worker blocks until ownership and native policy agree.
	if _, err = kube(ctx, cm, "replace", "-f", "-", "-o", "json"); err != nil {
		return nil, err
	}
	patch := []map[string]any{{"op": "test", "path": "/metadata/uid", "value": policy.ClusterUID}, {"op": "test", "path": "/metadata/resourceVersion", "value": physicalString(meta, "resourceVersion")}}
	if meta["annotations"] == nil {
		patch = append(patch, map[string]any{"op": "add", "path": "/metadata/annotations", "value": map[string]any{}})
	}
	patch = append(patch, map[string]any{"op": "add", "path": "/metadata/annotations/fugue.pro~1cnpg-count-policy", "value": physicalString(physicalMap(cm, "metadata"), "uid")})
	if _, ok := physicalMap(physicalMap(cluster, "spec"), "backup")["retentionPolicy"]; ok {
		patch = append(patch, map[string]any{"op": "remove", "path": "/spec/backup/retentionPolicy"})
	}
	patchJSON, _ := json.Marshal(patch)
	if _, err = kube(ctx, nil, "patch", "clusters.postgresql.cnpg.io", policy.Cluster, "--type=json", "-p", string(patchJSON), "-o", "json"); err != nil {
		return nil, fmt.Errorf("intent saved but activation failed; retry configure safely: %w", err)
	}
	verifiedCM, cluster, verified, err := loadPhysicalBackup(ctx, kube, opts)
	if err != nil {
		return nil, fmt.Errorf("configuration written but read-back verification failed: %w", err)
	}
	if verified != policy || !physicalBackupActivated(verifiedCM, cluster) {
		return nil, fmt.Errorf("configuration changed during activation; inspect physical status")
	}
	return map[string]any{"policy": policy, "configured": true, "cleanup_blocked": !physicalBackupHealthy(cluster), "warning": "Retains successful backup count, not days. Recovery before the oldest retained backup will no longer be possible. R2 retention does not free local PostgreSQL WAL space."}, nil
}

func (c *CLI) newAdminPhysicalBackupCommand() *cobra.Command {
	opts := physicalBackupOptions{namespace: "fugue-system", policy: "fugue-control-plane-backup-retention"}
	cmd := &cobra.Command{Use: "physical", Short: "Manage CNPG physical backup retention and Barman shutdown", Long: "Manage the installed CNPG physical-backup policy through Kubernetes credentials (or an explicitly selected SSH host). This operator path does not require the Fugue API or database. The executor must first be installed through the declarative release workflow. Logical dump policies are separate."}
	f := cmd.PersistentFlags()
	f.StringVar(&opts.namespace, "namespace", opts.namespace, "Control-plane Kubernetes namespace")
	f.StringVar(&opts.policy, "policy", opts.policy, "Installed count-retention policy ConfigMap")
	f.StringVar(&opts.ssh, "ssh", "", "Use kubectl on this explicitly selected SSH host alias")
	f.StringVar(&opts.kubeconfig, "kubeconfig", "", "Kubeconfig path (on SSH host when --ssh is used)")
	f.StringVar(&opts.kubeContext, "kube-context", "", "Kubernetes context")
	status := &cobra.Command{Use: "status", Short: "Read physical backup intent, native policy, health and last cleanup receipt", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		kube, err := physicalKubeRunner(opts)
		if err != nil {
			return err
		}
		cm, cluster, policy, err := loadPhysicalBackup(cmd.Context(), kube, opts)
		if err != nil {
			return err
		}
		receipt, err := kube(cmd.Context(), nil, "get", "configmap", opts.policy+"-receipt", "-o", "json")
		if err != nil {
			return err
		}
		var observation any
		_ = json.Unmarshal([]byte(physicalString(physicalMap(receipt, "data"), "receipt.json")), &observation)
		shutdown, err := inspectPhysicalBarmanShutdown(cmd.Context(), kube, opts, cm, cluster, policy)
		if err != nil {
			return err
		}
		return c.renderResourceResult(map[string]any{"policy": policy, "activated": physicalBackupActivated(cm, cluster), "database_healthy": physicalBackupHealthy(cluster), "database_phase": physicalString(physicalMap(cluster, "status"), "phase"), "native_recovery_window": physicalMap(physicalMap(cluster, "spec"), "backup")["retentionPolicy"], "last_receipt": observation, "barman": shutdown})
	}}
	var count int
	var ack bool
	configure := &cobra.Command{Use: "configure", Short: "Persist and activate successful-backup count retention", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		if !ack {
			return fmt.Errorf("--ack-reduced-recovery is required: older backups and their unneeded WAL will be permanently removed")
		}
		kube, err := physicalKubeRunner(opts)
		if err != nil {
			return err
		}
		out, err := configurePhysicalBackup(cmd.Context(), kube, opts, count)
		if err != nil {
			return err
		}
		return c.renderResourceResult(out)
	}}
	configure.Flags().IntVar(&count, "retain-count", 0, "Number of successful physical backups to retain (1–100, not days)")
	_ = configure.MarkFlagRequired("retain-count")
	configure.Flags().BoolVar(&ack, "ack-reduced-recovery", false, "Acknowledge permanent removal of older recovery points")
	plan := &cobra.Command{Use: "plan", Short: "Read candidate successful Backup records without deleting or starting a Job", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		kube, err := physicalKubeRunner(opts)
		if err != nil {
			return err
		}
		_, _, policy, err := loadPhysicalBackup(cmd.Context(), kube, opts)
		if err != nil {
			return err
		}
		list, err := kube(cmd.Context(), nil, "get", "backups.postgresql.cnpg.io", "--chunk-size=0", "-o", "json")
		if err != nil {
			return err
		}
		items, _ := list["items"].([]any)
		if len(items) > 500 {
			return fmt.Errorf("backup inventory exceeds safety bound")
		}
		ids := []string{}
		for _, item := range items {
			b, _ := item.(map[string]any)
			spec, status := physicalMap(b, "spec"), physicalMap(b, "status")
			if physicalString(physicalMap(spec, "cluster"), "name") == policy.Cluster && physicalString(status, "phase") == "completed" {
				ids = append(ids, physicalString(status, "backupId"))
			}
		}
		sort.Sort(sort.Reverse(sort.StringSlice(ids)))
		return c.renderResourceResult(map[string]any{"policy": policy, "completed_backup_records": ids, "remote_catalogue_verified": false, "warning": "Kubernetes Backup records can outlive remote data. The Barman worker revalidates the actual catalogue, keep annotations, database health and active backups before removal."})
	}}
	var dryRun, wait bool
	reconcile := &cobra.Command{Use: "reconcile", Short: "Start the installed Barman retention Job; --dry-run performs engine preview only", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		kube, err := physicalKubeRunner(opts)
		if err != nil {
			return err
		}
		cm, cluster, policy, err := loadPhysicalBackup(cmd.Context(), kube, opts)
		if err != nil {
			return err
		}
		if !policy.Enabled || !physicalBackupActivated(cm, cluster) || !physicalBackupHealthy(cluster) {
			return fmt.Errorf("count policy is disabled, not activated or database is unhealthy; cleanup is blocked")
		}
		cron, err := kube(cmd.Context(), nil, "get", "cronjob", opts.policy, "-o", "json")
		if err != nil {
			return err
		}
		spec := physicalMap(physicalMap(cron, "spec"), "jobTemplate")["spec"]
		job := map[string]any{"apiVersion": "batch/v1", "kind": "Job", "metadata": map[string]any{"generateName": opts.policy + "-manual-", "namespace": opts.namespace}, "spec": spec}
		jobSpec, ok := spec.(map[string]any)
		if !ok {
			return fmt.Errorf("invalid installed worker template")
		}
		if dryRun {
			pod := physicalMap(physicalMap(jobSpec, "template"), "spec")
			cs, _ := pod["containers"].([]any)
			if len(cs) != 1 {
				return fmt.Errorf("invalid installed worker template")
			}
			container, _ := cs[0].(map[string]any)
			env, _ := container["env"].([]any)
			found := false
			for _, v := range env {
				e, _ := v.(map[string]any)
				if physicalString(e, "name") == "RETENTION_DRY_RUN" {
					e["value"] = "true"
					found = true
				}
			}
			if !found {
				return fmt.Errorf("installed worker does not support dry-run")
			}
		}
		created, err := kube(cmd.Context(), job, "create", "-f", "-", "-o", "json")
		if err != nil {
			return err
		}
		name := physicalString(physicalMap(created, "metadata"), "name")
		if !wait {
			return c.renderResourceResult(map[string]any{"job": name, "dry_run": dryRun, "completed": false})
		}
		deadline := time.Now().Add(5 * time.Minute)
		for time.Now().Before(deadline) {
			current, e := kube(cmd.Context(), nil, "get", "job", name, "-o", "json")
			if e != nil {
				return e
			}
			s := physicalMap(current, "status")
			if value, _ := s["failed"].(float64); value > 0 {
				return fmt.Errorf("retention Job %s failed or was blocked; inspect physical status and Job logs", name)
			}
			if value, _ := s["succeeded"].(float64); value > 0 {
				return c.renderResourceResult(map[string]any{"job": name, "dry_run": dryRun, "completed": true})
			}
			select {
			case <-cmd.Context().Done():
				return cmd.Context().Err()
			case <-time.After(2 * time.Second):
			}
		}
		return fmt.Errorf("timed out waiting for retention Job %s", name)
	}}
	reconcile.Flags().BoolVar(&dryRun, "dry-run", false, "Ask Barman to preview without deleting backups or WAL")
	reconcile.Flags().BoolVar(&wait, "wait", false, "Wait up to five minutes for the Job")
	cmd.AddCommand(status, plan, configure, reconcile, c.newPhysicalBarmanDisableCommand(&opts), c.newPhysicalBarmanPurgeCommand(&opts))
	return cmd
}
