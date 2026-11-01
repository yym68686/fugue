package cli

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

const physicalHistoryCapability = "fugue.pro/cnpg-history-purge"

type physicalHistoryOptions struct {
	execute, ackAll, ackQuiet, dryRun, wait, status bool
	planSHA                                         string
}

func (o physicalHistoryOptions) validate() error {
	if o.status && (o.execute || o.ackAll || o.ackQuiet || o.dryRun || o.planSHA != "") {
		return fmt.Errorf("--status cannot be combined with preview or execution flags")
	}
	if o.execute {
		if o.dryRun || !o.ackAll || !o.ackQuiet || !regexp.MustCompile(`^[a-f0-9]{64}$`).MatchString(o.planSHA) {
			return fmt.Errorf("execution requires --plan-sha256 <preview SHA256>, --ack-delete-all-history and --ack-no-external-writers; cannot combine --execute with --dry-run")
		}
	} else if o.ackAll || o.ackQuiet || o.planSHA != "" {
		return fmt.Errorf("execution acknowledgements and --plan-sha256 require --execute; omit them for preview")
	}
	return nil
}

func physicalHistoryReceipt(ctx context.Context, kube physicalKubeFunc, opts physicalBackupOptions) (map[string]any, error) {
	cm, err := kube(ctx, nil, "get", "configmap", opts.policy+"-history-receipt", "-o", "json")
	if err != nil {
		return nil, err
	}
	meta := physicalMap(cm, "metadata")
	if physicalString(meta, "name") != opts.policy+"-history-receipt" || physicalString(meta, "namespace") != opts.namespace {
		return nil, fmt.Errorf("invalid history receipt identity")
	}
	var receipt map[string]any
	if err := json.Unmarshal([]byte(physicalString(physicalMap(cm, "data"), "receipt.json")), &receipt); err != nil || receipt == nil {
		return nil, fmt.Errorf("invalid history receipt")
	}
	return receipt, nil
}

func startPhysicalHistory(ctx context.Context, kube physicalKubeFunc, opts physicalBackupOptions, options physicalHistoryOptions) (map[string]any, error) {
	if err := options.validate(); err != nil {
		return nil, err
	}
	if options.status {
		return physicalHistoryReceipt(ctx, kube, opts)
	}
	cm, cluster, policy, err := loadPhysicalBackup(ctx, kube, opts)
	if err != nil {
		return nil, err
	}
	shutdown, err := inspectPhysicalBarmanShutdown(ctx, kube, opts, cm, cluster, policy)
	if err != nil {
		return nil, err
	}
	if !shutdown.Completed || !physicalBackupHealthy(cluster) || physicalMap(physicalMap(cluster, "spec"), "backup")["retentionPolicy"] != nil {
		return nil, fmt.Errorf("Barman shutdown must be complete and the database healthy before archive retirement; this command does not disable Barman")
	}
	if err := physicalCheckArchiveRecoveries(ctx, kube, policy); err != nil {
		return nil, err
	}
	cron := shutdown.cron
	if physicalString(physicalMap(physicalMap(cron, "metadata"), "annotations"), physicalHistoryCapability) != "v1" {
		return nil, fmt.Errorf("installed executor does not support history retirement; upgrade it through the declarative release workflow")
	}
	jobSpec := physicalMap(physicalMap(cron, "spec"), "jobTemplate")["spec"]
	spec, ok := jobSpec.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("invalid installed history worker template")
	}
	pod := physicalMap(physicalMap(spec, "template"), "spec")
	containers, _ := pod["containers"].([]any)
	if len(containers) != 1 || physicalString(pod, "serviceAccountName") != opts.policy {
		return nil, fmt.Errorf("invalid installed history worker template")
	}
	container, ok := containers[0].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("invalid installed history worker container")
	}
	volumes, _ := pod["volumes"].([]any)
	codeName := ""
	for _, item := range volumes {
		volume, _ := item.(map[string]any)
		if physicalString(volume, "name") == "code" {
			codeName = physicalString(physicalMap(volume, "configMap"), "name")
		}
	}
	if !strings.HasPrefix(codeName, opts.policy+"-") || !regexp.MustCompile(`^[a-z0-9.-]+$`).MatchString(codeName) {
		return nil, fmt.Errorf("missing pinned history worker code")
	}
	code, err := kube(ctx, nil, "get", "configmap", codeName, "-o", "json")
	if err != nil {
		return nil, err
	}
	if code["immutable"] != true || physicalString(physicalMap(code, "data"), "history.py") == "" || physicalString(physicalMap(code, "data"), "worker.py") == "" {
		return nil, fmt.Errorf("installed history worker code is unavailable or mutable")
	}
	if options.execute {
		receipt, err := physicalHistoryReceipt(ctx, kube, opts)
		if err != nil {
			return nil, err
		}
		observed, e := time.Parse(time.RFC3339Nano, physicalString(receipt, "observed_at"))
		if physicalString(receipt, "state") != "planned" || physicalString(receipt, "plan_sha256") != options.planSHA || e != nil || time.Since(observed) < 0 || time.Since(observed) > time.Hour {
			return nil, fmt.Errorf("a matching successful preview from the last hour is required; preview again")
		}
	}
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		return nil, err
	}
	operation := hex.EncodeToString(random)
	env := []any{}
	previousEnv, _ := container["env"].([]any)
	for _, item := range previousEnv {
		e, _ := item.(map[string]any)
		name := physicalString(e, "name")
		if !strings.HasPrefix(name, "HISTORY_") && name != "POLICY_NAMESPACE" && name != "POLICY_NAME" {
			env = append(env, e)
		}
	}
	for key, value := range map[string]string{"POLICY_NAMESPACE": opts.namespace, "POLICY_NAME": opts.policy, "HISTORY_OPERATION_ID": operation, "HISTORY_PLAN_SHA256": options.planSHA, "HISTORY_EXECUTE": fmt.Sprint(options.execute)} {
		env = append(env, map[string]any{"name": key, "value": value})
	}
	env = append(env, map[string]any{"name": "HISTORY_JOB_UID", "valueFrom": map[string]any{"fieldRef": map[string]any{"fieldPath": "metadata.labels['batch.kubernetes.io/controller-uid']"}}})
	container["env"] = env
	container["command"] = []any{"python3", "/retention/history.py"}
	delete(container, "args")
	spec["activeDeadlineSeconds"], spec["backoffLimit"] = 1200, 0
	job := map[string]any{"apiVersion": "batch/v1", "kind": "Job", "metadata": map[string]any{"generateName": opts.policy + "-history-", "namespace": opts.namespace, "annotations": map[string]any{"fugue.pro/history-operation": operation}}, "spec": spec}
	created, err := kube(ctx, job, "create", "-f", "-", "-o", "json")
	if err != nil {
		return nil, err
	}
	name, jobUID := physicalString(physicalMap(created, "metadata"), "name"), physicalString(physicalMap(created, "metadata"), "uid")
	if name == "" || jobUID == "" {
		return nil, fmt.Errorf("created history Job has no identity; inspect Kubernetes before retrying")
	}
	result := map[string]any{"job": name, "operation_id": operation, "job_uid": jobUID, "dry_run": !options.execute, "completed": false}
	if !options.wait {
		return result, nil
	}
	deadline := time.Now().Add(22 * time.Minute)
	for time.Now().Before(deadline) {
		current, err := kube(ctx, nil, "get", "job", name, "-o", "json")
		if err != nil {
			return nil, err
		}
		if physicalString(physicalMap(current, "metadata"), "uid") != jobUID {
			return nil, fmt.Errorf("history Job identity changed")
		}
		status := physicalMap(current, "status")
		failed, _ := status["failed"].(float64)
		succeeded, _ := status["succeeded"].(float64)
		if failed > 0 || succeeded > 0 {
			receipt, err := physicalHistoryReceipt(ctx, kube, opts)
			if err != nil {
				return nil, err
			}
			if physicalString(receipt, "operation_id") != operation || physicalString(receipt, "job_uid") != jobUID {
				return nil, fmt.Errorf("history Job %s has no matching receipt; do not treat a stale receipt as success", name)
			}
			result["receipt"] = receipt
			state := physicalString(receipt, "state")
			if failed > 0 || (!options.execute && state != "planned") || (options.execute && (state != "purged" || receipt["verified_empty"] != true)) {
				return nil, fmt.Errorf("history Job %s did not complete (%s): %s; read purge --status and preview remaining data before retrying", name, state, physicalString(receipt, "reason"))
			}
			result["completed"] = true
			return result, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return nil, fmt.Errorf("timed out waiting for history Job %s; the Job was not cancelled, inspect purge --status before retrying", name)
}

func (c *CLI) newPhysicalBarmanPurgeCommand(opts *physicalBackupOptions) *cobra.Command {
	options := physicalHistoryOptions{}
	cmd := &cobra.Command{Use: "purge", Short: "Preview retirement of ALL disabled Barman history; deletion requires --execute", Long: "Preview the pinned Barman repository, including base backups, WAL and unfinished multipart uploads. Creates a Kubernetes Job and receipt but does not delete R2 data by default. Execution permanently removes ALL historical Barman restore points, not just backups older than retain-count. Does not touch Longhorn, local WAL, Secrets or Backup records. Exclude all external writers before execution; Kubernetes shutdown alone cannot fence external object-store credentials.", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := options.validate(); err != nil {
				return err
			}
			kube, err := physicalKubeRunner(*opts)
			if err != nil {
				return err
			}
			out, err := startPhysicalHistory(cmd.Context(), kube, *opts, options)
			if err != nil {
				return err
			}
			return c.renderResourceResult(out)
		}}
	f := cmd.Flags()
	f.BoolVar(&options.execute, "execute", false, "Permanently remove ALL history after validating a fresh preview")
	f.BoolVar(&options.dryRun, "dry-run", false, "Explicit preview (also the default); no R2 deletion")
	f.BoolVar(&options.ackAll, "ack-delete-all-history", false, "Acknowledge irreversible loss of every historical Barman restore point")
	f.BoolVar(&options.ackQuiet, "ack-no-external-writers", false, "Confirm all external writers to this archive are excluded; DELETE is not atomically fenced")
	f.StringVar(&options.planSHA, "plan-sha256", "", "Exact SHA256 from a successful preview no older than one hour")
	f.BoolVar(&options.wait, "wait", true, "Wait for a matching Job receipt (up to 22 minutes); false returns Job identity")
	f.BoolVar(&options.status, "status", false, "Read the last history receipt without creating a Job")
	return cmd
}
