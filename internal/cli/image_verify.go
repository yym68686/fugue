package cli

import (
	"context"
	"fmt"
	"time"

	"fugue/internal/model"
	"github.com/spf13/cobra"
)

type imageVerification struct {
	Image           model.Image           `json:"image"`
	EvidenceKind    string                `json:"evidence_kind"`
	ObservedAt      time.Time             `json:"observed_at"`
	InventoryReady  bool                  `json:"inventory_ready"`
	Verified        bool                  `json:"verified"`
	FreshReplicas   []model.ImageReplica  `json:"fresh_replicas"`
	MissingEvidence []string              `json:"missing_evidence"`
	Task            *model.NodeUpdateTask `json:"task,omitempty"`
	NextCommands    []string              `json:"next_commands,omitempty"`
}

func (c *CLI) newImageVerifyCommand() *cobra.Command {
	var probe, wait bool
	var node string
	var timeout, maxAge time.Duration
	cmd := &cobra.Command{Use: "verify <image>", Short: "Check replica inventory or actively verify one node's local image graph", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if timeout <= 0 || maxAge <= 0 {
			return fmt.Errorf("--timeout and --max-age must be positive")
		}
		if probe && node == "" {
			return fmt.Errorf("--probe requires an explicit --node; host maintenance permissions are required")
		}
		if !probe && node != "" {
			return fmt.Errorf("--node requires --probe")
		}
		client, err := c.newClient()
		if err != nil {
			return err
		}
		item, err := c.resolveImage(client, args[0])
		if err != nil {
			return err
		}
		result := imageVerification{Image: item, EvidenceKind: "inventory", ObservedAt: time.Now().UTC(), FreshReplicas: []model.ImageReplica{}, MissingEvidence: []string{}}
		var runErr error
		if probe {
			if item.CanonicalDigest == "" {
				return withExitCode(fmt.Errorf("image has no canonical digest; active verification cannot bind an immutable target"), ExitCodeIndeterminate)
			}
			task, err := client.CreateNodeUpdateTask(nodeUpdateTaskCreateRequest{ClusterNodeName: node, Type: model.NodeUpdateTaskTypeVerifyImageCache, Payload: map[string]string{"image_id": item.ID, "image_ref": item.ImageRef, "digest": item.CanonicalDigest, "app_id": item.AppID}})
			if err != nil {
				return err
			}
			result.EvidenceKind = "node_graph_probe"
			result.Task = &task
			result.NextCommands = []string{"fugue admin node-updater task ls --node-updater " + shellSingleQuote(task.NodeUpdaterID), "fugue image verify " + shellSingleQuote(item.ID)}
			if wait {
				ctx, cancel := context.WithTimeout(cmd.Context(), timeout)
				defer cancel()
				client.context = ctx
				task, runErr = waitImageVerifyTask(ctx, client, task)
				result.Task = &task
			}
		}
		replicas, err := client.listImageReplicas(item.ID)
		if err != nil {
			result.MissingEvidence = append(result.MissingEvidence, "replica_inventory")
			if runErr == nil {
				runErr = err
			}
		} else {
			result.ObservedAt = time.Now().UTC()
			for _, replica := range replicas {
				if imageReplicaFresh(replica, item.CanonicalDigest, result.ObservedAt, maxAge) {
					result.FreshReplicas = append(result.FreshReplicas, replica)
				}
			}
			minimum := item.MinAvailableReplicaCount
			if minimum < 1 {
				minimum = 1
			}
			result.InventoryReady = len(result.FreshReplicas) >= minimum
			if probe && result.Task != nil && result.Task.Status == model.NodeUpdateTaskStatusCompleted {
				for _, replica := range result.FreshReplicas {
					if replica.ClusterNodeName == node && !result.Task.CreatedAt.IsZero() && !replica.LastVerifiedAt.Before(result.Task.CreatedAt) {
						result.Verified = true
					}
				}
			}
		}
		if !probe {
			result.MissingEvidence = append(result.MissingEvidence, "active_probe_not_requested")
		}
		if probe && !result.Verified {
			result.MissingEvidence = append(result.MissingEvidence, "completed_probe_with_fresh_matching_replica")
			if wait && runErr == nil {
				runErr = withExitCode(fmt.Errorf("active verification has no matching fresh replica evidence"), ExitCodeIndeterminate)
			}
		}
		if !probe && !result.InventoryReady && runErr == nil {
			runErr = withExitCode(fmt.Errorf("inventory does not establish the required fresh replica count"), ExitCodeIndeterminate)
		}
		if c.wantsJSON() {
			if err := c.writeJSON(result); err != nil {
				return err
			}
		} else {
			if err := writeKeyValues(c.stdout, kvPair{Key: "image_id", Value: item.ID}, kvPair{Key: "evidence_kind", Value: result.EvidenceKind}, kvPair{Key: "inventory_ready", Value: fmt.Sprint(result.InventoryReady)}, kvPair{Key: "active_target_verified", Value: fmt.Sprint(result.Verified)}, kvPair{Key: "fresh_replicas", Value: fmt.Sprint(len(result.FreshReplicas))}); err != nil {
				return err
			}
			if result.Task != nil {
				c.progressf("task_id=%s task_status=%s", result.Task.ID, result.Task.Status)
			}
		}
		return runErr
	}}
	cmd.Flags().BoolVar(&probe, "probe", false, "Queue a real image-graph verification on one explicit node; does not synthesize a replica report")
	cmd.Flags().StringVar(&node, "node", "", "Exact cluster node name to verify")
	cmd.Flags().BoolVar(&wait, "wait", true, "Wait for the active probe; inventory checks do not enqueue work")
	cmd.Flags().DurationVar(&timeout, "timeout", 5*time.Minute, "Maximum wait for an active probe")
	cmd.Flags().DurationVar(&maxAge, "max-age", 15*time.Minute, "Maximum accepted age of replica verification evidence")
	return cmd
}
func imageReplicaFresh(replica model.ImageReplica, digest string, now time.Time, maxAge time.Duration) bool {
	if replica.Status != model.ImageReplicaStatusPresent || digest == "" || replica.Digest != digest || replica.LastVerifiedAt == nil {
		return false
	}
	if replica.LastVerifiedAt.After(now.Add(15*time.Second)) || now.Sub(*replica.LastVerifiedAt) > maxAge {
		return false
	}
	return replica.LeaseExpiresAt == nil || replica.LeaseExpiresAt.After(now)
}
func waitImageVerifyTask(ctx context.Context, client *Client, last model.NodeUpdateTask) (model.NodeUpdateTask, error) {
	scoped := *client
	scoped.context = ctx
	for {
		tasks, err := scoped.ListNodeUpdateTasks(last.NodeUpdaterID, "")
		if err != nil {
			return last, err
		}
		for _, task := range tasks {
			if task.ID == last.ID {
				last = task
				switch task.Status {
				case model.NodeUpdateTaskStatusCompleted:
					return last, nil
				case model.NodeUpdateTaskStatusFailed, model.NodeUpdateTaskStatusCanceled:
					return last, withExitCode(fmt.Errorf("image probe %s: %s", task.Status, redactDiagnosticString(task.ErrorMessage)), ExitCodeSystemFault)
				}
			}
		}
		if err := waitRequestRetry(ctx, 2*time.Second); err != nil {
			return last, withExitCode(err, ExitCodeIndeterminate)
		}
	}
}
