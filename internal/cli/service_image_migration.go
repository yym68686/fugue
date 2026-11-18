package cli

import (
	"fmt"
	"net/http"
	"net/url"
	"time"

	"fugue/internal/model"
	"fugue/internal/pgimage"
	"github.com/spf13/cobra"
)

type postgresImageMigrationRequest struct {
	Image                  string `json:"image,omitempty"`
	Apply                  bool   `json:"apply"`
	Acknowledge            bool   `json:"acknowledge_downtime"`
	Expected               string `json:"expected_config_hash,omitempty"`
	DrainBudget            int    `json:"drain_budget_seconds,omitempty"`
	AcknowledgeTermination bool   `json:"acknowledge_connection_termination,omitempty"`
}
type postgresImageMigrationResponse struct {
	Plan      pgimage.Plan                  `json:"plan"`
	DryRun    bool                          `json:"dry_run"`
	Operation *model.Operation              `json:"operation,omitempty"`
	Timing    *postgresImageMigrationTiming `json:"timing,omitempty"` // CLI-derived after --wait.
}

type postgresImageMigrationTiming struct {
	QueueMS     int64          `json:"queue_ms"`
	ExecutionMS int64          `json:"execution_ms"`
	Endpoint    map[string]any `json:"stable_service_probe"`
}

func getPostgresImageMigrationTiming(client *Client, op model.Operation) *postgresImageMigrationTiming {
	if op.StartedAt == nil || op.CompletedAt == nil {
		return nil
	}
	timing := &postgresImageMigrationTiming{
		QueueMS:     op.StartedAt.Sub(op.CreatedAt).Milliseconds(),
		ExecutionMS: op.CompletedAt.Sub(*op.StartedAt).Milliseconds(),
		Endpoint:    map[string]any{"status": "unavailable"},
	}
	items, err := client.GetOperationEvidence(op.ID, true)
	if err != nil {
		return timing
	}
	for i := len(items) - 1; i >= 0; i-- {
		if items[i].Type == pgimage.EndpointTimingEvidence && items[i].Payload != nil {
			timing.Endpoint = items[i].Payload
			break
		}
	}
	return timing
}

func (c *CLI) newServicePostgresMigrateImageCommand() *cobra.Command {
	var request postgresImageMigrationRequest
	var wait bool
	cmd := &cobra.Command{Use: "migrate-image <service>", Short: "Plan or explicitly apply a verified same-version baked-manager image migration (brief downtime)", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if cmd.Flags().Changed("drain-budget-seconds") && (request.DrainBudget == 0 || !pgimage.ValidDrainBudget(request.DrainBudget)) {
			return fmt.Errorf("--drain-budget-seconds must be between 10 and 15")
		}
		if (request.Apply && request.DrainBudget != 0 && !request.AcknowledgeTermination) || (request.DrainBudget == 0 && request.AcknowledgeTermination) {
			return fmt.Errorf("fast drain apply requires --drain-budget-seconds and --acknowledge-connection-termination")
		}
		if request.Apply && (!request.Acknowledge || request.Expected == "") {
			return fmt.Errorf("--apply requires --acknowledge-downtime and --expected-config-hash from a preceding plan")
		}
		client, err := c.newClient()
		if err != nil {
			return err
		}
		service, err := c.resolveNamedService(client, args[0])
		if err != nil {
			return err
		}
		var response postgresImageMigrationResponse
		if err = client.doJSON(http.MethodPost, "/v1/backing-services/"+url.PathEscape(service.ID)+"/migrate-image", request, &response); err != nil {
			return err
		}
		if response.Plan.ServiceID != service.ID || response.Plan.ConfigHash == "" || response.DryRun == request.Apply ||
			response.Plan.DrainBudgetSeconds != request.DrainBudget || (!request.Apply && request.DrainBudget != 0 && response.Plan.Drain == nil) {
			return fmt.Errorf("image migration response identity or plan is invalid")
		}
		if request.Apply {
			if response.Operation == nil || response.Operation.ID == "" || response.Operation.Type != pgimage.OperationType || response.Operation.ServiceID != service.ID {
				return fmt.Errorf("image migration operation identity is invalid")
			}
			if wait {
				ops, err := c.waitForOperations(client, []model.Operation{*response.Operation})
				if err != nil {
					return err
				}
				if len(ops) != 1 || ops[0].Status != model.OperationStatusCompleted {
					return fmt.Errorf("database image migration did not complete")
				}
				response.Operation = &ops[0]
				if request.DrainBudget != 0 {
					response.Timing = getPostgresImageMigrationTiming(client, ops[0])
				}
			}
		}
		if c.wantsJSON() {
			response.Operation = redactOperationPtrForOutput(response.Operation)
			return c.writeJSON(response)
		}
		if response.DryRun {
			fmt.Fprintf(cmd.OutOrStdout(), "Database image migration plan (no changes made)\nService: %s\nOwnership: %s (preserved)\nConsumer operation anchor: %s\nTarget: %s\nConfig hash: %s\nBrief downtime required; live identity preflight runs before mutation.\n", service.ID, response.Plan.Ownership, response.Plan.AppID, response.Plan.TargetImage, response.Plan.ConfigHash)
			if sample := response.Plan.Drain; sample != nil {
				fmt.Fprintf(cmd.OutOrStdout(), "Smart shutdown budget: %ds. At expiry, active connections may be terminated and uncommitted transactions rolled back.\n", response.Plan.DrainBudgetSeconds)
				fmt.Fprintf(cmd.OutOrStdout(), "Drain preflight: %s\n", sample.Status)
				if sample.Sessions != nil && sample.ActiveTransactions != nil && sample.IdleInTransaction != nil && sample.OldestTransactionMS != nil {
					fmt.Fprintf(cmd.OutOrStdout(), "Sessions: %d, active transactions: %d, idle in transaction: %d, oldest transaction: %dms\n", *sample.Sessions, *sample.ActiveTransactions, *sample.IdleInTransaction, *sample.OldestTransactionMS)
				}
				if sample.Reason != "" {
					fmt.Fprintln(cmd.OutOrStdout(), sample.Reason)
				}
			}
		} else {
			fmt.Fprintf(cmd.OutOrStdout(), "Database image migration %s: %s\n", response.Operation.ID, response.Operation.Status)
			if timing := response.Timing; timing != nil {
				fmt.Fprintf(cmd.OutOrStdout(), "Queue: %s; execution through verification: %s\n", time.Duration(timing.QueueMS)*time.Millisecond, time.Duration(timing.ExecutionMS)*time.Millisecond)
				if ms, ok := timing.Endpoint["observed_failure_to_recovery_ms"].(float64); ok {
					fmt.Fprintf(cmd.OutOrStdout(), "Stable Service SQL probe, first failure to first recovery: %s (sampled, not client downtime)\n", time.Duration(ms)*time.Millisecond)
				} else {
					fmt.Fprintf(cmd.OutOrStdout(), "Stable Service SQL probe: %v\n", timing.Endpoint["status"])
				}
			}
		}
		return nil
	}}
	cmd.Flags().StringVar(&request.Image, "image", "", "Verified immutable target image (defaults to the platform's approved baked-manager image)")
	cmd.Flags().BoolVar(&request.Apply, "apply", false, "Apply the migration; without this flag only a configuration plan is returned")
	cmd.Flags().BoolVar(&request.Acknowledge, "acknowledge-downtime", false, "Acknowledge a brief database interruption")
	cmd.Flags().IntVar(&request.DrainBudget, "drain-budget-seconds", 0, "Optional 10-15 second smart shutdown budget for this operation only")
	cmd.Flags().BoolVar(&request.AcknowledgeTermination, "acknowledge-connection-termination", false, "Allow the shorter shutdown budget to end connections and roll back uncommitted transactions")
	cmd.Flags().StringVar(&request.Expected, "expected-config-hash", "", "Configuration hash from the preceding plan")
	cmd.Flags().BoolVar(&wait, "wait", true, "Wait for identity, storage and SQL verification")
	return cmd
}
