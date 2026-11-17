package cli

import (
	"fmt"
	"net/http"
	"net/url"

	"fugue/internal/model"
	"fugue/internal/pgretire"
	"github.com/spf13/cobra"
)

func (c *CLI) newServicePostgresRetireSourceCommand() *cobra.Command {
	var req struct {
		Apply     bool   `json:"apply"`
		SourceUID string `json:"expected_source_uid,omitempty"`
		PlanHash  string `json:"expected_plan_hash,omitempty"`
	}
	var wait bool
	cmd := &cobra.Command{Use: "retire-source <service>", Short: "Preview or retire an exact completed migration source Cluster/Pods while retaining all storage", Long: "Platform administrators only. Defaults to a fresh read-only identity plan. Apply requires the exact source UID and plan hash. Never deletes PVCs/PVs, Services, secrets or destination resources; never deploys or stops the consumer application. The durable exclusive operation rechecks live identities, source stopped state and destination SQL before removal and after verification.", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if req.Apply && (req.SourceUID == "" || req.PlanHash == "") {
			return fmt.Errorf("--apply requires --expected-source-uid and --expected-plan-hash from a preceding plan")
		}
		client, err := c.newClient()
		if err != nil {
			return err
		}
		svc, err := c.resolveNamedService(client, args[0])
		if err != nil {
			return err
		}
		var response struct {
			Plan      model.PostgresSourceRetirementPlan `json:"plan"`
			DryRun    bool                               `json:"dry_run"`
			Operation *model.Operation                   `json:"operation,omitempty"`
		}
		if err = client.doJSON(http.MethodPost, "/v1/backing-services/"+url.PathEscape(svc.ID)+"/retire-source", req, &response); err != nil {
			return err
		}
		p := response.Plan
		if p.ServiceID != svc.ID || !p.PreserveStorage || p.PlanHash == "" || p.PlanHash != pgretire.PlanHash(p) || p.SourceUID == "" || p.SourceUID == p.TargetUID || response.DryRun == req.Apply {
			return fmt.Errorf("source retirement response identity or storage contract is invalid")
		}
		if req.Apply {
			if p.SourceUID != req.SourceUID || p.PlanHash != req.PlanHash || response.Operation == nil || response.Operation.ID == "" || response.Operation.Type != pgretire.OperationType || response.Operation.ServiceID != svc.ID {
				return fmt.Errorf("retirement operation does not match the confirmed plan")
			}
			if wait {
				ops, e := c.waitForOperations(client, []model.Operation{*response.Operation})
				if e != nil {
					return e
				}
				if len(ops) != 1 || ops[0].Status != model.OperationStatusCompleted {
					return fmt.Errorf("source retirement did not complete; inspect its durable operation")
				}
				response.Operation = &ops[0]
			}
		}
		if c.wantsJSON() {
			response.Operation = redactOperationPtrForOutput(response.Operation)
			return c.writeJSON(response)
		}
		fmt.Fprintf(cmd.OutOrStdout(), "PostgreSQL source retirement (storage preserved)\nService: %s\nSource Cluster: %s\nSource UID: %s\nDestination: %s (unchanged)\nPods: %d\nRetained PVC/PV pairs: %d\nPlan hash: %s\n", p.ServiceID, p.SourceCluster, p.SourceUID, p.TargetCluster, len(p.Pods), len(p.Volumes), p.PlanHash)
		for _, v := range p.ProtectedVolumes {
			fmt.Fprintf(cmd.OutOrStdout(), "Protected unowned storage (read-only, unchanged): %s / %s\n", v.PVC, v.PV)
		}
		if response.DryRun {
			fmt.Fprintln(cmd.OutOrStdout(), "Preview only; no changes made.")
		} else {
			fmt.Fprintf(cmd.OutOrStdout(), "Operation %s: %s\n", response.Operation.ID, response.Operation.Status)
		}
		return nil
	}}
	cmd.Flags().BoolVar(&req.Apply, "apply", false, "Explicitly retire the identified source; default is a read-only preview")
	cmd.Flags().StringVar(&req.SourceUID, "expected-source-uid", "", "Exact source Cluster UID from the preceding plan")
	cmd.Flags().StringVar(&req.PlanHash, "expected-plan-hash", "", "Exact identity/configuration hash from the preceding plan")
	cmd.Flags().BoolVar(&wait, "wait", true, "Wait for source removal, retained storage and destination verification")
	return cmd
}
