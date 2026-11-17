package cli

import (
	"fmt"
	"net/http"
	"net/url"

	"fugue/internal/model"
	"fugue/internal/pgimage"
	"github.com/spf13/cobra"
)

type postgresImageMigrationRequest struct {
	Image       string `json:"image,omitempty"`
	Apply       bool   `json:"apply"`
	Acknowledge bool   `json:"acknowledge_downtime"`
	Expected    string `json:"expected_config_hash,omitempty"`
}
type postgresImageMigrationResponse struct {
	Plan      pgimage.Plan     `json:"plan"`
	DryRun    bool             `json:"dry_run"`
	Operation *model.Operation `json:"operation,omitempty"`
}

func (c *CLI) newServicePostgresMigrateImageCommand() *cobra.Command {
	var request postgresImageMigrationRequest
	var wait bool
	cmd := &cobra.Command{Use: "migrate-image <service>", Short: "Plan or explicitly apply a verified same-version baked-manager image migration (brief downtime)", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
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
		if response.Plan.ServiceID != service.ID || response.Plan.ConfigHash == "" || response.DryRun == request.Apply {
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
			}
		}
		if c.wantsJSON() {
			response.Operation = redactOperationPtrForOutput(response.Operation)
			return c.writeJSON(response)
		}
		if response.DryRun {
			fmt.Fprintf(cmd.OutOrStdout(), "Database image migration plan (no changes made)\nService: %s\nTarget: %s\nConfig hash: %s\nBrief downtime required; live identity preflight runs before mutation.\n", service.ID, response.Plan.TargetImage, response.Plan.ConfigHash)
		} else {
			fmt.Fprintf(cmd.OutOrStdout(), "Database image migration %s: %s\n", response.Operation.ID, response.Operation.Status)
		}
		return nil
	}}
	cmd.Flags().StringVar(&request.Image, "image", "", "Verified immutable target image (defaults to the platform's approved baked-manager image)")
	cmd.Flags().BoolVar(&request.Apply, "apply", false, "Apply the migration; without this flag only a configuration plan is returned")
	cmd.Flags().BoolVar(&request.Acknowledge, "acknowledge-downtime", false, "Acknowledge a brief database interruption")
	cmd.Flags().StringVar(&request.Expected, "expected-config-hash", "", "Configuration hash from the preceding plan")
	cmd.Flags().BoolVar(&wait, "wait", true, "Wait for identity, storage and SQL verification")
	return cmd
}
