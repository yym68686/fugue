package cli

import (
	"fmt"
	"strings"

	"fugue/internal/model"
	"github.com/spf13/cobra"
)

func (c *CLI) newServicePostgresRecoverCommand() *cobra.Command {
	var opts struct {
		RuntimeName, RuntimeID, NodeName, StorageSize, StorageClass string
		Apply, Wait                                                 bool
	}
	opts.Wait = true
	cmd := &cobra.Command{
		Use:   "recover <service>",
		Short: "Recover an independent managed Postgres service from a verified offline source",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(opts.StorageClass) == "" {
				return fmt.Errorf("--storage-class is required for an explicit recovery destination")
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			service, err := c.resolveNamedService(client, args[0])
			if err != nil {
				return err
			}
			runtimeID, err := resolveRuntimeSelection(client, opts.RuntimeID, opts.RuntimeName)
			if err != nil {
				return err
			}
			if strings.TrimSpace(runtimeID) == "" {
				return fmt.Errorf("--to or --runtime-id is required")
			}
			response, err := client.RecoverBackingService(service.ID, !opts.Apply, databaseLocalizeRequest{
				TargetRuntimeID: runtimeID, TargetNodeName: opts.NodeName,
				StorageSize: opts.StorageSize, StorageClassName: opts.StorageClass,
			})
			if err != nil {
				return err
			}
			if opts.Apply && opts.Wait && response.Operation != nil {
				finalOps, err := c.waitForOperations(client, []model.Operation{*response.Operation})
				if err != nil {
					return err
				}
				if len(finalOps) == 1 {
					response.Operation = &finalOps[0]
				}
				response.BackingService, err = client.GetBackingService(service.ID)
				if err != nil {
					return err
				}
			}
			result := map[string]any{"backing_service": redactBackingServiceForOutput(response.BackingService), "plan": response.Plan, "dry_run": !opts.Apply}
			if response.Operation != nil {
				result["operation"] = redactOperationForOutput(*response.Operation)
			}
			return c.renderResourceResult(result)
		},
	}
	cmd.Flags().StringVar(&opts.RuntimeName, "to", "", "Managed target runtime")
	cmd.Flags().StringVar(&opts.RuntimeID, "runtime-id", "", "Managed target runtime ID")
	cmd.Flags().StringVar(&opts.NodeName, "node", "", "Target Kubernetes node name")
	cmd.Flags().StringVar(&opts.StorageSize, "storage-size", "", "Requested destination size; never shrink observed source capacity")
	cmd.Flags().StringVar(&opts.StorageClass, "storage-class", "", "Explicit destination storage class")
	cmd.Flags().BoolVar(&opts.Apply, "apply", false, "Queue guarded recovery; default returns intent only")
	cmd.Flags().BoolVar(&opts.Wait, "wait", true, "Wait for recovery completion")
	return cmd
}
