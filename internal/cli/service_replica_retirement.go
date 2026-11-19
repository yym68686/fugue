package cli

import (
	"fmt"
	"strings"

	"fugue/internal/model"
	"github.com/spf13/cobra"
)

func (c *CLI) newServicePostgresRetireReplicaCommand() *cobra.Command {
	var node, uid string
	var acknowledge bool
	wait := true
	cmd := &cobra.Command{
		Use:   "retire-replica <service>",
		Short: "Retire the exact sole PostgreSQL standby without moving or restarting the primary",
		Long:  "Retire the sole standby on the specified Kubernetes node. Optionally require an observed Pod UID. The standby PVC is removed and the storage reclaim policy applies; the service loses HA. Refuses unsafe topology or storage. Waits until the original primary and SQL endpoint are verified.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(node) == "" || !acknowledge {
				return fmt.Errorf("retirement requires --node and --acknowledge-replica-removal")
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			svc, err := c.resolveNamedService(client, args[0])
			if err != nil {
				return err
			}
			response, err := client.RetireBackingServicePostgresReplica(svc.ID, strings.TrimSpace(node), strings.TrimSpace(uid))
			if err != nil {
				return err
			}
			op := response.Operation
			if op == nil || op.ID == "" || op.ServiceID != svc.ID || op.Type != model.OperationTypeDatabaseReplicaRetire {
				return fmt.Errorf("replica retirement did not return an operation for the selected service")
			}
			if wait {
				c.progressf("operation_id=%s", op.ID)
				ops, err := c.waitForOperations(client, []model.Operation{*op})
				if err != nil {
					return err
				}
				if len(ops) != 1 || ops[0].Status != model.OperationStatusCompleted {
					return fmt.Errorf("replica retirement %s has not completed; inspect the operation", op.ID)
				}
				op = &ops[0]
			}
			if c.wantsJSON() {
				return c.writeJSON(map[string]any{"operation_id": op.ID, "service_id": svc.ID, "status": op.Status, "message": op.ResultMessage})
			}
			_, err = fmt.Fprintf(c.stdout, "Operation %s (%s): %s\n", op.ID, op.Status, op.ResultMessage)
			return err
		},
	}
	cmd.Flags().StringVar(&node, "node", "", "Exact Kubernetes node of the standby")
	cmd.Flags().StringVar(&uid, "expected-pod-uid", "", "Observed Kubernetes UID of the standby Pod")
	cmd.Flags().BoolVar(&acknowledge, "acknowledge-replica-removal", false, "Acknowledge loss of HA and standby PVC removal")
	cmd.Flags().BoolVar(&wait, "wait", true, "Wait for verified retirement")
	return cmd
}
