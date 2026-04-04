package cli

import (
	"fmt"
	"io"
	"sort"
	"strings"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

func (c *CLI) newOpsCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "operation",
		Aliases: []string{"ops", "operations"},
		Short:   "Inspect operations and audit activity",
	}
	cmd.AddCommand(
		c.newOpsListCommand(),
		c.newOpsShowCommand(),
		c.newOpsWatchCommand(),
		c.newOpsAuditCommand(),
	)
	return cmd
}

func (c *CLI) newOpsListCommand() *cobra.Command {
	opts := struct {
		App string
	}{}
	cmd := &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list", "history"},
		Short:   "List operations",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			appID, err := c.resolveOpsAppID(client, opts.App)
			if err != nil {
				return err
			}
			operations, err := client.ListOperations(appID)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"operations": operations})
			}
			return writeOperationTable(c.stdout, operations)
		},
	}
	cmd.Flags().StringVar(&opts.App, "app", "", "Limit operations to one app")
	return cmd
}

func (c *CLI) newOpsShowCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "show <operation>",
		Aliases: []string{"get", "status"},
		Short:   "Show one operation",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			op, err := client.GetOperation(args[0])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"operation": op})
			}
			return renderOperation(c.stdout, op)
		},
	}
}

func (c *CLI) newOpsWatchCommand() *cobra.Command {
	opts := struct {
		App string
	}{}
	cmd := &cobra.Command{
		Use:     "watch [operation]",
		Aliases: []string{"wait"},
		Short:   "Watch an operation until it completes",
		Args:    cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			var op model.Operation
			switch {
			case len(args) == 1:
				op, err = client.GetOperation(args[0])
			case strings.TrimSpace(opts.App) != "":
				appID, resolveErr := c.resolveOpsAppID(client, opts.App)
				if resolveErr != nil {
					return resolveErr
				}
				operations, listErr := client.ListOperations(appID)
				if listErr != nil {
					return listErr
				}
				op, err = latestOperation(operations)
			default:
				return fmt.Errorf("operation id or --app is required")
			}
			if err != nil {
				return err
			}
			finalOps, err := c.waitForOperations(client, []model.Operation{op})
			if err != nil {
				return err
			}
			if len(finalOps) > 0 {
				op = finalOps[0]
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"operation": op})
			}
			return renderOperation(c.stdout, op)
		},
	}
	cmd.Flags().StringVar(&opts.App, "app", "", "Watch the most recent operation for an app")
	return cmd
}

func (c *CLI) newOpsAuditCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "audit",
		Aliases: []string{"events"},
		Short:   "List audit events",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			events, err := client.ListAuditEvents()
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"audit_events": events})
			}
			return writeAuditEventTable(c.stdout, events)
		},
	}
}

func (c *CLI) resolveOpsAppID(client *Client, appRef string) (string, error) {
	if strings.TrimSpace(appRef) == "" {
		return "", nil
	}
	app, err := c.resolveNamedApp(client, appRef)
	if err != nil {
		return "", err
	}
	return app.ID, nil
}

func latestOperation(operations []model.Operation) (model.Operation, error) {
	if len(operations) == 0 {
		return model.Operation{}, fmt.Errorf("no operations found")
	}
	sorted := append([]model.Operation(nil), operations...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].CreatedAt.After(sorted[j].CreatedAt)
	})
	return sorted[0], nil
}

func renderOperation(w io.Writer, op model.Operation) error {
	pairs := []kvPair{
		{Key: "operation_id", Value: op.ID},
		{Key: "status", Value: op.Status},
		{Key: "type", Value: op.Type},
		{Key: "app_id", Value: op.AppID},
		{Key: "execution_mode", Value: op.ExecutionMode},
		{Key: "requested_by_type", Value: op.RequestedByType},
		{Key: "requested_by_id", Value: op.RequestedByID},
		{Key: "source_runtime_id", Value: op.SourceRuntimeID},
		{Key: "target_runtime_id", Value: op.TargetRuntimeID},
		{Key: "assigned_runtime_id", Value: op.AssignedRuntimeID},
		{Key: "result_message", Value: op.ResultMessage},
		{Key: "error_message", Value: op.ErrorMessage},
		{Key: "created_at", Value: formatTime(op.CreatedAt)},
		{Key: "updated_at", Value: formatTime(op.UpdatedAt)},
		{Key: "started_at", Value: formatOptionalTimePtr(op.StartedAt)},
		{Key: "completed_at", Value: formatOptionalTimePtr(op.CompletedAt)},
	}
	if op.DesiredReplicas != nil {
		pairs = append(pairs, kvPair{Key: "desired_replicas", Value: fmt.Sprintf("%d", *op.DesiredReplicas)})
	}
	return writeKeyValues(w, pairs...)
}
