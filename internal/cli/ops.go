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
		c.newOpsExplainCommand(),
		c.newOpsWatchCommand(),
		c.newOpsAuditCommand(),
	)
	return cmd
}

func (c *CLI) newOpsListCommand() *cobra.Command {
	opts := struct {
		App         string
		Project     string
		Limit       int
		All         bool
		ShowSecrets bool
	}{}
	cmd := &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list", "history"},
		Short:   "List operations",
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(opts.App) != "" && strings.TrimSpace(opts.Project) != "" {
				return fmt.Errorf("--app and --project cannot be used together")
			}
			if opts.All && opts.Limit > 0 {
				return fmt.Errorf("--all and --limit cannot be used together")
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}

			var (
				appID         string
				operations    []model.Operation
				appInventory  []model.App
				needInventory = strings.TrimSpace(opts.Project) != "" || !c.wantsJSON()
			)
			if strings.TrimSpace(opts.App) != "" {
				app, err := c.resolveNamedApp(client, opts.App)
				if err != nil {
					return err
				}
				appID = app.ID
				if needInventory {
					appInventory = []model.App{app}
				}
			}

			operations, err = client.ListOperations(appID)
			if err != nil {
				return err
			}

			if needInventory && len(appInventory) == 0 {
				appInventory, err = client.ListApps()
				if err != nil {
					return err
				}
			}

			projectID := ""
			if strings.TrimSpace(opts.Project) != "" {
				project, err := c.resolveNamedProject(client, opts.Project)
				if err != nil {
					return err
				}
				projectID = project.ID
				appIDs := make(map[string]struct{})
				for _, app := range appInventory {
					if strings.TrimSpace(app.ProjectID) != strings.TrimSpace(projectID) {
						continue
					}
					appIDs[strings.TrimSpace(app.ID)] = struct{}{}
				}
				operations = filterOperationsByAppIDs(operations, appIDs)
			}

			sortOperationsNewestFirst(operations)
			totalOperations := len(operations)
			limit := resolveOperationListLimit(opts.Limit, opts.All, appID, projectID, c.wantsJSON())
			if limit > 0 && len(operations) > limit {
				operations = operations[:limit]
			}

			if !opts.ShowSecrets {
				operations = redactOperationsForOutput(operations)
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"operations": operations})
			}
			if err := writeOperationTableWithApps(c.stdout, operations, mapAppNames(appInventory)); err != nil {
				return err
			}
			if limit > 0 && totalOperations > len(operations) {
				_, _ = fmt.Fprintf(c.stderr, "showing %d of %d operations; use --limit, --all, --app, or --project to narrow\n", len(operations), totalOperations)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&opts.App, "app", "", "Limit operations to one app")
	cmd.Flags().StringVar(&opts.Project, "project", "", "Limit operations to one project")
	cmd.Flags().IntVar(&opts.Limit, "limit", 0, "Maximum number of operations to show")
	cmd.Flags().BoolVar(&opts.All, "all", false, "Show the full operation list without the default text-mode limit")
	cmd.Flags().BoolVar(&opts.ShowSecrets, "show-secrets", false, "Show env values, passwords, and other sensitive fields")
	return cmd
}

func (c *CLI) newOpsShowCommand() *cobra.Command {
	opts := struct {
		ShowSecrets bool
	}{}
	cmd := &cobra.Command{
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
			diagnosis, err := c.tryLoadOperationDiagnosis(client, op)
			if err != nil {
				return err
			}
			if !opts.ShowSecrets {
				op = redactOperationForOutput(op)
			}
			if c.wantsJSON() {
				payload := map[string]any{"operation": op}
				if diagnosis != nil {
					payload["diagnosis"] = diagnosis
				}
				return writeJSON(c.stdout, payload)
			}
			return renderOperationWithDiagnosis(c.stdout, op, diagnosis)
		},
	}
	cmd.Flags().BoolVar(&opts.ShowSecrets, "show-secrets", false, "Show env values, passwords, and other sensitive fields")
	return cmd
}

func (c *CLI) newOpsExplainCommand() *cobra.Command {
	opts := struct {
		ShowSecrets bool
	}{}
	cmd := &cobra.Command{
		Use:     "explain <operation>",
		Aliases: []string{"diagnose"},
		Short:   "Explain why an operation is pending or waiting",
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
			diagnosis, err := client.GetOperationDiagnosis(op.ID)
			if err != nil {
				return err
			}
			if !opts.ShowSecrets {
				op = redactOperationForOutput(op)
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{
					"operation": op,
					"diagnosis": diagnosis,
				})
			}
			return renderOperationWithDiagnosis(c.stdout, op, &diagnosis)
		},
	}
	cmd.Flags().BoolVar(&opts.ShowSecrets, "show-secrets", false, "Show env values, passwords, and other sensitive fields")
	return cmd
}

func (c *CLI) newOpsWatchCommand() *cobra.Command {
	opts := struct {
		App         string
		ShowSecrets bool
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
			if !opts.ShowSecrets {
				op = redactOperationForOutput(op)
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"operation": op})
			}
			return renderOperation(c.stdout, op)
		},
	}
	cmd.Flags().StringVar(&opts.App, "app", "", "Watch the most recent operation for an app")
	cmd.Flags().BoolVar(&opts.ShowSecrets, "show-secrets", false, "Show env values, passwords, and other sensitive fields")
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

func redactOperationsForOutput(operations []model.Operation) []model.Operation {
	if len(operations) == 0 {
		return nil
	}
	out := make([]model.Operation, 0, len(operations))
	for _, operation := range operations {
		out = append(out, redactOperationForOutput(operation))
	}
	return out
}

func renderOperation(w io.Writer, op model.Operation) error {
	return renderOperationWithDiagnosis(w, op, nil)
}

func renderOperationWithDiagnosis(w io.Writer, op model.Operation, diagnosis *model.OperationDiagnosis) error {
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
	if diagnosis != nil {
		pairs = append(pairs,
			kvPair{Key: "diagnosis_category", Value: diagnosis.Category},
			kvPair{Key: "diagnosis_summary", Value: diagnosis.Summary},
			kvPair{Key: "diagnosis_hint", Value: diagnosis.Hint},
			kvPair{Key: "diagnosis_service", Value: diagnosis.Service},
		)
		if len(diagnosis.DependencyChain) > 0 {
			pairs = append(pairs, kvPair{Key: "diagnosis_dependency_chain", Value: strings.Join(diagnosis.DependencyChain, " -> ")})
		}
		if blockedBy := formatOperationDiagnosisBlockedBy(diagnosis.BlockedBy); blockedBy != "" {
			pairs = append(pairs, kvPair{Key: "diagnosis_blocked_by", Value: blockedBy})
		}
	}
	return writeKeyValues(w, pairs...)
}

func (c *CLI) tryLoadOperationDiagnosis(client *Client, op model.Operation) (*model.OperationDiagnosis, error) {
	switch strings.TrimSpace(op.Status) {
	case model.OperationStatusPending, model.OperationStatusRunning, model.OperationStatusWaitingAgent:
		return client.TryGetOperationDiagnosis(op.ID)
	default:
		return nil, nil
	}
}

func resolveOperationListLimit(requested int, showAll bool, appID, projectID string, jsonOutput bool) int {
	if showAll {
		return 0
	}
	if requested > 0 {
		return requested
	}
	if jsonOutput {
		return 0
	}
	if strings.TrimSpace(appID) != "" || strings.TrimSpace(projectID) != "" {
		return 0
	}
	return 20
}

func filterOperationsByAppIDs(operations []model.Operation, appIDs map[string]struct{}) []model.Operation {
	if len(appIDs) == 0 {
		return nil
	}
	filtered := make([]model.Operation, 0, len(operations))
	for _, operation := range operations {
		if _, ok := appIDs[strings.TrimSpace(operation.AppID)]; !ok {
			continue
		}
		filtered = append(filtered, operation)
	}
	return filtered
}

func sortOperationsNewestFirst(operations []model.Operation) {
	sort.Slice(operations, func(i, j int) bool {
		if operations[i].CreatedAt.Equal(operations[j].CreatedAt) {
			return operations[i].ID < operations[j].ID
		}
		return operations[i].CreatedAt.After(operations[j].CreatedAt)
	})
}

func formatOperationDiagnosisBlockedBy(blockers []model.OperationDiagnosisBlocker) string {
	if len(blockers) == 0 {
		return ""
	}
	parts := make([]string, 0, len(blockers))
	for _, blocker := range blockers {
		label := strings.TrimSpace(blocker.OperationID)
		if status := strings.TrimSpace(blocker.Status); status != "" {
			label += " " + status
		}
		if opType := strings.TrimSpace(blocker.Type); opType != "" {
			label += " " + opType
		}
		if appName := strings.TrimSpace(blocker.AppName); appName != "" {
			label += " " + appName
		}
		if service := strings.TrimSpace(blocker.Service); service != "" {
			label += " (" + service + ")"
		}
		parts = append(parts, label)
	}
	return strings.Join(parts, "; ")
}
