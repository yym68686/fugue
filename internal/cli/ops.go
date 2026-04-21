package cli

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"

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
		Types       []string
		Statuses    []string
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
				appID           string
				tenantIDFilter  string
				projectIDFilter string
				operations      []model.Operation
				appInventory    []model.App
				needInventory   bool
			)
			tenantIDFilter, projectIDFilter, err = c.resolveFilterSelections(client)
			if err != nil {
				return err
			}
			if strings.TrimSpace(opts.Project) != "" {
				project, err := c.resolveNamedProject(client, opts.Project)
				if err != nil {
					return err
				}
				projectIDFilter = project.ID
			}
			needInventory = strings.TrimSpace(projectIDFilter) != "" || !c.wantsJSON()
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

			if strings.TrimSpace(tenantIDFilter) != "" {
				operations = filterOperationsByTenantID(operations, tenantIDFilter)
			}
			if strings.TrimSpace(projectIDFilter) != "" {
				appIDs := make(map[string]struct{})
				for _, app := range appInventory {
					if strings.TrimSpace(app.ProjectID) != strings.TrimSpace(projectIDFilter) {
						continue
					}
					appIDs[strings.TrimSpace(app.ID)] = struct{}{}
				}
				operations = filterOperationsByAppIDs(operations, appIDs)
			}
			operations = filterOperationsByKinds(operations, opts.Types)
			operations = filterOperationsByStatuses(operations, opts.Statuses)

			sortOperationsNewestFirst(operations)
			totalOperations := len(operations)
			limit := resolveOperationListLimit(opts.Limit, opts.All, c.wantsJSON())
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
				_, _ = fmt.Fprintf(c.stderr, "showing %d of %d operations; use --limit, --all, --app, --project, --type, or --status to narrow\n", len(operations), totalOperations)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&opts.App, "app", "", "Limit operations to one app")
	cmd.Flags().StringVar(&opts.Project, "project", "", "Limit operations to one project")
	cmd.Flags().StringSliceVar(&opts.Types, "type", nil, "Limit operations to one or more operation types")
	cmd.Flags().StringSliceVar(&opts.Statuses, "status", nil, "Limit operations to one or more statuses")
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
		Short:   "Explain why an operation is pending, waiting, or failed",
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
	if err := writeKeyValues(w, pairs...); err != nil {
		return err
	}
	if diagnosis == nil {
		return nil
	}
	for _, evidence := range diagnosis.Evidence {
		if _, err := fmt.Fprintf(w, "evidence=%s\n", evidence); err != nil {
			return err
		}
	}
	if diagnosis.BuilderPlacement == nil {
		return nil
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "[builder_placement]"); err != nil {
		return err
	}
	if err := writeKeyValues(w,
		kvPair{Key: "profile", Value: strings.TrimSpace(diagnosis.BuilderPlacement.Profile)},
		kvPair{Key: "build_strategy", Value: strings.TrimSpace(diagnosis.BuilderPlacement.BuildStrategy)},
		kvPair{Key: "demand", Value: formatBuilderResourceSnapshot(diagnosis.BuilderPlacement.Demand)},
		kvPair{Key: "required_node_labels", Value: formatStringMapInline(diagnosis.BuilderPlacement.RequiredNodeLabels)},
		kvPair{Key: "active_reservations", Value: formatInt(len(diagnosis.BuilderPlacement.Reservations))},
		kvPair{Key: "active_locks", Value: formatInt(len(diagnosis.BuilderPlacement.Locks))},
	); err != nil {
		return err
	}
	if len(diagnosis.BuilderPlacement.Reservations) > 0 {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w, "[builder_reservations]"); err != nil {
			return err
		}
		if err := writeBuilderReservationTable(w, diagnosis.BuilderPlacement.Reservations); err != nil {
			return err
		}
	}
	if len(diagnosis.BuilderPlacement.Locks) > 0 {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w, "[builder_locks]"); err != nil {
			return err
		}
		if err := writeBuilderLockTable(w, diagnosis.BuilderPlacement.Locks); err != nil {
			return err
		}
	}
	if len(diagnosis.BuilderPlacement.Nodes) > 0 {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w, "[builder_nodes]"); err != nil {
			return err
		}
		if err := writeBuilderNodeInspectionTable(w, diagnosis.BuilderPlacement.Nodes); err != nil {
			return err
		}
	}
	return nil
}

func (c *CLI) tryLoadOperationDiagnosis(client *Client, op model.Operation) (*model.OperationDiagnosis, error) {
	switch strings.TrimSpace(op.Status) {
	case model.OperationStatusPending, model.OperationStatusRunning, model.OperationStatusWaitingAgent, model.OperationStatusFailed:
		return client.TryGetOperationDiagnosis(op.ID)
	default:
		return nil, nil
	}
}

func resolveOperationListLimit(requested int, showAll bool, jsonOutput bool) int {
	if showAll {
		return 0
	}
	if requested > 0 {
		return requested
	}
	if jsonOutput {
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

func filterOperationsByTenantID(operations []model.Operation, tenantID string) []model.Operation {
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		return operations
	}
	filtered := make([]model.Operation, 0, len(operations))
	for _, operation := range operations {
		if strings.TrimSpace(operation.TenantID) != tenantID {
			continue
		}
		filtered = append(filtered, operation)
	}
	return filtered
}

func filterOperationsByKinds(operations []model.Operation, requested []string) []model.Operation {
	allowed := normalizedFilterSet(requested)
	if len(allowed) == 0 {
		return operations
	}
	filtered := make([]model.Operation, 0, len(operations))
	for _, operation := range operations {
		if _, ok := allowed[strings.ToLower(strings.TrimSpace(operation.Type))]; !ok {
			continue
		}
		filtered = append(filtered, operation)
	}
	return filtered
}

func filterOperationsByStatuses(operations []model.Operation, requested []string) []model.Operation {
	allowed := normalizedFilterSet(requested)
	if len(allowed) == 0 {
		return operations
	}
	filtered := make([]model.Operation, 0, len(operations))
	for _, operation := range operations {
		if _, ok := allowed[strings.ToLower(strings.TrimSpace(operation.Status))]; !ok {
			continue
		}
		filtered = append(filtered, operation)
	}
	return filtered
}

func normalizedFilterSet(values []string) map[string]struct{} {
	normalized := make(map[string]struct{}, len(values))
	for _, raw := range values {
		for _, value := range strings.Split(raw, ",") {
			value = strings.ToLower(strings.TrimSpace(value))
			if value == "" {
				continue
			}
			normalized[value] = struct{}{}
		}
	}
	return normalized
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

func formatBuilderResourceSnapshot(value model.BuilderResourceSnapshot) string {
	parts := []string{}
	if value.CPUMilli != 0 {
		parts = append(parts, fmt.Sprintf("%dm CPU", value.CPUMilli))
	}
	if value.MemoryBytes != 0 {
		parts = append(parts, fmt.Sprintf("%s RAM", formatBytes(value.MemoryBytes)))
	}
	if value.EphemeralBytes != 0 {
		parts = append(parts, fmt.Sprintf("%s eph", formatBytes(value.EphemeralBytes)))
	}
	return strings.Join(parts, ", ")
}

func formatStringMapInline(values map[string]string) string {
	if len(values) == 0 {
		return ""
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s=%s", key, values[key]))
	}
	return strings.Join(parts, ",")
}

func writeBuilderReservationTable(w io.Writer, reservations []model.BuilderPlacementReservationInspection) error {
	sorted := append([]model.BuilderPlacementReservationInspection(nil), reservations...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].NodeName != sorted[j].NodeName {
			return sorted[i].NodeName < sorted[j].NodeName
		}
		return sorted[i].Name < sorted[j].Name
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "NAME\tNODE\tRENEWED\tEXPIRES\tDEMAND"); err != nil {
		return err
	}
	for _, reservation := range sorted {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%s\n",
			strings.TrimSpace(reservation.Name),
			firstNonEmpty(strings.TrimSpace(reservation.NodeName), "-"),
			firstNonEmpty(formatOptionalTimePtr(reservation.RenewedAt), "-"),
			firstNonEmpty(formatOptionalTimePtr(reservation.ExpiresAt), "-"),
			firstNonEmpty(formatBuilderResourceSnapshot(reservation.Demand), "-"),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeBuilderLockTable(w io.Writer, locks []model.BuilderPlacementLockInspection) error {
	sorted := append([]model.BuilderPlacementLockInspection(nil), locks...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].NodeName != sorted[j].NodeName {
			return sorted[i].NodeName < sorted[j].NodeName
		}
		return sorted[i].Name < sorted[j].Name
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "NAME\tNODE\tHOLDER\tRENEWED\tEXPIRES"); err != nil {
		return err
	}
	for _, lock := range sorted {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%s\n",
			strings.TrimSpace(lock.Name),
			firstNonEmpty(strings.TrimSpace(lock.NodeName), "-"),
			firstNonEmpty(strings.TrimSpace(lock.HolderIdentity), "-"),
			firstNonEmpty(formatOptionalTimePtr(lock.RenewedAt), "-"),
			firstNonEmpty(formatOptionalTimePtr(lock.ExpiresAt), "-"),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeBuilderNodeInspectionTable(w io.Writer, nodes []model.BuilderPlacementNodeInspection) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "RANK\tNODE\tHOST\tELIGIBLE\tREADY\tDISK\tMODE\tAVAILABLE\tREMAINING\tREASONS"); err != nil {
		return err
	}
	for _, node := range nodes {
		rank := ""
		if node.Rank > 0 {
			rank = formatInt(node.Rank)
		}
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			rank,
			strings.TrimSpace(node.NodeName),
			firstNonEmpty(strings.TrimSpace(node.Hostname), "-"),
			builderBoolLabel(node.Eligible),
			builderBoolLabel(node.Ready),
			builderDiskPressureLabel(node.DiskPressure),
			firstNonEmpty(strings.TrimSpace(node.NodeMode), "-"),
			firstNonEmpty(formatBuilderResourceSnapshot(node.Available), "-"),
			firstNonEmpty(formatBuilderResourceSnapshot(node.Remaining), "-"),
			firstNonEmpty(strings.Join(node.Reasons, "; "), "-"),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func builderBoolLabel(value bool) string {
	if value {
		return "yes"
	}
	return "no"
}

func builderDiskPressureLabel(value bool) string {
	if value {
		return "pressure"
	}
	return "ok"
}
