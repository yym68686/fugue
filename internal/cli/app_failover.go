package cli

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"

	failoverpkg "fugue/internal/failover"
	"fugue/internal/model"

	"github.com/spf13/cobra"
)

type appFailoverResult struct {
	App        model.App                 `json:"app"`
	Assessment failoverpkg.AppAssessment `json:"assessment"`
}

func (c *CLI) newAppContinuityCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "continuity",
		Short: "Audit and configure app continuity settings",
	}
	cmd.AddCommand(
		c.newAppContinuityAuditCommand(),
		c.newAppContinuitySetCommand(),
		c.newAppContinuityOffCommand(),
	)
	return cmd
}

func (c *CLI) newAppContinuityAuditCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "audit [app]",
		Aliases: []string{"ha", "dr"},
		Short:   "Audit failover readiness for apps",
		Args:    cobra.RangeArgs(0, 1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}

			runtimes, err := client.ListRuntimes()
			if err != nil {
				c.progressf("warning=runtime inventory unavailable: %v", err)
			}
			runtimeByID := mapRuntimesByID(runtimes)

			if len(args) == 1 {
				app, err := c.resolveNamedApp(client, args[0])
				if err != nil {
					return err
				}
				app, err = client.GetApp(app.ID)
				if err != nil {
					return err
				}
				result := buildAppFailoverResult(app, runtimeByID)
				if c.wantsJSON() {
					return writeJSON(c.stdout, result)
				}
				return writeAppFailoverStatus(c.stdout, result)
			}

			tenantID, projectID, err := c.resolveFilterSelections(client)
			if err != nil {
				return err
			}
			apps, err := client.ListApps()
			if err != nil {
				return err
			}
			results := buildAppFailoverResults(filterApps(apps, tenantID, projectID), runtimeByID)
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"assessments": results})
			}
			return writeAppFailoverTable(c.stdout, results)
		},
	}
}

func (c *CLI) newAppContinuitySetCommand() *cobra.Command {
	opts := struct {
		AppRuntimeName string
		AppRuntimeID   string
		DBRuntimeName  string
		DBRuntimeID    string
		Wait           bool
	}{Wait: true}
	cmd := &cobra.Command{
		Use:   "set <app>",
		Short: "Enable app and/or database continuity targets",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}

			request := patchAppContinuityRequest{}
			if strings.TrimSpace(opts.AppRuntimeName) != "" || strings.TrimSpace(opts.AppRuntimeID) != "" {
				runtimeID, err := resolveRuntimeSelection(client, opts.AppRuntimeID, opts.AppRuntimeName)
				if err != nil {
					return err
				}
				request.AppFailover = &appContinuityAppFailoverRequest{
					Enabled:         true,
					TargetRuntimeID: runtimeID,
				}
			}
			if strings.TrimSpace(opts.DBRuntimeName) != "" || strings.TrimSpace(opts.DBRuntimeID) != "" {
				runtimeID, err := resolveRuntimeSelection(client, opts.DBRuntimeID, opts.DBRuntimeName)
				if err != nil {
					return err
				}
				request.DatabaseFailover = &appContinuityDatabaseFailoverRequest{
					Enabled:         true,
					TargetRuntimeID: runtimeID,
				}
			}
			if request.AppFailover == nil && request.DatabaseFailover == nil {
				return fmt.Errorf("at least one of --app-to or --db-to is required")
			}

			response, err := client.PatchAppContinuity(app.ID, request)
			if err != nil {
				return err
			}
			if err := c.waitForOptionalOperation(client, response.Operation, opts.Wait); err != nil {
				return err
			}
			return c.renderAppContinuityResult(app.ID, response)
		},
	}
	cmd.Flags().StringVar(&opts.AppRuntimeName, "app-to", "", "Target runtime for app failover")
	cmd.Flags().StringVar(&opts.AppRuntimeID, "app-runtime-id", "", "Target runtime ID for app failover")
	cmd.Flags().StringVar(&opts.DBRuntimeName, "db-to", "", "Target runtime for database failover")
	cmd.Flags().StringVar(&opts.DBRuntimeID, "db-runtime-id", "", "Target runtime ID for database failover")
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait for the deploy operation to complete")
	_ = cmd.Flags().MarkHidden("app-runtime-id")
	_ = cmd.Flags().MarkHidden("db-runtime-id")
	return cmd
}

func (c *CLI) newAppContinuityOffCommand() *cobra.Command {
	opts := struct {
		App  bool
		DB   bool
		Wait bool
	}{Wait: true}
	cmd := &cobra.Command{
		Use:   "off <app>",
		Short: "Disable app and/or database continuity",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			disableApp := opts.App || (!opts.App && !opts.DB)
			disableDB := opts.DB || (!opts.App && !opts.DB)
			request := patchAppContinuityRequest{}
			if disableApp {
				request.AppFailover = &appContinuityAppFailoverRequest{Enabled: false}
			}
			if disableDB {
				request.DatabaseFailover = &appContinuityDatabaseFailoverRequest{Enabled: false}
			}
			response, err := client.PatchAppContinuity(app.ID, request)
			if err != nil {
				return err
			}
			if err := c.waitForOptionalOperation(client, response.Operation, opts.Wait); err != nil {
				return err
			}
			return c.renderAppContinuityResult(app.ID, response)
		},
	}
	cmd.Flags().BoolVar(&opts.App, "app", false, "Disable only app failover")
	cmd.Flags().BoolVar(&opts.DB, "db", false, "Disable only database failover")
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait for the deploy operation to complete")
	return cmd
}

func (c *CLI) newAppFailoverCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "failover",
		Short: "Execute app failover",
		RunE: func(cmd *cobra.Command, args []string) error {
			return fmt.Errorf("use \"fugue app failover run <app>\" to execute failover, or \"fugue app continuity audit [app]\" to audit readiness")
		},
	}
	cmd.AddCommand(c.newAppFailoverRunCommand())
	return cmd
}

func (c *CLI) newAppFailoverRunCommand() *cobra.Command {
	opts := struct {
		RuntimeName string
		RuntimeID   string
		Wait        bool
	}{Wait: true}
	cmd := &cobra.Command{
		Use:   "run <app>",
		Short: "Execute failover to a target runtime",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			targetRuntimeID := ""
			if strings.TrimSpace(opts.RuntimeName) != "" || strings.TrimSpace(opts.RuntimeID) != "" {
				targetRuntimeID, err = resolveRuntimeSelection(client, opts.RuntimeID, opts.RuntimeName)
				if err != nil {
					return err
				}
			}
			response, err := client.FailoverApp(app.ID, targetRuntimeID)
			if err != nil {
				return err
			}
			result := appCommandResult{Operation: &response.Operation}
			if opts.Wait {
				finalApp, err := c.waitForSingleApp(client, app.ID, response.Operation, true)
				if err != nil {
					return err
				}
				result.App = finalApp
			} else {
				result.App = &app
			}
			return c.renderAppCommandResult(result)
		},
	}
	cmd.Flags().StringVar(&opts.RuntimeName, "to", "", "Target runtime name")
	cmd.Flags().StringVar(&opts.RuntimeID, "runtime-id", "", "Target runtime ID")
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait for operation completion")
	_ = cmd.Flags().MarkHidden("runtime-id")
	return cmd
}

func (c *CLI) renderAppContinuityResult(appID string, result appContinuityResponse) error {
	if c.wantsJSON() {
		return writeJSON(c.stdout, map[string]any{
			"app_id":          appID,
			"app_failover":    result.AppFailover,
			"database":        result.Database,
			"already_current": result.AlreadyCurrent,
			"operation":       result.Operation,
		})
	}
	pairs := []kvPair{{Key: "app_id", Value: appID}}
	if result.Operation != nil {
		pairs = append(pairs, kvPair{Key: "operation_id", Value: result.Operation.ID})
	}
	if result.AlreadyCurrent {
		pairs = append(pairs, kvPair{Key: "already_current", Value: "true"})
	}
	if result.AppFailover != nil {
		pairs = append(pairs,
			kvPair{Key: "app_failover_enabled", Value: "true"},
			kvPair{Key: "app_failover_target_runtime_id", Value: result.AppFailover.TargetRuntimeID},
		)
	} else {
		pairs = append(pairs, kvPair{Key: "app_failover_enabled", Value: "false"})
	}
	if result.Database != nil && strings.TrimSpace(result.Database.FailoverTargetRuntimeID) != "" {
		pairs = append(pairs,
			kvPair{Key: "database_failover_enabled", Value: "true"},
			kvPair{Key: "database_failover_target_runtime_id", Value: result.Database.FailoverTargetRuntimeID},
		)
	} else {
		pairs = append(pairs, kvPair{Key: "database_failover_enabled", Value: "false"})
	}
	return writeKeyValues(c.stdout, pairs...)
}

func buildAppFailoverResults(apps []model.App, runtimeByID map[string]*model.Runtime) []appFailoverResult {
	results := make([]appFailoverResult, 0, len(apps))
	for _, app := range apps {
		results = append(results, buildAppFailoverResult(app, runtimeByID))
	}
	sort.Slice(results, func(i, j int) bool {
		left := results[i]
		right := results[j]
		leftSeverity := failoverSeverity(left.Assessment.Classification)
		rightSeverity := failoverSeverity(right.Assessment.Classification)
		if leftSeverity != rightSeverity {
			return leftSeverity < rightSeverity
		}
		return strings.Compare(left.App.Name, right.App.Name) < 0
	})
	return results
}

func buildAppFailoverResult(app model.App, runtimeByID map[string]*model.Runtime) appFailoverResult {
	runtime := runtimeByID[appRuntimeID(app)]
	return appFailoverResult{
		App:        app,
		Assessment: failoverpkg.AssessApp(app, runtime),
	}
}

func mapRuntimesByID(runtimes []model.Runtime) map[string]*model.Runtime {
	out := make(map[string]*model.Runtime, len(runtimes))
	for index := range runtimes {
		runtime := &runtimes[index]
		out[runtime.ID] = runtime
	}
	return out
}

func appRuntimeID(app model.App) string {
	runtimeID := strings.TrimSpace(app.Status.CurrentRuntimeID)
	if runtimeID != "" {
		return runtimeID
	}
	return strings.TrimSpace(app.Spec.RuntimeID)
}

func failoverSeverity(classification string) int {
	switch strings.TrimSpace(strings.ToLower(classification)) {
	case failoverpkg.AppClassificationBlocked:
		return 0
	case failoverpkg.AppClassificationCaution:
		return 1
	default:
		return 2
	}
}

func writeAppFailoverTable(w io.Writer, results []appFailoverResult) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "APP\tCLASS\tREPLICAS\tRUNTIME\tNOTES"); err != nil {
		return err
	}
	for _, result := range results {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%d/%d\t%s\t%s\n",
			result.App.Name,
			result.Assessment.Classification,
			result.App.Status.CurrentReplicas,
			result.App.Spec.Replicas,
			formatFailoverRuntime(result.Assessment),
			result.Assessment.Summary,
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeAppFailoverStatus(w io.Writer, result appFailoverResult) error {
	pairs := []kvPair{
		{Key: "app_id", Value: result.App.ID},
		{Key: "name", Value: result.App.Name},
		{Key: "classification", Value: result.Assessment.Classification},
		{Key: "summary", Value: result.Assessment.Summary},
		{Key: "desired_replicas", Value: fmt.Sprintf("%d", result.App.Spec.Replicas)},
		{Key: "current_replicas", Value: fmt.Sprintf("%d", result.App.Status.CurrentReplicas)},
		{Key: "runtime_id", Value: result.Assessment.RuntimeID},
		{Key: "runtime_type", Value: result.Assessment.RuntimeType},
		{Key: "runtime_status", Value: result.Assessment.RuntimeStatus},
		{Key: "blockers", Value: strings.Join(result.Assessment.Blockers, "; ")},
		{Key: "warnings", Value: strings.Join(result.Assessment.Warnings, "; ")},
	}
	return writeKeyValues(w, pairs...)
}

func formatFailoverRuntime(assessment failoverpkg.AppAssessment) string {
	if strings.TrimSpace(assessment.RuntimeID) == "" {
		return ""
	}
	if strings.TrimSpace(assessment.RuntimeType) == "" {
		return assessment.RuntimeID
	}
	return assessment.RuntimeType + ":" + assessment.RuntimeID
}
