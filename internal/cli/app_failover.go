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
	App                       model.App                    `json:"app"`
	Assessment                failoverpkg.AppAssessment    `json:"assessment"`
	BackupReadiness           string                       `json:"backup_readiness,omitempty"`
	BackupPosture             []model.BackupPosture        `json:"backup_posture,omitempty"`
	ZeroDowntime              *model.AppZeroDowntimePolicy `json:"zero_downtime,omitempty"`
	ReleaseTraffic            *model.AppTrafficPolicy      `json:"release_traffic,omitempty"`
	ActiveReleases            []model.AppRelease           `json:"active_releases,omitempty"`
	RecentReleaseAttempts     []model.ReleaseAttempt       `json:"recent_release_attempts,omitempty"`
	GateFailureCount          int                          `json:"gate_failure_count,omitempty"`
	LastGateFailureEvidenceID string                       `json:"last_gate_failure_evidence_id,omitempty"`
}

func (c *CLI) newAppContinuityCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "continuity",
		Short: "Audit and configure app continuity settings",
		Example: strings.TrimSpace(`
  fugue app continuity audit api
  fugue app continuity enable api --zero-downtime safe
  fugue app continuity show api
`),
	}
	cmd.AddCommand(
		c.newAppContinuityAuditCommand(),
		c.newAppContinuityShowCommand(),
		c.newAppContinuityEnableCommand(),
		c.newAppContinuityDisableCommand(),
	)
	return cmd
}

func (c *CLI) newAppContinuityEnableCommand() *cobra.Command {
	cmd := c.newAppContinuitySetCommand()
	cmd.Use = "enable <app>"
	cmd.Aliases = []string{"on", "set"}
	cmd.Short = "Enable app and/or database continuity targets"
	return cmd
}

func (c *CLI) newAppContinuityDisableCommand() *cobra.Command {
	cmd := c.newAppContinuityOffCommand()
	cmd.Use = "disable <app>"
	cmd.Aliases = []string{"off"}
	cmd.Short = "Disable app and/or database continuity"
	return cmd
}

func (c *CLI) newAppContinuityAuditCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "audit [app]",
		Aliases: []string{"ha", "dr"},
		Short:   "Audit failover readiness for apps",
		Example: strings.TrimSpace(`
  fugue app continuity audit
  fugue app continuity audit api
`),
		Args: cobra.RangeArgs(0, 1),
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
				c.attachAppBackupReadiness(client, &result)
				c.attachAppSafeRolloutAudit(client, &result)
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
			for index := range results {
				c.attachAppBackupReadiness(client, &results[index])
				c.attachAppSafeRolloutAudit(client, &results[index])
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"assessments": results})
			}
			return writeAppFailoverTable(c.stdout, results)
		},
	}
}

func (c *CLI) newAppContinuityShowCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "show <app>",
		Short: "Show app continuity settings",
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
			app, err = client.GetApp(app.ID)
			if err != nil {
				return err
			}
			continuity := model.NormalizeAppContinuityPolicy(app.Spec.Continuity)
			response := appContinuityResponse{
				AppFailover:  app.Spec.Failover,
				ZeroDowntime: nil,
				Database:     app.Spec.Postgres,
			}
			if continuity != nil {
				response.ZeroDowntime = continuity.ZeroDowntime
			}
			return c.renderAppContinuityResult(app.ID, response)
		},
	}
}

func (c *CLI) newAppContinuitySetCommand() *cobra.Command {
	opts := struct {
		AppRuntimeName        string
		AppRuntimeID          string
		DBRuntimeName         string
		DBRuntimeID           string
		ZeroDowntimeMode      string
		Canary                bool
		InitialCanaryWeight   int
		MinObservationSeconds int
		RollbackWindowSeconds int
		RetireGraceSeconds    int
		RebalanceNow          bool
		Wait                  bool
	}{Wait: true, InitialCanaryWeight: 1, MinObservationSeconds: 60}
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
					RebalanceNow:    opts.RebalanceNow,
				}
			}
			if strings.TrimSpace(opts.ZeroDowntimeMode) != "" {
				mode := strings.TrimSpace(strings.ToLower(opts.ZeroDowntimeMode))
				if mode == "drain" || mode == "drain-only" {
					mode = model.AppZeroDowntimeModeDrainOnly
				}
				if mode != model.AppZeroDowntimeModeDrainOnly && mode != model.AppZeroDowntimeModeSafe {
					return fmt.Errorf("--zero-downtime must be drain_only or safe")
				}
				req := &appContinuityZeroDowntimeRequest{
					Enabled:               true,
					Mode:                  mode,
					RollbackWindowSeconds: opts.RollbackWindowSeconds,
					RetireGraceSeconds:    opts.RetireGraceSeconds,
				}
				if mode == model.AppZeroDowntimeModeSafe {
					req.Strategy = model.AppZeroDowntimeStrategyStableCandidate
					req.Canary = &model.AppRolloutCanarySpec{
						Enabled:               opts.Canary,
						InitialWeight:         opts.InitialCanaryWeight,
						MaxWeight:             100,
						StepWeights:           []int{opts.InitialCanaryWeight, 5, 25, 50, 100},
						MinObservationSeconds: opts.MinObservationSeconds,
					}
				}
				request.ZeroDowntime = req
			}
			if request.AppFailover == nil && request.DatabaseFailover == nil && request.ZeroDowntime == nil {
				return fmt.Errorf("at least one of --app-to, --db-to, or --zero-downtime is required")
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
	cmd.Flags().StringVar(&opts.ZeroDowntimeMode, "zero-downtime", "", "Enable zero downtime policy: drain_only or safe")
	cmd.Flags().BoolVar(&opts.Canary, "canary", true, "Enable safe rollout candidate canary")
	cmd.Flags().IntVar(&opts.InitialCanaryWeight, "initial-canary-weight", opts.InitialCanaryWeight, "Initial safe rollout candidate canary weight")
	cmd.Flags().IntVar(&opts.MinObservationSeconds, "min-observation-seconds", opts.MinObservationSeconds, "Minimum observation seconds per safe rollout canary step")
	cmd.Flags().IntVar(&opts.RollbackWindowSeconds, "rollback-window-seconds", 0, "Seconds to keep previous stable as rollback target")
	cmd.Flags().IntVar(&opts.RetireGraceSeconds, "retire-grace-seconds", 0, "Minimum seconds before retiring previous stable")
	cmd.Flags().BoolVar(&opts.RebalanceNow, "rebalance-now", false, "Clear any pending managed Postgres placement hold when configuring database failover")
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait for the deploy operation to complete")
	_ = cmd.Flags().MarkHidden("app-runtime-id")
	_ = cmd.Flags().MarkHidden("db-runtime-id")
	return cmd
}

func (c *CLI) newAppContinuityOffCommand() *cobra.Command {
	opts := struct {
		App          bool
		DB           bool
		ZeroDowntime bool
		RebalanceNow bool
		Wait         bool
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
			disableApp := opts.App || (!opts.App && !opts.DB && !opts.ZeroDowntime)
			disableDB := opts.DB || (!opts.App && !opts.DB && !opts.ZeroDowntime)
			request := patchAppContinuityRequest{}
			if disableApp {
				request.AppFailover = &appContinuityAppFailoverRequest{Enabled: false}
			}
			if disableDB {
				request.DatabaseFailover = &appContinuityDatabaseFailoverRequest{
					Enabled:      false,
					RebalanceNow: opts.RebalanceNow,
				}
			}
			if opts.ZeroDowntime {
				request.ZeroDowntime = &appContinuityZeroDowntimeRequest{Enabled: false}
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
	cmd.Flags().BoolVar(&opts.ZeroDowntime, "zero-downtime", false, "Disable only zero downtime rollout policy")
	cmd.Flags().BoolVar(&opts.RebalanceNow, "rebalance-now", false, "Clear any pending managed Postgres placement hold while disabling database failover")
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait for the deploy operation to complete")
	return cmd
}

func (c *CLI) newAppFailoverCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "failover",
		Short: "Inspect, configure, and execute app failover",
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}
	cmd.AddCommand(
		c.newAppFailoverStatusCommand(),
		c.newAppFailoverPolicyCommand(),
		hideCompatCommand(c.newAppFailoverConfigureCommand(), "fugue app failover policy set"),
		hideCompatCommand(c.newAppFailoverDisableCommand(), "fugue app failover policy clear"),
		c.newAppFailoverRunCommand(),
	)
	return cmd
}

func (c *CLI) newAppFailoverStatusCommand() *cobra.Command {
	cmd := c.newAppContinuityAuditCommand()
	cmd.Use = "status [app]"
	cmd.Aliases = []string{"audit", "show", "get"}
	cmd.Short = "Show failover readiness for one app or all visible apps"
	return cmd
}

func (c *CLI) newAppFailoverConfigureCommand() *cobra.Command {
	cmd := c.newAppContinuitySetCommand()
	cmd.Use = "configure <app>"
	cmd.Aliases = []string{"set", "enable"}
	cmd.Short = "Configure app and/or database failover targets"
	return cmd
}

func (c *CLI) newAppFailoverDisableCommand() *cobra.Command {
	cmd := c.newAppContinuityOffCommand()
	cmd.Use = "disable <app>"
	cmd.Aliases = []string{"off"}
	cmd.Short = "Disable app and/or database failover"
	return cmd
}

func (c *CLI) newAppFailoverPolicyCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "policy",
		Short: "Inspect and update failover policy targets",
	}
	cmd.AddCommand(
		c.newAppFailoverPolicySetCommand(),
		c.newAppFailoverPolicyClearCommand(),
	)
	return cmd
}

func (c *CLI) newAppFailoverPolicySetCommand() *cobra.Command {
	cmd := c.newAppContinuitySetCommand()
	cmd.Use = "set <app>"
	cmd.Aliases = []string{"configure"}
	cmd.Short = "Set app and/or database failover targets"
	return cmd
}

func (c *CLI) newAppFailoverPolicyClearCommand() *cobra.Command {
	cmd := c.newAppContinuityOffCommand()
	cmd.Use = "clear <app>"
	cmd.Aliases = []string{"disable", "off"}
	cmd.Short = "Clear app and/or database failover targets"
	return cmd
}

func (c *CLI) newAppFailoverRunCommand() *cobra.Command {
	opts := struct {
		RuntimeName string
		RuntimeID   string
		Wait        bool
	}{Wait: true}
	cmd := &cobra.Command{
		Use:     "exec <app>",
		Aliases: []string{"run"},
		Short:   "Execute failover to a target runtime",
		Args:    cobra.ExactArgs(1),
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
			"zero_downtime":   result.ZeroDowntime,
			"database":        result.Database,
			"already_current": result.AlreadyCurrent,
			"operation":       redactOperationPtrForOutput(result.Operation),
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
	if result.ZeroDowntime != nil && result.ZeroDowntime.Enabled {
		pairs = append(pairs,
			kvPair{Key: "zero_downtime_enabled", Value: "true"},
			kvPair{Key: "zero_downtime_mode", Value: result.ZeroDowntime.Mode},
			kvPair{Key: "zero_downtime_strategy", Value: result.ZeroDowntime.Strategy},
		)
		if result.ZeroDowntime.Canary != nil {
			pairs = append(pairs,
				kvPair{Key: "zero_downtime_canary_enabled", Value: fmt.Sprintf("%t", result.ZeroDowntime.Canary.Enabled)},
				kvPair{Key: "zero_downtime_initial_weight", Value: fmt.Sprintf("%d", result.ZeroDowntime.Canary.InitialWeight)},
			)
		}
	} else {
		pairs = append(pairs, kvPair{Key: "zero_downtime_enabled", Value: "false"})
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

func (c *CLI) attachAppBackupReadiness(client *Client, result *appFailoverResult) {
	if client == nil || result == nil || strings.TrimSpace(result.App.ID) == "" {
		return
	}
	status, err := client.GetAppBackupStatus(result.App.ID)
	if err != nil {
		result.BackupReadiness = "unknown"
		return
	}
	result.BackupPosture = append([]model.BackupPosture(nil), status.Posture...)
	result.BackupReadiness = summarizeAppBackupReadiness(status.Posture)
}

func (c *CLI) attachAppSafeRolloutAudit(client *Client, result *appFailoverResult) {
	if client == nil || result == nil || strings.TrimSpace(result.App.ID) == "" {
		return
	}
	if continuity := model.NormalizeAppContinuityPolicy(result.App.Spec.Continuity); continuity != nil && continuity.ZeroDowntime != nil {
		result.ZeroDowntime = continuity.ZeroDowntime
	}
	if result.ZeroDowntime == nil || !result.ZeroDowntime.Enabled || result.ZeroDowntime.Mode != model.AppZeroDowntimeModeSafe {
		return
	}
	if releases, err := client.ListAppReleases(result.App.ID); err == nil {
		if releases.Traffic != nil {
			traffic := *releases.Traffic
			result.ReleaseTraffic = &traffic
		}
		for _, release := range releases.Releases {
			if appReleaseActiveForContinuityAudit(release) {
				result.ActiveReleases = append(result.ActiveReleases, release)
			}
		}
	}
	attempts, err := client.ListAppReleaseAttempts(result.App.ID)
	if err != nil {
		return
	}
	if len(attempts) > 5 {
		attempts = attempts[:5]
	}
	result.RecentReleaseAttempts = append([]model.ReleaseAttempt(nil), attempts...)
	for _, attempt := range attempts {
		evidence, err := client.GetAppReleaseAttemptEvidence(result.App.ID, attempt.ID, false)
		if err != nil {
			continue
		}
		for _, item := range evidence {
			if item.Type != model.OperationEvidenceTypeAppReleaseGateFailure {
				continue
			}
			result.GateFailureCount++
			if result.LastGateFailureEvidenceID == "" {
				result.LastGateFailureEvidenceID = item.ID
			}
		}
	}
}

func appReleaseActiveForContinuityAudit(release model.AppRelease) bool {
	switch strings.TrimSpace(release.Role) {
	case model.AppReleaseRoleStable, model.AppReleaseRoleCandidate, model.AppReleaseRolePrevious:
	default:
		return false
	}
	switch strings.TrimSpace(release.Status) {
	case model.AppReleaseStatusReady, model.AppReleaseStatusServing, model.AppReleaseStatusDraining:
		return true
	default:
		return false
	}
}

func summarizeAppBackupReadiness(posture []model.BackupPosture) string {
	if len(posture) == 0 {
		return "unknown"
	}
	hasReady := false
	hasBlocked := false
	hasDisabled := false
	for _, item := range posture {
		switch strings.TrimSpace(strings.ToLower(item.Status)) {
		case "ready", model.BackupPolicyStatusActive:
			hasReady = true
		case "blocked", model.BackupPolicyStatusBlockedNoBackend, model.BackupPolicyStatusError, model.BackupRunStatusFailed:
			hasBlocked = true
		case model.BackupPolicyStatusDisabled, "":
			hasDisabled = true
		default:
			if strings.TrimSpace(item.Status) != "" {
				hasReady = true
			}
		}
	}
	switch {
	case hasBlocked:
		return "blocked"
	case hasReady:
		return "enabled"
	case hasDisabled:
		return "disabled"
	default:
		return "unknown"
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
	if _, err := fmt.Fprintln(tw, "APP\tCLASS\tREPLICAS\tRUNTIME\tBACKUP\tNOTES"); err != nil {
		return err
	}
	for _, result := range results {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%d/%d\t%s\t%s\t%s\n",
			result.App.Name,
			result.Assessment.Classification,
			result.App.Status.CurrentReplicas,
			result.App.Spec.Replicas,
			formatFailoverRuntime(result.Assessment),
			blankDash(result.BackupReadiness),
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
		{Key: "backup_readiness", Value: result.BackupReadiness},
		{Key: "blockers", Value: strings.Join(result.Assessment.Blockers, "; ")},
		{Key: "warnings", Value: strings.Join(result.Assessment.Warnings, "; ")},
	}
	if result.ZeroDowntime != nil {
		pairs = append(pairs,
			kvPair{Key: "zero_downtime_enabled", Value: fmt.Sprintf("%t", result.ZeroDowntime.Enabled)},
			kvPair{Key: "zero_downtime_mode", Value: result.ZeroDowntime.Mode},
			kvPair{Key: "zero_downtime_strategy", Value: result.ZeroDowntime.Strategy},
		)
		if result.ZeroDowntime.Canary != nil {
			pairs = append(pairs,
				kvPair{Key: "zero_downtime_canary_enabled", Value: fmt.Sprintf("%t", result.ZeroDowntime.Canary.Enabled)},
				kvPair{Key: "zero_downtime_initial_weight", Value: fmt.Sprintf("%d", result.ZeroDowntime.Canary.InitialWeight)},
				kvPair{Key: "zero_downtime_step_weights", Value: strings.Join(intsToStrings(result.ZeroDowntime.Canary.StepWeights), ",")},
			)
		}
	} else {
		pairs = append(pairs, kvPair{Key: "zero_downtime_enabled", Value: "false"})
	}
	if result.ReleaseTraffic != nil {
		pairs = append(pairs,
			kvPair{Key: "release_traffic_mode", Value: result.ReleaseTraffic.Mode},
			kvPair{Key: "stable_release_id", Value: result.ReleaseTraffic.StableReleaseID},
			kvPair{Key: "candidate_release_id", Value: result.ReleaseTraffic.CandidateReleaseID},
			kvPair{Key: "stable_weight", Value: fmt.Sprintf("%d", result.ReleaseTraffic.StableWeight)},
			kvPair{Key: "candidate_weight", Value: fmt.Sprintf("%d", result.ReleaseTraffic.CandidateWeight)},
		)
	}
	pairs = append(pairs,
		kvPair{Key: "active_release_count", Value: fmt.Sprintf("%d", len(result.ActiveReleases))},
		kvPair{Key: "recent_release_attempt_count", Value: fmt.Sprintf("%d", len(result.RecentReleaseAttempts))},
		kvPair{Key: "gate_failure_count", Value: fmt.Sprintf("%d", result.GateFailureCount)},
		kvPair{Key: "last_gate_failure_evidence_id", Value: result.LastGateFailureEvidenceID},
	)
	return writeKeyValues(w, pairs...)
}

func intsToStrings(values []int) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		out = append(out, fmt.Sprintf("%d", value))
	}
	return out
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
