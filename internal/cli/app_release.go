package cli

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

const appImageSyncRolloutWaitTimeout = 2 * time.Minute

func (c *CLI) newAppReleaseCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "release",
		Aliases: []string{"releases", "image", "images"},
		Short:   "Inspect and operate app releases",
	}
	cmd.AddCommand(
		c.newAppReleaseCanaryCommand(),
		c.newAppReleaseTrafficCommand(),
		c.newAppReleaseProbeCommand(),
		c.newAppReleaseGateCommand(),
		c.newAppReleasePromoteCommand(),
		c.newAppReleaseAbortCommand(),
		c.newAppReleaseListCommand(),
		c.newAppReleaseAttemptsCommand(),
		c.newAppReleaseStatusCommand(),
		c.newAppReleaseExplainCommand(),
		c.newAppReleaseDebugBundleCommand(),
		c.newAppReleaseTrackingCommand(),
		c.newAppReleasePruneCommand(),
		c.newAppReleasePolicyCommand(),
		hideCompatCommand(c.newAppReleaseDeployCommand(), "fugue app deploy"),
		hideCompatCommand(c.newAppReleaseRebuildCommand(), "fugue app build"),
		hideCompatCommand(c.newAppReleaseRollbackCommand(), "fugue app rollback"),
	)
	return cmd
}

func (c *CLI) newAppReleaseAttemptsCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "attempts <app>",
		Short: "List user-visible release attempts for an app",
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
			attempts, err := client.ListAppReleaseAttempts(app.ID)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"release_attempts": attempts})
			}
			return writeReleaseAttemptTable(c.stdout, attempts)
		},
	}
	return cmd
}

func (c *CLI) newAppReleaseStatusCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "status <app>",
		Short: "Show the latest release attempt status for an app",
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
			attempts, err := client.ListAppReleaseAttempts(app.ID)
			if err != nil {
				return err
			}
			if len(attempts) == 0 {
				if c.wantsJSON() {
					return writeJSON(c.stdout, map[string]any{"release_attempt": nil})
				}
				_, err := fmt.Fprintln(c.stdout, "no release attempts recorded")
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"release_attempt": attempts[0]})
			}
			return writeReleaseAttemptSummary(c.stdout, attempts[0])
		},
	}
}

func (c *CLI) newAppReleaseExplainCommand() *cobra.Command {
	opts := struct {
		AttemptID string
	}{}
	cmd := &cobra.Command{
		Use:   "explain <app>",
		Short: "Explain a release attempt timeline and evidence",
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
			attemptID := strings.TrimSpace(opts.AttemptID)
			if attemptID == "" {
				attempts, err := client.ListAppReleaseAttempts(app.ID)
				if err != nil {
					return err
				}
				if len(attempts) == 0 {
					return fmt.Errorf("no release attempts recorded for app %s", app.Name)
				}
				attemptID = attempts[0].ID
			}
			attempt, err := client.GetAppReleaseAttempt(app.ID, attemptID)
			if err != nil {
				return err
			}
			timeline, err := client.GetAppReleaseAttemptTimeline(app.ID, attemptID)
			if err != nil {
				return err
			}
			evidence, err := client.GetAppReleaseAttemptEvidence(app.ID, attemptID, false)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"release_attempt": attempt, "timeline": timeline, "evidence": evidence})
			}
			if err := writeReleaseAttemptSummary(c.stdout, attempt); err != nil {
				return err
			}
			if _, err := fmt.Fprintln(c.stdout); err != nil {
				return err
			}
			if _, err := fmt.Fprintln(c.stdout, "[timeline]"); err != nil {
				return err
			}
			if err := writeReleaseTimelineTable(c.stdout, timeline); err != nil {
				return err
			}
			if len(evidence) == 0 {
				return nil
			}
			if _, err := fmt.Fprintln(c.stdout); err != nil {
				return err
			}
			if _, err := fmt.Fprintln(c.stdout, "[evidence]"); err != nil {
				return err
			}
			return writeOperationEvidenceTable(c.stdout, evidence)
		},
	}
	cmd.Flags().StringVar(&opts.AttemptID, "attempt", "", "Release attempt id to explain; defaults to latest")
	return cmd
}

func (c *CLI) newAppReleaseDebugBundleCommand() *cobra.Command {
	opts := struct {
		AttemptID string
		Output    string
	}{}
	cmd := &cobra.Command{
		Use:   "debug-bundle <app>",
		Short: "Export a redacted release attempt debug bundle",
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
			attemptID := strings.TrimSpace(opts.AttemptID)
			if attemptID == "" {
				attempts, err := client.ListAppReleaseAttempts(app.ID)
				if err != nil {
					return err
				}
				if len(attempts) == 0 {
					return fmt.Errorf("no release attempts recorded for app %s", app.Name)
				}
				attemptID = attempts[0].ID
			}
			bundle, err := client.GetAppReleaseAttemptDebugBundle(app.ID, attemptID)
			if err != nil {
				return err
			}
			if strings.TrimSpace(opts.Output) != "" {
				if shouldWriteZipBundle(opts.Output) {
					data, err := client.GetAppReleaseAttemptDebugBundleZip(app.ID, attemptID)
					if err != nil {
						return err
					}
					if err := writeBytesFile(opts.Output, data); err != nil {
						return err
					}
				} else if err := writeJSONFile(opts.Output, bundle); err != nil {
					return err
				}
				if c.wantsJSON() {
					return writeJSON(c.stdout, map[string]any{"output": opts.Output})
				}
				_, err := fmt.Fprintf(c.stdout, "wrote release debug bundle: %s\n", opts.Output)
				return err
			}
			return writeJSON(c.stdout, map[string]any{"bundle": bundle})
		},
	}
	cmd.Flags().StringVar(&opts.AttemptID, "attempt", "", "Release attempt id; defaults to latest")
	cmd.Flags().StringVar(&opts.Output, "output", "", "Write the debug bundle JSON to a local file")
	return cmd
}

func (c *CLI) newAppReleaseCanaryCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "canary",
		Short: "Create and control weighted candidate releases",
	}
	cmd.AddCommand(c.newAppReleaseCanaryStartCommand())
	return cmd
}

func (c *CLI) newAppReleaseCanaryStartCommand() *cobra.Command {
	opts := struct {
		SourceRef        string
		ResolvedImageRef string
		UpstreamURL      string
		RuntimeID        string
		Traffic          int
	}{Traffic: 0}
	cmd := &cobra.Command{
		Use:   "start <app>",
		Short: "Create a candidate release and optionally send traffic to it",
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
			if opts.Traffic < 0 || opts.Traffic > 100 {
				return fmt.Errorf("--traffic must be between 0 and 100")
			}
			if _, err := client.GetAppTrafficPolicy(app.ID); err != nil {
				return err
			}
			status := model.AppReleaseStatusReady
			if strings.TrimSpace(opts.UpstreamURL) == "" {
				status = model.AppReleaseStatusCreating
			}
			created, err := client.CreateAppRelease(app.ID, appReleaseCreateCLIRequest{
				Role:             model.AppReleaseRoleCandidate,
				SourceRef:        opts.SourceRef,
				ResolvedImageRef: opts.ResolvedImageRef,
				UpstreamURL:      opts.UpstreamURL,
				RuntimeID:        opts.RuntimeID,
				Status:           status,
			})
			if err != nil {
				return err
			}
			traffic, err := client.PatchAppTrafficPolicy(app.ID, appTrafficPatchCLIRequest{
				Mode:               model.AppTrafficModeCanary,
				CandidateReleaseID: created.Release.ID,
				StableWeight:       intPtr(100 - opts.Traffic),
				CandidateWeight:    intPtr(opts.Traffic),
			})
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"release": created.Release, "traffic": traffic.Traffic})
			}
			return writeKeyValues(c.stdout,
				kvPair{Key: "app_id", Value: app.ID},
				kvPair{Key: "candidate_release_id", Value: created.Release.ID},
				kvPair{Key: "candidate_status", Value: created.Release.Status},
				kvPair{Key: "stable_weight", Value: fmt.Sprintf("%d", traffic.Traffic.StableWeight)},
				kvPair{Key: "candidate_weight", Value: fmt.Sprintf("%d", traffic.Traffic.CandidateWeight)},
			)
		},
	}
	cmd.Flags().StringVar(&opts.SourceRef, "source-ref", "", "Source image or git reference for the candidate release")
	cmd.Flags().StringVar(&opts.ResolvedImageRef, "resolved-image", "", "Resolved candidate image reference")
	cmd.Flags().StringVar(&opts.UpstreamURL, "candidate-upstream", "", "Candidate upstream base URL to receive weighted traffic")
	cmd.Flags().StringVar(&opts.RuntimeID, "runtime", "", "Runtime id associated with the candidate")
	cmd.Flags().IntVar(&opts.Traffic, "traffic", opts.Traffic, "Initial candidate traffic percentage")
	return cmd
}

func (c *CLI) newAppReleaseTrafficCommand() *cobra.Command {
	opts := struct {
		StableWeight       int
		CandidateWeight    int
		CandidateReleaseID string
		Mode               string
	}{StableWeight: -1, CandidateWeight: -1, Mode: model.AppTrafficModeCanary}
	cmd := &cobra.Command{
		Use:   "traffic <app>",
		Short: "Set stable/candidate traffic weights",
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
			current, err := client.GetAppTrafficPolicy(app.ID)
			if err != nil {
				return err
			}
			stable := opts.StableWeight
			candidate := opts.CandidateWeight
			if stable < 0 && candidate < 0 {
				return fmt.Errorf("--stable or --candidate is required")
			}
			if stable < 0 {
				stable = 100 - candidate
			}
			if candidate < 0 {
				candidate = 100 - stable
			}
			if stable < 0 || candidate < 0 || stable+candidate != 100 {
				return fmt.Errorf("stable and candidate weights must be non-negative and sum to 100")
			}
			candidateReleaseID := firstNonEmpty(opts.CandidateReleaseID, current.Traffic.CandidateReleaseID)
			response, err := client.PatchAppTrafficPolicy(app.ID, appTrafficPatchCLIRequest{
				Mode:               opts.Mode,
				CandidateReleaseID: candidateReleaseID,
				StableWeight:       intPtr(stable),
				CandidateWeight:    intPtr(candidate),
			})
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return writeTrafficPolicySummary(c.stdout, response.Traffic)
		},
	}
	cmd.Flags().IntVar(&opts.StableWeight, "stable", opts.StableWeight, "Stable traffic percentage")
	cmd.Flags().IntVar(&opts.CandidateWeight, "candidate", opts.CandidateWeight, "Candidate traffic percentage")
	cmd.Flags().StringVar(&opts.CandidateReleaseID, "candidate-release", "", "Candidate release id")
	cmd.Flags().StringVar(&opts.Mode, "mode", opts.Mode, "Traffic mode")
	return cmd
}

func (c *CLI) newAppReleaseProbeCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "probe <app> [release-id]",
		Short: "Run active probes against a release upstream",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			releaseID, err := resolveCLIReleaseID(client, app.ID, args, model.AppReleaseRoleCandidate)
			if err != nil {
				return err
			}
			response, err := client.ProbeAppRelease(app.ID, releaseID, nil)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return writeProbeResults(c.stdout, response.Status, response.Results)
		},
	}
}

func (c *CLI) newAppReleaseGateCommand() *cobra.Command {
	opts := struct {
		WindowSeconds    int
		MinRequests      int
		Max5xxRate       float64
		MaxUpstreamRate  float64
		MaxP95TTFBMS     int
		MaxP99DurationMS int
	}{WindowSeconds: 600, Max5xxRate: 0.01, MaxUpstreamRate: 0.005, MaxP95TTFBMS: 2000, MaxP99DurationMS: 30000}
	cmd := &cobra.Command{
		Use:   "gate <app> [release-id]",
		Short: "Evaluate release health gates",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			releaseID, err := resolveCLIReleaseID(client, app.ID, args, model.AppReleaseRoleCandidate)
			if err != nil {
				return err
			}
			response, err := client.EvaluateAppReleaseGate(app.ID, releaseID, model.AppReleaseGatePolicy{
				WindowSeconds:              opts.WindowSeconds,
				MinCandidateRequests:       opts.MinRequests,
				Max5xxRate:                 opts.Max5xxRate,
				MaxEdgeUpstreamErrorRate:   opts.MaxUpstreamRate,
				MaxP95TTFBMilliseconds:     opts.MaxP95TTFBMS,
				MaxP99DurationMilliseconds: opts.MaxP99DurationMS,
			})
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return writeGateSummary(c.stdout, response.Gate)
		},
	}
	cmd.Flags().IntVar(&opts.WindowSeconds, "window-seconds", opts.WindowSeconds, "Observation window in seconds")
	cmd.Flags().IntVar(&opts.MinRequests, "min-requests", opts.MinRequests, "Minimum candidate request count")
	cmd.Flags().Float64Var(&opts.Max5xxRate, "max-5xx-rate", opts.Max5xxRate, "Maximum 5xx ratio")
	cmd.Flags().Float64Var(&opts.MaxUpstreamRate, "max-upstream-error-rate", opts.MaxUpstreamRate, "Maximum edge upstream error ratio")
	cmd.Flags().IntVar(&opts.MaxP95TTFBMS, "max-p95-ttfb-ms", opts.MaxP95TTFBMS, "Maximum p95 TTFB in milliseconds")
	cmd.Flags().IntVar(&opts.MaxP99DurationMS, "max-p99-duration-ms", opts.MaxP99DurationMS, "Maximum p99 duration in milliseconds")
	return cmd
}

func (c *CLI) newAppReleasePromoteCommand() *cobra.Command {
	opts := struct{ To int }{To: 100}
	cmd := &cobra.Command{
		Use:   "promote <app> [release-id]",
		Short: "Promote a candidate to a traffic percentage or stable",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			releaseID, err := resolveCLIReleaseID(client, app.ID, args, model.AppReleaseRoleCandidate)
			if err != nil {
				return err
			}
			response, err := client.PromoteAppRelease(app.ID, releaseID, opts.To)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return writeTrafficPolicySummary(c.stdout, response.Traffic)
		},
	}
	cmd.Flags().IntVar(&opts.To, "to", opts.To, "Candidate traffic percentage; 100 finalizes as stable")
	return cmd
}

func (c *CLI) newAppReleaseAbortCommand() *cobra.Command {
	opts := struct {
		MarkFailed bool
		Reason     string
	}{}
	cmd := &cobra.Command{
		Use:   "abort <app> [release-id]",
		Short: "Route all traffic back to stable",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			releaseID, err := resolveCLIReleaseID(client, app.ID, args, model.AppReleaseRoleCandidate)
			if err != nil {
				return err
			}
			response, err := client.AbortAppRelease(app.ID, releaseID, opts.MarkFailed, opts.Reason)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return writeTrafficPolicySummary(c.stdout, response.Traffic)
		},
	}
	cmd.Flags().BoolVar(&opts.MarkFailed, "mark-failed", false, "Mark the candidate release failed")
	cmd.Flags().StringVar(&opts.Reason, "reason", "", "Failure reason when marking failed")
	return cmd
}

func (c *CLI) newAppReleaseTrackingCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "tracking <app>",
		Aliases: []string{"track", "auto-update"},
		Short:   "Inspect and configure external image tracking",
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
			response, err := client.GetAppImageTracking(app.ID)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			if response.Tracking == nil {
				return writeKeyValues(c.stdout,
					kvPair{Key: "app_id", Value: app.ID},
					kvPair{Key: "enabled", Value: "false"},
				)
			}
			return writeAppImageTrackingSummary(c.stdout, *response.Tracking)
		},
	}
	cmd.AddCommand(
		c.newAppReleaseTrackingSetCommand(),
		c.newAppReleaseTrackingDisableCommand(),
		c.newAppReleaseTrackingSyncCommand(),
		c.newAppReleaseTrackingHistoryCommand(),
		c.newAppReleaseTrackingDiagnoseCommand(),
	)
	return cmd
}

func (c *CLI) newAppReleaseTrackingSetCommand() *cobra.Command {
	opts := struct {
		ImageRef string
		Disabled bool
	}{}
	cmd := &cobra.Command{
		Use:   "set <app>",
		Short: "Track an external image ref for automatic updates",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(opts.ImageRef) == "" {
				return fmt.Errorf("--image is required")
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			response, err := client.PutAppImageTracking(app.ID, opts.ImageRef, !opts.Disabled)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			if response.Tracking == nil {
				return writeKeyValues(c.stdout, kvPair{Key: "app_id", Value: app.ID}, kvPair{Key: "enabled", Value: "false"})
			}
			return writeAppImageTrackingSummary(c.stdout, *response.Tracking)
		},
	}
	cmd.Flags().StringVar(&opts.ImageRef, "image", "", "External image ref to track, for example ghcr.io/owner/app:latest")
	cmd.Flags().BoolVar(&opts.Disabled, "disabled", false, "Create or update the tracking record disabled")
	return cmd
}

func (c *CLI) newAppReleaseTrackingDisableCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "disable <app>",
		Short: "Disable external image tracking",
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
			current, err := client.GetAppImageTracking(app.ID)
			if err != nil {
				return err
			}
			if current.Tracking == nil {
				return fmt.Errorf("app image tracking is not configured")
			}
			response, err := client.PutAppImageTracking(app.ID, current.Tracking.ImageRef, false)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			if response.Tracking == nil {
				return writeKeyValues(c.stdout, kvPair{Key: "app_id", Value: app.ID}, kvPair{Key: "enabled", Value: "false"})
			}
			return writeAppImageTrackingSummary(c.stdout, *response.Tracking)
		},
	}
}

func (c *CLI) newAppReleaseTrackingSyncCommand() *cobra.Command {
	opts := struct {
		Wait bool
	}{Wait: true}
	cmd := &cobra.Command{
		Use:   "sync <app>",
		Short: "Check the tracked image digest and queue an update when it changed",
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
			response, err := c.syncAppImageAndWait(client, app.ID, opts.Wait)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			pairs := []kvPair{
				{Key: "app_id", Value: app.ID},
				{Key: "digest", Value: response.Digest},
				{Key: "changed", Value: fmt.Sprintf("%t", response.Changed)},
				{Key: "already_current", Value: fmt.Sprintf("%t", response.AlreadyCurrent)},
			}
			if response.RolloutPending {
				pairs = append(pairs, kvPair{Key: "rollout_pending", Value: "true"})
			}
			if strings.TrimSpace(response.AppPhase) != "" {
				pairs = append(pairs, kvPair{Key: "app_phase", Value: strings.TrimSpace(response.AppPhase)})
			}
			if response.ReleaseAttempt != nil {
				pairs = append(pairs,
					kvPair{Key: "release_attempt_id", Value: response.ReleaseAttempt.ID},
					kvPair{Key: "release_attempt_status", Value: response.ReleaseAttempt.Status},
				)
			}
			if response.Operation != nil {
				pairs = append(pairs, kvPair{Key: "operation_id", Value: response.Operation.ID})
				if strings.EqualFold(response.Operation.Type, model.OperationTypeImport) {
					pairs = append(pairs, kvPair{Key: "phase", Value: "image_import"})
				} else if strings.EqualFold(response.Operation.Type, model.OperationTypeDeploy) {
					pairs = append(pairs, kvPair{Key: "phase", Value: "deploy_rollout"})
				}
			}
			if strings.TrimSpace(response.Message) != "" {
				pairs = append(pairs, kvPair{Key: "message", Value: response.Message})
			}
			return writeKeyValues(c.stdout, pairs...)
		},
	}
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait for queued operation completion")
	return cmd
}

func (c *CLI) newAppReleaseTrackingHistoryCommand() *cobra.Command {
	opts := struct {
		Limit int
	}{Limit: 20}
	cmd := &cobra.Command{
		Use:   "history <app>",
		Short: "Show recent image tracking decisions for an app",
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
			response, err := client.GetAppImageTrackingHistory(app.ID, opts.Limit)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return writeAppImageTrackingHistory(c.stdout, response)
		},
	}
	cmd.Flags().IntVar(&opts.Limit, "limit", opts.Limit, "Maximum recent decisions to show")
	return cmd
}

func (c *CLI) newAppReleaseTrackingDiagnoseCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "diagnose <app>",
		Short: "Explain the current image tracking state for an app",
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
			response, err := client.GetAppImageTrackingDiagnosis(app.ID)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return writeAppImageTrackingDiagnosis(c.stdout, response.Diagnosis)
		},
	}
}

func (c *CLI) syncAppImageAndWait(client *Client, appID string, wait bool) (appImageSyncResponse, error) {
	const maxAttempts = 3
	var response appImageSyncResponse
	for attempt := 0; attempt < maxAttempts; attempt++ {
		var err error
		response, err = client.SyncAppImage(appID, "", "manual", "")
		if err != nil {
			return appImageSyncResponse{}, err
		}
		if !wait {
			return response, nil
		}

		deferredByActiveOperation := response.Operation != nil && !response.Changed && !response.AlreadyCurrent
		if response.Operation != nil {
			app, op, err := c.waitForAppImageSyncOperation(client, appID, *response.Operation)
			if err != nil {
				return appImageSyncResponse{}, err
			}
			if op != nil {
				response.Operation = op
			}
			if app != nil {
				response.AppPhase = strings.TrimSpace(app.Status.Phase)
				response.RolloutPending = appImageSyncCLIRolloutPending(*app)
			}
		}

		if response.RolloutPending {
			app, err := c.waitForAppImageSyncRollout(client, appID)
			if err != nil {
				return appImageSyncResponse{}, err
			}
			response.AppPhase = strings.TrimSpace(app.Status.Phase)
			response.RolloutPending = appImageSyncCLIRolloutPending(app)
		}

		if deferredByActiveOperation && attempt < maxAttempts-1 {
			continue
		}
		return response, nil
	}
	return response, nil
}

func (c *CLI) waitForAppImageSyncOperation(client *Client, appID string, op model.Operation) (*model.App, *model.Operation, error) {
	app, finalOp, err := c.waitForSingleAppOperation(client, appID, op, true)
	if err != nil {
		return app, finalOp, err
	}
	if finalOp == nil ||
		!strings.EqualFold(strings.TrimSpace(finalOp.Type), model.OperationTypeImport) ||
		!strings.EqualFold(strings.TrimSpace(finalOp.Status), model.OperationStatusCompleted) {
		return app, finalOp, nil
	}
	deployID := queuedDeployOperationID(finalOp.ResultMessage)
	if deployID == "" {
		return app, finalOp, nil
	}
	deployOp, err := client.GetOperation(deployID)
	if err != nil {
		return app, finalOp, err
	}
	return c.waitForSingleAppOperation(client, appID, deployOp, true)
}

func (c *CLI) waitForAppImageSyncRollout(client *Client, appID string) (model.App, error) {
	deadline := time.Now().Add(appImageSyncRolloutWaitTimeout)
	transientErrors := deployWaitTransientErrorTracker{}
	lastPhase := ""
	for {
		app, err := client.GetApp(appID)
		if err != nil {
			retry, retryErr := transientErrors.shouldRetry(c, err)
			if retryErr != nil {
				return model.App{}, retryErr
			}
			if retry {
				sleepDeployWaitPoll()
				continue
			}
			return model.App{}, err
		}
		transientErrors.reset()
		if !appImageSyncCLIRolloutPending(app) {
			return app, nil
		}
		phase := strings.TrimSpace(app.Status.Phase)
		if phase != lastPhase {
			c.progressf("app_phase=%s rollout_pending=true", phase)
			lastPhase = phase
		}
		if time.Now().After(deadline) {
			c.progressf("warning=app rollout still pending after %s; returning latest status", appImageSyncRolloutWaitTimeout)
			return app, nil
		}
		sleepDeployWaitPoll()
	}
}

func appImageSyncCLIRolloutPending(app model.App) bool {
	if app.Spec.Replicas <= 0 {
		return false
	}
	if app.Status.CurrentReplicas < app.Spec.Replicas {
		return true
	}
	phase := strings.ToLower(strings.TrimSpace(app.Status.Phase))
	return phase != "" && phase != "deployed"
}

func (c *CLI) newAppReleaseListCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "ls <app>",
		Aliases: []string{"list", "show"},
		Short:   "List release images for an app",
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
			inventory, err := client.GetAppImages(app.ID)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, inventory)
			}
			if err := writeKeyValues(c.stdout,
				kvPair{Key: "app_id", Value: inventory.AppID},
				kvPair{Key: "registry_configured", Value: fmt.Sprintf("%t", inventory.RegistryConfigured)},
				kvPair{Key: "version_count", Value: fmt.Sprintf("%d", inventory.Summary.VersionCount)},
				kvPair{Key: "reclaimable", Value: formatBytes(inventory.Summary.ReclaimableSizeBytes)},
			); err != nil {
				return err
			}
			if len(inventory.Versions) == 0 {
				return nil
			}
			if _, err := fmt.Fprintln(c.stdout); err != nil {
				return err
			}
			return writeAppImageTable(c.stdout, inventory.Versions)
		},
	}
}

func (c *CLI) newAppDeployShortcutCommand() *cobra.Command {
	cmd := c.newAppReleaseDeployCommand()
	cmd.Use = "deploy <app>"
	cmd.Short = "Deploy the app's current desired spec"
	return cmd
}

func (c *CLI) newAppRedeployCommand() *cobra.Command {
	cmd := c.newAppReleaseDeployCommand()
	cmd.Use = "redeploy <app>"
	cmd.Aliases = []string{"apply"}
	cmd.Short = "Compatibility alias for app deploy"
	return cmd
}

func (c *CLI) newAppRebuildShortcutCommand() *cobra.Command {
	cmd := c.newAppReleaseRebuildCommand()
	cmd.Use = "build <app>"
	cmd.Short = "Build an app from its source definition"
	return cmd
}

func (c *CLI) newAppRollbackShortcutCommand() *cobra.Command {
	cmd := c.newAppReleaseRollbackCommand()
	cmd.Use = "rollback <app> [image-ref]"
	cmd.Short = "Rollback to a previous release image"
	return cmd
}

func (c *CLI) newAppReleaseRebuildCommand() *cobra.Command {
	opts := struct {
		Branch          string
		ImageRef        string
		SourceDir       string
		DockerfilePath  string
		BuildContextDir string
		RepoToken       string
		ClearFiles      bool
		Wait            bool
	}{Wait: true}
	cmd := &cobra.Command{
		Use:   "rebuild <app>",
		Short: "Rebuild an app from its source definition",
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
			response, err := client.RebuildApp(app.ID, rebuildPlanRequest{
				Branch:          opts.Branch,
				ImageRef:        opts.ImageRef,
				SourceDir:       opts.SourceDir,
				DockerfilePath:  opts.DockerfilePath,
				BuildContextDir: opts.BuildContextDir,
				RepoAuthToken:   opts.RepoToken,
				ClearFiles:      opts.ClearFiles,
			})
			if err != nil {
				return err
			}
			if err := c.waitForOptionalOperation(client, &response.Operation, opts.Wait); err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{
					"app_id":    app.ID,
					"operation": redactOperationForOutput(response.Operation),
					"build":     response.Build,
				})
			}
			return writeKeyValues(c.stdout,
				kvPair{Key: "app_id", Value: app.ID},
				kvPair{Key: "operation_id", Value: response.Operation.ID},
				kvPair{Key: "source_type", Value: response.Build.SourceType},
				kvPair{Key: "build_strategy", Value: response.Build.BuildStrategy},
				kvPair{Key: "image_ref", Value: firstNonEmpty(response.Build.ResolvedImageRef, response.Build.ImageRef)},
			)
		},
	}
	cmd.Flags().StringVar(&opts.Branch, "branch", "", "Override the source branch")
	cmd.Flags().StringVar(&opts.ImageRef, "image", "", "Override the source image reference")
	cmd.Flags().StringVar(&opts.SourceDir, "source-dir", "", "Override the source directory")
	cmd.Flags().StringVar(&opts.DockerfilePath, "dockerfile", "", "Override the Dockerfile path")
	cmd.Flags().StringVar(&opts.BuildContextDir, "context", "", "Override the Docker build context")
	cmd.Flags().StringVar(&opts.RepoToken, "repo-token", "", "Repository auth token for private Git sources")
	cmd.Flags().BoolVar(&opts.ClearFiles, "clear-files", false, "Remove declarative app files before rebuilding")
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait for operation completion")
	return cmd
}

func (c *CLI) newAppReleaseDeployCommand() *cobra.Command {
	opts := struct {
		Wait bool
	}{Wait: true}
	cmd := &cobra.Command{
		Use:   "deploy <app>",
		Short: "Deploy the app's current desired spec",
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
			response, err := client.DeployApp(app.ID, nil)
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
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait for operation completion")
	return cmd
}

func (c *CLI) newAppReleaseRollbackCommand() *cobra.Command {
	opts := struct {
		Wait bool
	}{Wait: true}
	cmd := &cobra.Command{
		Use:   "rollback <app> [image-ref]",
		Short: "Rollback to a previous release image",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			inventory, err := client.GetAppImages(app.ID)
			if err != nil {
				return err
			}
			imageRef := ""
			if len(args) == 2 {
				imageRef = strings.TrimSpace(args[1])
			} else {
				candidate, err := defaultRollbackImage(inventory.Versions)
				if err != nil {
					return err
				}
				imageRef = candidate.ImageRef
			}
			response, err := client.RedeployAppImage(app.ID, imageRef)
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
			if c.wantsJSON() {
				var payloadApp *model.App
				if result.App != nil {
					appCopy := redactAppForOutput(*result.App)
					payloadApp = &appCopy
				}
				return writeJSON(c.stdout, map[string]any{
					"app":       payloadApp,
					"operation": redactOperationPtrForOutput(result.Operation),
					"image":     response.Image,
				})
			}
			if err := c.renderAppCommandResult(result); err != nil {
				return err
			}
			if response.Image != nil {
				_, err = fmt.Fprintf(c.stdout, "release_image=%s\n", response.Image.ImageRef)
				return err
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait for operation completion")
	return cmd
}

func (c *CLI) newAppReleasePruneCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "prune <app>",
		Short: "Delete stale release images that can be reclaimed",
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
			inventory, err := client.GetAppImages(app.ID)
			if err != nil {
				return err
			}
			candidates := pruneCandidates(inventory.Versions)
			if c.wantsJSON() {
				results := make([]appImageDeleteResponse, 0, len(candidates))
				for _, version := range candidates {
					result, err := client.DeleteAppImage(app.ID, version.ImageRef)
					if err != nil {
						return err
					}
					results = append(results, result)
				}
				return writeJSON(c.stdout, map[string]any{
					"app_id":  app.ID,
					"deleted": results,
				})
			}
			if len(candidates) == 0 {
				return writeKeyValues(c.stdout, kvPair{Key: "app_id", Value: app.ID}, kvPair{Key: "deleted_images", Value: "0"})
			}
			deleted := 0
			reclaimed := int64(0)
			for _, version := range candidates {
				result, err := client.DeleteAppImage(app.ID, version.ImageRef)
				if err != nil {
					return err
				}
				if result.Deleted || result.AlreadyMissing {
					deleted++
					reclaimed += result.ReclaimedSizeBytes
				}
			}
			return writeKeyValues(c.stdout,
				kvPair{Key: "app_id", Value: app.ID},
				kvPair{Key: "deleted_images", Value: fmt.Sprintf("%d", deleted)},
				kvPair{Key: "reclaimed", Value: formatBytes(reclaimed)},
			)
		},
	}
}

func (c *CLI) newAppReleasePolicyCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "policy",
		Short: "Show and update release retention policy",
	}
	cmd.AddCommand(
		c.newAppReleasePolicyShowCommand(),
		c.newAppReleasePolicySetCommand(),
	)
	return cmd
}

func (c *CLI) newAppReleasePolicyShowCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "show <app>",
		Short: "Show how many release images Fugue retains",
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
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{
					"app":                app,
					"image_mirror_limit": model.EffectiveAppImageMirrorLimit(app.Spec.ImageMirrorLimit),
				})
			}
			return writeKeyValues(c.stdout,
				kvPair{Key: "app", Value: app.Name},
				kvPair{Key: "release_retain", Value: formatImageMirrorLimit(app.Spec.ImageMirrorLimit)},
			)
		},
	}
}

func (c *CLI) newAppReleasePolicySetCommand() *cobra.Command {
	opts := struct {
		Retain int
	}{Retain: model.DefaultAppImageMirrorLimit}
	cmd := &cobra.Command{
		Use:   "set <app>",
		Short: "Set how many release images Fugue retains",
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
			response, err := client.SetAppImageMirrorLimit(app.ID, opts.Retain)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			if err := writeKeyValues(c.stdout,
				kvPair{Key: "app", Value: response.App.Name},
				kvPair{Key: "release_retain", Value: formatImageMirrorLimit(response.App.Spec.ImageMirrorLimit)},
				kvPair{Key: "already_current", Value: fmt.Sprintf("%t", response.AlreadyCurrent)},
			); err != nil {
				return err
			}
			return nil
		},
	}
	cmd.Flags().IntVar(&opts.Retain, "retain", opts.Retain, "How many release images to retain")
	return cmd
}

func formatImageMirrorLimit(value int) string {
	return fmt.Sprintf("%d", model.EffectiveAppImageMirrorLimit(value))
}

func defaultRollbackImage(versions []appImageVersion) (appImageVersion, error) {
	candidates := make([]appImageVersion, 0, len(versions))
	for _, version := range versions {
		if version.Current || !version.RedeploySupported {
			continue
		}
		candidates = append(candidates, version)
	}
	sort.Slice(candidates, func(i, j int) bool {
		left := timeValue(candidates[i].LastDeployedAt)
		right := timeValue(candidates[j].LastDeployedAt)
		return left.After(right)
	})
	if len(candidates) == 0 {
		return appImageVersion{}, fmt.Errorf("no previous redeployable release is available")
	}
	return candidates[0], nil
}

func pruneCandidates(versions []appImageVersion) []appImageVersion {
	out := make([]appImageVersion, 0, len(versions))
	for _, version := range versions {
		if version.Current {
			continue
		}
		if !version.DeleteSupported && !version.RedeploySupported {
			continue
		}
		if version.DeleteSupported || version.ReclaimableSizeBytes > 0 {
			out = append(out, version)
		}
	}
	return out
}

func timeValue(value *time.Time) time.Time {
	if value == nil {
		return time.Time{}
	}
	return value.UTC()
}

func resolveCLIReleaseID(client *Client, appID string, args []string, role string) (string, error) {
	if len(args) >= 2 && strings.TrimSpace(args[1]) != "" {
		return strings.TrimSpace(args[1]), nil
	}
	response, err := client.ListAppReleases(appID)
	if err != nil {
		return "", err
	}
	role = strings.TrimSpace(role)
	candidates := make([]model.AppRelease, 0, len(response.Releases))
	for _, release := range response.Releases {
		if role != "" && strings.TrimSpace(release.Role) != role {
			continue
		}
		candidates = append(candidates, release)
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].CreatedAt.After(candidates[j].CreatedAt)
	})
	if len(candidates) == 0 {
		return "", fmt.Errorf("no %s release found; pass release-id explicitly", role)
	}
	return candidates[0].ID, nil
}

func writeTrafficPolicySummary(w interface{ Write([]byte) (int, error) }, policy model.AppTrafficPolicy) error {
	return writeKeyValues(w,
		kvPair{Key: "app_id", Value: policy.AppID},
		kvPair{Key: "mode", Value: policy.Mode},
		kvPair{Key: "stable_release_id", Value: policy.StableReleaseID},
		kvPair{Key: "candidate_release_id", Value: policy.CandidateReleaseID},
		kvPair{Key: "stable_weight", Value: fmt.Sprintf("%d", policy.StableWeight)},
		kvPair{Key: "candidate_weight", Value: fmt.Sprintf("%d", policy.CandidateWeight)},
	)
}

func writeProbeResults(w interface{ Write([]byte) (int, error) }, status string, results []model.AppReleaseProbeResult) error {
	pairs := []kvPair{{Key: "status", Value: status}, {Key: "probe_count", Value: fmt.Sprintf("%d", len(results))}}
	for idx, result := range results {
		prefix := fmt.Sprintf("probe_%d", idx+1)
		pairs = append(pairs,
			kvPair{Key: prefix + "_name", Value: firstNonEmpty(result.Name, result.Path)},
			kvPair{Key: prefix + "_status", Value: result.Status},
			kvPair{Key: prefix + "_status_code", Value: fmt.Sprintf("%d", result.StatusCode)},
			kvPair{Key: prefix + "_ttfb_ms", Value: fmt.Sprintf("%d", result.TTFBMillis)},
			kvPair{Key: prefix + "_duration_ms", Value: fmt.Sprintf("%d", result.DurationMillis)},
		)
		if strings.TrimSpace(result.Error) != "" {
			pairs = append(pairs, kvPair{Key: prefix + "_error", Value: result.Error})
		}
	}
	return writeKeyValues(w, pairs...)
}

func writeGateSummary(w interface{ Write([]byte) (int, error) }, gate model.AppReleaseGateResult) error {
	pairs := []kvPair{
		{Key: "status", Value: gate.Status},
		{Key: "release_id", Value: gate.ReleaseID},
		{Key: "role", Value: gate.Role},
		{Key: "window", Value: gate.Window},
		{Key: "failure_count", Value: fmt.Sprintf("%d", len(gate.Failures))},
		{Key: "warning_count", Value: fmt.Sprintf("%d", len(gate.Warnings))},
	}
	for idx, failure := range gate.Failures {
		pairs = append(pairs, kvPair{Key: fmt.Sprintf("failure_%d", idx+1), Value: failure})
	}
	for idx, warning := range gate.Warnings {
		pairs = append(pairs, kvPair{Key: fmt.Sprintf("warning_%d", idx+1), Value: warning})
	}
	return writeKeyValues(w, pairs...)
}

func writeReleaseAttemptSummary(w io.Writer, attempt model.ReleaseAttempt) error {
	return writeKeyValues(w,
		kvPair{Key: "release_attempt_id", Value: attempt.ID},
		kvPair{Key: "app_id", Value: attempt.AppID},
		kvPair{Key: "status", Value: attempt.Status},
		kvPair{Key: "confidence", Value: attempt.Confidence},
		kvPair{Key: "trigger_type", Value: attempt.TriggerType},
		kvPair{Key: "root_operation_id", Value: attempt.RootOperationID},
		kvPair{Key: "source_operation_id", Value: attempt.SourceOperationID},
		kvPair{Key: "image_ref", Value: attempt.ImageRef},
		kvPair{Key: "target_digest", Value: shortDigest(attempt.TargetDigest)},
		kvPair{Key: "previous_digest", Value: shortDigest(attempt.PreviousDigest)},
		kvPair{Key: "failure_operation_id", Value: attempt.FailureOperationID},
		kvPair{Key: "failure_evidence_id", Value: attempt.FailureEvidenceID},
		kvPair{Key: "summary", Value: attempt.Summary},
		kvPair{Key: "started_at", Value: formatTime(attempt.StartedAt)},
		kvPair{Key: "finished_at", Value: formatOptionalTimePtr(attempt.FinishedAt)},
	)
}

func writeReleaseAttemptTable(w io.Writer, attempts []model.ReleaseAttempt) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "STARTED_AT\tATTEMPT\tSTATUS\tCONFIDENCE\tTRIGGER\tTARGET_DIGEST\tROOT_OPERATION\tSUMMARY"); err != nil {
		return err
	}
	for _, attempt := range attempts {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			formatTime(attempt.StartedAt),
			attempt.ID,
			attempt.Status,
			attempt.Confidence,
			attempt.TriggerType,
			shortDigest(attempt.TargetDigest),
			attempt.RootOperationID,
			oneLine(attempt.Summary),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeReleaseTimelineTable(w io.Writer, entries []model.ReleaseTimelineEntry) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "AT\tTYPE\tSTATUS\tOPERATION\tEVIDENCE\tSUMMARY"); err != nil {
		return err
	}
	for _, entry := range entries {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%s\t%s\n",
			formatTime(entry.At),
			entry.Type,
			entry.Status,
			entry.OperationID,
			entry.EvidenceID,
			oneLine(entry.Summary),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func intPtr(value int) *int {
	return &value
}
