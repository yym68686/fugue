package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformsafety"

	"github.com/spf13/cobra"
)

func (c *CLI) newAdminArtifactCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "artifact",
		Short: "Inspect and release platform state artifacts",
	}
	cmd.AddCommand(
		c.newAdminArtifactCreateCommand(),
		c.newAdminArtifactListCommand(),
		c.newAdminArtifactShowCommand(),
		c.newAdminArtifactDiffCommand(),
		c.newAdminArtifactValidateCommand(),
		c.newAdminArtifactReleaseCommand(),
		c.newAdminArtifactVerifyLKGCommand(),
		c.newAdminArtifactRollbackCommand(),
		c.newAdminArtifactConsumersCommand(),
		c.newAdminArtifactLKGCommand(),
	)
	return cmd
}

func (c *CLI) newAdminArtifactCreateCommand() *cobra.Command {
	opts := struct {
		Kind               string
		Scope              string
		Generation         string
		File               string
		CompatibilityFloor string
	}{Scope: "global"}
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create a platform artifact draft from a JSON content file",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(opts.Kind) == "" || strings.TrimSpace(opts.File) == "" {
				return fmt.Errorf("--kind and --file are required")
			}
			raw, err := os.ReadFile(strings.TrimSpace(opts.File))
			if err != nil {
				return fmt.Errorf("read artifact content: %w", err)
			}
			var content map[string]any
			if err := json.Unmarshal(raw, &content); err != nil {
				return fmt.Errorf("decode artifact content JSON: %w", err)
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			artifact, err := client.CreatePlatformArtifact(model.PlatformArtifactCreateRequest{
				ArtifactKind:       opts.Kind,
				Scope:              model.PlatformArtifactScope{ScopeType: "global", Key: opts.Scope},
				Generation:         opts.Generation,
				Content:            content,
				CompatibilityFloor: opts.CompatibilityFloor,
			})
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"artifact": artifact})
			}
			return writePlatformArtifact(c.stdout, artifact)
		},
	}
	cmd.Flags().StringVar(&opts.Kind, "kind", "", "Artifact kind")
	cmd.Flags().StringVar(&opts.Scope, "scope", opts.Scope, "Artifact scope key")
	cmd.Flags().StringVar(&opts.Generation, "generation", "", "Optional explicit generation")
	cmd.Flags().StringVar(&opts.File, "file", "", "JSON file containing artifact content")
	cmd.Flags().StringVar(&opts.CompatibilityFloor, "compatibility-floor", "", "Optional minimum consumer version")
	return cmd
}

func (c *CLI) newAdminArtifactListCommand() *cobra.Command {
	opts := struct {
		Kind   string
		Scope  string
		Status string
		Limit  int
	}{Limit: 100}
	cmd := &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List platform artifacts",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			artifacts, err := client.ListPlatformArtifacts(opts.Kind, opts.Scope, opts.Status, opts.Limit)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"artifacts": artifacts})
			}
			return writePlatformArtifactTable(c.stdout, artifacts)
		},
	}
	cmd.Flags().StringVar(&opts.Kind, "kind", "", "Artifact kind")
	cmd.Flags().StringVar(&opts.Scope, "scope", "", "Scope key")
	cmd.Flags().StringVar(&opts.Status, "status", "", "Artifact status")
	cmd.Flags().IntVar(&opts.Limit, "limit", opts.Limit, "Maximum artifacts")
	return cmd
}

func (c *CLI) newAdminArtifactShowCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "show <artifact-id-or-generation>",
		Short: "Show platform artifact metadata",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			artifact, err := client.GetPlatformArtifact(args[0])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"artifact": artifact})
			}
			return writePlatformArtifact(c.stdout, artifact)
		},
	}
}

func (c *CLI) newAdminArtifactDiffCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "diff <generation-a> <generation-b>",
		Short: "Compare two artifact generations without printing values",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			left, err := client.GetPlatformArtifact(args[0])
			if err != nil {
				return err
			}
			right, err := client.GetPlatformArtifact(args[1])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"left": left, "right": right, "diff": platformArtifactSafeDiff(left, right)})
			}
			return writePlatformArtifactDiff(c.stdout, left, right)
		},
	}
}

func (c *CLI) newAdminArtifactValidateCommand() *cobra.Command {
	opts := struct{ DryRun bool }{DryRun: true}
	cmd := &cobra.Command{
		Use:   "validate <artifact-id-or-generation>",
		Short: "Validate a platform artifact",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			response, err := client.ValidatePlatformArtifact(args[0], opts.DryRun)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return writePlatformArtifactValidation(c.stdout, response)
		},
	}
	cmd.Flags().BoolVar(&opts.DryRun, "dry-run", opts.DryRun, "Only validate without updating artifact status")
	return cmd
}

func (c *CLI) newAdminArtifactReleaseCommand() *cobra.Command {
	opts := model.PlatformArtifactReleaseRequest{}
	overrideOpts := struct {
		KernelBreakGlass bool
		TTL              time.Duration
		Confirmation     string
		Target           string
	}{TTL: 5 * time.Minute}
	cmd := &cobra.Command{
		Use:   "release <artifact-id-or-generation>",
		Short: "Release a validated platform artifact",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(opts.ReleaseChannel) == "" {
				return fmt.Errorf("--channel is required: shadow, gray, or full")
			}
			if opts.ForcePublish {
				opts.SoftOverride = true
				opts.ForcePublish = false
			}
			if opts.SoftOverride && overrideOpts.KernelBreakGlass {
				return fmt.Errorf("--soft-override and --kernel-break-glass are mutually exclusive")
			}
			if opts.SoftOverride && strings.TrimSpace(opts.Reason) == "" {
				return fmt.Errorf("--reason is required with --soft-override")
			}
			if overrideOpts.KernelBreakGlass {
				if overrideOpts.TTL <= 0 || overrideOpts.TTL > platformsafety.KernelBreakGlassMaxTTL {
					return fmt.Errorf("--break-glass-ttl must be positive and no greater than %s", platformsafety.KernelBreakGlassMaxTTL)
				}
				if strings.TrimSpace(overrideOpts.Confirmation) == "" || strings.TrimSpace(overrideOpts.Target) == "" {
					return fmt.Errorf("--confirm-kernel-bypass and --confirm-target are required with --kernel-break-glass")
				}
				if strings.TrimSpace(overrideOpts.Confirmation) != platformsafety.KernelBreakGlassConfirmation {
					return fmt.Errorf("--confirm-kernel-bypass must exactly equal %s", platformsafety.KernelBreakGlassConfirmation)
				}
				if strings.TrimSpace(overrideOpts.Target) != strings.TrimSpace(args[0]) {
					return fmt.Errorf("--confirm-target must exactly match the artifact id or generation argument")
				}
				opts.KernelBreakGlass = &model.PlatformKernelBreakGlassRequest{
					ExpiresAt:          time.Now().UTC().Add(overrideOpts.TTL),
					Confirmation:       overrideOpts.Confirmation,
					TargetConfirmation: overrideOpts.Target,
				}
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			response, err := client.ReleasePlatformArtifact(args[0], opts)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return writePlatformArtifactRelease(c.stdout, response)
		},
	}
	cmd.Flags().StringVar(&opts.ReleaseChannel, "channel", "", "Release channel: shadow, gray, or full")
	cmd.Flags().StringVar(&opts.CanaryRuleRef, "canary-rule-ref", "", "Optional canary or gray rule reference")
	cmd.Flags().BoolVar(&opts.SoftOverride, "soft-override", false, "Skip non-kernel validation policy; requires --reason")
	cmd.Flags().BoolVar(&opts.ForcePublish, "force-publish", false, "Deprecated alias for --soft-override")
	_ = cmd.Flags().MarkDeprecated("force-publish", "use --soft-override; it cannot bypass the Platform Safety Kernel")
	cmd.Flags().BoolVar(&overrideOpts.KernelBreakGlass, "kernel-break-glass", false, "Use one operation-scoped Platform Safety Kernel recovery authorization")
	cmd.Flags().DurationVar(&overrideOpts.TTL, "break-glass-ttl", overrideOpts.TTL, "Kernel break-glass authorization validity, maximum 15m")
	cmd.Flags().StringVar(&overrideOpts.Confirmation, "confirm-kernel-bypass", "", "Must equal "+platformsafety.KernelBreakGlassConfirmation)
	cmd.Flags().StringVar(&overrideOpts.Target, "confirm-target", "", "Must exactly match the artifact id or generation")
	cmd.Flags().StringVar(&opts.Reason, "reason", "", "Release or override reason")
	cmd.Flags().StringVar(&opts.IdempotencyKey, "idempotency-key", "", "Stable idempotency key for retrying the same release")
	return cmd
}

func (c *CLI) newAdminArtifactVerifyLKGCommand() *cobra.Command {
	opts := model.PlatformArtifactVerifyLKGRequest{}
	cmd := &cobra.Command{
		Use:   "verify-lkg <release-id>",
		Short: "Verify a serving release and promote it to verified LKG",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if opts.FencingToken <= 0 || strings.TrimSpace(opts.Reason) == "" {
				return fmt.Errorf("--fencing-token and --reason are required")
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			response, err := client.VerifyPlatformArtifactReleaseLKG(args[0], opts)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return writePlatformArtifactRelease(c.stdout, response)
		},
	}
	cmd.Flags().Int64Var(&opts.FencingToken, "fencing-token", 0, "Current release fencing token")
	cmd.Flags().StringVar(&opts.Reason, "reason", "", "Verification reason")
	cmd.Flags().BoolVar(&opts.AllowInitialLKG, "allow-initial-lkg", false, "Explicitly allow an initial shadow release to seed verified LKG")
	cmd.Flags().BoolVar(&opts.Evidence.ConsumerConvergence, "consumer-convergence", false, "Required consumers converged")
	cmd.Flags().BoolVar(&opts.Evidence.LocalProbe, "local-probe", false, "Local apply and serving probes passed")
	cmd.Flags().BoolVar(&opts.Evidence.PublicSynthetic, "public-synthetic", false, "Public synthetic probes passed")
	cmd.Flags().BoolVar(&opts.Evidence.WatchWindow, "watch-window", false, "Required watch window completed")
	cmd.Flags().BoolVar(&opts.Evidence.BaselineMonotonic, "baseline-monotonic", false, "No new or worsened baseline blocker")
	cmd.Flags().BoolVar(&opts.Evidence.DatabaseRollbackCompatible, "database-rollback-compatible", false, "Database remains rollback compatible")
	cmd.Flags().StringVar(&opts.Evidence.ExpectedConsumerSetID, "expected-consumer-set", "", "Expected consumer set identifier")
	cmd.Flags().StringArrayVar(&opts.Evidence.EvidenceRefs, "evidence-ref", nil, "Evidence reference (repeatable)")
	return cmd
}

func (c *CLI) newAdminArtifactRollbackCommand() *cobra.Command {
	opts := model.PlatformArtifactRollbackRequest{}
	overrideOpts := struct {
		KernelBreakGlass bool
		TTL              time.Duration
		Confirmation     string
		Target           string
	}{TTL: 5 * time.Minute}
	cmd := &cobra.Command{
		Use:   "rollback <artifact-id-or-generation>",
		Short: "Roll back by publishing a previous artifact generation",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(opts.ToGeneration) == "" || strings.TrimSpace(opts.Reason) == "" {
				return fmt.Errorf("--to-generation and --reason are required")
			}
			if opts.ForcePublish {
				opts.SoftOverride = true
				opts.ForcePublish = false
			}
			if opts.SoftOverride && overrideOpts.KernelBreakGlass {
				return fmt.Errorf("--soft-override and --kernel-break-glass are mutually exclusive")
			}
			if overrideOpts.KernelBreakGlass {
				if overrideOpts.TTL <= 0 || overrideOpts.TTL > platformsafety.KernelBreakGlassMaxTTL {
					return fmt.Errorf("--break-glass-ttl must be positive and no greater than %s", platformsafety.KernelBreakGlassMaxTTL)
				}
				if strings.TrimSpace(overrideOpts.Confirmation) == "" || strings.TrimSpace(overrideOpts.Target) == "" {
					return fmt.Errorf("--confirm-kernel-bypass and --confirm-target are required with --kernel-break-glass")
				}
				if strings.TrimSpace(overrideOpts.Confirmation) != platformsafety.KernelBreakGlassConfirmation {
					return fmt.Errorf("--confirm-kernel-bypass must exactly equal %s", platformsafety.KernelBreakGlassConfirmation)
				}
				if strings.TrimSpace(overrideOpts.Target) != strings.TrimSpace(opts.ToGeneration) {
					return fmt.Errorf("--confirm-target must exactly match --to-generation")
				}
				opts.KernelBreakGlass = &model.PlatformKernelBreakGlassRequest{
					ExpiresAt:          time.Now().UTC().Add(overrideOpts.TTL),
					Confirmation:       overrideOpts.Confirmation,
					TargetConfirmation: overrideOpts.Target,
				}
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			response, err := client.RollbackPlatformArtifact(args[0], opts)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return writePlatformArtifactRelease(c.stdout, response)
		},
	}
	cmd.Flags().StringVar(&opts.ReleaseChannel, "channel", "full", "Release channel: shadow, gray, or full")
	cmd.Flags().StringVar(&opts.ToGeneration, "to-generation", "", "Previously validated generation to publish")
	cmd.Flags().StringVar(&opts.Reason, "reason", "", "Rollback reason")
	cmd.Flags().BoolVar(&opts.SoftOverride, "soft-override", false, "Skip non-kernel validation policy")
	cmd.Flags().BoolVar(&opts.ForcePublish, "force-publish", false, "Deprecated alias for --soft-override")
	_ = cmd.Flags().MarkDeprecated("force-publish", "use --soft-override; it cannot bypass the Platform Safety Kernel")
	cmd.Flags().BoolVar(&overrideOpts.KernelBreakGlass, "kernel-break-glass", false, "Use one operation-scoped Platform Safety Kernel recovery authorization")
	cmd.Flags().DurationVar(&overrideOpts.TTL, "break-glass-ttl", overrideOpts.TTL, "Kernel break-glass authorization validity, maximum 15m")
	cmd.Flags().StringVar(&overrideOpts.Confirmation, "confirm-kernel-bypass", "", "Must equal "+platformsafety.KernelBreakGlassConfirmation)
	cmd.Flags().StringVar(&overrideOpts.Target, "confirm-target", "", "Must exactly match --to-generation")
	cmd.Flags().StringVar(&opts.CanaryRuleRef, "canary-rule-ref", "", "Optional canary or gray rule reference")
	return cmd
}

func (c *CLI) newAdminArtifactConsumersCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "consumers <artifact-id-or-generation>",
		Short: "List consumers for an artifact scope",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			consumers, err := client.ListPlatformArtifactConsumers(args[0])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"consumers": consumers})
			}
			return writePlatformConsumers(c.stdout, consumers)
		},
	}
}

func (c *CLI) newAdminArtifactLKGCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "lkg <artifact-id-or-generation>",
		Short: "Show last-known-good snapshot for an artifact scope",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			lkg, err := client.GetPlatformArtifactLKG(args[0])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"lkg": lkg})
			}
			return writePlatformLKG(c.stdout, lkg)
		},
	}
}

func (c *CLI) newAdminFailureContractCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "failure-contract",
		Aliases: []string{"failure-contracts"},
		Short:   "Inspect subsystem failure contracts",
	}
	cmd.AddCommand(&cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List subsystem failure contracts",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			contracts, err := client.ListSubsystemFailureContracts()
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"contracts": contracts})
			}
			return writeFailureContractTable(c.stdout, contracts)
		},
	})
	cmd.AddCommand(&cobra.Command{
		Use:   "show <subsystem>",
		Short: "Show one subsystem failure contract",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			contract, err := client.GetSubsystemFailureContract(args[0])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"contract": contract})
			}
			return writeFailureContract(c.stdout, contract)
		},
	})
	return cmd
}

func writePlatformArtifactTable(w io.Writer, artifacts []model.PlatformArtifact) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "ID\tKIND\tSCOPE\tGENERATION\tSTATUS\tHASH\tUPDATED"); err != nil {
		return err
	}
	for _, artifact := range artifacts {
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			artifact.ID,
			artifact.ArtifactKind,
			firstNonEmpty(artifact.ScopeKey, "-"),
			artifact.Generation,
			artifact.Status,
			shortHash(artifact.ContentHash),
			formatTime(artifact.UpdatedAt),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writePlatformArtifact(w io.Writer, artifact model.PlatformArtifact) error {
	if err := writeKeyValues(w,
		kvPair{Key: "id", Value: artifact.ID},
		kvPair{Key: "kind", Value: artifact.ArtifactKind},
		kvPair{Key: "scope", Value: firstNonEmpty(artifact.ScopeKey, "-")},
		kvPair{Key: "generation", Value: artifact.Generation},
		kvPair{Key: "status", Value: artifact.Status},
		kvPair{Key: "content_hash", Value: artifact.ContentHash},
		kvPair{Key: "compatibility_floor", Value: firstNonEmpty(artifact.CompatibilityFloor, "-")},
		kvPair{Key: "top_level_keys", Value: strings.Join(platformArtifactTopLevelKeys(artifact), ",")},
		kvPair{Key: "created_at", Value: formatTime(artifact.CreatedAt)},
		kvPair{Key: "updated_at", Value: formatTime(artifact.UpdatedAt)},
	); err != nil {
		return err
	}
	if len(artifact.ValidationResults) == 0 {
		return nil
	}
	_, _ = fmt.Fprintln(w)
	return writePlatformArtifactValidationResults(w, artifact.ValidationResults)
}

func writePlatformArtifactValidation(w io.Writer, response platformArtifactValidationEnvelope) error {
	if err := writeKeyValues(w,
		kvPair{Key: "artifact", Value: response.Artifact.ID},
		kvPair{Key: "generation", Value: response.Artifact.Generation},
		kvPair{Key: "pass", Value: fmt.Sprintf("%t", response.Pass)},
		kvPair{Key: "dry_run", Value: fmt.Sprintf("%t", response.DryRun)},
	); err != nil {
		return err
	}
	_, _ = fmt.Fprintln(w)
	return writePlatformArtifactValidationResults(w, response.Results)
}

func writePlatformArtifactValidationResults(w io.Writer, results []model.PlatformArtifactValidationResult) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "CHECK\tPASS\tSEVERITY\tMESSAGE"); err != nil {
		return err
	}
	for _, result := range results {
		if _, err := fmt.Fprintf(tw, "%s\t%t\t%s\t%s\n", result.Name, result.Pass, result.Severity, firstNonEmpty(result.Message, "-")); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writePlatformArtifactRelease(w io.Writer, response platformArtifactReleaseEnvelope) error {
	return writeKeyValues(w,
		kvPair{Key: "artifact", Value: response.Artifact.ID},
		kvPair{Key: "generation", Value: response.Artifact.Generation},
		kvPair{Key: "release", Value: response.Release.ID},
		kvPair{Key: "channel", Value: response.Release.ReleaseChannel},
		kvPair{Key: "status", Value: response.Release.Status},
		kvPair{Key: "verification_state", Value: firstNonEmpty(response.Release.VerificationState, "-")},
		kvPair{Key: "fencing_token", Value: fmt.Sprintf("%d", response.Release.FencingToken)},
		kvPair{Key: "pinned_rollback_generation", Value: firstNonEmpty(response.Release.PinnedRollbackGeneration, "-")},
		kvPair{Key: "override_mode", Value: firstNonEmpty(response.Release.OverrideMode, "none")},
		kvPair{Key: "override_expires_at", Value: formatPlatformOptionalTime(response.Release.OverrideExpiresAt)},
		kvPair{Key: "bypassed_invariants", Value: strings.Join(response.Release.BypassedInvariants, ",")},
		kvPair{Key: "message", Value: response.Message.ID},
		kvPair{Key: "lkg_generation", Value: platformLKGGeneration(response.LKG)},
	)
}

func formatPlatformOptionalTime(value *time.Time) string {
	if value == nil {
		return "-"
	}
	return formatTime(value.UTC())
}

func writePlatformArtifactDiff(w io.Writer, left, right model.PlatformArtifact) error {
	diff := platformArtifactSafeDiff(left, right)
	return writeKeyValues(w,
		kvPair{Key: "left_generation", Value: left.Generation},
		kvPair{Key: "right_generation", Value: right.Generation},
		kvPair{Key: "left_hash", Value: left.ContentHash},
		kvPair{Key: "right_hash", Value: right.ContentHash},
		kvPair{Key: "hash_equal", Value: fmt.Sprintf("%t", left.ContentHash == right.ContentHash)},
		kvPair{Key: "added_keys", Value: strings.Join(diff["added_keys"], ",")},
		kvPair{Key: "removed_keys", Value: strings.Join(diff["removed_keys"], ",")},
		kvPair{Key: "common_keys", Value: strings.Join(diff["common_keys"], ",")},
		kvPair{Key: "values", Value: "redacted"},
	)
}

func platformArtifactSafeDiff(left, right model.PlatformArtifact) map[string][]string {
	leftKeys := stringSet(platformArtifactTopLevelKeys(left))
	rightKeys := stringSet(platformArtifactTopLevelKeys(right))
	return map[string][]string{
		"added_keys":   sortedSetDifference(rightKeys, leftKeys),
		"removed_keys": sortedSetDifference(leftKeys, rightKeys),
		"common_keys":  sortedSetIntersection(leftKeys, rightKeys),
	}
}

func platformArtifactTopLevelKeys(artifact model.PlatformArtifact) []string {
	keys := make([]string, 0, len(artifact.Content))
	for key := range artifact.Content {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func writePlatformConsumers(w io.Writer, consumers []model.PlatformConsumerInstance) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "CONSUMER\tCOMPONENT\tNODE\tDESIRED\tACTUAL\tLKG\tAPPLY\tPROBE\tUPDATED"); err != nil {
		return err
	}
	for _, consumer := range consumers {
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			consumer.ConsumerID,
			firstNonEmpty(consumer.Component, "-"),
			firstNonEmpty(consumer.NodeID, "-"),
			firstNonEmpty(consumer.DesiredGeneration, "-"),
			firstNonEmpty(consumer.ActualGeneration, "-"),
			firstNonEmpty(consumer.LKGGeneration, "-"),
			firstNonEmpty(consumer.ApplyStatus, "-"),
			firstNonEmpty(consumer.ProbeStatus, "-"),
			formatTime(consumer.UpdatedAt),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writePlatformLKG(w io.Writer, lkg *model.PlatformLKGSnapshot) error {
	if lkg == nil {
		_, err := fmt.Fprintln(w, "No LKG snapshot.")
		return err
	}
	return writeKeyValues(w,
		kvPair{Key: "id", Value: lkg.ID},
		kvPair{Key: "artifact", Value: lkg.ArtifactID},
		kvPair{Key: "kind", Value: lkg.ArtifactKind},
		kvPair{Key: "scope", Value: lkg.ScopeKey},
		kvPair{Key: "generation", Value: lkg.Generation},
		kvPair{Key: "content_hash", Value: lkg.ContentHash},
		kvPair{Key: "verified_by_release", Value: firstNonEmpty(lkg.VerifiedByReleaseID, "-")},
		kvPair{Key: "evidence_hash", Value: firstNonEmpty(lkg.VerificationEvidenceHash, "-")},
		kvPair{Key: "expires_at", Value: formatTime(lkg.ExpiresAt)},
	)
}

func writeFailureContractTable(w io.Writer, contracts []model.SubsystemFailureContract) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "SUBSYSTEM\tMODES\tSIGNALS\tAUTO_QUARANTINE\tAUTO_REPAIR\tHUMAN_REQUIRED"); err != nil {
		return err
	}
	for _, contract := range contracts {
		if _, err := fmt.Fprintf(tw, "%s\t%d\t%d\t%t\t%t\t%t\n",
			contract.Subsystem,
			len(contract.FailureModes),
			len(contract.DetectionSignals),
			contract.AutomaticQuarantineAllowed,
			contract.AutomaticRepairAllowed,
			contract.HumanApprovalRequired,
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeFailureContract(w io.Writer, contract model.SubsystemFailureContract) error {
	return writeKeyValues(w,
		kvPair{Key: "subsystem", Value: contract.Subsystem},
		kvPair{Key: "summary", Value: firstNonEmpty(contract.Summary, "-")},
		kvPair{Key: "failure_modes", Value: failureModeIDs(contract.FailureModes)},
		kvPair{Key: "detection_signals", Value: detectionSignalNames(contract.DetectionSignals)},
		kvPair{Key: "automatic_quarantine", Value: fmt.Sprintf("%t", contract.AutomaticQuarantineAllowed)},
		kvPair{Key: "automatic_repair", Value: fmt.Sprintf("%t", contract.AutomaticRepairAllowed)},
		kvPair{Key: "human_required", Value: fmt.Sprintf("%t", contract.HumanApprovalRequired)},
		kvPair{Key: "runbook", Value: firstNonEmpty(contract.RunbookRef, "-")},
	)
}

func failureModeIDs(modes []model.FailureMode) string {
	values := make([]string, 0, len(modes))
	for _, mode := range modes {
		values = append(values, mode.ID)
	}
	return strings.Join(values, ",")
}

func detectionSignalNames(signals []model.DetectionSignal) string {
	values := make([]string, 0, len(signals))
	for _, signal := range signals {
		values = append(values, signal.Name)
	}
	return strings.Join(values, ",")
}

func platformLKGGeneration(lkg *model.PlatformLKGSnapshot) string {
	if lkg == nil {
		return "-"
	}
	return lkg.Generation
}

func shortHash(value string) string {
	value = strings.TrimSpace(value)
	if len(value) <= 19 {
		return value
	}
	if strings.HasPrefix(value, "sha256:") && len(value) > len("sha256:")+12 {
		return value[:len("sha256:")+12]
	}
	return value[:19]
}

func stringSet(values []string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, value := range values {
		out[value] = struct{}{}
	}
	return out
}

func sortedSetDifference(left, right map[string]struct{}) []string {
	out := []string{}
	for value := range left {
		if _, ok := right[value]; !ok {
			out = append(out, value)
		}
	}
	sort.Strings(out)
	return out
}

func sortedSetIntersection(left, right map[string]struct{}) []string {
	out := []string{}
	for value := range left {
		if _, ok := right[value]; ok {
			out = append(out, value)
		}
	}
	sort.Strings(out)
	return out
}
