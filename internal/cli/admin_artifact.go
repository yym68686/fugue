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

func (c *CLI) newAdminArtifactCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "artifact",
		Short: "Inspect and release platform state artifacts",
	}
	cmd.AddCommand(
		c.newAdminArtifactListCommand(),
		c.newAdminArtifactShowCommand(),
		c.newAdminArtifactDiffCommand(),
		c.newAdminArtifactValidateCommand(),
		c.newAdminArtifactReleaseCommand(),
		c.newAdminArtifactRollbackCommand(),
		c.newAdminArtifactConsumersCommand(),
		c.newAdminArtifactLKGCommand(),
	)
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
	cmd := &cobra.Command{
		Use:   "release <artifact-id-or-generation>",
		Short: "Release a validated platform artifact",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(opts.ReleaseChannel) == "" {
				return fmt.Errorf("--channel is required: shadow, gray, or full")
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
	cmd.Flags().BoolVar(&opts.ForcePublish, "force-publish", false, "Publish despite validation status; requires --reason")
	cmd.Flags().StringVar(&opts.Reason, "reason", "", "Reason for release or force publish")
	return cmd
}

func (c *CLI) newAdminArtifactRollbackCommand() *cobra.Command {
	opts := model.PlatformArtifactRollbackRequest{}
	cmd := &cobra.Command{
		Use:   "rollback <artifact-id-or-generation>",
		Short: "Roll back by publishing a previous artifact generation",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(opts.ToGeneration) == "" || strings.TrimSpace(opts.Reason) == "" {
				return fmt.Errorf("--to-generation and --reason are required")
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
	cmd.Flags().BoolVar(&opts.ForcePublish, "force-publish", false, "Publish despite validation status")
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
		kvPair{Key: "message", Value: response.Message.ID},
		kvPair{Key: "lkg_generation", Value: platformLKGGeneration(response.LKG)},
	)
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
