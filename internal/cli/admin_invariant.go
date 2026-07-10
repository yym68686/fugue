package cli

import (
	"fmt"
	"io"
	"sort"
	"text/tabwriter"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

func (c *CLI) newAdminInvariantCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "invariant",
		Short: "Inspect platform safety and resilience invariants",
	}
	cmd.AddCommand(
		c.newAdminInvariantListCommand(),
		c.newAdminInvariantShowCommand(),
		c.newAdminInvariantInventoryCommand(),
	)
	return cmd
}

func (c *CLI) newAdminInvariantListCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List registered platform invariants",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			invariants, err := client.ListInvariantDefinitions()
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"invariants": invariants})
			}
			return writeInvariantDefinitionTable(c.stdout, invariants)
		},
	}
}

func (c *CLI) newAdminInvariantShowCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "show <invariant-id>",
		Short: "Show one registered platform invariant",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			invariant, err := client.GetInvariantDefinition(args[0])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"invariant": invariant})
			}
			return writeInvariantDefinition(c.stdout, invariant)
		},
	}
}

func (c *CLI) newAdminInvariantInventoryCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "inventory",
		Short: "Show the platform control-loop implementation inventory",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			inventory, err := client.GetPlatformControlInventory()
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"inventory": inventory})
			}
			if err := writeKeyValues(c.stdout,
				kvPair{Key: "artifact_kinds", Value: fmt.Sprintf("%d", len(inventory.ArtifactKinds))},
				kvPair{Key: "consumers", Value: fmt.Sprintf("%d", len(inventory.Consumers))},
				kvPair{Key: "gate_policies", Value: fmt.Sprintf("%d", len(inventory.GatePolicies))},
				kvPair{Key: "automatic_actions", Value: fmt.Sprintf("%d", len(inventory.AutomaticActions))},
				kvPair{Key: "release_signals", Value: fmt.Sprintf("%d", len(inventory.ReleaseSignals))},
				kvPair{Key: "synthetic_probes", Value: fmt.Sprintf("%d", len(inventory.SyntheticProbes))},
				kvPair{Key: "lkg_policies", Value: fmt.Sprintf("%d", len(inventory.LKGPolicies))},
				kvPair{Key: "mechanisms", Value: fmt.Sprintf("%d", len(inventory.Mechanisms))},
				kvPair{Key: "generated_at", Value: formatTime(inventory.GeneratedAt)},
			); err != nil {
				return err
			}
			if _, err := fmt.Fprintln(c.stdout); err != nil {
				return err
			}
			if err := writePlatformControlMechanismTable(c.stdout, inventory.Mechanisms); err != nil {
				return err
			}
			if _, err := fmt.Fprintln(c.stdout); err != nil {
				return err
			}
			return writePlatformLKGPolicyTable(c.stdout, inventory.LKGPolicies)
		},
	}
}

func writeInvariantDefinitionTable(w io.Writer, invariants []model.InvariantDefinition) error {
	sorted := append([]model.InvariantDefinition(nil), invariants...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ID < sorted[j].ID })
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "INVARIANT\tCATEGORY\tSCOPE\tMODE\tSEVERITY\tACTION\tRUNBOOK"); err != nil {
		return err
	}
	for _, invariant := range sorted {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			firstNonEmpty(invariant.ID, "-"),
			firstNonEmpty(invariant.Category, "-"),
			firstNonEmpty(invariant.Scope, "-"),
			firstNonEmpty(invariant.DefaultMode, "-"),
			firstNonEmpty(invariant.Severity, "-"),
			firstNonEmpty(invariant.AutomaticActionContractID, "-"),
			firstNonEmpty(invariant.RunbookRef, "-"),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeInvariantDefinition(w io.Writer, invariant model.InvariantDefinition) error {
	return writeKeyValues(w,
		kvPair{Key: "invariant_id", Value: firstNonEmpty(invariant.ID, "-")},
		kvPair{Key: "category", Value: firstNonEmpty(invariant.Category, "-")},
		kvPair{Key: "scope", Value: firstNonEmpty(invariant.Scope, "-")},
		kvPair{Key: "subject", Value: firstNonEmpty(invariant.Subject, "-")},
		kvPair{Key: "owner", Value: firstNonEmpty(invariant.Owner, "-")},
		kvPair{Key: "description", Value: firstNonEmpty(invariant.Description, "-")},
		kvPair{Key: "severity", Value: firstNonEmpty(invariant.Severity, "-")},
		kvPair{Key: "default_mode", Value: firstNonEmpty(invariant.DefaultMode, "-")},
		kvPair{Key: "hard_gate", Value: fmt.Sprintf("%t", invariant.HardGate)},
		kvPair{Key: "evidence_sources", Value: stringsJoin(invariant.EvidenceSources)},
		kvPair{Key: "evidence_max_age", Value: firstNonEmpty(invariant.EvidenceFreshnessPolicy.MaxAge, "-")},
		kvPair{Key: "minimum_sources", Value: fmt.Sprintf("%d", invariant.EvidenceFreshnessPolicy.MinimumSources)},
		kvPair{Key: "minimum_failure_domains", Value: fmt.Sprintf("%d", invariant.EvidenceFreshnessPolicy.MinimumFailureDomains)},
		kvPair{Key: "unknown_behavior", Value: firstNonEmpty(invariant.UnknownBehavior, "-")},
		kvPair{Key: "stale_behavior", Value: firstNonEmpty(invariant.StaleBehavior, "-")},
		kvPair{Key: "gate_policy", Value: firstNonEmpty(invariant.GatePolicyID, "-")},
		kvPair{Key: "automatic_action_contract", Value: firstNonEmpty(invariant.AutomaticActionContractID, "-")},
		kvPair{Key: "non_bypassable", Value: fmt.Sprintf("%t", invariant.NonBypassable)},
		kvPair{Key: "runbook", Value: firstNonEmpty(invariant.RunbookRef, "-")},
	)
}

func writePlatformControlMechanismTable(w io.Writer, mechanisms []model.PlatformControlMechanism) error {
	sorted := append([]model.PlatformControlMechanism(nil), mechanisms...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ID < sorted[j].ID })
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "MECHANISM\tCATEGORY\tSTATUS\tMODE\tIMPLEMENTATION"); err != nil {
		return err
	}
	for _, mechanism := range sorted {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%s\n",
			firstNonEmpty(mechanism.ID, "-"),
			firstNonEmpty(mechanism.Category, "-"),
			firstNonEmpty(mechanism.Status, "-"),
			firstNonEmpty(mechanism.Mode, "-"),
			firstNonEmpty(mechanism.ImplementationRef, "-"),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writePlatformLKGPolicyTable(w io.Writer, policies []model.PlatformLKGPolicyDefinition) error {
	sorted := append([]model.PlatformLKGPolicyDefinition(nil), policies...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ArtifactKind < sorted[j].ArtifactKind })
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "ARTIFACT\tSTORAGE\tPATH_ENV\tMAX_AGE\tMAX_STALE\tGENERATIONS\tARCHIVE_LIMIT\tEXPIRY"); err != nil {
		return err
	}
	for _, policy := range sorted {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%s\t%d\t%d\t%s\n",
			firstNonEmpty(policy.ArtifactKind, "-"),
			firstNonEmpty(policy.StorageLocation, "-"),
			firstNonEmpty(policy.CachePathEnv, "-"),
			firstNonEmpty(policy.MaxAge, "-"),
			firstNonEmpty(policy.MaxStale, "-"),
			policy.MinimumGenerations,
			policy.ArchiveLimit,
			firstNonEmpty(policy.ExpiryBehavior, "-"),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}
