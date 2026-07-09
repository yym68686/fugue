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

func (c *CLI) newAdminGateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "gate",
		Short: "Inspect and promote platform release safety gates",
	}
	cmd.AddCommand(
		c.newAdminGateListCommand(),
		c.newAdminGateShowCommand(),
		c.newAdminGatePromoteCommand(),
	)
	return cmd
}

func (c *CLI) newAdminGateListCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List platform gate policies",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			policies, err := client.ListGatePolicies()
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"policies": policies})
			}
			return writeGatePolicyTable(c.stdout, policies)
		},
	}
}

func (c *CLI) newAdminGateShowCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "show <gate-id>",
		Short: "Show one gate policy",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			policy, err := client.GetGatePolicy(args[0])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"policy": policy})
			}
			return writeGatePolicy(c.stdout, policy)
		},
	}
}

func (c *CLI) newAdminGatePromoteCommand() *cobra.Command {
	opts := struct {
		Mode    string
		Reason  string
		Canary  []string
		Release string
	}{}
	cmd := &cobra.Command{
		Use:   "promote <gate-id>",
		Short: "Promote a gate policy to shadow, canary, enforced, or disabled",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(opts.Mode) == "" {
				return fmt.Errorf("--mode is required")
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			response, err := client.PromoteGatePolicy(args[0], model.GatePolicyPromoteRequest{
				Mode:                opts.Mode,
				Reason:              opts.Reason,
				CanaryScopes:        opts.Canary,
				IntroducedByRelease: opts.Release,
			})
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			if err := writeGatePolicy(c.stdout, response.Policy); err != nil {
				return err
			}
			_, err = fmt.Fprintf(c.stdout, "\nartifact_generation=%s\nrelease_id=%s\n", response.Artifact.Generation, response.Release.ID)
			return err
		},
	}
	cmd.Flags().StringVar(&opts.Mode, "mode", "", "Target mode: shadow, canary, enforced, or disabled")
	cmd.Flags().StringVar(&opts.Reason, "reason", "", "Promotion reason")
	cmd.Flags().StringArrayVar(&opts.Canary, "canary-scope", nil, "Canary failure-domain scope (repeatable)")
	cmd.Flags().StringVar(&opts.Release, "introduced-by-release", "", "Git SHA or release id that introduced this gate")
	return cmd
}

func writeGatePolicyTable(w io.Writer, policies []model.GatePolicy) error {
	sorted := append([]model.GatePolicy(nil), policies...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ID < sorted[j].ID })
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "GATE\tMODE\tSCOPE\tKILL_SWITCH\tSOAK\tCANARY\tRUNBOOK"); err != nil {
		return err
	}
	for _, policy := range sorted {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			firstNonEmpty(policy.ID, "-"),
			firstNonEmpty(policy.Mode, "-"),
			firstNonEmpty(policy.Scope, "-"),
			firstNonEmpty(policy.KillSwitchEnv, "-"),
			firstNonEmpty(policy.SoakMinDuration, "-"),
			stringsJoin(policy.CanaryFailureDomains),
			firstNonEmpty(policy.RunbookRef, "-"),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeGatePolicy(w io.Writer, policy model.GatePolicy) error {
	return writeKeyValues(w,
		kvPair{Key: "gate_id", Value: firstNonEmpty(policy.ID, "-")},
		kvPair{Key: "description", Value: firstNonEmpty(policy.Description, "-")},
		kvPair{Key: "mode", Value: firstNonEmpty(policy.Mode, "-")},
		kvPair{Key: "default_mode", Value: firstNonEmpty(policy.DefaultMode, "-")},
		kvPair{Key: "scope", Value: firstNonEmpty(policy.Scope, "-")},
		kvPair{Key: "introduced_by_release", Value: firstNonEmpty(policy.IntroducedByRelease, "-")},
		kvPair{Key: "soak_started_at", Value: formatOptionalTimePtr(policy.SoakStartedAt)},
		kvPair{Key: "soak_min_duration", Value: firstNonEmpty(policy.SoakMinDuration, "-")},
		kvPair{Key: "minimum_samples", Value: fmt.Sprintf("%d", policy.MinimumSamples)},
		kvPair{Key: "minimum_failure_domains", Value: fmt.Sprintf("%d", policy.MinimumFailureDomains)},
		kvPair{Key: "canary_failure_domains", Value: stringsJoin(policy.CanaryFailureDomains)},
		kvPair{Key: "kill_switch", Value: firstNonEmpty(policy.KillSwitchEnv, "-")},
		kvPair{Key: "rollback_on", Value: stringsJoin(policy.RollbackOn)},
		kvPair{Key: "runbook", Value: firstNonEmpty(policy.RunbookRef, "-")},
		kvPair{Key: "updated_at", Value: formatTime(policy.UpdatedAt)},
		kvPair{Key: "updated_by", Value: firstNonEmpty(policy.UpdatedBy, "-")},
		kvPair{Key: "promotion_reason", Value: firstNonEmpty(policy.PromotionReason, "-")},
	)
}
