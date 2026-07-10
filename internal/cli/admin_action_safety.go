package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"text/tabwriter"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

func (c *CLI) newAdminActionContractCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "action-contract",
		Short: "Inspect bounded automatic action contracts",
	}
	cmd.AddCommand(
		c.newAdminActionContractListCommand(),
		c.newAdminActionContractShowCommand(),
	)
	return cmd
}

func (c *CLI) newAdminActionContractListCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List registered automatic action contracts",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			contracts, err := client.ListAutomaticActionContracts()
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"contracts": contracts})
			}
			return writeAutomaticActionContractTable(c.stdout, contracts)
		},
	}
}

func (c *CLI) newAdminActionContractShowCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "show <contract-id>",
		Short: "Show one automatic action contract",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			contract, err := client.GetAutomaticActionContract(args[0])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"contract": contract})
			}
			return writeAutomaticActionContract(c.stdout, contract)
		},
	}
}

func (c *CLI) newAdminActionSafetyCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "action-safety",
		Short: "Dry-run the central automatic action safety evaluator",
	}
	cmd.AddCommand(c.newAdminActionSafetyEvaluateCommand())
	return cmd
}

func (c *CLI) newAdminActionSafetyEvaluateCommand() *cobra.Command {
	opts := struct {
		File string
	}{}
	cmd := &cobra.Command{
		Use:   "evaluate",
		Short: "Evaluate a JSON action request without executing the action",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(opts.File) == "" {
				return fmt.Errorf("--file is required; use - for stdin")
			}
			request, err := readActionSafetyRequest(opts.File, cmd.InOrStdin())
			if err != nil {
				return err
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			decision, err := client.EvaluateActionSafety(request)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"decision": decision})
			}
			return writeActionSafetyDecision(c.stdout, decision)
		},
	}
	cmd.Flags().StringVarP(&opts.File, "file", "f", "", "ActionSafetyRequest JSON file, or - for stdin")
	return cmd
}

func readActionSafetyRequest(path string, stdin io.Reader) (model.ActionSafetyRequest, error) {
	var reader io.Reader
	if strings.TrimSpace(path) == "-" {
		reader = stdin
	} else {
		file, err := os.Open(strings.TrimSpace(path))
		if err != nil {
			return model.ActionSafetyRequest{}, fmt.Errorf("open action safety request: %w", err)
		}
		defer file.Close()
		reader = file
	}
	var request model.ActionSafetyRequest
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return model.ActionSafetyRequest{}, fmt.Errorf("decode action safety request: %w", err)
	}
	return request, nil
}

func writeAutomaticActionContractTable(w io.Writer, contracts []model.AutomaticActionContract) error {
	sorted := append([]model.AutomaticActionContract(nil), contracts...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ID < sorted[j].ID })
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "CONTRACT\tACTION\tSCOPE\tGATE\tTTL\tENABLE_ENV\tKILL_SWITCH\tRUNBOOK"); err != nil {
		return err
	}
	for _, contract := range sorted {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			firstNonEmpty(contract.ID, "-"),
			firstNonEmpty(contract.ActionType, "-"),
			firstNonEmpty(contract.Scope, "-"),
			firstNonEmpty(contract.GatePolicyID, "-"),
			firstNonEmpty(contract.TTL, "-"),
			firstNonEmpty(contract.EnableEnv, "-"),
			firstNonEmpty(contract.KillSwitchEnv, "-"),
			firstNonEmpty(contract.RunbookRef, "-"),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeAutomaticActionContract(w io.Writer, contract model.AutomaticActionContract) error {
	return writeKeyValues(w,
		kvPair{Key: "contract_id", Value: firstNonEmpty(contract.ID, "-")},
		kvPair{Key: "action_type", Value: firstNonEmpty(contract.ActionType, "-")},
		kvPair{Key: "scope", Value: firstNonEmpty(contract.Scope, "-")},
		kvPair{Key: "trigger_invariant", Value: firstNonEmpty(contract.TriggerInvariant, "-")},
		kvPair{Key: "required_evidence", Value: stringsJoin(contract.RequiredEvidence)},
		kvPair{Key: "gate_policy", Value: firstNonEmpty(contract.GatePolicyID, "-")},
		kvPair{Key: "ttl", Value: firstNonEmpty(contract.TTL, "-")},
		kvPair{Key: "minimum_samples", Value: fmt.Sprintf("%d", contract.MinimumSamples)},
		kvPair{Key: "minimum_failure_domains", Value: fmt.Sprintf("%d", contract.MinimumFailureDomains)},
		kvPair{Key: "soak_min_duration", Value: firstNonEmpty(contract.SoakMinDuration, "-")},
		kvPair{Key: "recovery_condition", Value: firstNonEmpty(contract.RecoveryCondition, "-")},
		kvPair{Key: "rollback_action", Value: firstNonEmpty(contract.RollbackAction, "-")},
		kvPair{Key: "enable_env", Value: firstNonEmpty(contract.EnableEnv, "-")},
		kvPair{Key: "kill_switch", Value: firstNonEmpty(contract.KillSwitchEnv, "-")},
		kvPair{Key: "allowed_modes", Value: stringsJoin(contract.AllowedModes)},
		kvPair{Key: "requires_rollback_target", Value: fmt.Sprintf("%t", contract.RequiresRollbackTarget)},
		kvPair{Key: "requires_audit", Value: fmt.Sprintf("%t", contract.RequiresAudit)},
		kvPair{Key: "requires_wal", Value: fmt.Sprintf("%t", contract.RequiresWAL)},
		kvPair{Key: "requires_idempotency_key", Value: fmt.Sprintf("%t", contract.RequiresIdempotencyKey)},
		kvPair{Key: "requires_fencing_token", Value: fmt.Sprintf("%t", contract.RequiresFencingToken)},
		kvPair{Key: "runbook", Value: firstNonEmpty(contract.RunbookRef, "-")},
	)
}

func writeActionSafetyDecision(w io.Writer, decision model.ActionSafetyDecision) error {
	if err := writeKeyValues(w,
		kvPair{Key: "pass", Value: fmt.Sprintf("%t", decision.Pass)},
		kvPair{Key: "allowed", Value: fmt.Sprintf("%t", decision.Allowed)},
		kvPair{Key: "would_action", Value: fmt.Sprintf("%t", decision.WouldAction)},
		kvPair{Key: "production_mutation_allowed", Value: fmt.Sprintf("%t", decision.ProductionMutationAllowed)},
		kvPair{Key: "effective_mode", Value: firstNonEmpty(decision.EffectiveMode, "-")},
		kvPair{Key: "contract_id", Value: firstNonEmpty(decision.ContractID, "-")},
		kvPair{Key: "gate_policy", Value: firstNonEmpty(decision.GatePolicyID, "-")},
		kvPair{Key: "subject", Value: firstNonEmpty(decision.Subject, "-")},
		kvPair{Key: "expires_at", Value: formatOptionalTimePtr(decision.ExpiresAt)},
		kvPair{Key: "blast_radius_pass", Value: fmt.Sprintf("%t", decision.BlastRadius.Pass)},
		kvPair{Key: "blast_radius_reason", Value: firstNonEmpty(decision.BlastRadius.Reason, "-")},
		kvPair{Key: "generated_at", Value: formatTime(decision.GeneratedAt)},
	); err != nil {
		return err
	}
	if len(decision.Violations) == 0 {
		return nil
	}
	if _, err := fmt.Fprintln(w, "violations:"); err != nil {
		return err
	}
	for _, violation := range decision.Violations {
		if _, err := fmt.Fprintf(w, "- %s: %s\n", violation.Code, violation.Message); err != nil {
			return err
		}
	}
	return nil
}
