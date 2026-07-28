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

func (c *CLI) newAdminAutomationCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "automation",
		Short: "Inspect unified system and user automation policies",
	}
	cmd.AddCommand(
		c.newAdminAutomationListCommand(),
		c.newAdminAutomationShowCommand(),
	)
	return cmd
}

func (c *CLI) newAdminAutomationListCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List managed automation policies",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			response, err := client.ListAutomationPolicies()
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return writeAutomationPolicyTable(c.stdout, response.Policies)
		},
	}
}

func (c *CLI) newAdminAutomationShowCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "show <policy-id>",
		Short: "Show one managed automation policy",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			policy, err := client.GetAutomationPolicy(args[0])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, model.AutomationPolicyResponse{Policy: policy})
			}
			return writeAutomationPolicy(c.stdout, policy)
		},
	}
}

func writeAutomationPolicyTable(w io.Writer, policies []model.AutomationPolicy) error {
	sorted := append([]model.AutomationPolicy(nil), policies...)
	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[i].ID < sorted[j].ID
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "POLICY\tOWNER\tKIND\tSCOPE\tMODE\tACTIONS\tRULES\tMANAGED"); err != nil {
		return err
	}
	for _, policy := range sorted {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%s\t%s\t%d\t%t\n",
			firstNonEmpty(policy.ID, "-"),
			firstNonEmpty(policy.OwnerType, "-"),
			firstNonEmpty(policy.Kind, "-"),
			automationScopeDisplay(policy.Scope),
			firstNonEmpty(policy.Mode, "-"),
			automationActionTypes(policy.Rules),
			len(policy.Rules),
			policy.Managed,
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeAutomationPolicy(w io.Writer, policy model.AutomationPolicy) error {
	if err := writeKeyValues(w,
		kvPair{Key: "policy_id", Value: firstNonEmpty(policy.ID, "-")},
		kvPair{Key: "name", Value: firstNonEmpty(policy.Name, "-")},
		kvPair{Key: "description", Value: firstNonEmpty(policy.Description, "-")},
		kvPair{Key: "kind", Value: firstNonEmpty(policy.Kind, "-")},
		kvPair{Key: "owner_type", Value: firstNonEmpty(policy.OwnerType, "-")},
		kvPair{Key: "tenant_id", Value: firstNonEmpty(policy.TenantID, "-")},
		kvPair{Key: "project_id", Value: firstNonEmpty(policy.ProjectID, "-")},
		kvPair{Key: "scope", Value: automationScopeDisplay(policy.Scope)},
		kvPair{Key: "mode", Value: firstNonEmpty(policy.Mode, "-")},
		kvPair{Key: "priority", Value: fmt.Sprintf("%d", policy.Priority)},
		kvPair{Key: "managed", Value: fmt.Sprintf("%t", policy.Managed)},
		kvPair{Key: "source_ref", Value: firstNonEmpty(policy.SourceRef, "-")},
		kvPair{Key: "generation", Value: fmt.Sprintf("%d", policy.Generation)},
		kvPair{Key: "created_at", Value: formatTime(policy.CreatedAt)},
		kvPair{Key: "updated_at", Value: formatTime(policy.UpdatedAt)},
	); err != nil {
		return err
	}
	if len(policy.Metadata) > 0 {
		if _, err := fmt.Fprintln(w, "metadata:"); err != nil {
			return err
		}
		if err := writeStringMap(w, policy.Metadata); err != nil {
			return err
		}
	}
	for index, rule := range policy.Rules {
		if _, err := fmt.Fprintf(w, "\nrule[%d]:\n", index); err != nil {
			return err
		}
		if err := writeKeyValues(w,
			kvPair{Key: "id", Value: firstNonEmpty(rule.ID, "-")},
			kvPair{Key: "description", Value: firstNonEmpty(rule.Description, "-")},
			kvPair{Key: "trigger_type", Value: firstNonEmpty(rule.Trigger.Type, "-")},
			kvPair{Key: "trigger_source", Value: firstNonEmpty(rule.Trigger.Source, "-")},
			kvPair{Key: "invariant_id", Value: firstNonEmpty(rule.Trigger.InvariantID, "-")},
			kvPair{Key: "required_evidence", Value: stringsJoin(rule.Trigger.RequiredEvidence)},
			kvPair{Key: "minimum_samples", Value: fmt.Sprintf("%d", rule.Trigger.MinimumSamples)},
			kvPair{Key: "minimum_failure_domains", Value: fmt.Sprintf("%d", rule.Trigger.MinimumFailureDomains)},
			kvPair{Key: "action_type", Value: firstNonEmpty(rule.Action.Type, "-")},
			kvPair{Key: "gate_policy", Value: firstNonEmpty(rule.Safety.GatePolicyID, "-")},
			kvPair{Key: "action_contract", Value: firstNonEmpty(rule.Safety.ActionContractID, "-")},
			kvPair{Key: "ttl", Value: firstNonEmpty(rule.Safety.TTL, "-")},
			kvPair{Key: "recovery_condition", Value: firstNonEmpty(rule.Safety.RecoveryCondition, "-")},
			kvPair{Key: "rollback_action", Value: firstNonEmpty(rule.Safety.RollbackAction, "-")},
			kvPair{Key: "requires_rollback_target", Value: fmt.Sprintf("%t", rule.Safety.RequiresRollbackTarget)},
			kvPair{Key: "requires_audit", Value: fmt.Sprintf("%t", rule.Safety.RequiresAudit)},
			kvPair{Key: "requires_wal", Value: fmt.Sprintf("%t", rule.Safety.RequiresWAL)},
			kvPair{Key: "requires_idempotency_key", Value: fmt.Sprintf("%t", rule.Safety.RequiresIdempotencyKey)},
			kvPair{Key: "requires_fencing_token", Value: fmt.Sprintf("%t", rule.Safety.RequiresFencingToken)},
		); err != nil {
			return err
		}
		if len(rule.Action.Parameters) > 0 {
			if _, err := fmt.Fprintln(w, "action_parameters:"); err != nil {
				return err
			}
			if err := writeStringMap(w, rule.Action.Parameters); err != nil {
				return err
			}
		}
	}
	return nil
}

func automationScopeDisplay(scope model.AutomationScope) string {
	scopeType := strings.TrimSpace(scope.Type)
	scopeID := strings.TrimSpace(scope.ID)
	switch {
	case scopeType == "" && scopeID == "":
		return "-"
	case scopeID == "":
		return scopeType
	default:
		return scopeType + ":" + scopeID
	}
}

func automationActionTypes(rules []model.AutomationRule) string {
	if len(rules) == 0 {
		return "-"
	}
	values := make(map[string]struct{}, len(rules))
	for _, rule := range rules {
		value := strings.TrimSpace(rule.Action.Type)
		if value != "" {
			values[value] = struct{}{}
		}
	}
	if len(values) == 0 {
		return "-"
	}
	sorted := make([]string, 0, len(values))
	for value := range values {
		sorted = append(sorted, value)
	}
	sort.Strings(sorted)
	return strings.Join(sorted, ",")
}
