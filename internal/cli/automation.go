package cli

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

const (
	automationAppRestartActionType = "restart_app"
	automationRequestOutcomeSource = "app_request_outcomes"
	automationHTTPStatusMetric     = "http_status"
)

type automationCreateOptions struct {
	Name                  string
	Description           string
	Mode                  string
	Priority              int
	SourceRef             string
	RuleID                string
	RuleDescription       string
	Window                string
	StatusCodes           []int
	MinimumSamples        int
	MinimumFailureDomains int
	RequiredEvidence      []string
	Reason                string
	Metadata              []string
}

type automationUpdateOptions struct {
	ExpectedGeneration    int64
	Name                  string
	Description           string
	Mode                  string
	Priority              int
	SourceRef             string
	RuleID                string
	RuleDescription       string
	Window                string
	StatusCodes           []int
	MinimumSamples        int
	MinimumFailureDomains int
	RequiredEvidence      []string
	ClearRequiredEvidence bool
	Reason                string
	ClearReason           bool
	Metadata              []string
	ClearMetadata         bool
}

func (c *CLI) newAutomationCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "automation",
		Aliases: []string{"automations"},
		Short:   "Manage bounded application recovery policies",
		Long: strings.TrimSpace(`
Automation policies observe typed Fugue evidence and propose guarded actions.

The initial user surface is deliberately bounded to application request
outcomes and the restart_app action. Policies may be disabled or evaluated in
shadow mode; creating a policy does not authorize a production restart.
`),
	}
	cmd.AddCommand(
		c.newAutomationListCommand(),
		c.newAutomationShowCommand(),
		c.newAutomationCreateCommand(),
		c.newAutomationUpdateCommand(),
		c.newAutomationDeleteCommand(),
	)
	return cmd
}

func (c *CLI) newAutomationListCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List visible user automation policies",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			tenantID, projectID, err := c.resolveFilterSelections(client)
			if err != nil {
				return err
			}
			response, err := client.ListUserAutomationPolicies(tenantID, projectID)
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

func (c *CLI) newAutomationShowCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "show <policy-id>",
		Short: "Show one visible user automation policy",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			policyID := strings.TrimSpace(args[0])
			client, err := c.newClient()
			if err != nil {
				return err
			}
			policy, err := client.GetUserAutomationPolicy(policyID)
			if err != nil {
				return err
			}
			if policy.ID != policyID {
				return fmt.Errorf("automation lookup returned policy %q instead of %q", policy.ID, policyID)
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, model.AutomationPolicyResponse{Policy: policy})
			}
			return writeAutomationPolicy(c.stdout, policy)
		},
	}
}

func (c *CLI) newAutomationCreateCommand() *cobra.Command {
	opts := automationCreateOptions{
		Mode:                  model.GatePolicyModeDisabled,
		RuleID:                "restart-on-unavailable",
		Window:                "2m",
		StatusCodes:           []int{503, 504},
		MinimumSamples:        3,
		MinimumFailureDomains: 1,
	}
	cmd := &cobra.Command{
		Use:   "create <app>",
		Short: "Create a disabled or shadow application recovery policy",
		Long: strings.TrimSpace(`
Create a typed policy that observes repeated 5xx request outcomes for one app
and proposes restart_app through Fugue's registered safety contract.

The default is disabled. Shadow mode records evaluation decisions but cannot
restart the application.
`),
		Example: strings.TrimSpace(`
fugue automation create api --name "API unavailable recovery"
fugue automation create api --name "API unavailable recovery" --mode shadow --window 2m --status-code 503,504
`),
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			request, err := buildCreateAutomationPolicyRequest(app, opts)
			if err != nil {
				return err
			}
			policy, err := client.CreateUserAutomationPolicy(request)
			if err != nil {
				return err
			}
			if policy.OwnerType != model.AutomationOwnerUser ||
				policy.Managed ||
				policy.Kind != model.AutomationPolicyKindAppRecovery ||
				policy.Scope.Type != model.AutomationScopeApp ||
				policy.Scope.ID != app.ID {
				return fmt.Errorf("automation create returned a policy outside the requested app-recovery scope")
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, model.AutomationPolicyResponse{Policy: policy})
			}
			return writeAutomationPolicy(c.stdout, policy)
		},
	}
	cmd.Flags().StringVar(&opts.Name, "name", "", "Policy name")
	cmd.Flags().StringVar(&opts.Description, "description", "", "Policy description")
	cmd.Flags().StringVar(&opts.Mode, "mode", opts.Mode, "Policy mode: disabled or shadow")
	cmd.Flags().IntVar(&opts.Priority, "priority", 0, "Non-negative policy priority")
	cmd.Flags().StringVar(&opts.SourceRef, "source-ref", "", "Optional external configuration reference")
	cmd.Flags().StringVar(&opts.RuleID, "rule-id", opts.RuleID, "Stable rule identifier")
	cmd.Flags().StringVar(&opts.RuleDescription, "rule-description", "", "Rule description")
	cmd.Flags().StringVar(&opts.Window, "window", opts.Window, "Request metric window between 1s and 15m")
	cmd.Flags().IntSliceVar(&opts.StatusCodes, "status-code", opts.StatusCodes, "Server-error status code to observe; comma-separated or repeatable")
	cmd.Flags().IntVar(&opts.MinimumSamples, "minimum-samples", opts.MinimumSamples, "Minimum matching request samples")
	cmd.Flags().IntVar(&opts.MinimumFailureDomains, "minimum-failure-domains", opts.MinimumFailureDomains, "Minimum independent failure domains")
	cmd.Flags().StringArrayVar(&opts.RequiredEvidence, "require-evidence", nil, "Additional evidence identifier; repeatable")
	cmd.Flags().StringVar(&opts.Reason, "reason", "", "Restart proposal reason, at most 500 bytes")
	cmd.Flags().StringArrayVar(&opts.Metadata, "metadata", nil, "Policy metadata as KEY=VALUE; repeatable")
	_ = cmd.MarkFlagRequired("name")
	return cmd
}

func (c *CLI) newAutomationUpdateCommand() *cobra.Command {
	opts := automationUpdateOptions{}
	cmd := &cobra.Command{
		Use:   "update <policy-id>",
		Short: "CAS-update a user automation policy",
		Long: strings.TrimSpace(`
Update only the explicitly supplied fields. --generation is mandatory and is
sent unchanged as the compare-and-swap boundary; the CLI never silently
retries a stale write.

Rule-level flags target the only rule automatically. For a multi-rule policy,
select the rule explicitly with --rule-id.
`),
		Example: strings.TrimSpace(`
fugue automation update automation_policy_123 --generation 1 --mode shadow
fugue automation update automation_policy_123 --generation 2 --window 3m --status-code 502,503,504
`),
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			policyID := strings.TrimSpace(args[0])
			if opts.ExpectedGeneration <= 0 {
				return fmt.Errorf("generation must be a positive integer")
			}
			if !automationUpdateHasChanges(cmd) {
				return fmt.Errorf("at least one automation field must be changed")
			}
			if cmd.Flags().Changed("metadata") && opts.ClearMetadata {
				return fmt.Errorf("--metadata and --clear-metadata cannot be used together")
			}
			if cmd.Flags().Changed("require-evidence") && opts.ClearRequiredEvidence {
				return fmt.Errorf("--require-evidence and --clear-required-evidence cannot be used together")
			}
			if cmd.Flags().Changed("reason") && opts.ClearReason {
				return fmt.Errorf("--reason and --clear-reason cannot be used together")
			}

			client, err := c.newClient()
			if err != nil {
				return err
			}
			current, err := client.GetUserAutomationPolicy(policyID)
			if err != nil {
				return err
			}
			if current.ID != policyID {
				return fmt.Errorf("automation lookup returned policy %q instead of %q", current.ID, policyID)
			}
			request, err := buildUpdateAutomationPolicyRequest(cmd, current, opts)
			if err != nil {
				return err
			}
			policy, err := client.UpdateUserAutomationPolicy(policyID, request)
			if err != nil {
				return err
			}
			if policy.ID != policyID {
				return fmt.Errorf("automation update returned policy %q instead of %q", policy.ID, policyID)
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, model.AutomationPolicyResponse{Policy: policy})
			}
			return writeAutomationPolicy(c.stdout, policy)
		},
	}
	cmd.Flags().Int64Var(&opts.ExpectedGeneration, "generation", 0, "Expected current policy generation")
	cmd.Flags().StringVar(&opts.Name, "name", "", "Replace the policy name")
	cmd.Flags().StringVar(&opts.Description, "description", "", "Replace or clear the policy description")
	cmd.Flags().StringVar(&opts.Mode, "mode", "", "Replace the policy mode: disabled or shadow")
	cmd.Flags().IntVar(&opts.Priority, "priority", 0, "Replace the non-negative policy priority")
	cmd.Flags().StringVar(&opts.SourceRef, "source-ref", "", "Replace or clear the external configuration reference")
	cmd.Flags().StringVar(&opts.RuleID, "rule-id", "", "Rule identifier for rule-level changes")
	cmd.Flags().StringVar(&opts.RuleDescription, "rule-description", "", "Replace or clear the selected rule description")
	cmd.Flags().StringVar(&opts.Window, "window", "", "Replace the selected request metric window")
	cmd.Flags().IntSliceVar(&opts.StatusCodes, "status-code", nil, "Replace selected server-error status codes; comma-separated or repeatable")
	cmd.Flags().IntVar(&opts.MinimumSamples, "minimum-samples", 0, "Replace the selected minimum matching samples")
	cmd.Flags().IntVar(&opts.MinimumFailureDomains, "minimum-failure-domains", 0, "Replace the selected minimum failure domains")
	cmd.Flags().StringArrayVar(&opts.RequiredEvidence, "require-evidence", nil, "Replace selected additional evidence identifiers; repeatable")
	cmd.Flags().BoolVar(&opts.ClearRequiredEvidence, "clear-required-evidence", false, "Remove user-added evidence requirements from the selected rule")
	cmd.Flags().StringVar(&opts.Reason, "reason", "", "Replace the selected restart proposal reason")
	cmd.Flags().BoolVar(&opts.ClearReason, "clear-reason", false, "Remove the selected restart proposal reason")
	cmd.Flags().StringArrayVar(&opts.Metadata, "metadata", nil, "Replace policy metadata with KEY=VALUE entries; repeatable")
	cmd.Flags().BoolVar(&opts.ClearMetadata, "clear-metadata", false, "Remove all policy metadata")
	_ = cmd.MarkFlagRequired("generation")
	return cmd
}

func (c *CLI) newAutomationDeleteCommand() *cobra.Command {
	var expectedGeneration int64
	cmd := &cobra.Command{
		Use:     "delete <policy-id>",
		Aliases: []string{"rm", "remove"},
		Short:   "CAS-delete a user automation policy",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			policyID := strings.TrimSpace(args[0])
			if expectedGeneration <= 0 {
				return fmt.Errorf("generation must be a positive integer")
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			response, err := client.DeleteUserAutomationPolicy(policyID, expectedGeneration)
			if err != nil {
				return err
			}
			if response.Policy.ID != policyID {
				return fmt.Errorf("automation delete returned policy %q instead of %q", response.Policy.ID, policyID)
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return writeKeyValues(c.stdout,
				kvPair{Key: "deleted", Value: fmt.Sprintf("%t", response.Deleted)},
				kvPair{Key: "policy_id", Value: firstNonEmpty(response.Policy.ID, "-")},
				kvPair{Key: "generation", Value: fmt.Sprintf("%d", response.Policy.Generation)},
			)
		},
	}
	cmd.Flags().Int64Var(&expectedGeneration, "generation", 0, "Expected current policy generation")
	_ = cmd.MarkFlagRequired("generation")
	return cmd
}

func buildCreateAutomationPolicyRequest(app model.App, opts automationCreateOptions) (model.CreateAutomationPolicyRequest, error) {
	name := strings.TrimSpace(opts.Name)
	if name == "" {
		return model.CreateAutomationPolicyRequest{}, fmt.Errorf("name is required")
	}
	mode, err := normalizeUserAutomationMode(opts.Mode)
	if err != nil {
		return model.CreateAutomationPolicyRequest{}, err
	}
	window, err := normalizeAutomationMetricWindow(opts.Window)
	if err != nil {
		return model.CreateAutomationPolicyRequest{}, err
	}
	statusCodes, err := normalizeAutomationStatusCodes(opts.StatusCodes)
	if err != nil {
		return model.CreateAutomationPolicyRequest{}, err
	}
	if err := validateAutomationEvidenceLimits(opts.MinimumSamples, opts.MinimumFailureDomains); err != nil {
		return model.CreateAutomationPolicyRequest{}, err
	}
	if err := validateAutomationPriority(opts.Priority); err != nil {
		return model.CreateAutomationPolicyRequest{}, err
	}
	ruleID := strings.TrimSpace(opts.RuleID)
	if ruleID == "" {
		return model.CreateAutomationPolicyRequest{}, fmt.Errorf("rule id is required")
	}
	requiredEvidence, err := normalizeAutomationEvidence(opts.RequiredEvidence)
	if err != nil {
		return model.CreateAutomationPolicyRequest{}, err
	}
	parameters, err := automationReasonParameters(opts.Reason)
	if err != nil {
		return model.CreateAutomationPolicyRequest{}, err
	}
	metadata, err := parseAutomationMetadata(opts.Metadata)
	if err != nil {
		return model.CreateAutomationPolicyRequest{}, err
	}
	if strings.TrimSpace(app.ID) == "" ||
		strings.TrimSpace(app.TenantID) == "" ||
		strings.TrimSpace(app.ProjectID) == "" {
		return model.CreateAutomationPolicyRequest{}, fmt.Errorf("resolved app is missing its tenant, project, or app id")
	}
	return model.CreateAutomationPolicyRequest{
		TenantID:    strings.TrimSpace(app.TenantID),
		ProjectID:   strings.TrimSpace(app.ProjectID),
		Name:        name,
		Description: strings.TrimSpace(opts.Description),
		Kind:        model.AutomationPolicyKindAppRecovery,
		Scope: model.AutomationScope{
			Type: model.AutomationScopeApp,
			ID:   strings.TrimSpace(app.ID),
		},
		Mode:      mode,
		Priority:  opts.Priority,
		SourceRef: strings.TrimSpace(opts.SourceRef),
		Rules: []model.AutomationRuleInput{{
			ID:          ruleID,
			Description: strings.TrimSpace(opts.RuleDescription),
			Trigger: model.AutomationTriggerInput{
				Type:   model.AutomationTriggerRequestMetric,
				Source: automationRequestOutcomeSource,
				RequestMetric: &model.AutomationRequestMetricSelector{
					Metric:      automationHTTPStatusMetric,
					Window:      window,
					StatusCodes: statusCodes,
				},
				RequiredEvidence:      requiredEvidence,
				MinimumSamples:        opts.MinimumSamples,
				MinimumFailureDomains: opts.MinimumFailureDomains,
			},
			Action: model.AutomationActionInput{
				Type:       automationAppRestartActionType,
				Parameters: parameters,
			},
		}},
		Metadata: metadata,
	}, nil
}

func buildUpdateAutomationPolicyRequest(
	cmd *cobra.Command,
	current model.AutomationPolicy,
	opts automationUpdateOptions,
) (model.UpdateAutomationPolicyRequest, error) {
	rules, err := automationRuleInputsFromPolicy(current)
	if err != nil {
		return model.UpdateAutomationPolicyRequest{}, err
	}
	request := model.UpdateAutomationPolicyRequest{
		ExpectedGeneration: opts.ExpectedGeneration,
		Name:               current.Name,
		Description:        current.Description,
		Mode:               current.Mode,
		Priority:           current.Priority,
		SourceRef:          current.SourceRef,
		Rules:              rules,
		Metadata:           cloneStringMap(current.Metadata),
	}

	if cmd.Flags().Changed("name") {
		request.Name = strings.TrimSpace(opts.Name)
		if request.Name == "" {
			return model.UpdateAutomationPolicyRequest{}, fmt.Errorf("name cannot be empty")
		}
	}
	if cmd.Flags().Changed("description") {
		request.Description = strings.TrimSpace(opts.Description)
	}
	if cmd.Flags().Changed("mode") {
		request.Mode, err = normalizeUserAutomationMode(opts.Mode)
		if err != nil {
			return model.UpdateAutomationPolicyRequest{}, err
		}
	}
	if cmd.Flags().Changed("priority") {
		if err := validateAutomationPriority(opts.Priority); err != nil {
			return model.UpdateAutomationPolicyRequest{}, err
		}
		request.Priority = opts.Priority
	}
	if cmd.Flags().Changed("source-ref") {
		request.SourceRef = strings.TrimSpace(opts.SourceRef)
	}
	if opts.ClearMetadata {
		request.Metadata = nil
	} else if cmd.Flags().Changed("metadata") {
		request.Metadata, err = parseAutomationMetadata(opts.Metadata)
		if err != nil {
			return model.UpdateAutomationPolicyRequest{}, err
		}
	}

	if automationUpdateHasRuleChanges(cmd) {
		index, err := selectAutomationRule(rules, opts.RuleID)
		if err != nil {
			return model.UpdateAutomationPolicyRequest{}, err
		}
		rule := &request.Rules[index]
		if rule.Trigger.RequestMetric == nil {
			return model.UpdateAutomationPolicyRequest{}, fmt.Errorf("rule %q has no request metric selector", rule.ID)
		}
		if cmd.Flags().Changed("rule-description") {
			rule.Description = strings.TrimSpace(opts.RuleDescription)
		}
		if cmd.Flags().Changed("window") {
			rule.Trigger.RequestMetric.Window, err = normalizeAutomationMetricWindow(opts.Window)
			if err != nil {
				return model.UpdateAutomationPolicyRequest{}, err
			}
		}
		if cmd.Flags().Changed("status-code") {
			rule.Trigger.RequestMetric.StatusCodes, err = normalizeAutomationStatusCodes(opts.StatusCodes)
			if err != nil {
				return model.UpdateAutomationPolicyRequest{}, err
			}
		}
		if cmd.Flags().Changed("minimum-samples") {
			if err := validateAutomationEvidenceLimits(opts.MinimumSamples, rule.Trigger.MinimumFailureDomains); err != nil {
				return model.UpdateAutomationPolicyRequest{}, err
			}
			rule.Trigger.MinimumSamples = opts.MinimumSamples
		}
		if cmd.Flags().Changed("minimum-failure-domains") {
			if err := validateAutomationEvidenceLimits(rule.Trigger.MinimumSamples, opts.MinimumFailureDomains); err != nil {
				return model.UpdateAutomationPolicyRequest{}, err
			}
			rule.Trigger.MinimumFailureDomains = opts.MinimumFailureDomains
		}
		if opts.ClearRequiredEvidence {
			rule.Trigger.RequiredEvidence = nil
		} else if cmd.Flags().Changed("require-evidence") {
			rule.Trigger.RequiredEvidence, err = normalizeAutomationEvidence(opts.RequiredEvidence)
			if err != nil {
				return model.UpdateAutomationPolicyRequest{}, err
			}
		}
		if opts.ClearReason {
			delete(rule.Action.Parameters, "reason")
			if len(rule.Action.Parameters) == 0 {
				rule.Action.Parameters = nil
			}
		} else if cmd.Flags().Changed("reason") {
			rule.Action.Parameters, err = automationReasonParameters(opts.Reason)
			if err != nil {
				return model.UpdateAutomationPolicyRequest{}, err
			}
		}
	}
	return request, nil
}

func automationRuleInputsFromPolicy(policy model.AutomationPolicy) ([]model.AutomationRuleInput, error) {
	if policy.OwnerType != model.AutomationOwnerUser ||
		policy.Managed ||
		policy.Kind != model.AutomationPolicyKindAppRecovery ||
		policy.Scope.Type != model.AutomationScopeApp {
		return nil, fmt.Errorf("policy %q is not a mutable user app-recovery policy", policy.ID)
	}
	if len(policy.Rules) == 0 {
		return nil, fmt.Errorf("policy %q has no rules", policy.ID)
	}
	out := make([]model.AutomationRuleInput, 0, len(policy.Rules))
	for _, rule := range policy.Rules {
		if rule.Trigger.Type != model.AutomationTriggerRequestMetric ||
			rule.Trigger.Source != automationRequestOutcomeSource ||
			rule.Trigger.RequestMetric == nil ||
			rule.Trigger.RequestMetric.Metric != automationHTTPStatusMetric ||
			len(rule.Trigger.RequestMetric.ErrorClasses) != 0 ||
			rule.Action.Type != automationAppRestartActionType {
			return nil, fmt.Errorf("policy %q rule %q is outside the supported app-recovery contract", policy.ID, rule.ID)
		}
		selector := *rule.Trigger.RequestMetric
		selector.StatusCodes = append([]int(nil), selector.StatusCodes...)
		selector.ErrorClasses = append([]string(nil), selector.ErrorClasses...)
		out = append(out, model.AutomationRuleInput{
			ID:          rule.ID,
			Description: rule.Description,
			Trigger: model.AutomationTriggerInput{
				Type:                  rule.Trigger.Type,
				Source:                rule.Trigger.Source,
				RequestMetric:         &selector,
				RequiredEvidence:      append([]string(nil), rule.Trigger.RequiredEvidence...),
				MinimumSamples:        rule.Trigger.MinimumSamples,
				MinimumFailureDomains: rule.Trigger.MinimumFailureDomains,
			},
			Action: model.AutomationActionInput{
				Type:       rule.Action.Type,
				Parameters: cloneStringMap(rule.Action.Parameters),
			},
		})
	}
	return out, nil
}

func selectAutomationRule(rules []model.AutomationRuleInput, ruleID string) (int, error) {
	ruleID = strings.TrimSpace(ruleID)
	if ruleID == "" {
		if len(rules) == 1 {
			return 0, nil
		}
		return -1, fmt.Errorf("--rule-id is required when changing a multi-rule policy")
	}
	for index, rule := range rules {
		if rule.ID == ruleID {
			return index, nil
		}
	}
	return -1, fmt.Errorf("automation rule %q not found", ruleID)
}

func automationUpdateHasChanges(cmd *cobra.Command) bool {
	for _, name := range []string{
		"name",
		"description",
		"mode",
		"priority",
		"source-ref",
		"rule-description",
		"window",
		"status-code",
		"minimum-samples",
		"minimum-failure-domains",
		"require-evidence",
		"clear-required-evidence",
		"reason",
		"clear-reason",
		"metadata",
		"clear-metadata",
	} {
		if cmd.Flags().Changed(name) {
			return true
		}
	}
	return false
}

func automationUpdateHasRuleChanges(cmd *cobra.Command) bool {
	for _, name := range []string{
		"rule-description",
		"window",
		"status-code",
		"minimum-samples",
		"minimum-failure-domains",
		"require-evidence",
		"clear-required-evidence",
		"reason",
		"clear-reason",
	} {
		if cmd.Flags().Changed(name) {
			return true
		}
	}
	return false
}

func normalizeUserAutomationMode(raw string) (string, error) {
	mode := strings.TrimSpace(strings.ToLower(raw))
	switch mode {
	case model.GatePolicyModeDisabled, model.GatePolicyModeShadow:
		return mode, nil
	default:
		return "", fmt.Errorf("automation mode must be disabled or shadow")
	}
}

func normalizeAutomationMetricWindow(raw string) (string, error) {
	value := strings.TrimSpace(raw)
	window, err := time.ParseDuration(value)
	if err != nil || window < time.Second || window > 15*time.Minute {
		return "", fmt.Errorf("automation metric window must be between 1s and 15m")
	}
	return value, nil
}

func normalizeAutomationStatusCodes(values []int) ([]int, error) {
	if len(values) == 0 {
		return nil, fmt.Errorf("at least one server-error status code is required")
	}
	if len(values) > 100 {
		return nil, fmt.Errorf("at most 100 status codes are allowed")
	}
	out := append([]int(nil), values...)
	sort.Ints(out)
	writeIndex := 0
	for _, value := range out {
		if value < 500 || value > 599 {
			return nil, fmt.Errorf("automation status codes must be between 500 and 599")
		}
		if writeIndex > 0 && out[writeIndex-1] == value {
			continue
		}
		out[writeIndex] = value
		writeIndex++
	}
	return out[:writeIndex], nil
}

func validateAutomationEvidenceLimits(minimumSamples, minimumFailureDomains int) error {
	if minimumSamples < 0 || minimumSamples > 10_000 {
		return fmt.Errorf("minimum samples must be between 0 and 10000")
	}
	if minimumFailureDomains < 0 || minimumFailureDomains > 1_000 {
		return fmt.Errorf("minimum failure domains must be between 0 and 1000")
	}
	return nil
}

func validateAutomationPriority(priority int) error {
	if priority < 0 || int64(priority) > 2_147_483_647 {
		return fmt.Errorf("priority must be between 0 and 2147483647")
	}
	return nil
}

func normalizeAutomationEvidence(values []string) ([]string, error) {
	if len(values) == 0 {
		return nil, nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, raw := range values {
		value := strings.TrimSpace(raw)
		if value == "" {
			return nil, fmt.Errorf("required evidence identifier cannot be empty")
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out, nil
}

func automationReasonParameters(raw string) (map[string]string, error) {
	reason := strings.TrimSpace(raw)
	if reason == "" {
		return nil, nil
	}
	if len(reason) > 500 {
		return nil, fmt.Errorf("automation reason must not exceed 500 bytes")
	}
	return map[string]string{"reason": reason}, nil
}

func parseAutomationMetadata(values []string) (map[string]string, error) {
	if len(values) == 0 {
		return nil, nil
	}
	out := make(map[string]string, len(values))
	for _, raw := range values {
		key, value, ok := strings.Cut(raw, "=")
		if !ok {
			return nil, fmt.Errorf("automation metadata %q must use KEY=VALUE", raw)
		}
		key = strings.TrimSpace(key)
		if key == "" {
			return nil, fmt.Errorf("automation metadata %q has an empty key", raw)
		}
		if _, exists := out[key]; exists {
			return nil, fmt.Errorf("automation metadata contains duplicate key %q", key)
		}
		out[key] = strings.TrimSpace(value)
	}
	return out, nil
}
