package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

func (c *CLI) newAdminReleaseGuardSignalsCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "signals",
		Aliases: []string{"signal"},
		Short:   "Manage explicit release guard workload signals",
	}
	cmd.AddCommand(
		c.newAdminReleaseGuardSignalsListCommand(),
		c.newAdminReleaseGuardSignalsAddCommand(),
		c.newAdminReleaseGuardSignalsRemoveCommand(),
	)
	return cmd
}

func (c *CLI) newAdminReleaseGuardSignalsListCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List active release guard signals",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			status, err := client.GetReleaseGuardStatus("")
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"signals": status.ReleaseSignals, "status": status})
			}
			return writeReleaseSignalTable(c.stdout, status.ReleaseSignals)
		},
	}
}

func (c *CLI) newAdminReleaseGuardSignalsAddCommand() *cobra.Command {
	opts := struct {
		Name       string
		Subject    string
		CheckName  string
		OwnerScope string
		GateScope  string
		Mode       string
		Reason     string
		Disabled   bool
	}{
		OwnerScope: model.ReleaseSignalOwnerScopeTenantWorkload,
		GateScope:  model.ReleaseSignalGateScopeControlPlane,
		Mode:       model.ReleaseSignalModeHardGate,
	}
	cmd := &cobra.Command{
		Use:   "add <signal-id>",
		Short: "Add or replace a release guard signal",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(opts.Subject) == "" {
				return fmt.Errorf("--subject is required, for example app:uni-api-web")
			}
			if strings.TrimSpace(opts.Reason) == "" {
				return fmt.Errorf("--reason is required for release guard signal changes")
			}
			signal := model.ReleaseSignal{
				ID:         strings.TrimSpace(args[0]),
				Name:       strings.TrimSpace(opts.Name),
				Enabled:    !opts.Disabled,
				OwnerScope: model.NormalizeReleaseSignalOwnerScope(opts.OwnerScope),
				GateScope:  model.NormalizeReleaseSignalGateScope(opts.GateScope),
				Mode:       model.NormalizeReleaseSignalMode(opts.Mode),
				Subject:    strings.TrimSpace(opts.Subject),
				CheckName:  strings.TrimSpace(opts.CheckName),
				Reason:     strings.TrimSpace(opts.Reason),
				CreatedAt:  time.Now().UTC().Format(time.RFC3339),
				UpdatedAt:  time.Now().UTC().Format(time.RFC3339),
			}
			if signal.ID == "" {
				return fmt.Errorf("signal id is required")
			}
			if signal.OwnerScope == "" || signal.GateScope == "" || signal.Mode == "" {
				return fmt.Errorf("owner scope, gate scope, and mode must be valid")
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			policy, err := loadReleaseSignalPolicy(client)
			if err != nil {
				return err
			}
			policy = upsertReleaseSignal(policy, signal)
			artifact, err := publishReleaseSignalPolicy(client, policy, opts.Reason)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"artifact": artifact, "policy": policy, "signal": signal})
			}
			return writeReleaseSignalPublish(c.stdout, artifact, policy)
		},
	}
	cmd.Flags().StringVar(&opts.Name, "name", "", "Human-readable signal name")
	cmd.Flags().StringVar(&opts.Subject, "subject", "", "Robustness subject to match, for example app:my-app")
	cmd.Flags().StringVar(&opts.CheckName, "check", "app_continuity_invariant", "Robustness check name to match")
	cmd.Flags().StringVar(&opts.OwnerScope, "owner-scope", opts.OwnerScope, "Owner scope: platform, first_party_service, or tenant_workload")
	cmd.Flags().StringVar(&opts.GateScope, "gate-scope", opts.GateScope, "Gate scope: control_plane, edge_rollout, runtime_rollout, tenant_traffic, or report_only")
	cmd.Flags().StringVar(&opts.Mode, "mode", opts.Mode, "Signal mode: report_only, soft_gate, canary_gate, rollback_gate, or hard_gate")
	cmd.Flags().StringVar(&opts.Reason, "reason", "", "Audit reason for publishing the release guard policy")
	cmd.Flags().BoolVar(&opts.Disabled, "disabled", false, "Create the signal disabled")
	return cmd
}

func (c *CLI) newAdminReleaseGuardSignalsRemoveCommand() *cobra.Command {
	opts := struct{ Reason string }{}
	cmd := &cobra.Command{
		Use:     "rm <signal-id>",
		Aliases: []string{"remove", "delete"},
		Short:   "Remove a release guard signal",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(opts.Reason) == "" {
				return fmt.Errorf("--reason is required for release guard signal changes")
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			policy, err := loadReleaseSignalPolicy(client)
			if err != nil {
				return err
			}
			var removed model.ReleaseSignal
			policy, removed, err = removeReleaseSignal(policy, args[0])
			if err != nil {
				return err
			}
			artifact, err := publishReleaseSignalPolicy(client, policy, opts.Reason)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"artifact": artifact, "policy": policy, "removed": removed})
			}
			return writeReleaseSignalPublish(c.stdout, artifact, policy)
		},
	}
	cmd.Flags().StringVar(&opts.Reason, "reason", "", "Audit reason for publishing the release guard policy")
	return cmd
}

func loadReleaseSignalPolicy(client *Client) (model.ReleaseSignalPolicy, error) {
	response, err := client.GetPlatformStateArtifact(model.PlatformArtifactKindReleaseGuardPolicy, "global", model.PlatformArtifactReleaseChannelFull)
	if err != nil {
		return model.ReleaseSignalPolicy{}, err
	}
	if response.Artifact == nil {
		return model.ReleaseSignalPolicy{Version: "v1"}, nil
	}
	return releaseSignalPolicyFromArtifact(*response.Artifact)
}

func releaseSignalPolicyFromArtifact(artifact model.PlatformArtifact) (model.ReleaseSignalPolicy, error) {
	policy := model.ReleaseSignalPolicy{Version: "v1"}
	if len(artifact.Content) == 0 {
		return policy, nil
	}
	raw, err := json.Marshal(artifact.Content)
	if err != nil {
		return policy, err
	}
	if err := json.Unmarshal(raw, &policy); err != nil {
		return policy, err
	}
	if strings.TrimSpace(policy.Version) == "" {
		policy.Version = "v1"
	}
	return policy, nil
}

func upsertReleaseSignal(policy model.ReleaseSignalPolicy, signal model.ReleaseSignal) model.ReleaseSignalPolicy {
	if strings.TrimSpace(policy.Version) == "" {
		policy.Version = "v1"
	}
	for index := range policy.Signals {
		if strings.EqualFold(policy.Signals[index].ID, signal.ID) {
			if strings.TrimSpace(signal.CreatedAt) == "" {
				signal.CreatedAt = policy.Signals[index].CreatedAt
			}
			policy.Signals[index] = signal
			return policy
		}
	}
	policy.Signals = append(policy.Signals, signal)
	return policy
}

func removeReleaseSignal(policy model.ReleaseSignalPolicy, id string) (model.ReleaseSignalPolicy, model.ReleaseSignal, error) {
	id = strings.TrimSpace(id)
	for index, signal := range policy.Signals {
		if strings.EqualFold(signal.ID, id) {
			policy.Signals = append(policy.Signals[:index], policy.Signals[index+1:]...)
			return policy, signal, nil
		}
	}
	return policy, model.ReleaseSignal{}, fmt.Errorf("release signal %q not found", id)
}

func publishReleaseSignalPolicy(client *Client, policy model.ReleaseSignalPolicy, reason string) (model.PlatformArtifact, error) {
	content, err := releaseSignalPolicyContent(policy)
	if err != nil {
		return model.PlatformArtifact{}, err
	}
	generation := "release_signals_" + time.Now().UTC().Format("20060102T150405.000000000Z")
	artifact, err := client.CreatePlatformArtifact(model.PlatformArtifactCreateRequest{
		ArtifactKind: model.PlatformArtifactKindReleaseGuardPolicy,
		Scope:        model.PlatformArtifactScope{ScopeType: "global"},
		Generation:   generation,
		Content:      content,
		Metadata: map[string]string{
			"reason": strings.TrimSpace(reason),
		},
	})
	if err != nil {
		return model.PlatformArtifact{}, err
	}
	validation, err := client.ValidatePlatformArtifact(artifact.ID, false)
	if err != nil {
		return model.PlatformArtifact{}, err
	}
	if !validation.Pass {
		return model.PlatformArtifact{}, fmt.Errorf("release guard policy validation failed")
	}
	release, err := client.ReleasePlatformArtifact(artifact.ID, model.PlatformArtifactReleaseRequest{
		ReleaseChannel: model.PlatformArtifactReleaseChannelFull,
		Reason:         strings.TrimSpace(reason),
	})
	if err != nil {
		return model.PlatformArtifact{}, err
	}
	return release.Artifact, nil
}

func releaseSignalPolicyContent(policy model.ReleaseSignalPolicy) (map[string]any, error) {
	raw, err := json.Marshal(policy)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func writeReleaseSignalTable(w io.Writer, signals []model.ReleaseSignal) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "ID\tENABLED\tOWNER\tGATE\tMODE\tSUBJECT\tCHECK\tREASON"); err != nil {
		return err
	}
	for _, signal := range signals {
		if _, err := fmt.Fprintf(tw, "%s\t%t\t%s\t%s\t%s\t%s\t%s\t%s\n",
			signal.ID,
			signal.Enabled,
			firstNonEmpty(signal.OwnerScope, "-"),
			firstNonEmpty(signal.GateScope, "-"),
			firstNonEmpty(signal.Mode, "-"),
			firstNonEmpty(signal.Subject, "-"),
			firstNonEmpty(signal.CheckName, "-"),
			firstNonEmpty(signal.Reason, "-"),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeReleaseSignalPublish(w io.Writer, artifact model.PlatformArtifact, policy model.ReleaseSignalPolicy) error {
	if err := writeKeyValues(w,
		kvPair{Key: "artifact", Value: artifact.ID},
		kvPair{Key: "generation", Value: artifact.Generation},
		kvPair{Key: "status", Value: artifact.Status},
		kvPair{Key: "signals", Value: fmt.Sprintf("%d", len(policy.Signals))},
	); err != nil {
		return err
	}
	if len(policy.Signals) == 0 {
		return nil
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	return writeReleaseSignalTable(w, policy.Signals)
}
