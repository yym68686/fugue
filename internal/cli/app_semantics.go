package cli

import (
	"fmt"
	"github.com/spf13/cobra"
	"strings"
)

func renamedCommand(cmd *cobra.Command, use string) *cobra.Command {
	cmd.Use = use
	cmd.Hidden = false
	cmd.Deprecated = ""
	cmd.Aliases = nil
	if cmd.Annotations != nil {
		delete(cmd.Annotations, "fugue.deprecation")
		delete(cmd.Annotations, "fugue.replacement")
	}
	return cmd
}
func (c *CLI) newAppImageCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "image", Short: "Inspect app image inventory, retention and external image tracking"}
	cmd.AddCommand(c.newAppReleaseListCommand(), renamedCommand(c.newAppReleasePolicyCommand(), "retention"), c.newAppReleaseTrackingCommand(), c.newAppReleasePruneCommand())
	return cmd
}
func (c *CLI) newAppTrafficCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "traffic", Short: "Inspect or update app traffic intent"}
	var observed bool
	show := &cobra.Command{Use: "show <app>", Short: "Show stable/candidate traffic intent without changing it", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		client, err := c.newClient()
		if err != nil {
			return err
		}
		app, err := c.resolveNamedApp(client, args[0])
		if err != nil {
			return err
		}
		response, err := client.GetAppTrafficPolicy(app.ID)
		if err != nil {
			return err
		}
		if observed {
			samples := c.observeAppRoute(client, app)
			return c.renderResourceResult(map[string]any{"schema_version": 1, "traffic": response.Traffic, "releases": response.Releases, "evidence_kind": "control_plane_intent", "observed_state": samples.State, "observations": samples, "measured_global_split": "unknown"})
		}
		if c.wantsJSON() {
			return c.writeJSON(map[string]any{"schema_version": 1, "traffic": response.Traffic, "releases": response.Releases, "evidence_kind": "control_plane_intent", "observed_state": "unknown"})
		}
		if err := writeTrafficPolicySummary(c.stdout, response.Traffic); err != nil {
			return err
		}
		return writeKeyValues(c.stdout, kvPair{Key: "evidence_kind", Value: "control_plane_intent"}, kvPair{Key: "observed_state", Value: "unknown (inspect route/consumer evidence separately)"})
	}}
	show.Flags().BoolVar(&observed, "observed", false, "Include recent edge route decision samples; does not infer a global traffic split")
	cmd.AddCommand(show, renamedCommand(c.newAppReleaseTrafficCommand(), "set <app>"))
	return cmd
}
func (c *CLI) newReleaseVersionsCommand() *cobra.Command {
	return &cobra.Command{Use: "versions <app>", Short: "List serving deployment versions and their traffic policy", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		client, err := c.newClient()
		if err != nil {
			return err
		}
		app, err := c.resolveNamedApp(client, args[0])
		if err != nil {
			return err
		}
		response, err := client.ListAppReleases(app.ID)
		if err != nil {
			return err
		}
		if c.wantsJSON() {
			return c.writeJSON(response)
		}
		for _, r := range response.Releases {
			fmt.Fprintf(c.stdout, "%s\t%s\t%s\t%s\n", r.ID, r.Role, r.Status, r.ResolvedImageRef)
		}
		return nil
	}}
}
func (c *CLI) newReleaseVersionCommand() *cobra.Command {
	return &cobra.Command{Use: "version <app> <release-id>", Short: "Show one serving deployment version", Args: cobra.ExactArgs(2), RunE: func(cmd *cobra.Command, args []string) error {
		client, err := c.newClient()
		if err != nil {
			return err
		}
		app, err := c.resolveNamedApp(client, args[0])
		if err != nil {
			return err
		}
		response, err := client.ListAppReleases(app.ID)
		if err != nil {
			return err
		}
		for _, r := range response.Releases {
			if r.ID == args[1] {
				if c.wantsJSON() {
					return c.writeJSON(map[string]any{"release": r})
				}
				return writeKeyValues(c.stdout, kvPair{Key: "release_id", Value: r.ID}, kvPair{Key: "role", Value: r.Role}, kvPair{Key: "status", Value: r.Status}, kvPair{Key: "image", Value: r.ResolvedImageRef})
			}
		}
		return withExitCode(fmt.Errorf("release %q not found for app %q", args[1], app.Name), ExitCodeNotFound)
	}}
}
func (c *CLI) newReleaseAttemptCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "attempt", Short: "Inspect release execution attempts, timelines and evidence"}
	cmd.AddCommand(renamedCommand(c.newAppReleaseAttemptsCommand(), "ls <app>"), c.newAppReleaseStatusCommand(), c.newAppReleaseExplainCommand(), renamedCommand(c.newAppReleaseDebugBundleCommand(), "bundle <app>"))
	return cmd
}
func (c *CLI) newRolloutPolicyCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "policy", Short: "Inspect or configure zero-downtime rollout policy"}
	show := renamedCommand(c.newAppContinuityShowCommand(), "show <app>")
	show.Short = "Show rollout and continuity policy without changing it"
	set := renamedCommand(c.newAppContinuitySetCommand(), "set <app>")
	set.Short = "Configure zero-downtime rollout policy"
	set.Example = "fugue app rollout policy set my-app --zero-downtime safe"
	_ = set.MarkFlagRequired("zero-downtime")
	original := set.RunE
	set.RunE = func(cmd *cobra.Command, args []string) error {
		for _, name := range []string{"app-to", "db-to", "app-runtime-id", "db-runtime-id", "rebalance-now"} {
			if cmd.Flags().Changed(name) {
				return fmt.Errorf("--%s belongs to app failover policy set", name)
			}
		}
		if value, _ := cmd.Flags().GetString("zero-downtime"); strings.TrimSpace(value) == "" {
			return fmt.Errorf("--zero-downtime is required")
		}
		return original(cmd, args)
	}
	for _, name := range []string{"app-to", "db-to", "app-runtime-id", "db-runtime-id", "rebalance-now"} {
		_ = set.Flags().MarkHidden(name)
	}
	clear := renamedCommand(c.newAppContinuityOffCommand(), "clear <app>")
	clear.Short = "Disable only the zero-downtime rollout policy"
	off := clear.RunE
	clear.RunE = func(cmd *cobra.Command, args []string) error {
		for _, name := range []string{"app", "db", "rebalance-now"} {
			if cmd.Flags().Changed(name) {
				return fmt.Errorf("--%s belongs to app failover policy clear", name)
			}
		}
		if err := cmd.Flags().Set("zero-downtime", "true"); err != nil {
			return err
		}
		return off(cmd, args)
	}
	for _, name := range []string{"app", "db", "rebalance-now", "zero-downtime"} {
		_ = clear.Flags().MarkHidden(name)
	}
	cmd.AddCommand(show, set, clear)
	return cmd
}
