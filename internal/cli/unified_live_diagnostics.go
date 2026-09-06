package cli

import (
	"strings"

	"fugue/internal/livediagnostics"

	"github.com/spf13/cobra"
)

// newUnifiedDiagnosticsCommand is the single user-facing entry point for the
// two deliberately separate authorization surfaces. The subcommands dispatch
// to the existing app-scoped and platform-scoped APIs; they never broaden a
// target's namespace or permission boundary.
func (c *CLI) newUnifiedDiagnosticsCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "diagnostics",
		Short: "Run bounded diagnostics against apps or Fugue platform targets",
		Long: strings.TrimSpace(`
Run a bounded live diagnostic session using the target's existing safety and
authorization boundary.

Use "diagnostics app" for a tenant application, "diagnostics platform" for a
Fugue control-plane component, or "diagnostics node-process" for an explicitly
allowlisted host process. The command keeps those target classes separate so
an app diagnostic cannot become an arbitrary host or namespace probe.
`),
		Example: strings.TrimSpace(`
fugue diagnostics app start review00 --kind cpu-profile --wait
fugue diagnostics platform start --component api --kind memory-profile --wait
fugue diagnostics node-process start --node ns101351 --process k3s-agent --kind process-snapshot --wait
`),
	}

	app := &cobra.Command{
		Use:     "app",
		Short:   "Diagnose a tenant application",
		Long:    "Run app-scoped diagnostics against a ready Pod selected from the named app.",
		Example: "fugue diagnostics app start review00 --kind cpu-profile --wait",
	}
	app.AddCommand(
		c.newAppDiagnosticsStartCommand(),
		c.newAppDiagnosticsListCommand(),
		c.newAppDiagnosticsShowCommand(),
		c.newAppDiagnosticsReportCommand(),
		c.newAppDiagnosticsCancelCommand(),
	)

	platformOpts := platformDiagnosticCommandOptions{controlNS: "fugue-system", releaseInstance: "fugue"}
	platform := &cobra.Command{
		Use:     "platform",
		Short:   "Diagnose a Fugue platform component",
		Long:    "Run platform-scoped diagnostics against Fugue or kube-system components.",
		Example: "fugue diagnostics platform start --component api --kind memory-profile --wait",
	}
	platform.AddCommand(
		c.newAdminDiagnosticsStartCommandForTarget(&platformOpts, livediagnostics.TargetPlatformComponent),
		c.newAdminDiagnosticsListCommand(&platformOpts),
		c.newAdminDiagnosticsShowCommand(&platformOpts),
		c.newAdminDiagnosticsReportCommand(&platformOpts),
		c.newAdminDiagnosticsCancelCommand(&platformOpts),
	)
	addPlatformDiagnosticFlags(platform, &platformOpts)

	nodeOpts := platformDiagnosticCommandOptions{controlNS: "fugue-system", releaseInstance: "fugue"}
	node := &cobra.Command{
		Use:     "node-process",
		Short:   "Diagnose an allowlisted host process",
		Long:    "Run platform diagnostics against an explicitly allowlisted host process.",
		Example: "fugue diagnostics node-process start --node ns101351 --process k3s-agent --kind process-snapshot --wait",
	}
	node.AddCommand(
		c.newAdminDiagnosticsStartCommandForTarget(&nodeOpts, livediagnostics.TargetNodeProcess),
		c.newAdminDiagnosticsListCommand(&nodeOpts),
		c.newAdminDiagnosticsShowCommand(&nodeOpts),
		c.newAdminDiagnosticsReportCommand(&nodeOpts),
		c.newAdminDiagnosticsCancelCommand(&nodeOpts),
	)
	addPlatformDiagnosticFlags(node, &nodeOpts)

	cmd.AddCommand(app, platform, node)
	return cmd
}
