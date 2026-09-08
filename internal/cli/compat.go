package cli

import "github.com/spf13/cobra"

const cliCompatibilityRemovalVersion = "v0.2.0"

func hideCompatCommand(cmd *cobra.Command, replacement string) *cobra.Command {
	if cmd == nil {
		return nil
	}
	markCompatibilityTree(cmd, replacement)
	return cmd
}
func markCompatibilityTree(cmd *cobra.Command, replacement string) {
	cmd.Hidden = true
	if cmd.Annotations == nil {
		cmd.Annotations = map[string]string{}
	}
	cmd.Annotations["fugue.replacement"] = replacement
	if replacement != "" {
		cmd.Annotations["fugue.deprecation"] = "use \"" + replacement + "\" instead; compatibility removal is scheduled for " + cliCompatibilityRemovalVersion
	}
	for _, child := range cmd.Commands() {
		markCompatibilityTree(child, replacement+" "+child.Name())
	}
}
func hideCompatCommandTree(cmd *cobra.Command) {
	if cmd != nil {
		markCompatibilityTree(cmd, "")
	}
}

// Override conceptual renames whose leaf names are not a suffix-preserving map.
func finalizeCompatibility(root *cobra.Command) {
	replacements := map[string]string{
		"fugue template":                "fugue deploy inspect",
		"fugue template inspect":        "fugue deploy inspect",
		"fugue template inspect github": "fugue deploy inspect github",
		"fugue app sync":                "fugue app source sync",
		"fugue app sync status":         "fugue app source sync status",
		"fugue app sync run":            "fugue app source sync run",
		"fugue app sync resume":         "fugue app source sync resume",
		"fugue app route show":          "fugue app domain primary verify",
		"fugue app continuity":          "fugue app failover",
		"fugue app continuity audit":    "fugue app failover status",
		"fugue app continuity show":     "fugue app rollout policy show",
		"fugue app continuity enable":   "fugue app failover policy set",
		"fugue app continuity disable":  "fugue app failover policy clear",
	}
	walkHelpCommands(root, func(cmd *cobra.Command) {
		if next, ok := replacements[cmd.CommandPath()]; ok {
			cmd.Annotations["fugue.replacement"] = next
			cmd.Annotations["fugue.deprecation"] = "use \"" + next + "\" instead; compatibility removal is scheduled for " + cliCompatibilityRemovalVersion
		}
	})
}
