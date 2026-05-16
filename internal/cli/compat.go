package cli

import "github.com/spf13/cobra"

func hideCompatCommand(cmd *cobra.Command, replacement string) *cobra.Command {
	if cmd == nil {
		return nil
	}
	cmd.Hidden = true
	if replacement != "" {
		cmd.Deprecated = "use \"" + replacement + "\" instead"
	}
	for _, child := range cmd.Commands() {
		hideCompatCommandTree(child)
	}
	return cmd
}

func hideCompatCommandTree(cmd *cobra.Command) {
	if cmd == nil {
		return
	}
	cmd.Hidden = true
	for _, child := range cmd.Commands() {
		hideCompatCommandTree(child)
	}
}
