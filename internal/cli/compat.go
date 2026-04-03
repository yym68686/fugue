package cli

import "github.com/spf13/cobra"

func hideCompatCommand(cmd *cobra.Command, replacement string) *cobra.Command {
	cmd.Hidden = true
	if replacement != "" {
		cmd.Deprecated = "use \"" + replacement + "\" instead"
	}
	return cmd
}
