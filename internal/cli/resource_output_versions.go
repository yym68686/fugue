package cli

import (
	"fmt"
	"github.com/spf13/cobra"
	"strings"
)

func (c *CLI) registerVersionedResourceOutput(root *cobra.Command) {
	walkHelpCommands(root, func(cmd *cobra.Command) {
		path := cmd.CommandPath()
		schema := ""
		switch {
		case strings.HasPrefix(path, "fugue app image "):
			schema = "fugue.app-image.v1"
		case path == "fugue app release versions" || path == "fugue app release version":
			schema = "fugue.app-release.v1"
		case strings.HasPrefix(path, "fugue app release attempt "):
			schema = "fugue.release-attempt.v1"
		}
		if schema == "" || cmd.RunE == nil {
			return
		}
		var version string
		run := cmd.RunE
		cmd.Flags().StringVar(&version, "output-version", "legacy", "JSON contract: legacy preserves the original object; v1 adds a typed data envelope")
		cmd.RunE = func(cmd *cobra.Command, args []string) error {
			if version != "legacy" && version != "v1" {
				return fmt.Errorf("output-version must be legacy or v1")
			}
			if version == "v1" {
				if !c.wantsJSON() {
					return fmt.Errorf("output-version v1 requires --json")
				}
				previous := c.jsonSchema
				c.jsonSchema = schema
				defer func() { c.jsonSchema = previous }()
			}
			return run(cmd, args)
		}
	})
}
