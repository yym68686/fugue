package cli

import (
	"fmt"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

type commandFlagInfo struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	Default    string `json:"default"`
	Usage      string `json:"usage"`
	Required   bool   `json:"required"`
	Hidden     bool   `json:"hidden"`
	Deprecated string `json:"deprecated,omitempty"`
}
type commandInfo struct {
	Path           string            `json:"path"`
	Use            string            `json:"use"`
	Effect         string            `json:"effect"`
	Scope          string            `json:"scope"`
	OutputContract string            `json:"output_contract"`
	Details        string            `json:"details"`
	Summary        string            `json:"summary"`
	Aliases        []string          `json:"aliases,omitempty"`
	Runnable       bool              `json:"runnable"`
	Hidden         bool              `json:"hidden"`
	Deprecated     string            `json:"deprecated,omitempty"`
	Replacement    string            `json:"replacement,omitempty"`
	Flags          []commandFlagInfo `json:"flags"`
	Example        string            `json:"example,omitempty"`
}

func commandCatalog(root *cobra.Command, includeHidden bool) []commandInfo {
	entries := []commandInfo{}
	walkHelpCommands(root, func(cmd *cobra.Command) {
		hidden := cmd.Hidden || hasHiddenParent(cmd)
		if hidden && !includeHidden {
			return
		}
		entry := commandInfo{Path: cmd.CommandPath(), Use: cmd.Use, Effect: commandEffect(cmd), Scope: "server_authorized_target", OutputContract: "existing-command-json; errors=cli-error-v1", Details: cmd.Long, Summary: cmd.Short, Aliases: cmd.Aliases, Runnable: cmd.Runnable(), Hidden: hidden, Deprecated: firstNonEmpty(cmd.Deprecated, cmd.Annotations["fugue.deprecation"]), Replacement: cmd.Annotations["fugue.replacement"], Example: cmd.Example, Flags: []commandFlagInfo{}}
		flags := pflag.NewFlagSet(cmd.Name(), pflag.ContinueOnError)
		flags.AddFlagSet(cmd.InheritedFlags())
		flags.AddFlagSet(cmd.Flags())
		flags.VisitAll(func(f *pflag.Flag) {
			if f.Hidden && !includeHidden {
				return
			}
			required := len(f.Annotations[cobra.BashCompOneRequiredFlag]) > 0
			entry.Flags = append(entry.Flags, commandFlagInfo{f.Name, f.Value.Type(), f.DefValue, f.Usage, required, f.Hidden, f.Deprecated})
		})
		entries = append(entries, entry)
	})
	sort.Slice(entries, func(i, j int) bool { return entries[i].Path < entries[j].Path })
	return entries
}
func (c *CLI) newCatalogHelpCommand(root *cobra.Command) *cobra.Command {
	var all bool
	var format string
	cmd := &cobra.Command{Use: "help [command...]", Short: "Show command help or the machine-readable command catalog", RunE: func(cmd *cobra.Command, args []string) error {
		target := root
		if len(args) > 0 {
			if args[0] == "search" {
				query := strings.ToLower(strings.Join(args[1:], " "))
				matches := []commandInfo{}
				for _, entry := range commandCatalog(root, all) {
					if strings.Contains(strings.ToLower(entry.Path+" "+entry.Summary), query) {
						matches = append(matches, entry)
					}
				}
				if c.wantsJSON() {
					return c.writeJSON(map[string]any{"schema_version": 1, "commands": matches})
				}
				for _, entry := range matches {
					fmt.Fprintf(c.stdout, "%s\t%s\n", entry.Path, entry.Summary)
				}
				return nil
			}
			found, remaining, err := root.Find(args)
			if err != nil {
				return err
			}
			if len(remaining) > 0 {
				return fmt.Errorf("unknown command %q", strings.Join(remaining, " "))
			}
			target = found
		}
		if format != "text" && format != "markdown" {
			return fmt.Errorf("unsupported help format %q", format)
		}
		if format == "markdown" {
			fmt.Fprintln(c.stdout, "# Fugue CLI command catalog")
			for _, entry := range commandCatalog(target, all) {
				fmt.Fprintf(c.stdout, "\n## %s\n\n%s\n\nUsage: `%s`\n\nEffect: %s\n\n", entry.Path, entry.Summary, entry.Use, entry.Effect)
				if entry.Deprecated != "" {
					fmt.Fprintln(c.stdout, entry.Deprecated)
				}
				fmt.Fprintln(c.stdout, "| Flag | Type | Default | Description |\n| --- | --- | --- | --- |")
				for _, flag := range entry.Flags {
					fmt.Fprintf(c.stdout, "| --%s | %s | %s | %s |\n", flag.Name, flag.Type, strings.ReplaceAll(flag.Default, "|", "\\|"), strings.ReplaceAll(flag.Usage, "|", "\\|"))
				}
			}
			return nil
		}
		if c.wantsJSON() {
			return c.writeJSON(map[string]any{"schema_version": 1, "commands": commandCatalog(target, all)})
		}
		return target.Help()
	}}
	cmd.Flags().StringVar(&format, "format", "text", "Help format: text or markdown")
	cmd.Flags().BoolVar(&all, "all", false, "Include deprecated and hidden compatibility commands")
	return cmd
}

// Effects describe the maximum action category, not authorization. Unknown
// handlers are conservatively marked as potentially mutating. The server is
// always authoritative for permission and state-machine eligibility.
func commandEffect(cmd *cobra.Command) string {
	if !cmd.Runnable() {
		return "group"
	}
	path := cmd.CommandPath()
	for _, prefix := range []string{"fugue api request", "fugue workflow run", "fugue app db query", "fugue admin cluster exec", "fugue web diagnose", "fugue app request"} {
		if strings.HasPrefix(path, prefix) {
			return "caller_defined"
		}
	}
	if path == "fugue admin billing cap" {
		return "may_change_state"
	}
	switch cmd.Name() {
	case "ls", "list", "show", "get", "status", "overview", "history", "versions", "version", "timeline", "evidence", "usage", "meta", "tables", "schema", "requests", "traces", "metrics", "inventory", "result", "search", "logs", "export", "download", "help":
		return "read"
	case "watch", "wait", "console", "cockpit", "top":
		return "observe"
	case "inspect", "check", "verify", "probe", "diagnose", "doctor", "explain", "gate", "plan", "prune-plan", "repair-plan":
		return "inspect_or_probe"
	default:
		return "may_change_state"
	}
}
