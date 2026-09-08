package cli

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"strings"
)

//go:embed removed_commands.json
var removedCommandCatalog []byte

type commandMigration struct {
	Path        string `json:"path"`
	Replacement string `json:"replacement"`
	Notes       string `json:"notes"`
}

var parsedCommandMigrations = func() []commandMigration {
	var items []commandMigration
	if err := json.Unmarshal(removedCommandCatalog, &items); err != nil {
		panic(err)
	}
	return items
}()

func commandMigrations() []commandMigration { return parsedCommandMigrations }

// Runs before Cobra parsing or any pre-run hooks (including credential save).
// Only command words appear in errors; option values may contain secrets.
func removedCommandError(root *cobra.Command, args []string) error {
	words := []string{}
	flags := map[string]bool{}
	walkHelpCommands(root, func(cmd *cobra.Command) {
		cmd.Flags().VisitAll(func(f *pflag.Flag) {
			flags["--"+f.Name] = f.NoOptDefVal == ""
			if f.Shorthand != "" {
				flags["-"+f.Shorthand] = f.NoOptDefVal == ""
			}
		})
		cmd.PersistentFlags().VisitAll(func(f *pflag.Flag) {
			flags["--"+f.Name] = f.NoOptDefVal == ""
			if f.Shorthand != "" {
				flags["-"+f.Shorthand] = f.NoOptDefVal == ""
			}
		})
	})
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			break
		}
		if strings.HasPrefix(arg, "-") {
			if !strings.Contains(arg, "=") && flags[arg] && i+1 < len(args) {
				i++
			}
			continue
		}
		words = append(words, arg)
	}
	joined := strings.Join(words, " ")
	for _, item := range commandMigrations() {
		if joined == item.Path || strings.HasPrefix(joined, item.Path+" ") {
			return withExitCode(fmt.Errorf("fugue %s was removed in v0.2.0; use %s. %s", item.Path, item.Replacement, item.Notes), ExitCodeUserInput)
		}
	}
	for _, arg := range args {
		flag := strings.SplitN(arg, "=", 2)[0]
		if flag == "--force-publish" && strings.HasPrefix(joined, "admin artifact ") {
			return withExitCode(fmt.Errorf("--force-publish was removed in v0.2.0; use --soft-override (subject to the existing safety kernel)"), ExitCodeUserInput)
		}
		if strings.HasPrefix(joined, "app failover policy ") && isRolloutOnlyFlag(strings.TrimPrefix(flag, "--")) {
			return withExitCode(fmt.Errorf("%s belongs to fugue app rollout policy set/clear; split combined rollout and failover changes into separate requests", flag), ExitCodeUserInput)
		}
	}
	return nil
}
func isRolloutOnlyFlag(name string) bool {
	switch name {
	case "zero-downtime", "canary", "initial-canary-weight", "min-observation-seconds", "rollback-window-seconds", "retire-grace-seconds":
		return true
	}
	return false
}
func removeCommandFlags(cmd *cobra.Command, remove func(string) bool) {
	kept := pflag.NewFlagSet(cmd.Name(), pflag.ContinueOnError)
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		if !remove(f.Name) {
			kept.AddFlag(f)
		}
	})
	cmd.ResetFlags()
	cmd.Flags().AddFlagSet(kept)
}
