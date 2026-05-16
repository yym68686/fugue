package cli

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

type envMutationOptions struct {
	File string
	Wait bool
}

type envCommandResult struct {
	AppName        string              `json:"app_name,omitempty"`
	AppID          string              `json:"app_id,omitempty"`
	Env            map[string]string   `json:"env"`
	Entries        []model.AppEnvEntry `json:"entries,omitempty"`
	Operation      *model.Operation    `json:"operation,omitempty"`
	AlreadyCurrent bool                `json:"already_current,omitempty"`
}

func (c *CLI) newEnvCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "env",
		Aliases: []string{"vars", "variables"},
		Short:   "Inspect and update app environment variables",
	}
	cmd.AddCommand(
		c.newEnvListCommand(),
		c.newEnvSetCommand(),
		c.newEnvRemoveCommand(),
		c.newEnvGeneratedCommand(),
	)
	return cmd
}

func (c *CLI) newEnvCompatCommand() *cobra.Command {
	return hideCompatCommand(c.newEnvCommand(), "fugue app env")
}

func (c *CLI) newEnvListCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "ls <app>",
		Aliases: []string{"list", "show"},
		Short:   "Show effective env vars for an app",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			response, err := client.GetAppEnv(app.ID)
			if err != nil {
				return err
			}
			return c.renderEnvCommandResult(envCommandResult{
				AppName: app.Name,
				AppID:   app.ID,
				Env:     response.Env,
				Entries: response.Entries,
			})
		},
	}
}

func (c *CLI) newEnvSetCommand() *cobra.Command {
	opts := envMutationOptions{Wait: true}
	cmd := &cobra.Command{
		Use:   "set <app> [KEY=VALUE...]",
		Short: "Set or update env vars on an app",
		Long: strings.TrimSpace(`
Set accepts inline KEY=VALUE pairs and/or an env file.

Inline values override keys loaded from --file when both are provided.
`),
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}

			fromFile := map[string]string{}
			if strings.TrimSpace(opts.File) != "" {
				fromFile, err = readEnvFile(strings.TrimSpace(opts.File))
				if err != nil {
					return err
				}
			}
			assignments, err := parseEnvAssignments(args[1:])
			if err != nil {
				return err
			}
			setVars := mergeStringMaps(fromFile, assignments)
			if len(setVars) == 0 {
				return fmt.Errorf("at least one KEY=VALUE pair or --file is required")
			}

			response, err := client.PatchAppEnv(app.ID, setVars, nil)
			if err != nil {
				return err
			}
			if err := c.waitForOptionalOperation(client, response.Operation, opts.Wait); err != nil {
				return err
			}
			if response.Operation != nil && opts.Wait {
				latest, err := client.GetAppEnv(app.ID)
				if err != nil {
					return err
				}
				response.Env = latest.Env
				response.Entries = latest.Entries
			}
			return c.renderEnvCommandResult(envCommandResult{
				AppName:        app.Name,
				AppID:          app.ID,
				Env:            response.Env,
				Entries:        response.Entries,
				Operation:      response.Operation,
				AlreadyCurrent: response.AlreadyCurrent,
			})
		},
	}
	cmd.Flags().StringVarP(&opts.File, "file", "f", "", "Env file to merge into the app env")
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait for the deploy operation to complete")
	return cmd
}

func (c *CLI) newEnvRemoveCommand() *cobra.Command {
	opts := struct {
		Wait bool
	}{Wait: true}
	cmd := &cobra.Command{
		Use:     "unset <app> <KEY...>",
		Aliases: []string{"rm", "remove", "delete"},
		Short:   "Remove env vars from an app",
		Args:    cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			keys, err := normalizeEnvKeys(args[1:])
			if err != nil {
				return err
			}

			response, err := client.PatchAppEnv(app.ID, nil, keys)
			if err != nil {
				return err
			}
			if err := c.waitForOptionalOperation(client, response.Operation, opts.Wait); err != nil {
				return err
			}
			if response.Operation != nil && opts.Wait {
				latest, err := client.GetAppEnv(app.ID)
				if err != nil {
					return err
				}
				response.Env = latest.Env
				response.Entries = latest.Entries
			}
			return c.renderEnvCommandResult(envCommandResult{
				AppName:        app.Name,
				AppID:          app.ID,
				Env:            response.Env,
				Entries:        response.Entries,
				Operation:      response.Operation,
				AlreadyCurrent: response.AlreadyCurrent,
			})
		},
	}
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait for the deploy operation to complete")
	return cmd
}

func (c *CLI) newEnvGeneratedCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "generated",
		Short: "Inspect and update generated app env vars",
		Long: strings.TrimSpace(`
Generated env vars are secret values Fugue creates and injects from app spec.
Use them for reusable secrets such as signing keys when the exact value should
not be committed to source or passed by hand.
`),
	}
	cmd.AddCommand(
		c.newEnvGeneratedShowCommand(),
		c.newEnvGeneratedSetCommand(),
		c.newEnvGeneratedUnsetCommand(),
	)
	return cmd
}

func (c *CLI) newEnvGeneratedShowCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "show <app>",
		Aliases: []string{"ls", "list", "get"},
		Short:   "Show generated env var specs for an app",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			app, err = client.GetApp(app.ID)
			if err != nil {
				return err
			}
			return c.renderGeneratedEnvState(app, nil, false)
		},
	}
}

func (c *CLI) newEnvGeneratedSetCommand() *cobra.Command {
	opts := struct {
		Generate string
		Encoding string
		Length   int
		Wait     bool
	}{
		Generate: model.AppGeneratedEnvGenerateRandom,
		Encoding: model.AppGeneratedEnvEncodingBase64URL,
		Length:   model.DefaultAppGeneratedEnvBytes,
		Wait:     true,
	}
	cmd := &cobra.Command{
		Use:   "set <app> <KEY...>",
		Short: "Set generated env var specs for an app",
		Args:  cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			keys, err := normalizeEnvKeys(args[1:])
			if err != nil {
				return err
			}
			if flagChanged(cmd, "length") && opts.Length <= 0 {
				return fmt.Errorf("--length must be greater than zero")
			}
			spec := model.NormalizeAppGeneratedEnvSpec(model.AppGeneratedEnvSpec{
				Generate: opts.Generate,
				Encoding: opts.Encoding,
				Length:   opts.Length,
			})
			if spec.Generate == "" {
				return fmt.Errorf("--generate must be random")
			}
			if spec.Encoding == "" {
				return fmt.Errorf("--encoding must be base64url, base64, or hex")
			}
			if spec.Length <= 0 {
				return fmt.Errorf("--length must be greater than zero")
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			response, alreadyCurrent, err := deployUpdatedAppSpec(client, app.ID, func(appSpec *model.AppSpec) error {
				generated := cloneAppGeneratedEnvSpecMap(appSpec.GeneratedEnv)
				if generated == nil {
					generated = map[string]model.AppGeneratedEnvSpec{}
				}
				for _, key := range keys {
					generated[key] = spec
				}
				appSpec.GeneratedEnv = generated
				return nil
			})
			if err != nil {
				return err
			}
			response, err = c.waitForAppSpecMutation(client, app.ID, response, opts.Wait)
			if err != nil {
				return err
			}
			return c.renderGeneratedEnvState(response.App, response.Operation, alreadyCurrent)
		},
	}
	cmd.Flags().StringVar(&opts.Generate, "generate", opts.Generate, "Generation strategy: random")
	cmd.Flags().StringVar(&opts.Encoding, "encoding", opts.Encoding, "Generated value encoding: base64url, base64, or hex")
	cmd.Flags().IntVar(&opts.Length, "length", opts.Length, "Random byte length before encoding")
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait for the deploy operation to complete")
	return cmd
}

func (c *CLI) newEnvGeneratedUnsetCommand() *cobra.Command {
	opts := struct {
		Wait bool
	}{Wait: true}
	cmd := &cobra.Command{
		Use:     "unset <app> <KEY...>",
		Aliases: []string{"delete", "remove", "rm"},
		Short:   "Remove generated env var specs from an app",
		Args:    cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			keys, err := normalizeEnvKeys(args[1:])
			if err != nil {
				return err
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			response, alreadyCurrent, err := deployUpdatedAppSpec(client, app.ID, func(appSpec *model.AppSpec) error {
				generated := cloneAppGeneratedEnvSpecMap(appSpec.GeneratedEnv)
				for _, key := range keys {
					delete(generated, key)
				}
				if len(generated) == 0 {
					generated = nil
				}
				appSpec.GeneratedEnv = generated
				return nil
			})
			if err != nil {
				return err
			}
			response, err = c.waitForAppSpecMutation(client, app.ID, response, opts.Wait)
			if err != nil {
				return err
			}
			return c.renderGeneratedEnvState(response.App, response.Operation, alreadyCurrent)
		},
	}
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait for the deploy operation to complete")
	return cmd
}

func parseEnvAssignments(args []string) (map[string]string, error) {
	if len(args) == 0 {
		return nil, nil
	}
	out := make(map[string]string, len(args))
	for _, raw := range args {
		key, value, ok := strings.Cut(raw, "=")
		if !ok {
			return nil, fmt.Errorf("env assignment %q must use KEY=VALUE", raw)
		}
		key = strings.TrimSpace(key)
		if key == "" {
			return nil, fmt.Errorf("env assignment %q has an empty key", raw)
		}
		out[key] = value
	}
	return out, nil
}

func normalizeEnvKeys(args []string) ([]string, error) {
	keys := make([]string, 0, len(args))
	seen := map[string]struct{}{}
	for _, raw := range args {
		key := strings.TrimSpace(raw)
		if key == "" {
			return nil, fmt.Errorf("env key cannot be empty")
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	return keys, nil
}

func mergeStringMaps(maps ...map[string]string) map[string]string {
	total := 0
	for _, current := range maps {
		total += len(current)
	}
	if total == 0 {
		return nil
	}
	out := make(map[string]string, total)
	for _, current := range maps {
		for key, value := range current {
			out[key] = value
		}
	}
	return out
}

func (c *CLI) waitForOptionalOperation(client *Client, op *model.Operation, wait bool) error {
	if op == nil || !wait {
		return nil
	}
	_, err := c.waitForOperations(client, []model.Operation{*op})
	return err
}

func (c *CLI) renderEnvCommandResult(result envCommandResult) error {
	if c.wantsJSON() {
		return writeJSON(c.stdout, result)
	}
	pairs := make([]kvPair, 0, 5)
	if strings.TrimSpace(result.AppName) != "" {
		label := strings.TrimSpace(result.AppName)
		if c.showIDs() && strings.TrimSpace(result.AppID) != "" && !strings.EqualFold(label, result.AppID) {
			label += " (" + result.AppID + ")"
		}
		pairs = append(pairs, kvPair{Key: "app", Value: label})
	}
	if strings.TrimSpace(result.AppID) != "" {
		pairs = append(pairs, kvPair{Key: "app_id", Value: result.AppID})
	}
	if result.Operation != nil {
		pairs = append(pairs, kvPair{Key: "operation_id", Value: result.Operation.ID})
	}
	if result.AlreadyCurrent {
		pairs = append(pairs, kvPair{Key: "already_current", Value: "true"})
	}
	entries := normalizeEnvEntries(result.Env, result.Entries)
	pairs = append(pairs, kvPair{Key: "env_count", Value: fmt.Sprintf("%d", len(entries))})
	if err := writeKeyValues(c.stdout, pairs...); err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
	}
	if len(pairs) > 0 {
		if _, err := fmt.Fprintln(c.stdout); err != nil {
			return err
		}
	}
	return writeEnvEntryTable(c.stdout, entries)
}

func (c *CLI) renderGeneratedEnvState(app model.App, operation *model.Operation, alreadyCurrent bool) error {
	generated := model.NormalizeAppGeneratedEnvSpecs(app.Spec.GeneratedEnv)
	if c.wantsJSON() {
		return writeJSON(c.stdout, map[string]any{
			"app":             app,
			"generated_env":   generated,
			"operation":       operation,
			"already_current": alreadyCurrent,
		})
	}
	pairs := []kvPair{
		{Key: "app", Value: formatDisplayName(app.Name, app.ID, c.showIDs())},
		{Key: "generated_env_count", Value: fmt.Sprintf("%d", len(generated))},
	}
	if operation != nil {
		pairs = append(pairs, kvPair{Key: "operation_id", Value: operation.ID})
	}
	if alreadyCurrent {
		pairs = append(pairs, kvPair{Key: "already_current", Value: "true"})
	}
	if err := writeKeyValues(c.stdout, pairs...); err != nil {
		return err
	}
	if len(generated) == 0 {
		return nil
	}
	if _, err := fmt.Fprintln(c.stdout); err != nil {
		return err
	}
	return writeGeneratedEnvTable(c.stdout, generated)
}

func writeGeneratedEnvTable(w io.Writer, generated map[string]model.AppGeneratedEnvSpec) error {
	keys := make([]string, 0, len(generated))
	for key := range generated {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "KEY\tGENERATE\tENCODING\tLENGTH"); err != nil {
		return err
	}
	for _, key := range keys {
		spec := model.NormalizeAppGeneratedEnvSpec(generated[key])
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%d\n", key, spec.Generate, spec.Encoding, spec.Length); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func normalizeEnvEntries(values map[string]string, entries []model.AppEnvEntry) []model.AppEnvEntry {
	if len(entries) > 0 {
		out := make([]model.AppEnvEntry, len(entries))
		copy(out, entries)
		sort.Slice(out, func(i, j int) bool {
			return out[i].Key < out[j].Key
		})
		return out
	}
	if len(values) == 0 {
		return nil
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]model.AppEnvEntry, 0, len(keys))
	for _, key := range keys {
		out = append(out, model.AppEnvEntry{Key: key, Value: values[key]})
	}
	return out
}

func writeEnvEntryTable(w io.Writer, entries []model.AppEnvEntry) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "KEY\tVALUE\tSOURCE\tREF\tOVERRIDES"); err != nil {
		return err
	}
	for _, entry := range entries {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%s\n",
			entry.Key,
			formatInlineTableValue(entry.Value),
			strings.TrimSpace(entry.Source),
			strings.TrimSpace(entry.SourceRef),
			strings.Join(entry.Overrides, ", "),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func formatInlineTableValue(value string) string {
	replacer := strings.NewReplacer(
		"\n", "\\n",
		"\r", "\\r",
		"\t", "\\t",
	)
	return replacer.Replace(value)
}
