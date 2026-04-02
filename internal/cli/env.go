package cli

import (
	"fmt"
	"strings"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

type envMutationOptions struct {
	File string
	Wait bool
}

type envCommandResult struct {
	AppID          string            `json:"app_id,omitempty"`
	Env            map[string]string `json:"env"`
	Operation      *model.Operation  `json:"operation,omitempty"`
	AlreadyCurrent bool              `json:"already_current,omitempty"`
}

func (c *CLI) newEnvCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "env",
		Short: "Inspect and update app environment variables",
	}
	cmd.AddCommand(
		c.newEnvListCommand(),
		c.newEnvSetCommand(),
		c.newEnvRemoveCommand(),
	)
	return cmd
}

func (c *CLI) newEnvListCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "list <app>",
		Aliases: []string{"ls", "show"},
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
				AppID: app.ID,
				Env:   response.Env,
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
			}
			return c.renderEnvCommandResult(envCommandResult{
				AppID:          app.ID,
				Env:            response.Env,
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
		Use:     "remove <app> <KEY...>",
		Aliases: []string{"rm", "delete", "unset"},
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
			}
			return c.renderEnvCommandResult(envCommandResult{
				AppID:          app.ID,
				Env:            response.Env,
				Operation:      response.Operation,
				AlreadyCurrent: response.AlreadyCurrent,
			})
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
	pairs := make([]kvPair, 0, 3)
	if strings.TrimSpace(result.AppID) != "" {
		pairs = append(pairs, kvPair{Key: "app_id", Value: result.AppID})
	}
	if result.Operation != nil {
		pairs = append(pairs, kvPair{Key: "operation_id", Value: result.Operation.ID})
	}
	if result.AlreadyCurrent {
		pairs = append(pairs, kvPair{Key: "already_current", Value: "true"})
	}
	if err := writeKeyValues(c.stdout, pairs...); err != nil {
		return err
	}
	if len(result.Env) == 0 {
		return nil
	}
	if len(pairs) > 0 {
		if _, err := fmt.Fprintln(c.stdout); err != nil {
			return err
		}
	}
	return writeStringMap(c.stdout, result.Env)
}
