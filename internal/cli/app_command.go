package cli

import (
	"fmt"
	"strings"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

func (c *CLI) newAppCommandCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "command",
		Aliases: []string{"cmd"},
		Short:   "Inspect and update the app startup command",
	}
	cmd.AddCommand(
		c.newAppCommandShowCommand(),
		c.newAppCommandSetCommand(),
		c.newAppCommandClearCommand(),
	)
	return cmd
}

func (c *CLI) newAppCommandShowCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "show <app>",
		Aliases: []string{"get", "status"},
		Short:   "Show the app startup command",
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
			return c.renderAppStartupCommandState(app, nil, startupCommandValue(app.Spec), app.Spec.Args, false)
		},
	}
}

func (c *CLI) newAppCommandSetCommand() *cobra.Command {
	opts := struct {
		Command string
		Args    []string
		Wait    bool
	}{Wait: true}
	cmd := &cobra.Command{
		Use:   "set <app> [shell-command]",
		Short: "Set or replace the app startup command",
		Long: strings.TrimSpace(`
Provide the startup command as a second argument or with --command.

The command is executed through "sh -lc" on the next deploy so you can pass a
normal shell fragment such as "python app.py" or "npm run start".
`),
		Args: cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			commandValue := ""
			var err error
			if len(opts.Args) == 0 || len(args) == 2 || strings.TrimSpace(opts.Command) != "" {
				commandValue, err = resolveAppCommandValue(args, opts.Command)
				if err != nil {
					return err
				}
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			if len(opts.Args) > 0 {
				response, alreadyCurrent, err := deployUpdatedAppSpec(client, app.ID, func(spec *model.AppSpec) error {
					if strings.TrimSpace(commandValue) != "" {
						applyCLIStartupCommand(spec, commandValue)
					}
					spec.Args = append([]string(nil), opts.Args...)
					return nil
				})
				if err != nil {
					return err
				}
				response, err = c.waitForAppSpecMutation(client, app.ID, response, opts.Wait)
				if err != nil {
					return err
				}
				return c.renderAppStartupCommandState(response.App, response.Operation, startupCommandValue(response.App.Spec), response.App.Spec.Args, alreadyCurrent)
			}
			response, err := client.PatchAppStartupCommand(app.ID, trimmedStringPointer(commandValue))
			if err != nil {
				return err
			}
			finalApp := app
			if response.App.ID != "" {
				finalApp = response.App
			}
			if opts.Wait && response.Operation != nil {
				waitedApp, err := c.waitForSingleApp(client, app.ID, *response.Operation, true)
				if err != nil {
					return err
				}
				if waitedApp != nil {
					finalApp = *waitedApp
				}
			}
			startupCommand := strings.TrimSpace(commandValue)
			if opts.Wait || response.AlreadyCurrent {
				startupCommand = startupCommandValue(finalApp.Spec)
			}
			return c.renderAppStartupCommandState(finalApp, response.Operation, startupCommand, finalApp.Spec.Args, response.AlreadyCurrent)
		},
	}
	cmd.Flags().StringVar(&opts.Command, "command", "", "Shell command to run on startup")
	cmd.Flags().StringArrayVar(&opts.Args, "arg", nil, "Container argument to append to the startup command (repeatable)")
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait for the deploy operation to complete")
	return cmd
}

func (c *CLI) newAppCommandClearCommand() *cobra.Command {
	opts := struct {
		Wait bool
	}{Wait: true}
	cmd := &cobra.Command{
		Use:     "clear <app>",
		Aliases: []string{"delete", "remove", "unset"},
		Short:   "Clear the app startup command and return to the image default",
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
			response, err := client.PatchAppStartupCommand(app.ID, trimmedStringPointer(""))
			if err != nil {
				return err
			}
			finalApp := app
			if response.App.ID != "" {
				finalApp = response.App
			}
			if opts.Wait && response.Operation != nil {
				waitedApp, err := c.waitForSingleApp(client, app.ID, *response.Operation, true)
				if err != nil {
					return err
				}
				if waitedApp != nil {
					finalApp = *waitedApp
				}
			}
			startupCommand := ""
			if opts.Wait || response.AlreadyCurrent {
				startupCommand = startupCommandValue(finalApp.Spec)
			}
			return c.renderAppStartupCommandState(finalApp, response.Operation, startupCommand, finalApp.Spec.Args, response.AlreadyCurrent)
		},
	}
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait for the deploy operation to complete")
	return cmd
}

func resolveAppCommandValue(args []string, flagValue string) (string, error) {
	positional := ""
	if len(args) == 2 {
		positional = strings.TrimSpace(args[1])
	}
	flagValue = strings.TrimSpace(flagValue)
	switch {
	case positional != "" && flagValue != "":
		return "", fmt.Errorf("startup command must be provided either as an argument or with --command")
	case positional != "":
		return positional, nil
	case flagValue != "":
		return flagValue, nil
	default:
		return "", fmt.Errorf("startup command is required")
	}
}

func (c *CLI) renderAppStartupCommandState(app model.App, operation *model.Operation, startupCommand string, args []string, alreadyCurrent bool) error {
	if c.wantsJSON() {
		payload := map[string]any{
			"app":             app,
			"startup_command": startupCommand,
			"args":            args,
			"already_current": alreadyCurrent,
		}
		if operation != nil {
			payload["operation"] = operation
		}
		return writeJSON(c.stdout, payload)
	}
	pairs := []kvPair{
		{Key: "app_id", Value: app.ID},
		{Key: "startup_command", Value: startupCommand},
	}
	if len(args) > 0 {
		pairs = append(pairs, kvPair{Key: "args", Value: strings.Join(args, " ")})
	}
	if operation != nil {
		pairs = append(pairs, kvPair{Key: "operation_id", Value: operation.ID})
	}
	if alreadyCurrent {
		pairs = append(pairs, kvPair{Key: "already_current", Value: "true"})
	}
	return writeKeyValues(c.stdout, pairs...)
}
