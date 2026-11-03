package cli

import (
	"fmt"
	"net/http"
	"path"
	"strconv"

	"fugue/internal/model"
	"github.com/spf13/cobra"
)

func (c *CLI) newAppNodeLossCommand() *cobra.Command {
	root := &cobra.Command{Use: "node-loss", Short: "Inspect or explicitly change the app Pod lifecycle on node loss"}
	render := func(app model.App, operation *model.Operation) error {
		if c.wantsJSON() {
			return c.writeJSON(map[string]any{"app": redactAppForOutput(app), "operation": redactOperationPtrForOutput(operation)})
		}
		mode := "legacy"
		seconds := "Kubernetes default"
		if p := app.Spec.NodeLoss; p != nil {
			mode = p.Mode
			if mode == model.AppNodeLossWait {
				seconds = "unlimited"
			}
			if mode == model.AppNodeLossReplace {
				seconds = "300"
				if p.TolerationSeconds != nil {
					seconds = strconv.FormatInt(*p.TolerationSeconds, 10)
				}
			}
		}
		pairs := []kvPair{{Key: "app_id", Value: app.ID}, {Key: "node_loss_mode", Value: mode}, {Key: "toleration_seconds", Value: seconds}}
		if operation != nil {
			pairs = append(pairs, kvPair{Key: "operation_id", Value: operation.ID})
		}
		return writeKeyValues(c.stdout, pairs...)
	}
	show := &cobra.Command{Use: "show <app>", Args: cobra.ExactArgs(1), Short: "Show the configured node loss policy", RunE: func(cmd *cobra.Command, args []string) error {
		client, err := c.newClient()
		if err != nil {
			return err
		}
		app, err := c.resolveNamedApp(client, args[0])
		if err != nil {
			return err
		}
		return render(app, nil)
	}}
	var mode string
	var seconds int64
	wait := true
	set := &cobra.Command{Use: "set <app>", Args: cobra.ExactArgs(1), Short: "Deploy a node loss policy (may replace application Pods)", Long: "Explicitly changes the application Pod lifecycle through a normal deployment. Does not enable failover, buy standby resources, or change placement or replicas. Existing Pod deletion cannot be undone.", RunE: func(cmd *cobra.Command, args []string) error {
		if !cmd.Flags().Changed("mode") {
			return fmt.Errorf("--mode is required")
		}
		policy := &model.AppNodeLossPolicy{Mode: mode}
		if cmd.Flags().Changed("toleration-seconds") {
			policy.TolerationSeconds = &seconds
		}
		if err := model.ValidateAppNodeLossPolicy(policy); err != nil {
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
		var response appPatchResponse
		if err = client.doJSON(http.MethodPatch, path.Join("/v1/apps", app.ID), map[string]any{"node_loss": policy}, &response); err != nil {
			return err
		}
		if wait && response.Operation != nil {
			updated, err := c.waitForSingleApp(client, app.ID, *response.Operation, true)
			if err != nil {
				return err
			}
			if updated != nil {
				response.App = *updated
			}
		}
		return render(response.App, response.Operation)
	}}
	set.Flags().StringVar(&mode, "mode", "", "wait, replace, or legacy")
	set.Flags().Int64Var(&seconds, "toleration-seconds", 300, "Eviction delay for replace; explicit 0 permits immediate eviction")
	set.Flags().BoolVar(&wait, "wait", true, "Wait for the deployment to complete")
	root.AddCommand(show, set)
	return root
}
