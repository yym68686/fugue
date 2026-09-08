package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"fugue/internal/model"
	"github.com/spf13/cobra"
	"net/http"
	"net/url"
	"time"
)

func (c *Client) GetAppRuntimeState(id string) (model.AppRuntimeState, error) {
	var result model.AppRuntimeState
	err := c.doJSON(http.MethodGet, "/v1/apps/"+url.PathEscape(id)+"/runtime-state", nil, &result)
	if err == nil && (result.SchemaVersion != 1 || result.AppID != id || len(result.DesiredSpecHash) != 64) {
		return result, withExitCode(fmt.Errorf("server did not return a supported runtime-state contract"), ExitCodeIndeterminate)
	}
	return result, err
}
func (c *Client) RestartAppForSpec(id, hash string) (restartAppResponse, error) {
	var result restartAppResponse
	req, err := http.NewRequest(http.MethodPost, c.resolveURL("/v1/apps/"+url.PathEscape(id)+"/restart"), nil)
	if err != nil {
		return result, err
	}
	req.Header.Set("If-Match", `"`+hash+`"`)
	raw, err := c.do(req)
	if err != nil {
		return result, err
	}
	err = json.Unmarshal(raw, &result)
	return result, err
}
func (c *CLI) newAppDriftCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "drift", Short: "Compare committed intent with live process, file, image and endpoint evidence"}
	for _, mode := range []string{"show", "check"} {
		mode := mode
		var scope string
		child := &cobra.Command{Use: mode + " <app>", Short: "Inspect runtime drift; check requires complete evidence and matching state", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			if scope != "all" && scope != "runtime" {
				return fmt.Errorf("scope must be all or runtime")
			}
			result, err := client.GetAppRuntimeState(app.ID)
			if err != nil {
				return err
			}
			var observations *routeObservation
			if scope == "all" {
				value := c.appendRouteDrift(client, app, &result)
				observations = &value
			}
			payload := struct {
				model.AppRuntimeState
				Scope             string            `json:"scope"`
				RouteObservations *routeObservation `json:"route_observations,omitempty"`
			}{result, scope, observations}
			if err := c.renderResourceResult(payload); err != nil {
				return err
			}
			if mode == "check" {
				return runtimeDriftError(result)
			}
			return nil
		}}
		child.Flags().StringVar(&scope, "scope", "all", "Inspection scope: all includes route decision samples; runtime checks processes and endpoints")
		cmd.AddCommand(child)
	}
	return cmd
}
func runtimeDriftError(result model.AppRuntimeState) error {
	switch result.State {
	case "in_sync", "inactive":
		return nil
	case "drifted":
		return withExitCode(fmt.Errorf("app runtime differs from committed intent"), ExitCodeSystemFault)
	default:
		return withExitCode(fmt.Errorf("app runtime equivalence is inconclusive; inspect missing evidence"), ExitCodeIndeterminate)
	}
}
func (c *CLI) newAppReconcileCommand() *cobra.Command {
	var apply, plan bool
	var expected string
	var timeout time.Duration
	cmd := &cobra.Command{Use: "reconcile <app>", Short: "Plan or reapply committed intent using an atomic spec precondition, then verify runtime facts", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if apply && plan {
			return fmt.Errorf("--plan and --apply cannot be used together")
		}
		if timeout <= 0 {
			return fmt.Errorf("--timeout must be positive")
		}
		client, err := c.newClient()
		if err != nil {
			return err
		}
		app, err := c.resolveNamedApp(client, args[0])
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(cmd.Context(), timeout)
		defer cancel()
		client.context = ctx
		before, err := client.GetAppRuntimeState(app.ID)
		if err != nil {
			return err
		}
		if expected != "" && expected != before.DesiredSpecHash {
			return withExitCode(fmt.Errorf("saved plan no longer matches committed intent"), ExitCodeUserInput)
		}
		result := map[string]any{"schema_version": 1, "dry_run": !apply, "before": before, "expected_spec_hash": before.DesiredSpecHash, "action": "restart_committed_spec", "code_build_required": false, "apply_scope": "committed_app_spec", "independent_routing_recovery": "fugue admin artifact ls --kind " + model.PlatformArtifactKindEdgeRouteBundle}
		if !apply {
			return c.renderResourceResult(result)
		}
		if len(before.PendingOperations) > 0 {
			return withExitCode(fmt.Errorf("an operation is active; wait before reconciling"), ExitCodeUserInput)
		}
		if before.State == "in_sync" || before.State == "inactive" {
			result["already_current"] = true
			return c.renderResourceResult(result)
		}
		response, err := client.RestartAppForSpec(app.ID, before.DesiredSpecHash)
		if err != nil {
			return err
		}
		result["operation"] = response.Operation
		operation, waitErr := waitOperation(ctx, client, response.Operation.ID, 2*time.Second)
		result["operation"] = operation
		if waitErr != nil {
			result["outcome"] = "unknown"
			result["next_command"] = "fugue operation wait " + shellSingleQuote(operation.ID)
			if err := c.renderResourceResult(result); err != nil {
				return err
			}
			return waitErr
		}
		var after model.AppRuntimeState
		for {
			after, err = client.GetAppRuntimeState(app.ID)
			if err == nil && after.State == "in_sync" {
				if response.DesiredSpecHash == "" || after.DesiredSpecHash != response.DesiredSpecHash {
					result["after"] = after
					result["outcome"] = "unknown"
					if err := c.renderResourceResult(result); err != nil {
						return err
					}
					return withExitCode(fmt.Errorf("intent changed during reconcile verification"), ExitCodeIndeterminate)
				}
				break
			}
			if err := waitRequestRetry(ctx, 2*time.Second); err != nil {
				result["after"] = after
				result["outcome"] = "unknown"
				if writeErr := c.renderResourceResult(result); writeErr != nil {
					return writeErr
				}
				return withExitCode(fmt.Errorf("reconcile operation completed but live equivalence was not established: %w", err), ExitCodeIndeterminate)
			}
		}
		result["after"] = after
		result["outcome"] = "succeeded"
		return c.renderResourceResult(result)
	}}
	cmd.Flags().BoolVar(&plan, "plan", false, "Only inspect intent and runtime evidence (the default)")
	cmd.Flags().BoolVar(&apply, "apply", false, "Apply the current committed spec and verify; never builds or selects another image")
	cmd.Flags().StringVar(&expected, "expected-spec-hash", "", "Require the hash from a previously inspected plan")
	cmd.Flags().DurationVar(&timeout, "timeout", 10*time.Minute, "Total observation and apply deadline; does not cancel server operations")
	return cmd
}
