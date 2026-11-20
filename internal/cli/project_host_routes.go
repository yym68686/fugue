package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strings"
	"time"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

func (c *CLI) newProjectHostRoutePlanCommand() *cobra.Command {
	var host string
	var routes, removals []string
	cmd := &cobra.Command{
		Use:   "plan <project>",
		Short: "Preview a scoped edit; print the JSON plan without changing production",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			edit := model.ProjectHostRouteEdit{Hostname: host, Paths: map[string]string{}, RemovePaths: removals}
			for _, assignment := range routes {
				prefix, app, ok := strings.Cut(assignment, "=")
				if !ok || prefix == "" || app == "" || edit.Paths[prefix] != "" {
					return fmt.Errorf("--route needs one unique /path=APP_ID assignment")
				}
				edit.Paths[prefix] = app
			}
			if host == "" || len(routes)+len(removals) == 0 {
				return fmt.Errorf("--host and at least one --route or --remove-path are required")
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			project, err := c.resolveNamedProject(client, args[0])
			if err != nil {
				return err
			}
			var plan model.ProjectHostRoutePlan
			if err := client.doJSON(http.MethodPost, projectHostRouteEndpoint(project.ID, "host-plan"), edit, &plan); err != nil {
				return err
			}
			return c.writeJSON(plan)
		},
	}
	cmd.Flags().StringVar(&host, "host", "", "Existing verified custom hostname")
	cmd.Flags().StringArrayVar(&routes, "route", nil, "Set or replace a path: /prefix=APP_ID; repeat to save atomically")
	cmd.Flags().StringArrayVar(&removals, "remove-path", nil, "Remove an explicit path and restore inherited matching; repeat as needed")
	return cmd
}

func (c *CLI) newProjectHostRouteStatusCommand() *cobra.Command {
	var file string
	cmd := &cobra.Command{
		Use:   "status <project>",
		Short: "Check whether a hostname plan is in the verified global publication (platform admin)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			project, err := c.resolveNamedProject(client, args[0])
			if err != nil {
				return err
			}
			plan, err := readProjectHostRoutePlan(file, project.ID)
			if err != nil {
				return err
			}
			status, err := client.projectHostRouteStatus(project.ID, plan)
			if err != nil {
				return err
			}
			return c.writeJSON(status)
		},
	}
	cmd.Flags().StringVar(&file, "plan", "", "JSON plan file; - reads stdin")
	return cmd
}

func projectHostRouteEndpoint(projectID, action string) string {
	return path.Join("/v1/projects", projectID, "routes", action)
}

func readProjectHostRoutePlan(file, projectID string) (model.ProjectHostRoutePlan, error) {
	var plan model.ProjectHostRoutePlan
	if file == "" {
		return plan, fmt.Errorf("--plan is required")
	}
	var data []byte
	var err error
	if file == "-" {
		data, err = io.ReadAll(os.Stdin)
	} else {
		data, err = os.ReadFile(file)
	}
	if err != nil {
		return plan, err
	}
	if err := json.Unmarshal(data, &plan); err != nil {
		return plan, err
	}
	if plan.ProjectID != projectID || plan.PlanID == "" || plan.Edit.Hostname == "" {
		return plan, fmt.Errorf("plan does not match the selected project")
	}
	return plan, nil
}

func (c *Client) projectHostRouteStatus(projectID string, plan model.ProjectHostRoutePlan) (model.ProjectHostRoutePublicationStatus, error) {
	var status model.ProjectHostRoutePublicationStatus
	err := c.doJSONWithTimeout(http.MethodPost, projectHostRouteEndpoint(projectID, "host-status"), plan, &status, 20*time.Second)
	return status, err
}

func (c *CLI) applyProjectHostRoutePlan(ctx context.Context, client *Client, projectID, file string, wait bool) error {
	plan, err := readProjectHostRoutePlan(file, projectID)
	if err != nil {
		return err
	}
	if wait {
		// Before mutating, verify the caller can read publication evidence.
		if _, err := client.projectHostRouteStatus(projectID, plan); err != nil {
			return err
		}
	}
	var saved model.ProjectHostRoutePlan
	if err := client.doJSON(http.MethodPost, projectHostRouteEndpoint(projectID, "host-apply"), plan, &saved); err != nil {
		return err
	}
	if !wait {
		return c.writeJSON(saved)
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Minute)
	defer cancel()
	for ctx.Err() == nil {
		status, err := client.projectHostRouteStatus(projectID, saved)
		if err == nil {
			switch status.State {
			case "published":
				return c.writeJSON(status)
			case "superseded":
				return fmt.Errorf("intent was saved but is no longer desired: %s", status.Reason)
			}
		}
		select {
		case <-ctx.Done():
		case <-time.After(10 * time.Second):
		}
	}
	return fmt.Errorf("intent was saved but global publication was not verified; resume with project routes status --plan: %w", ctx.Err())
}
