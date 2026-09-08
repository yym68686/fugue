package cli

import (
	"fmt"
	"fugue/internal/model"
	"github.com/spf13/cobra"
	"net/http"
	"net/url"
)

func (c *CLI) newDataWorkspaceDeleteCommand() *cobra.Command {
	var confirm bool
	cmd := &cobra.Command{Use: "delete <workspace>", Short: "Delete unreferenced workspace metadata; object storage is not immediately reclaimed", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		client, err := c.newClient()
		if err != nil {
			return err
		}
		workspace, err := client.GetDataWorkspace(args[0])
		if err != nil {
			return err
		}
		plan, err := client.dataDeletionPlan(workspace.Workspace.ID, "")
		if err != nil {
			return err
		}
		if !confirm {
			return c.renderResourceResult(plan)
		}
		if allowed, _ := plan["allowed"].(bool); !allowed {
			_ = c.renderResourceResult(plan)
			return withExitCode(fmt.Errorf("workspace has deletion blockers"), ExitCodeUserInput)
		}
		var result map[string]any
		if err := client.doJSON(http.MethodDelete, "/v1/data/workspaces/"+url.PathEscape(workspace.Workspace.ID), nil, &result); err != nil {
			return err
		}
		result["objects_reclaimed"] = false
		if c.wantsJSON() {
			return c.writeJSON(result)
		}
		return writeKeyValues(c.stdout, kvPair{Key: "workspace", Value: workspace.Workspace.Name}, kvPair{Key: "metadata_deleted", Value: "true"}, kvPair{Key: "objects_reclaimed", Value: "false"})
	}}
	cmd.Flags().BoolVar(&confirm, "confirm", false, "Confirm metadata deletion; server reference guards remain enforced")
	return cmd
}
func (c *CLI) newDataSnapshotDeleteCommand() *cobra.Command {
	var workspaceRef string
	var confirm bool
	cmd := &cobra.Command{Use: "delete <version-or-id>", Short: "Soft-delete an unreferenced snapshot; blob reclamation remains a separate GC action", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if workspaceRef == "" {
			cfg, err := readDataConfig(".")
			if err != nil {
				return err
			}
			workspaceRef = cfg.Workspace
		}
		client, err := c.newClient()
		if err != nil {
			return err
		}
		workspace, err := client.GetDataWorkspace(workspaceRef)
		if err != nil {
			return err
		}
		plan, err := client.dataDeletionPlan(workspace.Workspace.ID, args[0])
		if err != nil {
			return err
		}
		if !confirm {
			return c.renderResourceResult(plan)
		}
		if allowed, _ := plan["allowed"].(bool); !allowed {
			_ = c.renderResourceResult(plan)
			return withExitCode(fmt.Errorf("snapshot has deletion blockers"), ExitCodeUserInput)
		}
		var result map[string]any
		if err := client.doJSON(http.MethodDelete, "/v1/data/workspaces/"+url.PathEscape(workspace.Workspace.ID)+"/snapshots/"+url.PathEscape(args[0]), nil, &result); err != nil {
			return err
		}
		result["objects_reclaimed"] = false
		if c.wantsJSON() {
			return c.writeJSON(result)
		}
		return writeKeyValues(c.stdout, kvPair{Key: "snapshot", Value: args[0]}, kvPair{Key: "deleted", Value: "true"}, kvPair{Key: "objects_reclaimed", Value: "false"})
	}}
	cmd.Flags().StringVar(&workspaceRef, "workspace", "", "Workspace name or ID; defaults to the local data binding")
	cmd.Flags().BoolVar(&confirm, "confirm", false, "Confirm soft deletion subject to server reference guards")
	return cmd
}
func (c *CLI) newDataGrantListCommand() *cobra.Command {
	return &cobra.Command{Use: "ls [workspace]", Aliases: []string{"list"}, Short: "List short-lived data access grants without exporting credentials", Args: cobra.MaximumNArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		workspaceRef := ""
		if len(args) > 0 {
			workspaceRef = args[0]
		} else {
			cfg, err := readDataConfig(".")
			if err != nil {
				return err
			}
			workspaceRef = cfg.Workspace
		}
		client, err := c.newClient()
		if err != nil {
			return err
		}
		workspace, err := client.GetDataWorkspace(workspaceRef)
		if err != nil {
			return err
		}
		var result struct {
			Grants []model.DataGrant `json:"grants"`
		}
		if err := client.doJSON(http.MethodGet, "/v1/data/workspaces/"+url.PathEscape(workspace.Workspace.ID)+"/grants", nil, &result); err != nil {
			return err
		}
		for i := range result.Grants {
			result.Grants[i].TokenHash = ""
		}
		if c.wantsJSON() {
			return c.writeJSON(result)
		}
		for _, grant := range result.Grants {
			fmt.Fprintf(c.stdout, "%s\t%s\t%s\t%s\n", grant.ID, grant.SnapshotID, grant.Mode, grant.Status)
		}
		return nil
	}}
}

func (c *Client) dataDeletionPlan(workspaceID, snapshotID string) (map[string]any, error) {
	var result map[string]any
	err := c.doJSON(http.MethodGet, "/v1/data/workspaces/"+url.PathEscape(workspaceID)+"/deletion-plan?snapshot_id="+url.QueryEscape(snapshotID), nil, &result)
	return result, err
}
