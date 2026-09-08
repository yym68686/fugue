package cli

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"fugue/internal/model"
	"github.com/spf13/cobra"
)

func (c *Client) listImages(query url.Values) ([]model.Image, error) {
	var response struct {
		Images []model.Image `json:"images"`
	}
	err := c.doJSON(http.MethodGet, "/v1/images?"+query.Encode(), nil, &response)
	return response.Images, err
}
func (c *CLI) resolveImage(client *Client, ref string) (model.Image, error) {
	query := url.Values{}
	tenant, project, err := c.resolveFilterSelections(client)
	if err != nil {
		return model.Image{}, err
	}
	if tenant != "" {
		query.Set("tenant_id", tenant)
	}
	if project != "" {
		query.Set("project_id", project)
	}
	if strings.HasPrefix(ref, "img_") || strings.HasPrefix(ref, "image_") {
		var result struct {
			Image model.Image `json:"image"`
		}
		err := client.doJSON(http.MethodGet, "/v1/images/"+url.PathEscape(ref), nil, &result)
		if err != nil {
			return model.Image{}, err
		}
		if tenant != "" && result.Image.TenantID != tenant {
			return model.Image{}, withExitCode(fmt.Errorf("image is outside the selected tenant"), ExitCodeNotFound)
		}
		if project != "" {
			app, err := client.GetApp(result.Image.AppID)
			if err != nil {
				return model.Image{}, err
			}
			if app.ProjectID != project {
				return model.Image{}, withExitCode(fmt.Errorf("image is outside the selected project"), ExitCodeNotFound)
			}
		}
		return result.Image, nil
	}
	if strings.HasPrefix(ref, "sha256:") {
		query.Set("digest", ref)
	} else {
		query.Set("image_ref", ref)
	}
	images, err := client.listImages(query)
	if err != nil {
		return model.Image{}, err
	}
	matches := images
	if len(matches) == 0 {
		return model.Image{}, withExitCode(fmt.Errorf("image %q not found", ref), ExitCodeNotFound)
	}
	if len(matches) != 1 {
		return model.Image{}, withExitCode(fmt.Errorf("image %q has %d matches; use an exact image ID and scope", ref, len(matches)), ExitCodeUserInput)
	}
	return matches[0], nil
}
func (c *Client) listImageReplicas(id string) ([]model.ImageReplica, error) {
	var response struct {
		Replicas []model.ImageReplica `json:"replicas"`
	}
	err := c.doJSON(http.MethodGet, "/v1/images/"+url.PathEscape(id)+"/replicas", nil, &response)
	return response.Replicas, err
}
func (c *Client) listImageTransfers(query url.Values) ([]model.ImageReplicationTask, error) {
	var result struct {
		Tasks []model.ImageReplicationTask `json:"tasks"`
	}
	err := c.doJSON(http.MethodGet, "/v1/image-replication-tasks?"+query.Encode(), nil, &result)
	return result.Tasks, err
}

func (c *CLI) newImageCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "image", Short: "Manage immutable images, replicas, pins and replication tasks"}
	var appRef string
	ls := &cobra.Command{Use: "ls", Aliases: []string{"list"}, Short: "List authorized image inventory", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		client, err := c.newClient()
		if err != nil {
			return err
		}
		tenant, project, err := c.resolveFilterSelections(client)
		if err != nil {
			return err
		}
		q := url.Values{}
		if tenant != "" {
			q.Set("tenant_id", tenant)
		}
		if project != "" {
			q.Set("project_id", project)
		}
		if appRef != "" {
			app, err := c.resolveNamedApp(client, appRef)
			if err != nil {
				return err
			}
			q.Set("app_id", app.ID)
		}
		images, err := client.listImages(q)
		if err != nil {
			return err
		}
		if c.wantsJSON() {
			return c.writeJSON(map[string]any{"images": images})
		}
		for _, item := range images {
			fmt.Fprintf(c.stdout, "%s\t%s\t%s\t%s\n", item.ID, item.ImageRef, item.CanonicalDigest, item.LifecycleState)
		}
		return nil
	}}
	ls.Flags().StringVar(&appRef, "app", "", "Filter by app name or ID")
	cmd.AddCommand(ls, c.newImageReadCommand("show"), c.newImageReadCommand("replicas"), c.newImageReadCommand("pins"), c.newImagePinCommand(), c.newImageUnpinCommand(), c.newImageReplicateCommand(), c.newImageVerifyCommand(), c.newImageTransferCommand())
	return cmd
}
func (c *CLI) newImageReadCommand(kind string) *cobra.Command {
	return &cobra.Command{Use: kind + " <image>", Short: "Show image " + kind + " inventory", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		client, err := c.newClient()
		if err != nil {
			return err
		}
		item, err := c.resolveImage(client, args[0])
		if err != nil {
			return err
		}
		if kind == "show" {
			if c.wantsJSON() {
				return c.writeJSON(map[string]any{"image": item})
			}
			return writeKeyValues(c.stdout, kvPair{Key: "image_id", Value: item.ID}, kvPair{Key: "image_ref", Value: item.ImageRef}, kvPair{Key: "digest", Value: item.CanonicalDigest})
		}
		var result map[string]any
		if err := client.doJSON(http.MethodGet, "/v1/images/"+url.PathEscape(item.ID)+"/"+kind, nil, &result); err != nil {
			return err
		}
		return c.renderResourceResult(result)
	}}
}
func (c *CLI) newImagePinCommand() *cobra.Command {
	var ttl time.Duration
	var replicas int
	cmd := &cobra.Command{Use: "pin <image>", Short: "Protect an image from retirement", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if ttl < 0 || replicas < 1 {
			return fmt.Errorf("--ttl must be non-negative and --min-replicas must be positive")
		}
		client, err := c.newClient()
		if err != nil {
			return err
		}
		item, err := c.resolveImage(client, args[0])
		if err != nil {
			return err
		}
		req := map[string]any{"app_id": item.AppID, "reason": "user_pin", "min_replicas": replicas}
		if ttl > 0 {
			req["expires_at"] = time.Now().UTC().Add(ttl)
		}
		var result map[string]any
		if err := client.doJSON(http.MethodPost, "/v1/images/"+url.PathEscape(item.ID)+"/pins", req, &result); err != nil {
			return err
		}
		return c.renderResourceResult(result)
	}}
	cmd.Flags().DurationVar(&ttl, "ttl", 0, "Pin lifetime; zero keeps the pin until explicitly removed")
	cmd.Flags().IntVar(&replicas, "min-replicas", 1, "Minimum protected replicas")
	return cmd
}
func (c *CLI) newImageUnpinCommand() *cobra.Command {
	return &cobra.Command{Use: "unpin <image> <pin-id>", Short: "Remove a pin subject to server retention guards", Args: cobra.ExactArgs(2), RunE: func(cmd *cobra.Command, args []string) error {
		client, err := c.newClient()
		if err != nil {
			return err
		}
		item, err := c.resolveImage(client, args[0])
		if err != nil {
			return err
		}
		if err := client.doJSON(http.MethodDelete, "/v1/images/"+url.PathEscape(item.ID)+"/pins/"+url.PathEscape(args[1]), nil, nil); err != nil {
			return err
		}
		return c.renderResourceResult(map[string]any{"image_id": item.ID, "pin_id": args[1], "deleted": true, "objects_reclaimed": false})
	}}
}
func (c *CLI) newImageReplicateCommand() *cobra.Command {
	var runtimeRef, node string
	var wait bool
	var timeout time.Duration
	cmd := &cobra.Command{Use: "replicate <image>", Short: "Queue a digest-addressed image copy to a runtime or node", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if (runtimeRef == "") == (node == "") {
			return fmt.Errorf("exactly one of --runtime or --node is required")
		}
		if timeout <= 0 {
			return fmt.Errorf("--timeout must be positive")
		}
		client, err := c.newClient()
		if err != nil {
			return err
		}
		item, err := c.resolveImage(client, args[0])
		if err != nil {
			return err
		}
		req := map[string]string{"image_id": item.ID, "app_id": item.AppID, "priority": "warmup", "target_cluster_node_name": node}
		if runtimeRef != "" {
			runtimeID, err := resolveRuntimeSelection(client, "", runtimeRef)
			if err != nil {
				return err
			}
			req["target_runtime_id"] = runtimeID
		}
		var result struct {
			Task model.ImageReplicationTask `json:"task"`
		}
		if err := client.doJSON(http.MethodPost, "/v1/image-replication-tasks", req, &result); err != nil {
			return err
		}
		var runErr error
		if wait {
			ctx, cancel := context.WithTimeout(cmd.Context(), timeout)
			defer cancel()
			result.Task, runErr = waitImageTransfer(ctx, client, result.Task.ID)
		}
		if writeErr := c.renderResourceResult(map[string]any{"task": result.Task, "next_command": "fugue image transfer watch " + shellSingleQuote(result.Task.ID)}); writeErr != nil {
			return writeErr
		}
		return runErr
	}}
	cmd.Flags().StringVar(&runtimeRef, "runtime", "", "Target runtime name or ID")
	cmd.Flags().StringVar(&node, "node", "", "Target cluster node name")
	cmd.Flags().BoolVar(&wait, "wait", false, "Wait for the replication task")
	cmd.Flags().DurationVar(&timeout, "timeout", 10*time.Minute, "Maximum local wait")
	return cmd
}
func (c *CLI) newImageTransferCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "transfer", Short: "Inspect image replication tasks"}
	var status, imageRef string
	ls := &cobra.Command{Use: "ls", Short: "List image replication tasks", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		client, err := c.newClient()
		if err != nil {
			return err
		}
		tenant, project, err := c.resolveFilterSelections(client)
		if err != nil {
			return err
		}
		q := url.Values{"status": {status}, "tenant_id": {tenant}, "project_id": {project}}
		if imageRef != "" {
			item, err := c.resolveImage(client, imageRef)
			if err != nil {
				return err
			}
			q.Set("image_id", item.ID)
		}
		tasks, err := client.listImageTransfers(q)
		if err != nil {
			return err
		}
		return c.renderResourceResult(map[string]any{"tasks": tasks})
	}}
	ls.Flags().StringVar(&status, "status", "", "Task status")
	ls.Flags().StringVar(&imageRef, "image", "", "Image name, digest or ID")
	var timeout time.Duration
	watch := &cobra.Command{Use: "watch <task-id>", Aliases: []string{"wait"}, Short: "Wait for image replication to finish", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if timeout <= 0 {
			return fmt.Errorf("--timeout must be positive")
		}
		client, err := c.newClient()
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(cmd.Context(), timeout)
		defer cancel()
		tenant, project, err := c.resolveFilterSelections(client)
		if err != nil {
			return err
		}
		task, runErr := waitImageTransfer(ctx, client, args[0], url.Values{"tenant_id": {tenant}, "project_id": {project}})
		if err := c.renderResourceResult(map[string]any{"task": task}); err != nil {
			return err
		}
		return runErr
	}}
	watch.Flags().DurationVar(&timeout, "timeout", 10*time.Minute, "Maximum local wait; does not cancel the server task")
	cmd.AddCommand(ls, watch)
	return cmd
}
func waitImageTransfer(ctx context.Context, client *Client, id string, queries ...url.Values) (model.ImageReplicationTask, error) {
	scoped := *client
	scoped.context = ctx
	query := url.Values{}
	if len(queries) > 0 {
		query = queries[0]
	}
	last := model.ImageReplicationTask{ID: id}
	for {
		tasks, err := scoped.listImageTransfers(query)
		if err != nil {
			return last, err
		}
		found := false
		for _, task := range tasks {
			if task.ID != id {
				continue
			}
			found = true
			last = task
			switch task.Status {
			case "completed":
				return task, nil
			case "failed", "canceled":
				return task, withExitCode(fmt.Errorf("image replication %s: %s", task.Status, redactDiagnosticString(task.LastError)), ExitCodeSystemFault)
			}
		}
		if !found {
			return last, withExitCode(fmt.Errorf("image replication task %q not found", id), ExitCodeNotFound)
		}
		if err := waitRequestRetry(ctx, 2*time.Second); err != nil {
			return last, withExitCode(err, ExitCodeIndeterminate)
		}
	}
}
