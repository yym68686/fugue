package cli

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path"

	"fugue/internal/model"
	"github.com/spf13/cobra"
)

func (c *CLI) newAppRecoveryCommand() *cobra.Command {
	root := &cobra.Command{Use: "recovery", Short: "Start isolated recovery, inspect retained containers, or release an investigation hold"}
	var requestFile string
	start := &cobra.Command{Use: "start <app> --file request.json", Short: "Authorize one replacement with fresh storage and explicit business probes", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		raw, err := os.ReadFile(requestFile)
		if err != nil {
			return err
		}
		var req model.AppRecoveryStartRequest
		if err := json.Unmarshal(raw, &req); err != nil {
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
		if err := req.Validate(app.Spec); err != nil {
			return err
		}
		var out model.AppRecovery
		if err := client.doJSON(http.MethodPost, path.Join("/v1/apps", app.ID, "recoveries"), req, &out); err != nil {
			return err
		}
		return c.writeJSON(out)
	}}
	start.Flags().StringVar(&requestFile, "file", "", "Recovery request JSON (includes explicit retention, storage policy and probes)")
	_ = start.MarkFlagRequired("file")
	root.AddCommand(start)
	for _, action := range []string{"list", "status", "release", "inspect"} {
		action := action
		use := action + " <app>"
		count := 1
		if action != "list" {
			use += " <recovery-id>"
			count = 2
		}
		root.AddCommand(&cobra.Command{Use: use, Short: map[string]string{"list": "List recoveries", "status": "Show durable recovery status", "release": "Release retained evidence or cancel before cutover", "inspect": "Show exact Pod/container identities and investigation commands"}[action], Args: cobra.ExactArgs(count), RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			u := path.Join("/v1/apps", app.ID, "recoveries")
			if action == "list" {
				var out any
				if err := client.doJSON(http.MethodGet, u, nil, &out); err != nil {
					return err
				}
				return c.writeJSON(out)
			}
			u = path.Join(u, args[1])
			method := http.MethodGet
			if action == "release" {
				u = path.Join(u, "release")
				method = http.MethodPost
			}
			var out model.AppRecovery
			if err := client.doJSON(method, u, nil, &out); err != nil {
				return err
			}
			if action != "inspect" || c.wantsJSON() {
				return c.writeJSON(out)
			}
			if _, err := fmt.Fprintf(c.stdout, "Recovery %s: %s, evidence=%s\n", out.ID, out.Phase, out.EvidenceStatus); err != nil {
				return err
			}
			for _, pod := range out.SourcePods {
				if _, err := fmt.Fprintf(c.stdout, "Pod %s UID=%s node=%s\n", pod.Name, pod.UID, pod.Node); err != nil {
					return err
				}
				for _, container := range pod.Containers {
					if _, err := fmt.Fprintf(c.stdout, "  container=%s id=%s restarts=%d\n", container.Name, container.ID, container.RestartCount); err != nil {
						return err
					}
					if _, err := fmt.Fprintf(c.stdout, "  fugue admin cluster exec --namespace %s --pod %s --container %s -- <command...>\n", out.Namespace, pod.Name, container.Name); err != nil {
						return err
					}
				}
				if _, err := fmt.Fprintf(c.stdout, "  fugue app logs runtime %s --pod %s\n", app.ID, pod.Name); err != nil {
					return err
				}
			}
			return nil
		}})
	}
	return root
}
