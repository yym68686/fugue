package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

func (c *CLI) newAdminQuarantineCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "quarantine",
		Short: "Explain node or edge quarantine state",
	}
	cmd.AddCommand(&cobra.Command{
		Use:   "explain <node-or-edge>",
		Short: "Explain the latest quarantine/deep-health record for a node or edge",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			results, err := client.ListNodeDeepHealthResults()
			if err != nil {
				return err
			}
			result, ok := nodeDeepHealthResultByRef(results, args[0])
			if !ok {
				return fmt.Errorf("no quarantine/deep-health record found for %s", args[0])
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"result": result})
			}
			return writeNodeDeepHealth(c.stdout, result)
		},
	})
	return cmd
}

func (c *CLI) newAdminSyntheticCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "synthetic",
		Short: "Inspect public synthetic release probes",
	}
	opts := struct {
		ReleaseID string
		Dir       string
	}{}
	list := &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List synthetic/watch attribution records for a release",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := firstNonEmpty(strings.TrimSpace(opts.Dir), strings.TrimSpace(os.Getenv("FUGUE_RELEASE_ATTRIBUTION_DIR")))
			if dir == "" {
				return fmt.Errorf("--dir or FUGUE_RELEASE_ATTRIBUTION_DIR is required")
			}
			path := filepath.Join(dir, "release-safety-watch-windows.json")
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			var payload map[string]any
			if err := json.Unmarshal(data, &payload); err != nil {
				return err
			}
			if opts.ReleaseID != "" {
				payload["requested_release_id"] = opts.ReleaseID
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"synthetic": payload})
			}
			return writeStringMapAny(c.stdout, payload)
		},
	}
	list.Flags().StringVar(&opts.ReleaseID, "release", "", "Release id")
	list.Flags().StringVar(&opts.Dir, "dir", "", "Release attribution directory")
	cmd.AddCommand(list)
	return cmd
}

func nodeDeepHealthResultByRef(results []model.NodeDeepHealthResult, ref string) (model.NodeDeepHealthResult, bool) {
	ref = strings.TrimSpace(ref)
	for _, result := range results {
		for _, candidate := range []string{result.NodeUpdaterID, result.ClusterNodeName, result.RuntimeID, result.MachineID} {
			if strings.TrimSpace(candidate) == ref {
				return result, true
			}
		}
	}
	return model.NodeDeepHealthResult{}, false
}

func writeStringMapAny(w interface{ Write([]byte) (int, error) }, values map[string]any) error {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if _, err := fmt.Fprintf(w, "%s=%v\n", key, values[key]); err != nil {
			return err
		}
	}
	return nil
}
