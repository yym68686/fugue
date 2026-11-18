package cli

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

func (c *CLI) newAppStorageUnmountCommand() *cobra.Command {
	var plan, authorize, wait bool
	var expected, probeFile string
	cmd := &cobra.Command{
		Use:   "unmount <app> <mount-path>",
		Short: "Preview or safely remove a single persistent mount without deleting storage",
		Args:  cobra.ExactArgs(2),
		Example: "  fugue app storage unmount my-app /etc/app.conf --plan\n" +
			"  fugue app storage unmount my-app /etc/app.conf --expect <spec_sha256> --authorize-overlap --probe-file probe.json",
		RunE: func(cmd *cobra.Command, args []string) error {
			var request model.AppStorageUnmountRequest
			if plan {
				if expected != "" || probeFile != "" || authorize || cmd.Flags().Changed("wait") {
					return fmt.Errorf("--plan cannot be combined with execution flags")
				}
			} else {
				hash, err := hex.DecodeString(expected)
				if err != nil || len(hash) != 32 || expected != strings.ToLower(expected) || !authorize || probeFile == "" {
					return fmt.Errorf("run --plan first, then supply --expect <spec_sha256>, --authorize-overlap and --probe-file")
				}
				probe, err := readStorageUnmountProbe(probeFile)
				if err != nil {
					return err
				}
				request = model.AppStorageUnmountRequest{Path: args[1], AuthorizeOverlap: true, Probe: probe}
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			endpoint := "/v1/apps/" + url.PathEscape(app.ID) + "/storage/unmount"
			if plan {
				var out model.AppStorageUnmountPlan
				query := url.Values{"path": []string{args[1]}}
				if err := client.doJSON(http.MethodGet, endpoint+"?"+query.Encode(), nil, &out); err != nil {
					return err
				}
				if out.AppID != app.ID || out.Path != args[1] || len(out.SpecSHA256) != 64 {
					return fmt.Errorf("server returned an unrecognized unmount plan")
				}
				return c.writeJSON(out)
			}
			data, err := json.Marshal(request)
			if err != nil {
				return err
			}
			req, err := http.NewRequestWithContext(cmd.Context(), http.MethodPost, client.resolveURL(endpoint), bytes.NewReader(data))
			if err != nil {
				return err
			}
			req.Header.Set("If-Match", `"`+expected+`"`)
			req.Header.Set("Authorization", "Bearer "+client.token)
			req.Header.Set("Content-Type", "application/json")
			raw, err := client.do(req)
			if err != nil {
				return err
			}
			var out struct {
				Operation model.Operation `json:"operation"`
			}
			if err := json.Unmarshal(raw, &out); err != nil {
				return err
			}
			if out.Operation.ID == "" || out.Operation.AppID != app.ID || out.Operation.Type != model.OperationTypeAppStorageUnmount {
				return fmt.Errorf("server returned an unrecognized unmount operation")
			}
			if wait {
				operations, err := c.waitForOperations(client, []model.Operation{out.Operation})
				if err != nil {
					return err
				}
				out.Operation = operations[0]
			}
			return c.writeJSON(out)
		},
	}
	cmd.Flags().BoolVar(&plan, "plan", false, "Preview only; no changes")
	cmd.Flags().StringVar(&expected, "expect", "", "Specify spec_sha256 from the preview")
	cmd.Flags().BoolVar(&authorize, "authorize-overlap", false, "Authorize one temporary Pod with concurrent access to retained mounts")
	cmd.Flags().StringVar(&probeFile, "probe-file", "", "Business probe JSON file")
	cmd.Flags().BoolVar(&wait, "wait", true, "Wait for the business-gated rollout")
	return cmd
}

func readStorageUnmountProbe(probeFile string) (model.AppStorageUnmountProbe, error) {
	var probe model.AppStorageUnmountProbe
	f, err := os.Open(probeFile)
	if err != nil {
		return probe, err
	}
	defer f.Close()
	if st, err := f.Stat(); err != nil || !st.Mode().IsRegular() || st.Size() > 64<<10 {
		return probe, fmt.Errorf("probe file must be a regular JSON file no larger than 64 KiB")
	}
	d := json.NewDecoder(io.LimitReader(f, 64<<10))
	d.DisallowUnknownFields()
	if err := d.Decode(&probe); err != nil || d.Decode(new(json.RawMessage)) != io.EOF {
		return probe, fmt.Errorf("invalid business probe JSON")
	}
	return probe, probe.Validate()
}
