package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"fugue/internal/model"
	"github.com/spf13/cobra"
	"io"
	"net/http"
	"net/url"
	"os"
)

type appStaticEdgeDNSResponse struct {
	AppID    string                      `json:"app_id"`
	Hostname string                      `json:"hostname"`
	Revision string                      `json:"revision"`
	Routing  *model.AppStaticEdgeRouting `json:"routing,omitempty"`
}

func (cli *CLI) newStaticEdgeAppDNSCommand() *cobra.Command {
	group := &cobra.Command{Use: "dns", Short: "Configure platform hostname DNS candidates without managing static edge processes", Long: "Store an explicit per-app static/managed edge order. A saved configuration is desired state, not a completed DNS switch: the signed producer and DNS consumers must publish, verify TLS/route probes and observe recovery hysteresis. Inspect authoritative results with fugue admin dns answer-check."}
	for _, mode := range []string{"get", "set", "reset"} {
		var file, revision string
		cmd := &cobra.Command{Use: mode + " <app>", Short: mode + " the app's independent static edge DNS configuration", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
			client, err := cli.newClient()
			if err != nil {
				return err
			}
			client.context = cmd.Context()
			app, err := cli.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			path := "/v1/apps/" + url.PathEscape(app.ID) + "/route/static-edge"
			var current appStaticEdgeDNSResponse
			if err = client.doJSON(http.MethodGet, path, nil, &current); err != nil {
				return err
			}
			if mode == "get" {
				return cli.renderResourceResult(current)
			}
			var routing *model.AppStaticEdgeRouting
			if mode == "set" {
				raw, e := os.ReadFile(file)
				if e != nil {
					return e
				}
				decoder := json.NewDecoder(bytes.NewReader(raw))
				decoder.DisallowUnknownFields()
				if e = decoder.Decode(&routing); e != nil {
					return e
				}
				if decoder.Decode(new(any)) != io.EOF || routing == nil {
					return fmt.Errorf("one complete routing JSON object required")
				}
				if e = model.ValidateAppStaticEdgeRouting(routing); e != nil {
					return e
				}
			}
			if revision == "" {
				revision = current.Revision
			}
			request := struct {
				ExpectedRevision string                      `json:"expected_revision"`
				Routing          *model.AppStaticEdgeRouting `json:"routing,omitempty"`
			}{revision, routing}
			var result appStaticEdgeDNSResponse
			if err = client.doJSON(http.MethodPut, path, request, &result); err != nil {
				return err
			}
			return cli.renderResourceResult(result)
		}}
		if mode != "get" {
			cmd.Flags().StringVar(&revision, "expected-revision", "", "Route revision from dns get; defaults to a fresh read with compare-and-swap")
		}
		if mode == "set" {
			cmd.Flags().StringVar(&file, "file", "", "Complete AppStaticEdgeRouting JSON with registered public endpoints and mixed edge order")
			_ = cmd.MarkFlagRequired("file")
		}
		group.AddCommand(cmd)
	}
	return group
}
