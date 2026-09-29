package cli

import (
	"encoding/json"
	"errors"
	"time"

	c "fugue/internal/staticedgecontract"
	o "fugue/internal/staticedgeobserve"
	"github.com/spf13/cobra"
)

func (cli *CLI) staticEdgeObservabilityCommands() []*cobra.Command {
	obs := &cobra.Command{Use: "observability", Short: "Inspect independent request evidence coverage"}
	obs.AddCommand(cli.staticEdgeObservationCommand("status"))
	reqs := &cobra.Command{Use: "requests", Short: "Query bounded local request facts without the Fugue API"}
	for _, op := range []string{"explain", "slow", "export"} {
		reqs.AddCommand(cli.staticEdgeObservationCommand(op))
	}
	return []*cobra.Command{obs, reqs}
}
func (cli *CLI) staticEdgeObservationCommand(op string) *cobra.Command {
	var lookup, transport, out, source, stage string
	var since time.Duration
	var min float64
	var limit int
	var peers []string
	cmd := &cobra.Command{Use: op + " [context]", Args: cobra.MaximumNArgs(1), Short: op + " request observations through the independent manager", RunE: func(cmd *cobra.Command, args []string) error {
		if source != "direct" {
			return errors.New("this command uses direct independent management; central evidence is queried separately")
		}
		if stage != "request-body" {
			return errors.New("supported stage: request-body")
		}
		name := ""
		if len(args) > 0 {
			name = args[0]
		}
		cfg, e := loadStaticEdgeContext(name)
		if e != nil {
			return e
		}
		if transport != "" {
			cfg.Transport = transport
		}
		if e = validateStaticEdgeContext(cfg); e != nil {
			return e
		}
		req := c.Request{Schema: c.RPCSchema, EdgeID: cfg.EdgeID, RequestID: newStaticRequestID(), Operation: "observability-status"}
		if op != "status" {
			now := time.Now().UTC()
			q := o.Query{RequestID: lookup, Since: now.Add(-since), Until: now, MinReadMS: min, Limit: limit}
			if e = q.Validate(); e != nil {
				return e
			}
			if op != "slow" && lookup == "" {
				return errors.New("--request-id required")
			}
			req.Operation = "request-query"
			req.ObservationQuery = &q
		}
		response, e := staticEdgeCall(cmd.Context(), cfg, req)
		if e != nil {
			return e
		}
		results := []o.Result{}
		if response.Observations != nil {
			results = append(results, *response.Observations)
		}
		if len(peers) > 4 {
			return errors.New("at most four explicit peer contexts")
		}
		for _, peer := range peers {
			if op == "status" {
				return errors.New("peer queries require request evidence")
			}
			pc, e := loadStaticEdgeContext(peer)
			if e != nil {
				return e
			}
			if transport != "" {
				pc.Transport = transport
			}
			pr := req
			pr.EdgeID = pc.EdgeID
			pr.RequestID = newStaticRequestID()
			// A returned application ID resolves to the trusted ingress ID.
			if req.ObservationQuery != nil && lookup != "" && len(results) > 0 && len(results[0].Records) > 0 {
				q := *req.ObservationQuery
				q.RequestID = results[0].Records[0].RequestID
				pr.ObservationQuery = &q
			}
			v, e := staticEdgeCall(cmd.Context(), pc, pr)
			if e != nil {
				return e
			}
			if v.Observations == nil {
				return errors.New("peer did not return observation evidence")
			}
			results = append(results, *v.Observations)
		}
		var output any = response
		if op != "status" {
			output = map[string]any{"source": "direct", "results": results, "chain": o.Join(results)}
		}
		if op == "export" {
			if response.Observations == nil {
				return errors.New("manager did not return observation evidence")
			}
			if out == "" {
				return errors.New("--file required for evidence export")
			}
			raw, e := json.MarshalIndent(output, "", "  ")
			if e != nil {
				return e
			}
			if e = staticEdgeWriteFile(out, append(raw, '\n')); e != nil {
				return e
			}
			recordCount := 0
			for _, result := range results {
				recordCount += len(result.Records)
			}
			return cli.renderResourceResult(map[string]any{"file": out, "source": "direct", "records": recordCount})
		}
		return cli.renderResourceResult(output)
	}}
	f := cmd.Flags()
	f.StringVar(&transport, "transport", "", "Explicit mtls or ssh for this context and all peers; no automatic fallback")
	f.StringVar(&source, "source", "direct", "Evidence source: direct independent manager")
	f.StringVar(&stage, "stage", "request-body", "Observed stage")
	if op != "status" {
		f.StringSliceVar(&peers, "peer", nil, "Explicit independent peer contexts to join (maximum four)")
		f.StringVar(&lookup, "request-id", "", "Trusted ingress or returned application request ID")
		f.DurationVar(&since, "since", time.Hour, "Bounded lookup window, maximum 168h")
		f.IntVar(&limit, "limit", 100, "Maximum records, 1..200")
		if op == "slow" {
			f.Float64Var(&min, "min-read-ms", 1000, "Minimum observed Read wait in milliseconds")
		}
		if op == "export" {
			f.StringVar(&out, "file", "", "Private evidence file (JSON); contains no payload or credentials")
		}
	}
	return cmd
}
