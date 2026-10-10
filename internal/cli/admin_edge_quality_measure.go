package cli

import (
	"fmt"
	"net/http"
	"time"

	"fugue/internal/clientmeasurement"
	"fugue/internal/model"
	"github.com/spf13/cobra"
)

func (c *CLI) newAdminEdgeQualityMeasureCommand() *cobra.Command {
	var trafficClass, path, observer string
	var rounds int
	var interval time.Duration
	command := &cobra.Command{Use: "quality-measure <hostname>", Short: "Run authenticated same-content network measurements and retain complete outcomes; does not change DNS", Args: cobra.ExactArgs(1),
		Long:    "Measure identical generated response bytes from the current eligible physical edges using short-lived signed permits. The public Front attests the actual TCP peer and RTT; complete client outcomes retain throughput and explicit failures. Requires explicit edge.quality.observe authority and never publishes routing configuration.",
		Example: "fugue admin edge quality-measure app.example.test --traffic-class streaming --observer local-network --rounds 1",
		RunE: func(command *cobra.Command, args []string) error {
			if rounds < 1 || rounds > 30 || interval < time.Minute || interval > 5*time.Minute {
				return fmt.Errorf("measurement requires 1-30 rounds and a 1m-5m interval")
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			result := struct {
				RoutingAuthorized bool                          `json:"routing_authorized"`
				Reports           []model.EdgeClientProbeReport `json:"reports"`
				AcceptedDigests   []string                      `json:"accepted_digests"`
			}{Reports: []model.EdgeClientProbeReport{}, AcceptedDigests: []string{}}
			var nextRound time.Time
			for round := 0; round < rounds; round++ {
				if round > 0 {
					timer := time.NewTimer(max(0, time.Until(nextRound)))
					select {
					case <-command.Context().Done():
						timer.Stop()
						_ = c.writeJSON(result)
						return command.Context().Err()
					case <-timer.C:
					}
				}
				nextRound = time.Now().Add(interval)
				var plan model.EdgeClientProbePlan
				request := model.EdgeClientProbeRequest{Hostname: args[0], Path: path, TrafficClass: trafficClass, ObserverLabel: observer}
				if err := client.doJSONWithTimeout(http.MethodPost, "/v1/admin/edge-quality/client-probes", request, &plan, 20*time.Second); err != nil {
					_ = c.writeJSON(result)
					return err
				}
				report, err := clientmeasurement.MeasureRound(command.Context(), plan)
				result.Reports = append(result.Reports, report)
				if err != nil {
					_ = c.writeJSON(result)
					return err
				}
				var accepted struct {
					Accepted          bool   `json:"accepted"`
					RoutingAuthorized bool   `json:"routing_authorized"`
					ReportDigest      string `json:"report_digest"`
				}
				if err := client.doJSONWithTimeout(http.MethodPost, "/v1/admin/edge-quality/client-probes/report", report, &accepted, 10*time.Second); err != nil {
					_ = c.writeJSON(result)
					return err
				}
				if !accepted.Accepted || accepted.RoutingAuthorized || accepted.ReportDigest == "" {
					_ = c.writeJSON(result)
					return fmt.Errorf("unexpected client measurement retention response")
				}
				result.AcceptedDigests = append(result.AcceptedDigests, accepted.ReportDigest)
				fmt.Fprintf(command.ErrOrStderr(), "Client measurement round %d/%d retained; DNS unchanged.\n", round+1, rounds)
			}
			return c.writeJSON(result)
		}}
	command.Flags().StringVar(&trafficClass, "traffic-class", "", "Exact serving traffic class: streaming or dynamic_api")
	command.Flags().StringVar(&path, "path", "/", "Exact route path intercepted by the fixed-byte measurement endpoint")
	command.Flags().StringVar(&observer, "observer", "", "Operator label; public peer cohort is attested by the actual Front")
	command.Flags().IntVar(&rounds, "rounds", 1, "Bounded complete cross-edge rounds (1-30)")
	command.Flags().DurationVar(&interval, "interval", time.Minute, "Delay between rounds (1m-5m)")
	_ = command.MarkFlagRequired("traffic-class")
	_ = command.MarkFlagRequired("observer")
	return command
}
