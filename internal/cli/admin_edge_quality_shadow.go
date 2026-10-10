package cli

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"

	"fugue/internal/dnsserver"
	"fugue/internal/edgequality"
	"github.com/spf13/cobra"
)

func (c *CLI) newAdminEdgeQualityShadowCommand() *cobra.Command {
	command := &cobra.Command{Use: "quality-shadow", Short: "Capture and replay network-only physical-edge shadow; never changes DNS"}
	var trafficClass, scope, dnsNodeID string
	capture := &cobra.Command{Use: "capture <hostname>", Short: "Capture current evidence and explicit promotion blockers, not an actual DNS answer", Args: cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			query := url.Values{"traffic_class": {trafficClass}, "scope": {scope}}
			if dnsNodeID != "" {
				query.Set("dns_node_id", dnsNodeID)
			}
			var result edgequality.Receipt
			if err := client.doJSON(http.MethodGet, "/v1/edge/quality-shadow/"+url.PathEscape(args[0])+"?"+query.Encode(), nil, &result); err != nil {
				return err
			}
			return c.writeJSON(result)
		}}
	capture.Flags().StringVar(&trafficClass, "traffic-class", "", "Required explicit traffic class; never mixes streaming and static traffic")
	capture.Flags().StringVar(&scope, "scope", "global", "Exact evidence scope, not a country eligibility filter")
	capture.Flags().StringVar(&dnsNodeID, "dns-node-id", "", "Bind a replay-verified actual answer and exact route proofs from this public DNS process")
	_ = capture.MarkFlagRequired("traffic-class")
	replay := &cobra.Command{Use: "replay <receipt-file>", Short: "Verify and replay captured shadow input offline without API or credentials", Args: cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			file, err := os.Open(args[0])
			if err != nil {
				return err
			}
			defer file.Close()
			raw, err := io.ReadAll(io.LimitReader(file, (8<<20)+1))
			if err != nil {
				return err
			}
			if len(raw) > 8<<20 {
				return errors.New("shadow receipt exceeds 8 MiB")
			}
			var receipt edgequality.Receipt
			if err := json.Unmarshal(raw, &receipt); err != nil {
				return err
			}
			if len(receipt.Snapshot.ActualDNSReceipt) > 0 {
				var actual dnsserver.DNSDecisionReceipt
				if err := json.Unmarshal(receipt.Snapshot.ActualDNSReceipt, &actual); err != nil {
					return err
				}
				evidence, err := dnsserver.QualityEvidenceFromDNSDecision(actual, receipt.Snapshot.CapturedAt, time.Duration(receipt.Snapshot.Policy.EvidenceMaxAgeSeconds)*time.Second)
				if err != nil {
					return err
				}
				if evidence.Hostname != edgequality.DNSHostname(receipt.Snapshot) || evidence.Scope != receipt.Snapshot.Scope || evidence.EdgeID != receipt.Snapshot.CurrentEdgeID {
					return errors.New("captured DNS answer differs from shadow binding")
				}
			}
			result, err := edgequality.Replay(receipt)
			if err != nil {
				return err
			}
			return c.writeJSON(struct {
				Matched bool               `json:"matched"`
				Result  edgequality.Result `json:"result"`
			}{true, result})
		}}
	command.AddCommand(capture, replay)
	return command
}
