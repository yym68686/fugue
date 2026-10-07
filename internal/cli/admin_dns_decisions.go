package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"

	"fugue/internal/dnsserver"
	"github.com/spf13/cobra"
)

func (c *CLI) newAdminDNSDecisionsCommand() *cobra.Command {
	command := &cobra.Command{Use: "decisions", Short: "Inspect recorded DNS answers and replay original decisions offline"}
	var hostname, decisionID string
	limit := 5
	explain := &cobra.Command{Use: "explain <node-id>", Short: "Read retained actual answers from the selected public DNS process", Args: cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			if err := dnsserver.ValidateDNSDecisionFilter(hostname, decisionID, limit); err != nil {
				return err
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			query := url.Values{"hostname": {hostname}, "decision_id": {decisionID}, "limit": {strconv.Itoa(limit)}}
			var result json.RawMessage
			if err := client.doJSON(http.MethodGet, "/v1/admin/platform-state/dns-decisions/"+url.PathEscape(args[0])+"?"+query.Encode(), nil, &result); err != nil {
				return err
			}
			return c.writeJSON(result)
		}}
	explain.Flags().StringVar(&hostname, "hostname", "", "Only receipts for this hostname")
	explain.Flags().StringVar(&decisionID, "decision-id", "", "Only this recorded decision, not a fresh ranking")
	explain.Flags().IntVar(&limit, "limit", 5, "Maximum receipts (1-20); retention is bounded and absence is not proof")
	replay := &cobra.Command{Use: "replay <receipt-file>", Short: "Replay exported receipts without DNS, API, credentials or current rankings", Args: cobra.ExactArgs(1),
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
				return errors.New("DNS receipt file exceeds 8 MiB")
			}
			var envelope struct {
				Snapshot dnsserver.DNSDecisionSnapshot `json:"snapshot"`
			}
			if err := json.Unmarshal(raw, &envelope); err != nil {
				return err
			}
			receipts := envelope.Snapshot.Receipts
			if receipts == nil {
				var receipt dnsserver.DNSDecisionReceipt
				if err := json.Unmarshal(raw, &receipt); err != nil {
					return err
				}
				if receipt.Schema == "" {
					return errors.New("expected one receipt or explain response")
				}
				receipts = []dnsserver.DNSDecisionReceipt{receipt}
			}
			if len(receipts) == 0 || len(receipts) > 20 {
				return errors.New("expected 1-20 recorded decisions")
			}
			results := make([]dnsserver.DNSDecisionReplayResult, 0, len(receipts))
			for _, receipt := range receipts {
				result, err := dnsserver.ReplayDNSDecision(receipt)
				if err != nil {
					return fmt.Errorf("offline replay: %w", err)
				}
				results = append(results, result)
			}
			return c.writeJSON(results)
		}}
	command.AddCommand(explain, replay)
	return command
}
