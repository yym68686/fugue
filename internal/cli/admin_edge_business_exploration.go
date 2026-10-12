package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"slices"
	"time"

	"fugue/internal/dnsserver"
	"fugue/internal/edgequality"
	"fugue/internal/model"
	"github.com/spf13/cobra"
)

func (c *CLI) newAdminEdgeBusinessExplorationCommand() *cobra.Command {
	command := &cobra.Command{Use: "business-exploration", Short: "Capture, evaluate and replay passive business TCP evidence; never publishes routing"}
	var policyPath, trafficClass, dnsNodeID, pathPrefix string
	capture := &cobra.Command{Use: "capture <hostname>", Args: cobra.ExactArgs(1), Short: "Read retained business TCP and actual DNS evidence; never allocates business traffic",
		RunE: func(command *cobra.Command, args []string) error {
			var policy model.BusinessEdgeExplorationPolicy
			if err := readBusinessExplorationFile(policyPath, &policy); err != nil {
				return err
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			query := url.Values{"traffic_class": {trafficClass}, "scope": {"global"}, "dns_node_id": {dnsNodeID}, "path_prefix": {pathPrefix}}
			var shadow edgequality.Receipt
			if err := client.doJSON(http.MethodGet, "/v1/edge/quality-shadow/"+url.PathEscape(args[0])+"?"+query.Encode(), nil, &shadow); err != nil {
				return err
			}
			if _, err := edgequality.Replay(shadow); err != nil {
				return err
			}
			var actual dnsserver.DNSDecisionReceipt
			if err := json.Unmarshal(shadow.Snapshot.ActualDNSReceipt, &actual); err != nil {
				return err
			}
			evidence, err := dnsserver.QualityEvidenceFromDNSDecision(actual, shadow.Snapshot.CapturedAt, time.Duration(policy.EvidenceMaxAgeSeconds)*time.Second)
			if err != nil {
				return err
			}
			if actual.NodeID != dnsNodeID || evidence.Hostname != args[0] || shadow.Snapshot.Hostname != args[0] || shadow.Snapshot.PathPrefix != pathPrefix || shadow.Snapshot.TrafficClass != trafficClass || evidence.EdgeID != policy.BaselineEdgeID {
				return errors.New("business capture baseline or service context differs from actual DNS evidence")
			}
			input := edgequality.BusinessExplorationInput{Policy: policy, CapturedAt: shadow.Snapshot.CapturedAt, Hostname: args[0], PathPrefix: pathPrefix, TrafficClass: trafficClass,
				NetworkSamples: []model.EdgeNetworkSample{}, Candidates: []edgequality.Candidate{}}
			input.NetworkSamples = edgequality.BusinessSamplesFromBoundObservations(shadow.Snapshot, policy.CandidateEdgeIDs)
			if previous := evidence.BusinessAllocation; previous != nil && previous.Policy.IntentID == policy.IntentID {
				if !reflect.DeepEqual(previous.Policy, policy) {
					return errors.New("business intent settings changed without a new intent identity")
				}
				input.Previous = &previous.State
			} else {
				anchor, exists := policy.InitialDNSPublications[dnsNodeID]
				if !exists || actual.Publication.ServingLKG || actual.Publication.Rejected || evidence.Publication.Digest != anchor.Digest || evidence.Publication.FencingToken != anchor.FencingToken {
					return errors.New("business capture cannot initialize from an unfenced DNS predecessor")
				}
			}
			for _, candidate := range shadow.Snapshot.Candidates {
				if slices.Contains(policy.CandidateEdgeIDs, candidate.EdgeID) {
					input.Candidates = append(input.Candidates, candidate)
				}
			}
			receipt, err := edgequality.CaptureBusinessExploration(input)
			if err != nil {
				return err
			}
			return c.writeJSON(receipt)
		}}
	capture.Flags().StringVar(&policyPath, "policy", "", "Explicit business exploration policy file; does not activate it")
	capture.Flags().StringVar(&trafficClass, "traffic-class", "", "Exact business class: dynamic_api or streaming")
	capture.Flags().StringVar(&dnsNodeID, "dns-node-id", "", "Logical DNS node ID whose replayable answer binds the current baseline")
	capture.Flags().StringVar(&pathPrefix, "path-prefix", "", "Exact served route prefix used to filter retained TCP evidence")
	for _, name := range []string{"policy", "traffic-class", "dns-node-id", "path-prefix"} {
		_ = capture.MarkFlagRequired(name)
	}
	command.AddCommand(capture)
	command.AddCommand(&cobra.Command{Use: "evaluate <input-file>", Args: cobra.ExactArgs(1), Short: "Evaluate explicit intent and captured inputs without API calls or serving authority",
		RunE: func(command *cobra.Command, args []string) error {
			var input edgequality.BusinessExplorationInput
			if err := readBusinessExplorationFile(args[0], &input); err != nil {
				return err
			}
			receipt, err := edgequality.CaptureBusinessExploration(input)
			if err != nil {
				return err
			}
			return c.writeJSON(receipt)
		}})
	command.AddCommand(&cobra.Command{Use: "replay <receipt-file>", Args: cobra.ExactArgs(1), Short: "Verify deterministic calculation and receipt integrity, not origin authenticity",
		RunE: func(command *cobra.Command, args []string) error {
			var receipt edgequality.BusinessExplorationReceipt
			if err := readBusinessExplorationFile(args[0], &receipt); err != nil {
				return err
			}
			decision, err := edgequality.ReplayBusinessExploration(receipt)
			if err != nil {
				return err
			}
			return c.writeJSON(struct {
				Matched  bool                                    `json:"matched"`
				Decision edgequality.BusinessExplorationDecision `json:"decision"`
			}{true, decision})
		}})
	return command
}

func readBusinessExplorationFile(path string, target any) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, (32<<20)+1))
	if err != nil {
		return err
	}
	if len(raw) > 32<<20 {
		return errors.New("business exploration evidence exceeds 32 MiB")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if decoder.Decode(new(any)) != io.EOF {
		return errors.New("business exploration file contains trailing data")
	}
	return nil
}
