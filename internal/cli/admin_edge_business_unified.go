package cli

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"reflect"
	"slices"
	"sort"
	"time"

	"fugue/internal/dnsserver"
	"fugue/internal/edgequality"
	"fugue/internal/model"
)

func (c *CLI) captureUnifiedBusiness(policy model.BusinessEdgeExplorationPolicy, hostname, trafficClass, pathPrefix, selectedNode string) error {
	if _, exists := policy.InitialDNSPublications[selectedNode]; !exists {
		return errors.New("selected DNS node is outside unified intent")
	}
	client, err := c.newClient()
	if err != nil {
		return err
	}
	nodes := make([]string, 0, len(policy.InitialDNSPublications))
	for nodeID := range policy.InitialDNSPublications {
		nodes = append(nodes, nodeID)
	}
	sort.Strings(nodes)
	inputs := []edgequality.BusinessExplorationInput{}
	for _, nodeID := range nodes {
		query := url.Values{"traffic_class": {trafficClass}, "scope": {"global"}, "dns_node_id": {nodeID}, "path_prefix": {pathPrefix}}
		var source edgequality.Receipt
		if err := client.doJSON(http.MethodGet, "/v1/edge/quality-shadow/"+url.PathEscape(hostname)+"?"+query.Encode(), nil, &source); err != nil {
			return err
		}
		input, err := unifiedCLIInput(policy, hostname, trafficClass, pathPrefix, nodeID, source)
		if err != nil {
			return err
		}
		inputs = append(inputs, input)
	}
	input, err := edgequality.MergeUnifiedBusinessInputs(inputs)
	if err != nil {
		return err
	}
	receipt, err := edgequality.CaptureBusinessExploration(input)
	if err != nil {
		return err
	}
	return c.writeJSON(receipt)
}

func unifiedCLIInput(policy model.BusinessEdgeExplorationPolicy, hostname, trafficClass, pathPrefix, nodeID string, source edgequality.Receipt) (edgequality.BusinessExplorationInput, error) {
	input := edgequality.BusinessExplorationInput{Policy: policy, Hostname: hostname, TrafficClass: trafficClass, PathPrefix: pathPrefix, CapturedAt: source.Snapshot.CapturedAt, Candidates: []edgequality.Candidate{}, NetworkSamples: []model.EdgeNetworkSample{}}
	if _, err := edgequality.Replay(source); err != nil {
		return input, err
	}
	var actual dnsserver.DNSDecisionReceipt
	if err := json.Unmarshal(source.Snapshot.ActualDNSReceipt, &actual); err != nil {
		return input, err
	}
	evidence, err := dnsserver.QualityEvidenceFromDNSDecision(actual, source.Snapshot.CapturedAt, time.Duration(policy.EvidenceMaxAgeSeconds)*time.Second)
	if err != nil {
		return input, err
	}
	if actual.NodeID != nodeID || actual.QType != 1 || evidence.Hostname != hostname || evidence.Scope != "global" || source.Snapshot.Hostname != hostname || source.Snapshot.PathPrefix != pathPrefix || source.Snapshot.TrafficClass != trafficClass || len(source.Snapshot.Blockers) != 0 {
		return input, errors.New("unified capture differs from declared DNS or service context")
	}
	expected := policy.BaselineEdgeID
	if previous := evidence.BusinessAllocation; previous != nil && previous.Policy.IntentID == policy.IntentID {
		if !reflect.DeepEqual(previous.Policy, policy) || previous.State.Unified == nil {
			return input, errors.New("unified predecessor policy or version changed")
		}
		input.Previous = &previous.State
		expected = model.BusinessServingPrimary(previous)
	} else {
		anchor, exists := policy.InitialDNSPublications[nodeID]
		if !exists || actual.Publication.ServingLKG || actual.Publication.Rejected || evidence.Publication.Digest != anchor.Digest || evidence.Publication.FencingToken != anchor.FencingToken {
			return input, errors.New("unified initialization requires exact fenced predecessor")
		}
		if anchor.BaselineEdgeID != "" {
			expected = anchor.BaselineEdgeID
		}
		weights := map[string]int{evidence.EdgeID: 10000}
		if evidence.BusinessAllocation != nil {
			weights = model.BusinessExecutionWeights(evidence.BusinessAllocation, source.Snapshot.CapturedAt)
		}
		input.InitialDNSBasisPoints = map[string]map[string]int{nodeID: weights}
	}
	if expected != evidence.EdgeID {
		return input, errors.New("unified predecessor primary differs from explicit migration fact")
	}
	for _, candidate := range source.Snapshot.Candidates {
		if slices.Contains(policy.CandidateEdgeIDs, candidate.EdgeID) {
			input.Candidates = append(input.Candidates, candidate)
		}
	}
	input.NetworkSamples = edgequality.BusinessSamplesFromBoundObservations(source.Snapshot, policy.CandidateEdgeIDs)
	return input, nil
}
