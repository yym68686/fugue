package api

import (
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"time"

	"fugue/internal/dnsserver"
	"fugue/internal/edgequality"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func boundUnifiedBusinessInput(sources []edgequality.Receipt, route platformconfig.PhysicalQualityRoute, now time.Time) (edgequality.BusinessExplorationInput, map[string]dnsserver.QualityAnswerEvidence, error) {
	answers := map[string]dnsserver.QualityAnswerEvidence{}
	inputs := []edgequality.BusinessExplorationInput{}
	if route.BusinessExploration == nil || route.BusinessExploration.Version != model.BusinessEdgeUnifiedVersion || len(sources) != len(route.BusinessExploration.InitialDNSPublications) {
		return edgequality.BusinessExplorationInput{}, answers, errors.New("unified business publication lacks complete explicit DNS predecessor set")
	}
	previousNode := ""
	for _, source := range sources {
		var actual dnsserver.DNSDecisionReceipt
		if err := json.Unmarshal(source.Snapshot.ActualDNSReceipt, &actual); err != nil {
			return edgequality.BusinessExplorationInput{}, answers, err
		}
		if actual.NodeID <= previousNode {
			return edgequality.BusinessExplorationInput{}, answers, errors.New("unified DNS sources must be unique and canonically ordered")
		}
		previousNode = actual.NodeID
		input, answer, err := boundBusinessInput(source, route, now)
		if err != nil {
			return input, answers, err
		}
		answers[actual.NodeID] = answer
		inputs = append(inputs, input)
	}
	merged, err := edgequality.MergeUnifiedBusinessInputs(inputs)
	return merged, answers, err
}

func captureUnifiedBusinessPublications(sources []edgequality.Receipt, route platformconfig.PhysicalQualityRoute, now time.Time) (map[string]compiledPhysicalDNSSelection, error) {
	type boundSource struct {
		nodeID  string
		receipt edgequality.Receipt
	}
	ordered := []boundSource{}
	for _, source := range sources {
		var actual dnsserver.DNSDecisionReceipt
		if err := json.Unmarshal(source.Snapshot.ActualDNSReceipt, &actual); err != nil {
			return nil, err
		}
		ordered = append(ordered, boundSource{actual.NodeID, source})
	}
	sort.Slice(ordered, func(left, right int) bool { return ordered[left].nodeID < ordered[right].nodeID })
	canonical := []edgequality.Receipt{}
	for _, source := range ordered {
		canonical = append(canonical, source.receipt)
	}
	input, _, err := boundUnifiedBusinessInput(canonical, route, now)
	if err != nil {
		return nil, err
	}
	evaluation, err := edgequality.CaptureBusinessExploration(input)
	if err != nil {
		return nil, err
	}
	compiled := map[string]compiledPhysicalDNSSelection{}
	for _, source := range ordered {
		evidence := businessPublicationEvidence{Source: source.receipt, Sources: canonical, Evaluation: evaluation}
		selection, err := compileUnifiedBusinessSelection(evidence, route, now)
		if err != nil {
			return nil, err
		}
		raw, err := json.Marshal(evidence)
		if err != nil || len(raw) > 8<<20 {
			return nil, errors.New("unified business evidence exceeds bounded snapshot")
		}
		compiled[source.nodeID+"\x00"+route.Hostname] = compiledPhysicalDNSSelection{Selection: selection, Evidence: raw}
	}
	return compiled, nil
}

func compileUnifiedBusinessSelection(evidence businessPublicationEvidence, route platformconfig.PhysicalQualityRoute, now time.Time) (*model.DNSPhysicalSelection, error) {
	input, answers, err := boundUnifiedBusinessInput(evidence.Sources, route, now)
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(input, evidence.Evaluation.Input) {
		return nil, errors.New("unified evaluation differs from independently reconstructed DNS sources")
	}
	var actual dnsserver.DNSDecisionReceipt
	if err := json.Unmarshal(evidence.Source.Snapshot.ActualDNSReceipt, &actual); err != nil {
		return nil, err
	}
	matched := false
	for _, source := range evidence.Sources {
		if reflect.DeepEqual(source, evidence.Source) {
			matched = true
		}
	}
	answer, present := answers[actual.NodeID]
	if !matched || !present {
		return nil, errors.New("unified execution envelope is not bound to a captured DNS predecessor")
	}
	decision, err := edgequality.ReplayBusinessExploration(evidence.Evaluation)
	if err != nil {
		return nil, err
	}
	if decision.Action == "withhold" || decision.ServingAuthority || decision.State.Unified == nil {
		return nil, errors.New("unified primary unavailable; preserve independent verified recovery")
	}
	allocation := &model.DNSBusinessAllocation{ExecutionNodeID: actual.NodeID, Policy: input.Policy, State: decision.State, ValidUntil: decision.ValidUntil, ReceiptDigest: evidence.Evaluation.Digest}
	primary := model.BusinessServingPrimary(allocation)
	ordered := []string{primary}
	others := append([]string(nil), input.Policy.CandidateEdgeIDs...)
	sort.Strings(others)
	for _, edgeID := range others {
		if edgeID != primary {
			ordered = append(ordered, edgeID)
		}
	}
	sourceDigest, err := platformconfig.Digest(evidence.Sources)
	if err != nil {
		return nil, err
	}
	selection := &model.DNSPhysicalSelection{Version: model.DNSPhysicalBusinessSelectionVersion, QualityState: "learning", PrimaryEdgeID: primary, OrderedEdgeIDs: ordered, EvidenceDigest: sourceDigest, DNSReceiptID: answer.ReceiptID, LoadedDigest: answer.Publication.Digest, PolicyDigest: answer.Publication.PolicyDigest,
		Scope: "global", CapturedAt: input.CapturedAt, PrimarySince: &decision.State.Unified.PrimarySince, BusinessAllocation: allocation}
	if err := model.ValidateDNSPhysicalSelection(selection); err != nil {
		return nil, err
	}
	return selection, nil
}
