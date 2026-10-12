package api

import (
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"sort"
	"time"

	"fugue/internal/dnsserver"
	"fugue/internal/edgequality"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"github.com/miekg/dns"
)

type businessPublicationEvidence struct {
	Source     edgequality.Receipt                    `json:"source"`
	Evaluation edgequality.BusinessExplorationReceipt `json:"evaluation"`
}

func captureBusinessPublication(source edgequality.Receipt, route platformconfig.PhysicalQualityRoute, now time.Time) (compiledPhysicalDNSSelection, error) {
	input, _, err := boundBusinessInput(source, route, now)
	if err != nil {
		return compiledPhysicalDNSSelection{}, err
	}
	evaluation, err := edgequality.CaptureBusinessExploration(input)
	if err != nil {
		return compiledPhysicalDNSSelection{}, err
	}
	evidence := businessPublicationEvidence{Source: source, Evaluation: evaluation}
	selection, err := compileBoundBusinessSelection(evidence, route, now)
	if err != nil {
		return compiledPhysicalDNSSelection{}, err
	}
	raw, err := json.Marshal(evidence)
	if err != nil || len(raw) > 8<<20 {
		return compiledPhysicalDNSSelection{}, errors.New("business publication evidence exceeds bounded snapshot")
	}
	return compiledPhysicalDNSSelection{Selection: selection, Evidence: raw}, nil
}

func boundBusinessInput(source edgequality.Receipt, route platformconfig.PhysicalQualityRoute, now time.Time) (edgequality.BusinessExplorationInput, dnsserver.QualityAnswerEvidence, error) {
	snapshot := source.Snapshot
	if route.BusinessExploration == nil || snapshot.Hostname != route.Hostname || edgequality.DNSHostname(snapshot) != route.Hostname ||
		snapshot.PathPrefix != route.PathPrefix || snapshot.TrafficClass != route.TrafficClass || !reflect.DeepEqual(snapshot.Policy, route.Policy) ||
		len(snapshot.ServiceReceipts) != 0 || len(snapshot.Blockers) != 0 || !edgequality.IsDeliveryNetworkPolicy(snapshot.Policy.Version) {
		return edgequality.BusinessExplorationInput{}, dnsserver.QualityAnswerEvidence{}, errors.New("business publication source differs from explicit single-service intent")
	}
	if _, err := edgequality.Replay(source); err != nil {
		return edgequality.BusinessExplorationInput{}, dnsserver.QualityAnswerEvidence{}, err
	}
	var actual dnsserver.DNSDecisionReceipt
	if err := json.Unmarshal(snapshot.ActualDNSReceipt, &actual); err != nil {
		return edgequality.BusinessExplorationInput{}, dnsserver.QualityAnswerEvidence{}, err
	}
	if actual.QType != dns.TypeA {
		return edgequality.BusinessExplorationInput{}, dnsserver.QualityAnswerEvidence{}, errors.New("business publication requires exact IPv4 DNS evidence")
	}
	answer, err := dnsserver.QualityEvidenceFromCapturedDNSDecision(actual, snapshot.CapturedAt, now, time.Duration(route.BusinessExploration.EvidenceMaxAgeSeconds)*time.Second)
	if err != nil {
		return edgequality.BusinessExplorationInput{}, answer, err
	}
	if err := validateBoundPhysicalQualitySnapshot(snapshot, answer); err != nil {
		return edgequality.BusinessExplorationInput{}, answer, err
	}
	policy := *route.BusinessExploration
	if answer.EdgeID != policy.BaselineEdgeID {
		return edgequality.BusinessExplorationInput{}, answer, errors.New("business safe baseline differs from actual serving assignment")
	}
	input := edgequality.BusinessExplorationInput{Policy: policy, CapturedAt: snapshot.CapturedAt, Hostname: snapshot.Hostname, PathPrefix: snapshot.PathPrefix, TrafficClass: snapshot.TrafficClass,
		Candidates: []edgequality.Candidate{}, NetworkSamples: []model.EdgeNetworkSample{}}
	available := map[string]bool{}
	for _, candidate := range answer.Candidates {
		available[candidate.EdgeID] = true
	}
	for _, candidate := range snapshot.Candidates {
		if slices.Contains(policy.CandidateEdgeIDs, candidate.EdgeID) {
			if !available[candidate.EdgeID] {
				return input, answer, errors.New("business candidate absent from current signed DNS topology")
			}
			input.Candidates = append(input.Candidates, candidate)
		}
	}
	bound := map[string]bool{}
	for _, observation := range snapshot.Observations {
		if observation.ClientSource == "public_tcp_info" {
			bound[observation.ID] = true
		}
	}
	for _, sample := range snapshot.NetworkSamples {
		if sample.Source == "public_front_tcp_info_v1" && bound[sample.ID] && slices.Contains(policy.CandidateEdgeIDs, sample.EdgeID) {
			input.NetworkSamples = append(input.NetworkSamples, sample)
		}
	}
	if previous := answer.BusinessAllocation; previous != nil && previous.Policy.IntentID == policy.IntentID {
		if !reflect.DeepEqual(previous.Policy, policy) {
			return input, answer, errors.New("business intent changed without a new intent identity")
		}
		input.Previous = &previous.State
	} else {
		anchor, exists := policy.InitialDNSPublications[actual.NodeID]
		if !exists || actual.Publication.ServingLKG || actual.Publication.Rejected || answer.Publication.Digest != anchor.Digest || answer.Publication.FencingToken != anchor.FencingToken {
			return input, answer, errors.New("business initialization differs from explicitly fenced DNS node baseline; refusing to reset prior stops or cooldown")
		}
	}
	return input, answer, nil
}

func compileBoundBusinessSelection(evidence businessPublicationEvidence, route platformconfig.PhysicalQualityRoute, now time.Time) (*model.DNSPhysicalSelection, error) {
	input, answer, err := boundBusinessInput(evidence.Source, route, now)
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(input, evidence.Evaluation.Input) {
		return nil, errors.New("business evaluation differs from captured raw TCP, intent or serving predecessor")
	}
	decision, err := edgequality.ReplayBusinessExploration(evidence.Evaluation)
	if err != nil {
		return nil, err
	}
	if decision.Action == "withhold" || decision.ServingAuthority {
		return nil, errors.New("business baseline unavailable; retain independent verified serving recovery")
	}
	ordered := []string{input.Policy.BaselineEdgeID}
	others := append([]string(nil), input.Policy.CandidateEdgeIDs...)
	sort.Strings(others)
	for _, edgeID := range others {
		if edgeID != input.Policy.BaselineEdgeID {
			ordered = append(ordered, edgeID)
		}
	}
	selection := &model.DNSPhysicalSelection{Version: model.DNSPhysicalBusinessSelectionVersion, QualityState: "learning", PrimaryEdgeID: input.Policy.BaselineEdgeID,
		OrderedEdgeIDs: ordered, EvidenceDigest: evidence.Source.Digest, DNSReceiptID: answer.ReceiptID, LoadedDigest: answer.Publication.Digest, PolicyDigest: answer.Publication.PolicyDigest,
		Scope: "global", CapturedAt: input.CapturedAt, PrimarySince: answer.PrimarySince,
		BusinessAllocation: &model.DNSBusinessAllocation{Policy: input.Policy, State: decision.State, ValidUntil: decision.ValidUntil, ReceiptDigest: evidence.Evaluation.Digest}}
	if err := model.ValidateDNSPhysicalSelection(selection); err != nil {
		return nil, err
	}
	return selection, nil
}
