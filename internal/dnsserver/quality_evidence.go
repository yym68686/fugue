package dnsserver

import (
	"encoding/json"
	"errors"
	"reflect"
	"time"

	"fugue/internal/model"
	"fugue/internal/routeprobe"
)

type QualityRouteProof struct {
	EdgeID      string
	EdgeGroupID string
	Hostname    string
	Path        string
	Proof       routeprobe.Proof
}

type QualityAnswerEvidence struct {
	ReceiptID    string
	Hostname     string
	Scope        string
	EdgeID       string
	Publication  DNSDecisionPublication
	ObservedAt   time.Time
	PrimarySince *time.Time
	Candidates   []model.EdgeDNSAnswerCandidate
	Proofs       []QualityRouteProof
}

func QualityEvidenceFromDNSDecision(receipt DNSDecisionReceipt, now time.Time, maximumAge time.Duration) (QualityAnswerEvidence, error) {
	if maximumAge <= 0 || !receipt.WriteSucceeded || receipt.RCode != 0 || receipt.ObservedAt.IsZero() || receipt.ObservedAt.After(now) || now.Sub(receipt.ObservedAt) > maximumAge || receipt.AnswerPublication == nil {
		return QualityAnswerEvidence{}, errors.New("actual DNS answer is absent, failed or stale")
	}
	result, err := ReplayDNSDecision(receipt)
	if err != nil || !result.Matched {
		return QualityAnswerEvidence{}, errors.New("actual DNS answer replay failed")
	}
	var selected *DNSDecisionRecord
	for index := range result.Records {
		record := &result.Records[index]
		if record.RecordName != receipt.Hostname || len(record.Answered) != 1 {
			continue
		}
		if selected != nil {
			return QualityAnswerEvidence{}, errors.New("actual DNS answer has ambiguous selection stages")
		}
		selected = record
	}
	if selected == nil || selected.Answered[0].EdgeID == "" {
		return QualityAnswerEvidence{}, errors.New("actual DNS answer lacks one physical primary")
	}
	evidence := QualityAnswerEvidence{ReceiptID: receipt.DecisionID, Hostname: receipt.Hostname, Scope: selected.MatchedScopeKey,
		EdgeID: selected.Answered[0].EdgeID, Publication: *receipt.AnswerPublication, ObservedAt: receipt.ObservedAt,
		Candidates: append([]model.EdgeDNSAnswerCandidate(nil), selected.MaterializedCandidates...)}
	if selection := selected.Policy.PhysicalSelection; selected.Policy.PolicyKind == model.DNSAnswerPolicyKindPhysicalQuality && selection != nil {
		if model.ValidateDNSPhysicalSelection(selection) != nil || selection.CapturedAt.After(receipt.ObservedAt) {
			return QualityAnswerEvidence{}, errors.New("actual DNS physical assignment is invalid")
		}
		if selection.PrimaryEdgeID == evidence.EdgeID && selection.Scope == evidence.Scope && selection.PrimarySince != nil {
			primarySince := *selection.PrimarySince
			evidence.PrimarySince = &primarySince
		}
	}
	var input dnsDecisionReplay
	if err := json.Unmarshal(receipt.ReplayInput, &input); err != nil {
		return QualityAnswerEvidence{}, err
	}
	for _, stage := range input.Stages {
		if stage == nil || stage.Readiness == nil || !reflect.DeepEqual(stage.Publication, evidence.Publication) {
			continue
		}
		for _, entry := range stage.Entries {
			if entry.Record.Name != receipt.Hostname {
				continue
			}
			facts := validDNSReadinessFacts(&entry.Plan, stage.Readiness, entry.Facts, now)
			for _, probe := range entry.Plan.Probes {
				fact, present := facts[probe.ID]
				if !present {
					continue
				}
				for _, candidate := range evidence.Candidates {
					if candidate.EdgeID == probe.EdgeID && candidate.EdgeGroupID == probe.EdgeGroupID && candidate.IP == probe.Address {
						evidence.Proofs = append(evidence.Proofs, QualityRouteProof{EdgeID: probe.EdgeID, EdgeGroupID: probe.EdgeGroupID, Hostname: probe.Hostname, Path: probe.Path, Proof: fact.Proof})
						break
					}
				}
			}
		}
	}
	if len(evidence.Proofs) == 0 {
		return QualityAnswerEvidence{}, errors.New("actual DNS answer has no fresh route evidence")
	}
	return evidence, nil
}
