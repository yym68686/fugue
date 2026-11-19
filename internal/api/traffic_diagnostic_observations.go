package api

import (
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformcontrol"
)

const trafficFailureLimit = 12
const trafficAssessmentLimit = 8

// Local-only receipts preserve the assessment used by a failed read, not a
// later reconstruction from mutable consumer heartbeats. No additional I/O.
type trafficDiagnosticFailure struct {
	Sequence    uint64                        `json:"sequence"`
	ObservedAt  time.Time                     `json:"observed_at"`
	Stage       string                        `json:"stage"`
	Kind        string                        `json:"artifact_kind,omitempty"`
	Publication *capturePublication           `json:"publication,omitempty"`
	ExpectedSet string                        `json:"expected_set_hash,omitempty"`
	Revision    int64                         `json:"revision,omitempty"`
	EvaluatedAt time.Time                     `json:"evaluated_at"`
	Required    int                           `json:"required"`
	Passing     int                           `json:"passing"`
	Unexpected  int                           `json:"unexpected"`
	Assessments []trafficDiagnosticAssessment `json:"assessments,omitempty"`
	Omitted     int                           `json:"assessments_omitted"`
}

type trafficDiagnosticAssessment struct {
	Consumer    string    `json:"consumer_hash"`
	Required    bool      `json:"required"`
	State       string    `json:"state"`
	Reasons     []string  `json:"reasons"`
	Observed    bool      `json:"observed"`
	HeartbeatAt time.Time `json:"heartbeat_at"`
	FreshUntil  time.Time `json:"fresh_until"`
	ReleaseSet  string    `json:"release_set_hash,omitempty"`
	ExpectedSet string    `json:"expected_set_hash,omitempty"`
	Fence       int64     `json:"fencing_token"`
	Generation  int64     `json:"generation_sequence"`
}

func trafficDiagnosticState(value string) string {
	switch value {
	case model.InvariantEvidenceStatePass, model.InvariantEvidenceStateFail, model.InvariantEvidenceStateStale, model.InvariantEvidenceStateUnknown:
		return value
	default:
		return "unrecognized"
	}
}

func trafficDiagnosticReason(value string) string {
	switch value {
	case "consumer heartbeat is missing":
		return "heartbeat_missing"
	case "heartbeat observed_at is missing":
		return "heartbeat_timestamp_missing"
	case "consumer heartbeat is stale":
		return "heartbeat_stale"
	case "consumer identity does not match the expected component, node, artifact, or scope":
		return "identity_mismatch"
	case "consumer reports an expired LKG":
		return "lkg_expired"
	case "desired, actual, and LKG generations must all be reported":
		return "generation_missing"
	case "desired or applied candidate generation does not match the expected generation":
		return "generation_mismatch"
	case "apply status is not applied":
		return "not_applied"
	case "probe status is not passed":
		return "probe_not_passed"
	case "consumer protocol version is missing or incompatible":
		return "protocol_incompatible"
	case "consumer schema version is missing or incompatible":
		return "schema_incompatible"
	case "authoritative release and artifact binding unavailable":
		return "authority_unavailable"
	case "consumer is outside the signed traffic canary cohort":
		return "outside_canary"
	case "consumer identity is not verified":
		return "identity_unverified"
	case "consumer receipt belongs to another expectation or ReleaseSet":
		return "publication_mismatch"
	case "consumer receipt fence or artifact sequence does not match the active release":
		return "fence_or_sequence_mismatch"
	case "consumer receipt lacks trusted sequence or evidence identity":
		return "evidence_identity_missing"
	case "dns_public_backend_mismatch", "dns_public_backend_unavailable", "dns_declared_backend_mismatch", "dns_declared_backend_unavailable":
		return value
	default:
		if strings.HasPrefix(value, "consumer compatibility capability is missing: ") {
			return "capability_missing"
		}
		return "unrecognized"
	}
}

func (s *Server) retainTrafficDiagnosticFailure(receipt trafficDiagnosticFailure) uint64 {
	s.captures.mu.Lock()
	defer s.captures.mu.Unlock()
	s.captures.trafficFailureTotal++
	receipt.Sequence = s.captures.trafficFailureTotal
	receipt.ObservedAt = time.Now().UTC()
	if len(s.captures.trafficFailures) == trafficFailureLimit {
		copy(s.captures.trafficFailures, s.captures.trafficFailures[1:])
		s.captures.trafficFailures = s.captures.trafficFailures[:trafficFailureLimit-1]
		s.captures.trafficFailureEvicted++
	}
	s.captures.trafficFailures = append(s.captures.trafficFailures, receipt)
	return receipt.Sequence
}

func trafficConvergenceObservation(set model.PlatformExpectedConsumerSet, binding *platformcontrol.ConsumerReleaseBinding, status model.PlatformConsumerConvergenceStatus) trafficDiagnosticFailure {
	r := trafficDiagnosticFailure{Kind: set.ArtifactKind, ExpectedSet: captureIdentityHash(set.ID), Revision: set.Revision,
		EvaluatedAt: status.EvaluatedAt, Required: status.RequiredExpected, Passing: status.RequiredPassing, Unexpected: len(status.UnexpectedConsumers)}
	if binding != nil {
		publication := capturePublicationReference(binding.ReleaseSetID, "", binding.ArtifactReleaseID, binding.ReleaseChannel, binding.FencingToken)
		r.Publication = &publication
	}
	for _, a := range status.Assessments {
		if a.State == model.InvariantEvidenceStatePass {
			continue
		}
		if len(r.Assessments) == trafficAssessmentLimit {
			r.Omitted++
			continue
		}
		row := trafficDiagnosticAssessment{Consumer: captureIdentityHash(a.ConsumerID), Required: a.Required, State: trafficDiagnosticState(a.State)}
		for _, reason := range a.Reasons {
			code := trafficDiagnosticReason(reason)
			duplicate := false
			for _, prior := range row.Reasons {
				duplicate = duplicate || prior == code
			}
			if !duplicate {
				row.Reasons = append(row.Reasons, code)
			}
		}
		if a.FreshUntil != nil {
			row.FreshUntil = *a.FreshUntil
		}
		if a.Observed != nil {
			v := a.Observed
			row.Observed, row.HeartbeatAt = true, v.LastHeartbeatAt
			row.ReleaseSet, row.ExpectedSet = captureIdentityHash(v.ReleaseSetID), captureIdentityHash(v.ExpectedConsumerSetID)
			row.Fence, row.Generation = v.FencingToken, v.GenerationSequence
		}
		r.Assessments = append(r.Assessments, row)
	}
	return r
}
