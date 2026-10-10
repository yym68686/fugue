package edgequality

import (
	"errors"
	"time"

	"fugue/internal/clientmeasurement"
	"fugue/internal/model"
)

func ClientProbeObservations(snapshot Snapshot) ([]Observation, error) {
	observations := []Observation{}
	if len(snapshot.ClientProbeReports) > 0 && snapshot.Policy.Version != DeliveryNetworkPolicyVersion {
		return nil, errors.New("client measurement reports require the delivery evaluator")
	}
	candidates := map[string]Candidate{}
	for _, candidate := range snapshot.Candidates {
		candidates[candidate.EdgeID] = candidate
	}
	seen := map[string]bool{}
	for _, report := range snapshot.ClientProbeReports {
		cohort, err := clientmeasurement.ValidateReport(report)
		if err != nil {
			return nil, err
		}
		if seen[report.Plan.RoundID] {
			return nil, errors.New("duplicate client probe report")
		}
		seen[report.Plan.RoundID] = true
		if cohort == "" || snapshot.Scope != "global" && snapshot.Scope != cohort {
			continue
		}
		outcomes := map[string]model.EdgeClientProbeOutcome{}
		for _, outcome := range report.Outcomes {
			outcomes[outcome.AttemptID] = outcome
		}
		for _, permit := range report.Plan.Permits {
			candidate, exists := candidates[permit.EdgeID]
			outcome := outcomes[permit.AttemptID]
			if !exists || permit.Hostname != snapshot.Hostname || permit.TrafficClass != snapshot.TrafficClass || permit.RouteDigest != candidate.RouteGeneration || permit.EdgeGroupID != candidate.EdgeGroupID || !proofFresh(candidate, snapshot.Policy, snapshot.CapturedAt) ||
				outcome.CompletedAt.After(snapshot.CapturedAt) || outcome.CompletedAt.Before(snapshot.CapturedAt.Add(-time.Duration(snapshot.Policy.WindowSeconds)*time.Second)) {
				continue
			}
			observation := Observation{ID: "client-probe:" + permit.AttemptID, ClientProbeRoundID: report.Plan.RoundID, EdgeID: permit.EdgeID, Hostname: permit.Hostname, TrafficClass: permit.TrafficClass,
				Scope: snapshot.Scope, RouteGeneration: permit.RouteDigest, ObservedAt: outcome.CompletedAt, ClientSource: "authenticated_client_probe", ClientCohort: cohort}
			if outcome.Attestation != nil {
				observation.ClientNetworkMS = outcome.Attestation.ClientNetwork.RTTMS
			}
			switch outcome.Failure {
			case "":
				rate, failed := float64(outcome.BytesReceived)/outcome.BodySeconds, 0.0
				observation.DownloadBPS, observation.ClientFailureRate = &rate, &failed
			case "connect":
				failed := 1.0
				observation.ClientFailureRate = &failed
			case "body":
				if outcome.Attestation != nil {
					failed := 1.0
					observation.ClientFailureRate = &failed
				}
			}
			if observation.ClientNetworkMS != nil || observation.ClientFailureRate != nil {
				observations = append(observations, observation)
			}
		}
	}
	return observations, nil
}

func measuredClientSource(source string) bool {
	return source == "public_tcp_info" || source == "authenticated_client_probe"
}
