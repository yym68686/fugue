package dnsserver

import (
	"errors"
	"reflect"
	"time"

	"fugue/internal/cellpublication"
	"fugue/internal/config"
	"fugue/internal/dnsfacts"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

// Transport still checks the current publication fences and exact Pod/Service
// identities. This combines fresh actual serving readiness with immutable
// handoff equivalence (or one explicitly declared availability addition).
func VerifyServingAuthorityHandoff(configuration config.DNSConfig, checkpoint []byte, snapshot dnsfacts.Snapshot, previous model.PlatformArtifact, availability *platformconfig.DNSAvailabilityRoute, now time.Time) error {
	if err := VerifyServingObservation(configuration, checkpoint, snapshot, now); err != nil {
		return err
	}
	service := &Service{Config: configuration}
	record, err := service.decodeDNSCheckpoint(checkpoint)
	if err != nil {
		return errors.New("DNS handoff checkpoint authentication failed")
	}
	if availability != nil {
		return cellpublication.VerifyDNSAvailabilityHandoff(previous, record.Candidate.Artifact, configuration.DNSNodeID, service.platformDNSKeys(), *availability)
	}
	return cellpublication.VerifyDNSAuthorityHandoff(previous, record.Candidate.Artifact, configuration.DNSNodeID, service.platformDNSKeys())
}

func VerifyServingObservation(configuration config.DNSConfig, checkpoint []byte, snapshot dnsfacts.Snapshot, now time.Time) error {
	service := &Service{Config: configuration}
	record, err := service.decodeDNSCheckpoint(checkpoint)
	if err != nil {
		return errors.New("DNS observation checkpoint authentication failed")
	}
	payload, routeID, err := service.verifiedDNSCheckpointPayload(record)
	if err != nil {
		return errors.New("DNS observation artifact authentication failed")
	}
	planDigest, err := platformconfig.Digest(payload.Plan)
	until := record.AppliedAt.Add(time.Duration(payload.Policy.MaxStaleSeconds) * time.Second)
	if err != nil || snapshot.Schema != dnsfacts.Schema || snapshot.NodeID != record.NodeID || snapshot.EdgeGroupID != record.GroupID ||
		snapshot.PlanDigest != planDigest || snapshot.ParentDigest != record.Parent.ContentHash || snapshot.RouteArtifactID != routeID ||
		!reflect.DeepEqual(snapshot.Assignment, record.Candidate.Assignment) || !reflect.DeepEqual(snapshot.RouteSources, record.RouteSources) ||
		!snapshot.CheckpointValidUntil.Equal(until) || !until.After(now) || record.AppliedAt.After(now) ||
		snapshot.ObservedAt.IsZero() || snapshot.ObservedAt.After(snapshot.EvaluatedAt) || snapshot.EvaluatedAt.After(now) ||
		now.Sub(snapshot.EvaluatedAt) > time.Duration(payload.Policy.DNSReadiness.ProbeIntervalSeconds)*time.Second || !snapshot.Ready ||
		len(snapshot.Facts) != len(payload.Plan.Probes) {
		return errors.New("DNS observation authority or freshness mismatch")
	}
	requirements := make(map[string]platformconfig.DNSReadinessProbe, len(payload.Plan.Probes))
	for _, requirement := range payload.Plan.Probes {
		requirements[requirement.ID] = requirement
	}
	seen := map[string]bool{}
	facts := make([]dnsReadinessFact, 0, len(snapshot.Facts))
	for _, observed := range snapshot.Facts {
		requirement, exists := requirements[observed.ProbeID]
		if !exists || seen[observed.ProbeID] {
			return errors.New("DNS observation proof membership mismatch")
		}
		seen[observed.ProbeID] = true
		fact := dnsReadinessFact{ProbeID: observed.ProbeID, Proof: observed.Proof}
		if observed.Ready {
			fact = evaluateDNSReadinessFact(requirement, payload.Policy.DNSReadiness, observed.Proof, nil, now)
			if !fact.Ready || !reflect.DeepEqual(fact.Proof, observed.Proof) || observed.Proof.CheckedAt.After(snapshot.EvaluatedAt) ||
				!dnsFactMatchesRelease(fact, record.Parent, record.Candidate, routeID, payload) {
				return errors.New("DNS observation contains an invalid positive proof")
			}
		}
		if len(observed.ProbeHistory) > dnsProbeHistoryLimit {
			return errors.New("DNS observation recovery history exceeds bound")
		}
		for i, at := range observed.ProbeHistory {
			if at.IsZero() || at.After(observed.Proof.CheckedAt) || i > 0 && !at.After(observed.ProbeHistory[i-1]) {
				return errors.New("DNS observation recovery history is invalid")
			}
		}
		if len(observed.ProbeHistory) > 0 && !observed.ProbeHistory[len(observed.ProbeHistory)-1].Equal(observed.Proof.CheckedAt) {
			return errors.New("DNS observation recovery history belongs to another proof")
		}
		fact.ProbeHistory = append([]time.Time(nil), observed.ProbeHistory...)
		facts = append(facts, fact)
	}
	boundDNSRouteSourceFacts(payload, facts, now)
	state, err := buildDNSServingState(record, payload, routeID, configuration.DNSNodeID, configuration.EdgeGroupID, facts, snapshot.ObservedAt)
	if err != nil || !dnsServingReady(state, now) {
		return errors.New("DNS observation lacks complete record readiness")
	}
	if err := probeDNSServingSnapshot(state); err != nil {
		return errors.New("DNS observation cannot materialize all declared answers")
	}
	return nil
}
