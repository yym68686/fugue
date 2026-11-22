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
	return verifyDNSExecutorObservation(configuration, checkpoint, snapshot, now, servingObservation)
}

// VerifyUnreadyPredecessorObservation verifies the authority and probe membership
// of a degraded executor without treating it as a verified serving backend.
// Callers still need independent wire evidence tied to the selected listener;
// this verification never authorizes selection, rollback or a positive ACK.
func VerifyUnreadyPredecessorObservation(configuration config.DNSConfig, checkpoint []byte, snapshot dnsfacts.Snapshot, now time.Time) error {
	return verifyDNSExecutorObservation(configuration, checkpoint, snapshot, now, unreadyObservation)
}

// VerifyExpiredPredecessorObservation establishes only historical identity and
// a current observation of expiry. It cannot establish readiness or select a
// replacement. In particular an observation taken *before* expiry is invalid.
func VerifyExpiredPredecessorObservation(configuration config.DNSConfig, checkpoint []byte, snapshot dnsfacts.Snapshot, now time.Time) error {
	return verifyDNSExecutorObservation(configuration, checkpoint, snapshot, now, expiredObservation)
}

type executorObservationMode uint8

const (
	servingObservation executorObservationMode = iota
	unreadyObservation
	expiredObservation
)

func verifyDNSExecutorObservation(configuration config.DNSConfig, checkpoint []byte, snapshot dnsfacts.Snapshot, now time.Time, mode executorObservationMode) error {
	service := &Service{Config: configuration}
	record, payload, routeID, err := service.decodeDNSCheckpointPayload(checkpoint)
	if err != nil {
		return errors.New("DNS observation checkpoint authentication failed")
	}
	planDigest, err := platformconfig.Digest(payload.Plan)
	until := record.AppliedAt.Add(time.Duration(payload.Policy.MaxStaleSeconds) * time.Second)
	if err != nil || snapshot.Schema != dnsfacts.Schema || snapshot.NodeID != record.NodeID || snapshot.EdgeGroupID != record.GroupID ||
		snapshot.PlanDigest != planDigest || snapshot.ParentDigest != record.Parent.ContentHash || snapshot.RouteArtifactID != routeID ||
		!reflect.DeepEqual(snapshot.Assignment, record.Candidate.Assignment) || !reflect.DeepEqual(snapshot.RouteSources, record.RouteSources) ||
		!snapshot.CheckpointValidUntil.Equal(until) || len(snapshot.Facts) != len(payload.Plan.Probes) {
		return errors.New("DNS observation authority mismatch")
	}
	if record.AppliedAt.After(now) || snapshot.ObservedAt.IsZero() ||
		snapshot.ObservedAt.After(snapshot.EvaluatedAt) || snapshot.EvaluatedAt.After(now) {
		return errors.New("DNS observation temporal ordering mismatch")
	}
	if now.Sub(snapshot.EvaluatedAt) > time.Duration(payload.Policy.DNSReadiness.ProbeIntervalSeconds)*time.Second ||
		(mode != expiredObservation && !until.After(now)) {
		return errors.New("DNS observation has expired")
	}
	if mode == expiredObservation {
		if until.After(snapshot.EvaluatedAt) || snapshot.Ready || snapshot.RouteSources != nil ||
			snapshot.Assignment.ScopeKey != "global" || len(payload.Plan.Probes) == 0 {
			return errors.New("DNS predecessor is not an expired unready global checkpoint")
		}
	}
	requireReady := mode == servingObservation
	if requireReady && !snapshot.Ready {
		return errors.New("DNS observation lacks complete record readiness")
	}
	if !requireReady && snapshot.Ready {
		return errors.New("DNS predecessor observation reports ready")
	}
	evaluateAt := now
	if !requireReady {
		// Do not turn proof expiry in transit into evidence that the
		// predecessor was broken when the runtime snapshot was captured.
		evaluateAt = snapshot.EvaluatedAt
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
			if mode == expiredObservation {
				return errors.New("expired DNS predecessor reports a positive proof")
			}
			// A positive claim must be valid at capture, including its original
			// expiry and release binding. In transit a redundant endpoint may
			// expire; drop its readiness without renewing its lease or rejecting
			// another endpoint's still-valid proof. The final materialization
			// independently requires every record's quorum at verification time.
			fact = evaluateDNSReadinessFact(requirement, payload.Policy.DNSReadiness, observed.Proof, nil, snapshot.EvaluatedAt)
			if !fact.Ready || !reflect.DeepEqual(fact.Proof, observed.Proof) ||
				!dnsFactMatchesRelease(fact, record.Parent, record.Candidate, routeID, payload) {
				return errors.New("DNS observation contains an invalid positive proof")
			}
			fact = evaluateDNSReadinessFact(requirement, payload.Policy.DNSReadiness, observed.Proof, nil, evaluateAt)
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
	if mode == expiredObservation {
		return nil // No positive facts, exact authenticated membership; no serving authority.
	}
	boundDNSRouteSourceFacts(payload, facts, evaluateAt)
	state, err := buildDNSServingState(record, payload, routeID, configuration.DNSNodeID, configuration.EdgeGroupID, facts, snapshot.ObservedAt)
	if err != nil {
		return errors.New("DNS observation lacks complete record readiness")
	}
	if !requireReady {
		if dnsServingReady(state, evaluateAt) && checkDNSServingSnapshot(state, evaluateAt, false) == nil {
			return errors.New("DNS predecessor has complete record readiness")
		}
		return nil
	}
	if !dnsServingReady(state, now) {
		return errors.New("DNS observation lacks complete record readiness")
	}
	if err := probeDNSServingSnapshot(state); err != nil {
		return errors.New("DNS observation cannot materialize all declared answers")
	}
	return nil
}
