package store

import (
	"context"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
)

// These fixed phases partition admission wall time, not CPU time. They never
// enter an authorization decision or outlive the enclosing mutation receipt.
type platformAdmissionPhase uint8

const (
	admissionSetup platformAdmissionPhase = iota
	admissionExpiry
	admissionComposition
	admissionCohort
	admissionDNSReferences
	admissionMemberIntegrity
	admissionSourceAuthorization
	admissionPhysicalPolicy
	admissionLineage
	admissionTopology
	admissionCapabilityProjection
	admissionCapabilityFacts
	admissionCompleteness
	admissionPhaseCount
)

var platformAdmissionPhaseNames = [admissionPhaseCount]string{
	"setup", "expiry", "composition", "cohort", "dns_references",
	"member_integrity", "source_authorization", "physical_policy", "lineage",
	"topology", "capability_projection", "capability_facts", "completeness",
}

const platformAdmissionObservationLimit = 4

type platformAdmissionObservation struct {
	Started               time.Time               `json:"started_at"`
	Finished              time.Time               `json:"finished_at"`
	Failed                bool                    `json:"failed"`
	FinalPhase            string                  `json:"final_phase"`
	Stages                []platformMutationStage `json:"stages"`
	DNSReferences         []platformMutationStage `json:"dns_reference_stages,omitempty"`
	Artifacts             int                     `json:"artifacts"`
	ExpectedSets          int                     `json:"expected_sets"`
	ServingFacts          int                     `json:"serving_facts"`
	ShadowFacts           int                     `json:"shadow_facts"`
	RequiredConsumers     int                     `json:"required_consumers"`
	CapabilityProjections int                     `json:"capability_projections"`
	CapabilityFacts       int                     `json:"capability_facts_examined"`
}

type platformAdmissionTrace struct {
	mutation    *platformMutationTrace
	value       platformAdmissionObservation
	phase       platformAdmissionPhase
	last        time.Time
	duration    [admissionPhaseCount]time.Duration
	visited     [admissionPhaseCount]bool
	dnsDuration [dnsReferencePhaseCount]time.Duration
	dnsVisited  [dnsReferencePhaseCount]bool
}

type platformDNSReferencePhase uint8

const (
	dnsReferenceAuthentication platformDNSReferencePhase = iota
	dnsReferenceProducerBindings
	dnsReferenceDependencyDecode
	dnsReferenceSourceAuthorization
	dnsReferenceSourceObservation
	dnsReferenceDependencyLedger
	dnsReferencePhaseCount
)

var platformDNSReferencePhaseNames = [dnsReferencePhaseCount]string{
	"artifact_authentication", "producer_bindings", "dependency_decode",
	"source_authorization", "source_observation", "dependency_ledger",
}

// Nested wall-clock stages share the enclosing admission's clock and lifetime.
// The fixed array bounds retention regardless of artifact or dependency counts.
type platformDNSReferenceTrace struct {
	admission *platformAdmissionTrace
	phase     platformDNSReferencePhase
	last      time.Time
	duration  [dnsReferencePhaseCount]time.Duration
	visited   [dnsReferencePhaseCount]bool
}

func beginPlatformDNSReferenceTrace(admission *platformAdmissionTrace) *platformDNSReferenceTrace {
	if admission == nil {
		return nil
	}
	return &platformDNSReferenceTrace{admission: admission, last: admission.mutation.now()}
}

func (t *platformDNSReferenceTrace) advance(phase platformDNSReferencePhase) {
	if t == nil {
		return
	}
	now := t.admission.mutation.now()
	t.duration[t.phase] += now.Sub(t.last)
	t.visited[t.phase] = true
	t.phase, t.last = phase, now
}

func (t *platformDNSReferenceTrace) finish() {
	if t == nil {
		return
	}
	t.duration[t.phase] += t.admission.mutation.now().Sub(t.last)
	t.visited[t.phase] = true
	for phase, duration := range t.duration {
		if t.visited[phase] {
			t.admission.dnsDuration[phase] += duration
			t.admission.dnsVisited[phase] = true
		}
	}
}

func beginPlatformAdmissionTrace(ctx context.Context, state *storeProjection) *platformAdmissionTrace {
	mutation, _ := ctx.Value(platformMutationTraceKey{}).(*platformMutationTrace)
	if mutation == nil {
		return nil
	}
	if len(mutation.admissions) >= platformAdmissionObservationLimit {
		mutation.admissionsOmitted++
		return nil
	}
	now := mutation.now()
	t := &platformAdmissionTrace{mutation: mutation, last: now, value: platformAdmissionObservation{Started: now}}
	if state != nil {
		t.value.Artifacts, t.value.ExpectedSets = len(state.PlatformArtifacts), len(state.ExpectedConsumerSets)
		t.value.ServingFacts, t.value.ShadowFacts = len(state.PlatformConsumerInstances), len(state.PlatformShadowConsumers)
	}
	return t
}

func (t *platformAdmissionTrace) advance(phase platformAdmissionPhase) {
	if t == nil {
		return
	}
	now := t.mutation.now()
	t.duration[t.phase] += now.Sub(t.last)
	t.visited[t.phase] = true
	t.phase, t.last = phase, now
}

func (t *platformAdmissionTrace) finish(err error) {
	if t == nil {
		return
	}
	now := t.mutation.now()
	t.duration[t.phase] += now.Sub(t.last)
	t.visited[t.phase] = true
	t.value.Finished, t.value.Failed = now, err != nil
	t.value.FinalPhase = platformAdmissionPhaseNames[t.phase]
	for phase, duration := range t.duration {
		if t.visited[phase] {
			t.value.Stages = append(t.value.Stages, platformMutationStage{platformAdmissionPhaseNames[phase], duration.Milliseconds()})
		}
	}
	for phase, duration := range t.dnsDuration {
		if t.dnsVisited[phase] {
			t.value.DNSReferences = append(t.value.DNSReferences,
				platformMutationStage{platformDNSReferencePhaseNames[phase], duration.Milliseconds()})
		}
	}
	t.mutation.admissions = append(t.mutation.admissions, t.value)
}

func validateLeasedTrafficAdmissionObserved(ctx context.Context, state *storeProjection, parent model.PlatformArtifact, channel, canaryRef string, keys bundleauth.Keyring, now time.Time) error {
	return validateLeasedTrafficAdmissionWithTrace(state, parent, channel, canaryRef, keys, now, false, beginPlatformAdmissionTrace(ctx, state))
}

func validateVerifiedTrafficRecoveryAdmissionObserved(ctx context.Context, state *storeProjection, parent model.PlatformArtifact, channel, canaryRef string, keys bundleauth.Keyring, now time.Time, lkg *model.PlatformLKGSnapshot, guard *platformProducerReleaseGuard) error {
	return validateLeasedTrafficAdmissionWithTrace(state, parent, channel, canaryRef, keys, now,
		verifiedTrafficRecovery(parent, channel, keys, now, lkg, guard), beginPlatformAdmissionTrace(ctx, state))
}
