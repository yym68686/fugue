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
	Artifacts             int                     `json:"artifacts"`
	ExpectedSets          int                     `json:"expected_sets"`
	ServingFacts          int                     `json:"serving_facts"`
	ShadowFacts           int                     `json:"shadow_facts"`
	RequiredConsumers     int                     `json:"required_consumers"`
	CapabilityProjections int                     `json:"capability_projections"`
	CapabilityFacts       int                     `json:"capability_facts_examined"`
}

type platformAdmissionTrace struct {
	mutation *platformMutationTrace
	value    platformAdmissionObservation
	phase    platformAdmissionPhase
	last     time.Time
	duration [admissionPhaseCount]time.Duration
	visited  [admissionPhaseCount]bool
}

func beginPlatformAdmissionTrace(ctx context.Context, state *model.State) *platformAdmissionTrace {
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
	t.mutation.admissions = append(t.mutation.admissions, t.value)
}

func validateLeasedTrafficAdmissionObserved(ctx context.Context, state *model.State, parent model.PlatformArtifact, channel, canaryRef string, keys bundleauth.Keyring, now time.Time) error {
	return validateLeasedTrafficAdmissionWithTrace(state, parent, channel, canaryRef, keys, now, false, beginPlatformAdmissionTrace(ctx, state))
}

func validateVerifiedTrafficRecoveryAdmissionObserved(ctx context.Context, state *model.State, parent model.PlatformArtifact, channel, canaryRef string, keys bundleauth.Keyring, now time.Time, lkg *model.PlatformLKGSnapshot, guard *platformProducerReleaseGuard) error {
	return validateLeasedTrafficAdmissionWithTrace(state, parent, channel, canaryRef, keys, now,
		verifiedTrafficRecovery(parent, channel, keys, now, lkg, guard), beginPlatformAdmissionTrace(ctx, state))
}
