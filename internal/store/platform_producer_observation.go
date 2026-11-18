package store

import (
	"context"
	"time"
)

// These slots partition existing guard wall time, not CPU or lock hold time.
// They retain no input, identity, policy value, SQL or error text.
type platformProducerGuardPhase uint8

const (
	producerGuardReadLanes platformProducerGuardPhase = iota
	producerGuardReadLKG
	producerGuardReadPolicy
	producerGuardReadSources
	producerGuardReadMembers
	producerGuardReadDNSReferences
	producerGuardParent
	producerGuardPolicy
	producerGuardSourceAuthority
	producerGuardStaticSources
	producerGuardComposition
	producerGuardMemberIntegrity
	producerGuardDNSOutput
	producerGuardCohort
	producerGuardRoutePlacement
	producerGuardServingPhase
	producerGuardLane
	producerGuardPhaseCount
)

var platformProducerGuardPhaseNames = [producerGuardPhaseCount]string{
	"read_lanes", "read_lkg", "read_policy", "read_sources", "read_members", "read_dns_references",
	"parent_integrity", "policy_authority", "source_authority", "static_sources", "composition",
	"member_integrity", "dns_output", "cohort", "route_placement", "serving_phase", "lane",
}

type platformProducerGuardObservation struct {
	FinalPhase string                  `json:"final_phase"`
	Stages     []platformMutationStage `json:"stages"`
}

type platformProducerGuardTrace struct {
	mutation *platformMutationTrace
	phase    platformProducerGuardPhase
	last     time.Time
}

func beginPlatformProducerGuardTrace(ctx context.Context) *platformProducerGuardTrace {
	mutation, _ := ctx.Value(platformMutationTraceKey{}).(*platformMutationTrace)
	if mutation == nil {
		return nil
	}
	return &platformProducerGuardTrace{mutation: mutation, last: mutation.now()}
}

func (t *platformProducerGuardTrace) advance(phase platformProducerGuardPhase) {
	if t == nil {
		return
	}
	now := t.mutation.now()
	t.mutation.producerGuardDuration[t.phase] += now.Sub(t.last)
	t.mutation.producerGuardVisited[t.phase] = true
	t.phase, t.last = phase, now
}

func (t *platformProducerGuardTrace) finish() {
	if t == nil {
		return
	}
	t.advance(t.phase)
	t.mutation.producerGuardFinalPhase = platformProducerGuardPhaseNames[t.phase]
}

func (t *platformMutationTrace) producerGuardObservation() *platformProducerGuardObservation {
	if t.producerGuardFinalPhase == "" {
		return nil
	}
	out := &platformProducerGuardObservation{FinalPhase: t.producerGuardFinalPhase}
	for phase, duration := range t.producerGuardDuration {
		if t.producerGuardVisited[phase] {
			out.Stages = append(out.Stages, platformMutationStage{platformProducerGuardPhaseNames[phase], duration.Milliseconds()})
		}
	}
	return out
}
