package store

import (
	"context"
	"time"

	"fugue/internal/platformproducer"
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
	FinalPhase string                           `json:"final_phase"`
	Stages     []platformMutationStage          `json:"stages"`
	DNSOutput  []platformProducerDNSOutputStage `json:"dns_output_stages,omitempty"`
	DNSCache   *platformProducerDNSCacheCounts  `json:"dns_output_cache,omitempty"`
}

type platformProducerDNSOutputStage struct {
	Stage     string `json:"stage"`
	Calls     uint64 `json:"calls"`
	ElapsedMS int64  `json:"elapsed_ms"`
	duration  time.Duration
}

type platformProducerDNSCacheCounts struct {
	Hit           uint64 `json:"hit"`
	Miss          uint64 `json:"miss"`
	NotApplicable uint64 `json:"not_applicable"`
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

func (t *platformProducerGuardTrace) dnsOutputObserver() platformproducer.DNSOutputObserver {
	if t == nil {
		return nil
	}
	return t.observeDNSOutput
}

func (t *platformProducerGuardTrace) observeDNSOutput(stage platformproducer.DNSOutputValidationStage, elapsed time.Duration, cache platformproducer.DNSOutputCacheResult) {
	if stage >= platformproducer.DNSOutputStageCount {
		return
	}
	slot := &t.mutation.producerDNSOutput[stage]
	slot.Calls++
	slot.duration += elapsed
	switch cache {
	case platformproducer.DNSOutputCacheHit:
		t.mutation.producerDNSCache.Hit++
	case platformproducer.DNSOutputCacheMiss:
		t.mutation.producerDNSCache.Miss++
	case platformproducer.DNSOutputCacheNotApplicable:
		t.mutation.producerDNSCache.NotApplicable++
	}
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
	for stage, slot := range t.producerDNSOutput {
		if slot.Calls > 0 {
			out.DNSOutput = append(out.DNSOutput, platformProducerDNSOutputStage{
				Stage: platformproducer.DNSOutputValidationStage(stage).String(),
				Calls: slot.Calls, ElapsedMS: slot.duration.Milliseconds(),
			})
		}
	}
	if t.producerDNSCache != (platformProducerDNSCacheCounts{}) {
		cache := t.producerDNSCache
		out.DNSCache = &cache
	}
	return out
}
