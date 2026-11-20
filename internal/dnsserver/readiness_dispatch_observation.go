package dnsserver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sort"
	"time"

	"fugue/internal/dnsroutesource"
	"fugue/internal/platformconfig"
)

const dnsProbeDispatchLimit = 24

type dnsProbeDispatchTiming struct {
	previousExpiry, dispatched, started, finished time.Time
	priority                                      int
}

type dnsProbeDispatchSample struct {
	RequirementDigest  string    `json:"requirement_digest"`
	PhysicalDigest     string    `json:"physical_digest"`
	QueuePosition      int       `json:"queue_position"`
	CandidatePriority  int       `json:"candidate_priority,omitempty"`
	PriorExpiry        time.Time `json:"prior_scheduler_expiry,omitzero"`
	DispatchedAt       time.Time `json:"dispatched_at,omitzero"`
	StartedAt          time.Time `json:"started_at,omitzero"`
	FinishedAt         time.Time `json:"finished_at,omitzero"`
	RetryAtStop        time.Time `json:"target_retry_at_stop,omitzero"`
	ObservedProofUntil time.Time `json:"scheduler_proof_until,omitzero"`
	Result             string    `json:"scheduler_result"`
}

type dnsProbeDispatchSnapshot struct {
	StoppedAt time.Time                `json:"stopped_at"`
	StopState string                   `json:"stop_state"`
	Omitted   int                      `json:"omitted"`
	Samples   []dnsProbeDispatchSample `json:"samples"`
}

// This is a bounded sample of the scheduler's actual input and output, not a
// new fact or an assertion about a later query's selected publication. An
// authority variant can share a physical request without sharing its proof.
func (s *dnsReadinessScheduler) probeDispatchObservation(plan *platformconfig.DNSReadinessPlan, facts []dnsReadinessFact, timing []dnsProbeDispatchTiming, queues []dnsProbeTargetQueue, stopped time.Time, stopErr error) dnsProbeDispatchSnapshot {
	type member struct {
		index, queue, position int
		expires                time.Time
	}
	var out dnsProbeDispatchSnapshot
	out.StoppedAt = stopped
	switch {
	case errors.Is(stopErr, context.DeadlineExceeded):
		out.StopState = "deadline"
	case stopErr != nil:
		out.StopState = "canceled"
	default:
		out.StopState = "no_eligible_dispatch"
	}
	items := make([]member, 0, len(facts))
	for q, queue := range queues {
		for position, i := range queue.indices {
			expiry := timing[i].previousExpiry
			if timing[i].dispatched.IsZero() {
				// An unstarted item never had its expiry updated this sweep.
				expiry = s.expires[plan.Probes[i].ID]
			}
			items = append(items, member{i, q, position + 1, expiry})
		}
	}
	if stopErr == nil {
		allDispatched := true
		for _, queue := range queues {
			allDispatched = allDispatched && queue.next == len(queue.indices)
		}
		if allDispatched {
			out.StopState = "all_dispatched"
		}
	}
	// Retain the earliest prior expiries first; where no expiry is known, prefer
	// undispatched and rejected work. Exactly tied work follows queue order.
	rank := func(m member) int {
		if !m.expires.IsZero() {
			return 0
		}
		if timing[m.index].dispatched.IsZero() {
			return 1
		}
		if !facts[m.index].Ready {
			return 2
		}
		return 3
	}
	sort.SliceStable(items, func(i, j int) bool {
		a, b := items[i], items[j]
		if ra, rb := rank(a), rank(b); ra != rb {
			return ra < rb
		}
		return a.expires.Before(b.expires)
	})
	count := min(len(items), dnsProbeDispatchLimit)
	out.Omitted = len(items) - count
	out.Samples = make([]dnsProbeDispatchSample, 0, count)
	digest := func(value string) string {
		sum := sha256.Sum256([]byte(value))
		return hex.EncodeToString(sum[:12])
	}
	for _, m := range items[:count] {
		i := m.index
		probe, times := plan.Probes[i], timing[i]
		result := "probe_rejected"
		var observedProofUntil time.Time
		switch {
		case times.dispatched.IsZero():
			result = "not_dispatched"
		case facts[i].Ready:
			result = "scheduler_proof_observed"
			observedProofUntil = facts[i].Proof.ValidUntil
		case facts[i].Reason == "probe_unavailable":
			result = "probe_unavailable"
		case facts[i].Reason == "traffic_release_mismatch":
			result = "traffic_release_mismatch"
		}
		out.Samples = append(out.Samples, dnsProbeDispatchSample{
			RequirementDigest: digest(probe.ID), PhysicalDigest: digest(dnsroutesource.ProbeKey(probe)),
			QueuePosition: m.position, CandidatePriority: times.priority, PriorExpiry: m.expires,
			DispatchedAt: times.dispatched, StartedAt: times.started, FinishedAt: times.finished,
			RetryAtStop: s.targets[queues[m.queue].address].retryAt, Result: result, ObservedProofUntil: observedProofUntil,
		})
	}
	return out
}
