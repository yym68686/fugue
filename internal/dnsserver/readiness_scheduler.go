package dnsserver

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"maps"
	"os"
	"sort"
	"sync"
	"time"

	"fugue/internal/dnsroutesource"
	"fugue/internal/lkgcache"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/routeprobe"
)

type dnsProbeTargetState struct {
	failures int
	retryAt  time.Time
}

type dnsReadinessScheduler struct {
	historyStore *dnsProbeHistoryStore
	mu           sync.Mutex
	scope        string
	digest       string
	cursor       int
	lastStarted  time.Time
	attempts     map[string]time.Time
	expires      map[string]time.Time
	targets      map[string]dnsProbeTargetState
	preferred    map[string]bool
	candidates   map[string]bool
	logger       *log.Logger
	path         string
}

func (scheduler *dnsReadinessScheduler) prioritize(preferred, candidates map[string]bool) *dnsReadinessScheduler {
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	scheduler.preferred = maps.Clone(preferred)
	scheduler.candidates = maps.Clone(candidates)
	return scheduler
}

func (scheduler *dnsReadinessScheduler) bind(assignment model.PlatformConsumerAssignment, logger *log.Logger, paths ...string) *dnsReadinessScheduler {
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	scheduler.scope, _ = platformconfig.Digest(assignment)
	scheduler.logger = logger
	if len(paths) == 1 && paths[0] != "" {
		scheduler.path = paths[0] + ".probe-observations.json"
	}
	return scheduler
}

type dnsProbeTargetQueue struct {
	address  string
	indices  []int
	next     int
	active   int
	started  int
	outcomes map[string]int
	duration time.Duration
}

type dnsProbeResult struct {
	index             int
	target            int
	fact              dnsReadinessFact
	connectionFailure bool
	started           time.Time
	finished          time.Time
}

func (scheduler *dnsReadinessScheduler) collect(ctx context.Context, plan *platformconfig.DNSReadinessPlan, policy *platformconfig.DNSReadinessPolicy, probe dnsReadinessProbeFunc) []dnsReadinessFact {
	return scheduler.collectWithEvaluator(ctx, plan, policy, probe, nil)
}

// A physical source probe can cover multiple authorized publication variants.
// Its scheduler must use the expiry of the matched variant, not the expiry (or
// mismatch) of whichever variant supplied its wire request.
func (scheduler *dnsReadinessScheduler) collectWithEvaluator(ctx context.Context, plan *platformconfig.DNSReadinessPlan, policy *platformconfig.DNSReadinessPolicy, probe dnsReadinessProbeFunc, evaluate func(int, routeprobe.Proof, error, time.Time) dnsReadinessFact) []dnsReadinessFact {
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	started := time.Now().UTC()
	ctx, cancel := context.WithTimeout(ctx, time.Duration(policy.ProbeIntervalSeconds)*time.Second)
	defer cancel()
	digest, _ := platformconfig.Digest(struct {
		Scope  string
		Plan   *platformconfig.DNSReadinessPlan
		Policy *platformconfig.DNSReadinessPolicy
	}{scheduler.scope, plan, policy})
	if scheduler.digest != digest {
		scheduler.digest = digest
		scheduler.cursor = 0
		scheduler.lastStarted = time.Time{}
		scheduler.attempts = map[string]time.Time{}
		scheduler.expires = map[string]time.Time{}
		scheduler.targets = map[string]dnsProbeTargetState{}
	}
	interval := time.Duration(policy.ProbeIntervalSeconds) * time.Second
	refreshHorizon := started.Add(2 * interval)
	if !scheduler.lastStarted.IsZero() {
		// The configured interval bounds a scan, not the time between
		// scans: assignment reads, verification and reports share the loop.
		// Account for the observed spacing plus the next scan's budget,
		// but do not let a long outage dominate later scheduling.
		spacing := min(started.Sub(scheduler.lastStarted), 3*interval)
		refreshHorizon = started.Add(max(interval, spacing) + interval)
	}
	scheduler.lastStarted = started
	observationID, _ := platformconfig.Digest(struct {
		Context string
		Started time.Time
	}{digest, started})
	facts := make([]dnsReadinessFact, len(plan.Probes))
	byAddress := map[string][]int{}
	for index, requirement := range plan.Probes {
		facts[index] = dnsReadinessFact{ProbeID: requirement.ID, ObservationID: observationID, Reason: "not_observed"}
		byAddress[requirement.Address] = append(byAddress[requirement.Address], index)
	}
	addresses := make([]string, 0, len(byAddress))
	for address := range byAddress {
		addresses = append(addresses, address)
	}
	sort.Strings(addresses)
	queues := make([]dnsProbeTargetQueue, 0, len(addresses))
	for _, address := range addresses {
		indices := byAddress[address]
		// Prioritize a bounded batch of explicitly ordered candidate probes
		// in every sweep, even when ordinary proofs have the same deadline.
		// Rotating the oldest attempts first prevents a large pool pinning its
		// same members to the head. The remaining queue keeps normal fairness.
		const maxCandidatePriorityPerTarget = 8
		candidates := make([]int, 0, min(len(indices), maxCandidatePriorityPerTarget))
		if len(scheduler.candidates) != 0 {
			for _, index := range indices {
				if scheduler.candidates[dnsroutesource.ProbeKey(plan.Probes[index])] {
					candidates = append(candidates, index)
				}
			}
		}
		sort.SliceStable(candidates, func(i, j int) bool {
			return scheduler.attempts[plan.Probes[candidates[i]].ID].Before(scheduler.attempts[plan.Probes[candidates[j]].ID])
		})
		priority := make(map[int]int, min(len(candidates), maxCandidatePriorityPerTarget))
		for rank, index := range candidates[:min(len(candidates), maxCandidatePriorityPerTarget)] {
			priority[index] = rank + 1
		}
		sort.SliceStable(indices, func(left, right int) bool {
			if first, second := priority[indices[left]], priority[indices[right]]; first != second {
				return first != 0 && (second == 0 || first < second)
			}
			first, second := plan.Probes[indices[left]].ID, plan.Probes[indices[right]].ID
			firstAttempt, secondAttempt := scheduler.attempts[first], scheduler.attempts[second]
			firstExpiry, secondExpiry := scheduler.expires[first], scheduler.expires[second]
			// Only live observations receive deadline priority. Expired work
			// rejoins the normal bounded fairness rotation.
			firstRefreshDue := firstExpiry.After(started) && !firstExpiry.After(refreshHorizon)
			secondRefreshDue := secondExpiry.After(started) && !secondExpiry.After(refreshHorizon)
			if firstRefreshDue != secondRefreshDue {
				return firstRefreshDue
			}
			if firstRefreshDue && !firstExpiry.Equal(secondExpiry) {
				return firstExpiry.Before(secondExpiry)
			}
			firstAged := firstAttempt.IsZero() || started.Sub(firstAttempt) >= 2*interval
			secondAged := secondAttempt.IsZero() || started.Sub(secondAttempt) >= 2*interval
			if firstAged != secondAged {
				return firstAged
			}
			if firstAged {
				if firstAttempt.Equal(secondAttempt) {
					firstPreferred := scheduler.preferred[dnsroutesource.ProbeKey(plan.Probes[indices[left]])]
					secondPreferred := scheduler.preferred[dnsroutesource.ProbeKey(plan.Probes[indices[right]])]
					if firstPreferred != secondPreferred {
						return firstPreferred
					}
				}
				return firstAttempt.Before(secondAttempt)
			}
			refresh := func(identity string, attempted time.Time) time.Time {
				if expiry := scheduler.expires[identity]; !expiry.IsZero() {
					return expiry.Add(-interval)
				}
				return attempted.Add(interval)
			}
			firstRefresh, secondRefresh := refresh(first, firstAttempt), refresh(second, secondAttempt)
			if !firstRefresh.Equal(secondRefresh) {
				return firstRefresh.Before(secondRefresh)
			}
			return firstAttempt.Before(secondAttempt)
		})
		queues = append(queues, dnsProbeTargetQueue{address: address, indices: indices, outcomes: map[string]int{}})
	}
	if len(queues) == 0 || policy.MaxConcurrency <= 0 {
		return facts
	}
	concurrency := min(policy.MaxConcurrency, len(plan.Probes))
	results := make(chan dnsProbeResult, concurrency)
	active, attempted := 0, 0
	for {
		for active < concurrency && ctx.Err() == nil {
			eligible := 0
			for _, queue := range queues {
				if queue.next < len(queue.indices) && !scheduler.targets[queue.address].retryAt.After(time.Now()) {
					eligible++
				}
			}
			perTarget := max(1, (concurrency+max(1, eligible)-1)/max(1, eligible))
			selected := -1
			for offset := 0; offset < len(queues); offset++ {
				target := (scheduler.cursor + offset) % len(queues)
				queue := &queues[target]
				if queue.next < len(queue.indices) && queue.active < perTarget && !scheduler.targets[queue.address].retryAt.After(time.Now()) {
					selected = target
					break
				}
			}
			if selected < 0 {
				break
			}
			queue := &queues[selected]
			index := queue.indices[queue.next]
			queue.next++
			queue.active++
			queue.started++
			scheduler.cursor = (selected + 1) % len(queues)
			scheduler.attempts[plan.Probes[index].ID] = time.Now().UTC()
			active++
			attempted++
			go func(index, target int) {
				requirement := plan.Probes[index]
				started := time.Now().UTC()
				timeout := time.Duration(policy.ProbeTimeoutSeconds) * time.Second
				var proof routeprobe.Proof
				var err error
				if requirement.StaticEdge != nil {
					proof, err = routeprobe.ProbeStatic(ctx, requirement, timeout, time.Duration(platformconfig.DNSReadinessFactMaxAge(requirement, policy))*time.Second)
				} else {
					proof, err = probe(ctx, requirement.Hostname, requirement.Path, requirement.Address, requirement.State, timeout)
				}
				finished := time.Now().UTC()
				var fact dnsReadinessFact
				if dnsProbeSweepInterrupted(ctx, err, started.Add(timeout)) {
					// No endpoint verdict was produced. This still consumes a
					// scheduler attempt, but cannot erase or renew a live proof.
					fact = dnsReadinessFact{ProbeID: requirement.ID, Reason: "not_observed"}
				} else if evaluate != nil {
					fact = evaluate(index, proof, err, finished)
				} else {
					fact = evaluateDNSReadinessFact(requirement, policy, proof, err, finished)
				}
				results <- dnsProbeResult{index: index, target: target, fact: fact, connectionFailure: errors.Is(err, routeprobe.ErrConnectionUnavailable), started: started, finished: finished}
			}(index, selected)
		}
		if active == 0 {
			break
		}
		result := <-results
		active--
		queue := &queues[result.target]
		queue.active--
		queue.duration += result.finished.Sub(result.started)
		outcome := result.fact.Reason
		if result.fact.Ready {
			outcome = "proof_observed"
		}
		queue.outcomes[outcome]++
		result.fact.ObservationID = observationID
		if evaluate == nil && result.fact.Reason != "not_observed" {
			scheduler.observeProbeHistory(&result.fact, policy, result.finished)
		}
		facts[result.index] = result.fact
		if result.fact.Ready {
			scheduler.expires[result.fact.ProbeID] = result.fact.Proof.ValidUntil
		} else if (result.fact.Reason != "probe_unavailable" && result.fact.Reason != "not_observed") || !scheduler.expires[result.fact.ProbeID].After(result.finished) {
			delete(scheduler.expires, result.fact.ProbeID)
		}
		state := scheduler.targets[queue.address]
		if result.connectionFailure && ctx.Err() == nil {
			state.failures = min(state.failures+1, 6)
			delay := min(time.Duration(policy.ProbeTimeoutSeconds)*time.Second*time.Duration(1<<state.failures), time.Duration(policy.ProbeIntervalSeconds)*time.Second)
			state.retryAt = result.finished.Add(delay)
		} else if !result.connectionFailure && ctx.Err() == nil {
			state = dnsProbeTargetState{}
		}
		scheduler.targets[queue.address] = state
	}
	if scheduler.logger != nil || scheduler.path != "" {
		counts := map[string]int{}
		for _, fact := range facts {
			reason := fact.Reason
			if fact.Ready {
				reason = "proof_observed"
			}
			counts[reason]++
		}
		deadline, _ := ctx.Deadline()
		planDigest, _ := platformconfig.Digest(plan)
		targets := make([]map[string]any, 0, min(32, len(queues)))
		for _, queue := range queues[:min(32, len(queues))] {
			targets = append(targets, map[string]any{"address": queue.address, "probes": len(queue.indices), "started": queue.started, "unobserved": len(queue.indices) - queue.started, "outcomes": queue.outcomes, "duration_ms": queue.duration.Milliseconds(), "retry_at": scheduler.targets[queue.address].retryAt})
		}
		traceID, tracePhase := dnsSyncTraceIdentity(ctx)
		report, err := json.Marshal(map[string]any{"event": "dns_readiness_sweep", "sync_trace_id": traceID, "sync_phase": tracePhase, "observation_id": observationID, "scope_digest": scheduler.scope, "plan_digest": planDigest, "started_at": started, "finished_at": time.Now().UTC(), "deadline": deadline, "budget_exhausted": errors.Is(ctx.Err(), context.DeadlineExceeded), "probes": len(facts), "targets": len(queues), "attempted": attempted, "unobserved": len(facts) - attempted, "outcomes": counts, "target_observations": targets, "targets_omitted": len(queues) - len(targets)})
		if err == nil {
			if scheduler.logger != nil {
				scheduler.logger.Print(string(report))
			}
			if scheduler.path != "" {
				scheduler.saveObservation(report)
			}
		}
	}
	return facts
}

func dnsProbeSweepInterrupted(ctx context.Context, probeErr error, probeDeadline time.Time) bool {
	parentErr := ctx.Err()
	if parentErr == nil || !errors.Is(probeErr, parentErr) {
		return false
	}
	if parentErr == context.Canceled {
		return true
	}
	deadline, ok := ctx.Deadline()
	return ok && deadline.Before(probeDeadline)
}

func (scheduler *dnsReadinessScheduler) saveObservation(report json.RawMessage) {
	var previous []json.RawMessage
	if stat, err := os.Stat(scheduler.path); err == nil && stat.Size() <= 128<<10 {
		if raw, err := os.ReadFile(scheduler.path); err == nil {
			_ = json.Unmarshal(raw, &previous)
		}
	}
	if len(previous) > 7 {
		previous = previous[len(previous)-7:]
	}
	previous = append(previous, report)
	raw, err := json.Marshal(previous)
	if err == nil && len(raw) <= 128<<10 {
		err = lkgcache.AtomicWriteFile(scheduler.path, raw, 0600)
	}
	if err != nil && scheduler.logger != nil {
		scheduler.logger.Print("DNS probe observation persistence unavailable")
	}
}
