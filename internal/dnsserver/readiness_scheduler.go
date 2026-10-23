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
	attempts     map[string]time.Time
	expires      map[string]time.Time
	targets      map[string]dnsProbeTargetState
	preferred    map[string]bool
	logger       *log.Logger
	path         string
}

func (scheduler *dnsReadinessScheduler) prioritize(keys map[string]bool) *dnsReadinessScheduler {
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	scheduler.preferred = maps.Clone(keys)
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
		scheduler.attempts = map[string]time.Time{}
		scheduler.expires = map[string]time.Time{}
		scheduler.targets = map[string]dnsProbeTargetState{}
	}
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
		interval := time.Duration(policy.ProbeIntervalSeconds) * time.Second
		sort.SliceStable(indices, func(left, right int) bool {
			first, second := plan.Probes[indices[left]].ID, plan.Probes[indices[right]].ID
			firstAttempt, secondAttempt := scheduler.attempts[first], scheduler.attempts[second]
			firstExpiry, secondExpiry := scheduler.expires[first], scheduler.expires[second]
			// A sweep can occupy an entire interval. Renew an existing proof
			// before that sweep plus the next interval can exhaust its lease.
			// Never-observed work otherwise sorts ahead of every old proof and
			// can repeatedly leave a healthy serving endpoint without evidence.
			refreshHorizon := started.Add(2 * interval)
			firstRefreshDue := !firstExpiry.IsZero() && !firstExpiry.After(refreshHorizon)
			secondRefreshDue := !secondExpiry.IsZero() && !secondExpiry.After(refreshHorizon)
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
				proof, err := probe(ctx, requirement.Hostname, requirement.Path, requirement.Address, requirement.State, time.Duration(policy.ProbeTimeoutSeconds)*time.Second)
				finished := time.Now().UTC()
				results <- dnsProbeResult{index: index, target: target, fact: evaluateDNSReadinessFact(requirement, policy, proof, err, finished), connectionFailure: errors.Is(err, routeprobe.ErrConnectionUnavailable), started: started, finished: finished}
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
		scheduler.observeProbeHistory(&result.fact, policy)
		facts[result.index] = result.fact
		if result.fact.Ready {
			scheduler.expires[result.fact.ProbeID] = result.fact.Proof.ValidUntil
		} else {
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
		report, err := json.Marshal(map[string]any{"event": "dns_readiness_sweep", "observation_id": observationID, "scope_digest": scheduler.scope, "plan_digest": planDigest, "started_at": started, "finished_at": time.Now().UTC(), "deadline": deadline, "budget_exhausted": errors.Is(ctx.Err(), context.DeadlineExceeded), "probes": len(facts), "targets": len(queues), "attempted": attempted, "unobserved": len(facts) - attempted, "outcomes": counts, "target_observations": targets, "targets_omitted": len(queues) - len(targets)})
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
