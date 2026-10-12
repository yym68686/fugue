package dnsserver

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"sort"
	"sync"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/routeprobe"
)

type dnsProbeTargetState struct {
	failures int
	retryAt  time.Time
}

type dnsReadinessScheduler struct {
	mu       sync.Mutex
	scope    string
	digest   string
	cursor   int
	attempts map[string]time.Time
	targets  map[string]dnsProbeTargetState
	logger   *log.Logger
}

func (scheduler *dnsReadinessScheduler) bind(assignment model.PlatformConsumerAssignment, logger *log.Logger) *dnsReadinessScheduler {
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	scheduler.scope, _ = platformconfig.Digest(assignment)
	scheduler.logger = logger
	return scheduler
}

type dnsProbeTargetQueue struct {
	address string
	indices []int
	next    int
	active  int
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
		scheduler.targets = map[string]dnsProbeTargetState{}
	}
	facts := make([]dnsReadinessFact, len(plan.Probes))
	byAddress := map[string][]int{}
	for index, requirement := range plan.Probes {
		facts[index] = dnsReadinessFact{ProbeID: requirement.ID, Reason: "not_observed"}
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
		sort.SliceStable(indices, func(left, right int) bool {
			return scheduler.attempts[plan.Probes[indices[left]].ID].Before(scheduler.attempts[plan.Probes[indices[right]].ID])
		})
		queues = append(queues, dnsProbeTargetQueue{address: address, indices: indices})
	}
	if len(queues) == 0 || policy.MaxConcurrency <= 0 {
		return facts
	}
	concurrency := min(policy.MaxConcurrency, len(plan.Probes))
	perTarget := max(1, (concurrency+len(queues)-1)/len(queues))
	results := make(chan dnsProbeResult, concurrency)
	active, attempted := 0, 0
	for {
		for active < concurrency && ctx.Err() == nil {
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
		facts[result.index] = result.fact
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
	if scheduler.logger != nil {
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
		report, err := json.Marshal(map[string]any{"event": "dns_readiness_sweep", "scope_digest": scheduler.scope, "plan_digest": planDigest, "started_at": started, "finished_at": time.Now().UTC(), "deadline": deadline, "probes": len(facts), "targets": len(queues), "attempted": attempted, "unobserved": len(facts) - attempted, "outcomes": counts})
		if err == nil {
			scheduler.logger.Print(string(report))
		}
	}
	return facts
}
