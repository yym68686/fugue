package dnsserver

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"maps"
	"os"
	"slices"
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
	priority     dnsProbePriority
	blockerScope string
	blockers     map[string]time.Time
	logger       *log.Logger
	path         string
}

func (scheduler *dnsReadinessScheduler) prioritize(priority dnsProbePriority) *dnsReadinessScheduler {
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	scheduler.priority = dnsProbePriority{maps.Clone(priority.preferred), maps.Clone(priority.candidates), maps.Clone(priority.availability), maps.Clone(priority.sole), maps.Clone(priority.alternateCover)}
	return scheduler
}

// A failed detached query diagnoses scheduling demand, not readiness. Keep
// only a few recent host hints for the exact assignment that failed.
func (scheduler *dnsReadinessScheduler) noteQueryBlocker(a model.PlatformConsumerAssignment, hostname string, at time.Time) {
	if a.ExpectedConsumerSetID == "" || hostname == "" || at.IsZero() {
		return
	}
	scope, err := platformconfig.Digest(a)
	if err != nil {
		return
	}
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	if scheduler.blockerScope != scope {
		scheduler.blockerScope = scope
		scheduler.blockers = make(map[string]time.Time)
	}
	host := normalizeName(hostname)
	if newest := scheduler.blockers[host]; !at.After(newest) {
		return
	}
	scheduler.blockers[host] = at
	if len(scheduler.blockers) > 4 {
		oldest := host
		for key, checked := range scheduler.blockers {
			if checked.Before(scheduler.blockers[oldest]) || checked.Equal(scheduler.blockers[oldest]) && key > oldest {
				oldest = key
			}
		}
		delete(scheduler.blockers, oldest)
	}
}

func (scheduler *dnsReadinessScheduler) clearQueryBlockers(a model.PlatformConsumerAssignment) {
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	if len(scheduler.blockers) == 0 {
		return
	}
	scope, err := platformconfig.Digest(a)
	if err == nil && scheduler.blockerScope == scope {
		scheduler.blockerScope = ""
		scheduler.blockers = nil
	}
}

func (scheduler *dnsReadinessScheduler) candidatePriority(plan *platformconfig.DNSReadinessPlan, indices []int, now time.Time, freshness time.Duration) (map[int]int, int) {
	// All classes share the original eight-slot priority budget. Leave
	// reserved slots for availability and ranked work even when an exact
	// publication blocker is present, and keep oldest-attempt rotation.
	const maxPriority = 8
	const maxBlocked = 2
	var blocked, availability, ranked []int
	if scheduler.scope == scheduler.blockerScope && len(scheduler.blockers) > 0 {
		for _, i := range indices {
			at := scheduler.blockers[plan.Probes[i].Hostname]
			if !at.IsZero() && !at.After(now) && now.Sub(at) < freshness {
				blocked = append(blocked, i)
			}
		}
	}
	oldestFirst := func(ordered []int) {
		sort.SliceStable(ordered, func(i, j int) bool {
			return scheduler.attempts[plan.Probes[ordered[i]].ID].Before(scheduler.attempts[plan.Probes[ordered[j]].ID])
		})
	}
	sort.SliceStable(blocked, func(i, j int) bool {
		left, right := plan.Probes[blocked[i]], plan.Probes[blocked[j]]
		first, second := scheduler.priority.preferred[dnsroutesource.ProbeKey(left)], scheduler.priority.preferred[dnsroutesource.ProbeKey(right)]
		if first != second {
			return first
		}
		return scheduler.attempts[left.ID].Before(scheduler.attempts[right.ID])
	})
	blocked = blocked[:min(len(blocked), maxBlocked)]
	if len(scheduler.priority.candidates) != 0 || len(scheduler.priority.availability) != 0 {
		for _, i := range indices {
			if slices.Contains(blocked, i) {
				continue
			}
			key := dnsroutesource.ProbeKey(plan.Probes[i])
			switch {
			case scheduler.priority.availability[key]:
				availability = append(availability, i)
			case scheduler.priority.candidates[key]:
				ranked = append(ranked, i)
			}
		}
	}
	oldestFirst(availability)
	oldestFirst(ranked)
	unreserved := maxPriority - len(blocked)
	first := min(len(availability), max(unreserved/2, unreserved-len(ranked)))
	second := min(len(ranked), unreserved-first)
	result := make(map[int]int, len(blocked)+first+second)
	for rank, i := range blocked {
		result[i] = rank + 1
	}
	for rank, i := range availability[:first] {
		result[i] = len(blocked) + rank + 1
	}
	for rank, i := range ranked[:second] {
		result[i] = len(blocked) + first + rank + 1
	}
	return result, len(blocked)
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
	address       string
	indices       []int
	next          int
	active        int
	started       int
	blockerProbes int
	outcomes      map[string]int
	duration      time.Duration
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
	return scheduler.collectWindow(ctx, plan, policy, probe, evaluate, nil, false)
}

// previous is supplied only by a caller that has verified the unexpired facts
// against the exact signed assignment and plan. A deferred request returns no
// observation and cannot update recovery history or the original deadline.
func (scheduler *dnsReadinessScheduler) collectWindow(ctx context.Context, plan *platformconfig.DNSReadinessPlan, policy *platformconfig.DNSReadinessPolicy, probe dnsReadinessProbeFunc, evaluate func(int, routeprobe.Proof, error, time.Time) dnsReadinessFact, previous map[string]dnsReadinessFact, pending bool) []dnsReadinessFact {
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
	deferred := 0
	nextSweep := started.Add(platformDNSConsumerPollInterval + interval + time.Duration(policy.ProbeTimeoutSeconds)*time.Second)
	deferUntil := refreshHorizon
	// A rejected candidate also measures the cost of its retained LKG
	// scan and rejection reporting. Charging that entire gap again can
	// starve never-observed probes. Pending proofs must still cover this
	// full scan, a poll interval and a probe timeout; applying/serving
	// continues to check their exact original deadlines.
	if pending || nextSweep.After(deferUntil) {
		deferUntil = nextSweep
	}
	for index, requirement := range plan.Probes {
		facts[index] = dnsReadinessFact{ProbeID: requirement.ID, ObservationID: observationID, Reason: "not_observed"}
		if fact, ok := previous[requirement.ID]; ok && fact.Proof.ValidUntil.After(deferUntil) {
			deferred++
			continue
		}
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
		priority, blockerProbes := scheduler.candidatePriority(plan, indices, started, time.Duration(policy.FactFreshnessSeconds)*time.Second)
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
					firstPreferred := scheduler.priority.preferred[dnsroutesource.ProbeKey(plan.Probes[indices[left]])]
					secondPreferred := scheduler.priority.preferred[dnsroutesource.ProbeKey(plan.Probes[indices[right]])]
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
		balanceDNSRefreshQueue(indices, priority, plan, previous, started.Add(interval+time.Duration(policy.ProbeTimeoutSeconds)*time.Second))
		balanceDNSCoverageQueue(indices, priority, plan, scheduler.priority.sole, scheduler.priority.alternateCover)
		queues = append(queues, dnsProbeTargetQueue{address: address, indices: indices, blockerProbes: blockerProbes, outcomes: map[string]int{}})
	}
	concurrency := max(0, min(policy.MaxConcurrency, len(plan.Probes)-deferred))
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
		if deferred > 0 {
			counts["not_observed"] -= deferred
			counts["deferred_fresh"] = deferred
		}
		deadline, _ := ctx.Deadline()
		planDigest, _ := platformconfig.Digest(plan)
		targets := make([]map[string]any, 0, min(32, len(queues)))
		blockerAttempts := 0
		for _, queue := range queues {
			blockerAttempts += min(queue.started, queue.blockerProbes)
		}
		for _, queue := range queues[:min(32, len(queues))] {
			targets = append(targets, map[string]any{"address": queue.address, "probes": len(queue.indices), "started": queue.started, "unobserved": len(queue.indices) - queue.started, "outcomes": queue.outcomes, "duration_ms": queue.duration.Milliseconds(), "retry_at": scheduler.targets[queue.address].retryAt})
		}
		traceID, tracePhase := dnsSyncTraceIdentity(ctx)
		report, err := json.Marshal(map[string]any{"event": "dns_readiness_sweep", "sync_trace_id": traceID, "sync_phase": tracePhase, "observation_id": observationID, "scope_digest": scheduler.scope, "plan_digest": planDigest, "started_at": started, "finished_at": time.Now().UTC(), "deadline": deadline, "budget_exhausted": errors.Is(ctx.Err(), context.DeadlineExceeded), "probes": len(facts), "targets": len(queues), "attempted": attempted, "query_blocker_attempts": blockerAttempts, "deferred_fresh": deferred, "unobserved": len(facts) - attempted - deferred, "outcomes": counts, "target_observations": targets, "targets_omitted": len(queues) - len(targets)})
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

// Schedule sole dependencies and complementary RRset coverage before spending
// most of the same physical endpoint's slots on redundant alternatives. The
// normal explicit priority prefix, per-target backoff/concurrency and exact
// proof checks stay unchanged. Even under continuous singleton demand, each
// group of four tail slots still services ordinary work in its incoming order.
func balanceDNSCoverageQueue(indices []int, priority map[int]int, plan *platformconfig.DNSReadinessPlan, sole, alternate map[string]bool) {
	if (len(sole) == 0 && len(alternate) == 0) || len(indices) < 2 {
		return
	}
	first := 0
	for first < len(indices) && priority[indices[first]] != 0 {
		first++
	}
	tail := indices[first:]
	var necessary, alternatives, complementary, ordinary []int
	for _, index := range tail {
		key := dnsroutesource.ProbeKey(plan.Probes[index])
		if sole[key] {
			necessary = append(necessary, index)
		} else {
			alternatives = append(alternatives, index)
			if alternate[key] {
				complementary = append(complementary, index)
			} else {
				ordinary = append(ordinary, index)
			}
		}
	}
	// Preserve the original singleton share. Complementary work orders
	// only the existing alternatives share and still rotates other probes.
	interleaveDNSProbeQueues(alternatives, complementary, ordinary, 3)
	if len(necessary) == 0 {
		copy(tail, alternatives)
		return
	}
	interleaveDNSProbeQueues(tail, necessary, alternatives, 3)
}

// A verified fact which covers this whole sweep, but not the next poll, still
// needs renewal. Spending the whole budget on such facts first can, however,
// leave the same unsigned gaps in successive incomplete scans. Interleave
// those two demands within each physical endpoint, keeping the explicit
// blocker/availability prefix and stable order within either class. This
// orders requests only: it neither defers an attempt nor manufactures a fact.
func balanceDNSRefreshQueue(indices []int, priority map[int]int, plan *platformconfig.DNSReadinessPlan, previous map[string]dnsReadinessFact, through time.Time) {
	if len(previous) == 0 || len(indices) < 2 {
		return
	}
	first := 0
	for first < len(indices) && priority[indices[first]] != 0 {
		first++
	}
	rest := indices[first:]
	var needed, renewal []int
	for _, index := range rest {
		fact, ok := previous[plan.Probes[index].ID]
		if ok && fact.Ready && fact.Proof.ValidUntil.After(through) {
			renewal = append(renewal, index)
		} else {
			needed = append(needed, index)
		}
	}
	interleaveDNSProbeQueues(rest, needed, renewal, 2)
}

func interleaveDNSProbeQueues(dest, needed, renewal []int, share int) {
	if len(needed) == 0 || len(renewal) == 0 {
		return
	}
	out := dest[:0]
	for len(needed) > 0 && len(renewal) > 0 {
		n := min(share, len(needed))
		out = append(out, needed[:n]...)
		out = append(out, renewal[0])
		needed, renewal = needed[n:], renewal[1:]
	}
	out = append(out, needed...)
	copy(dest[len(out):], renewal)
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
