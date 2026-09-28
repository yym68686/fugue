package agentedge

import (
	"errors"
	"maps"
	"slices"
	"sort"
	"sync"
	"time"
)

type Measurement struct {
	EdgeID       string
	Address      string
	Success      bool
	Disqualified bool
	Latency      time.Duration
}

// Round binds local measurements to one previously verified permission. A
// duplicate round or a probe from a superseded grant cannot advance hysteresis.
type Round struct {
	GrantDigest  string
	ObservedAt   time.Time
	Measurements []Measurement
}

type observation struct {
	lastSuccess time.Time
	latency     time.Duration
	failures    int
}

type Choice struct {
	GrantDigest  string
	Origin       string
	Mode         string
	ValidUntil   time.Time
	Primary      Candidate
	Standbys     []Candidate
	PrimarySince time.Time
	Reason       string
	Degraded     bool
}

type Selector struct {
	mu           sync.Mutex
	grant        VerifiedGrant
	observations map[string]observation
	lastRound    time.Time
	primary      string
	primarySince time.Time
	reason       string
	challenger   string
	betterRounds int
}

// Install verifies first and keeps the prior permission intact on rejection.
// Network measurements survive a renewal only for unchanged endpoint identity,
// TLS origin and route requirements; none of their timestamps are renewed.
func (s *Selector) Install(raw []byte, keys map[string]TrustKey, audience, origin string, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	verified, err := Verify(raw, keys, audience, origin, &s.grant, now)
	if err != nil {
		return err
	}
	old := s.grant.signed.Grant
	next := verified.signed.Grant
	previous := make(map[string]Candidate, len(old.Candidates))
	for _, c := range old.Candidates {
		previous[c.EdgeID] = c
	}
	retained := make(map[string]observation, len(next.Candidates))
	for _, c := range next.Candidates {
		if p, exists := previous[c.EdgeID]; exists && old.Origin == next.Origin && p.Address == c.Address &&
			p.AuthorityCellID == c.AuthorityCellID && slices.Equal(p.RouteDigests, c.RouteDigests) && maps.Equal(p.FailureDomains, c.FailureDomains) {
			if observed, exists := s.observations[c.EdgeID]; exists {
				retained[c.EdgeID] = observed
			}
		}
	}
	if old.Policy != next.Policy {
		s.challenger, s.betterRounds = "", 0
	}
	s.grant, s.observations = verified, retained
	return nil
}

func (s *Selector) Observe(round Round, keys map[string]TrustKey, now time.Time) (Choice, error) {
	return s.observe(round, keys, now, true)
}

func (s *Selector) observe(round Round, keys map[string]TrustKey, now time.Time, enforceInterval bool) (Choice, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.grant.Live(keys, now) {
		return Choice{}, errors.New("no live Agent Edge permission")
	}
	g := s.grant.signed.Grant
	newRound := s.lastRound.IsZero() || round.ObservedAt.Sub(s.lastRound) >= time.Duration(g.Policy.ProbeIntervalSeconds)*time.Second
	if round.GrantDigest != s.grant.Digest() || round.ObservedAt.IsZero() || round.ObservedAt.After(now) ||
		round.ObservedAt.Before(g.IssuedAt) || !round.ObservedAt.After(s.lastRound) ||
		enforceInterval && !newRound ||
		now.Sub(round.ObservedAt) > time.Duration(g.Policy.FactMaxAgeSeconds)*time.Second || len(round.Measurements) != len(g.Candidates) {
		return Choice{}, errors.New("Agent Edge measurements are stale, replayed or belong to another grant")
	}
	candidates := map[string]Candidate{}
	for _, c := range g.Candidates {
		candidates[c.EdgeID] = c
	}
	seen := map[string]bool{}
	for _, m := range round.Measurements {
		c, known := candidates[m.EdgeID]
		if !known || seen[m.EdgeID] || c.Address != m.Address || m.Latency < 0 || m.Success && m.Disqualified ||
			m.Success && (m.Latency <= 0 || m.Latency > time.Duration(g.Policy.ProbeTimeoutMilliseconds)*time.Millisecond) {
			return Choice{}, errors.New("Agent Edge measurement target or timing is invalid")
		}
		seen[m.EdgeID] = true
	}
	if s.observations == nil {
		s.observations = map[string]observation{}
	}
	for _, m := range round.Measurements {
		o := s.observations[m.EdgeID]
		if m.Success {
			if o.latency == 0 {
				o.latency = m.Latency
			} else {
				o.latency = (3*o.latency + m.Latency) / 4
			}
			o.lastSuccess, o.failures = round.ObservedAt, 0
		} else if m.Disqualified {
			// Authenticated negative proofs and identity failures revoke local
			// eligibility immediately; only transient absence uses hysteresis.
			o.failures = g.Policy.FailureThreshold
		} else {
			o.failures = min(o.failures+1, g.Policy.FailureThreshold)
		}
		s.observations[m.EdgeID] = o
	}
	s.lastRound = round.ObservedAt
	return s.choose(now, newRound)
}

// installObserved replaces authorization and measured eligibility together.
// The caller probes before entering this critical section; failed staging or
// persistence leaves the previous positive grant at its original expiry.
func (s *Selector) installObserved(raw []byte, keys map[string]TrustKey, audience, origin string, round Round, now time.Time, persist func(VerifiedGrant) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	staged := &Selector{grant: s.grant, observations: maps.Clone(s.observations), lastRound: s.lastRound, primary: s.primary, primarySince: s.primarySince, reason: s.reason, challenger: s.challenger, betterRounds: s.betterRounds}
	if err := staged.Install(raw, keys, audience, origin, now); err != nil {
		return err
	}
	if _, err := staged.observe(round, keys, now, false); err != nil {
		// Authenticated negative evidence cannot retain eligibility merely
		// because the replacement failed its hard floor.
		for _, m := range round.Measurements {
			if !m.Disqualified {
				continue
			}
			for _, c := range s.grant.signed.Grant.Candidates {
				if c.EdgeID == m.EdgeID && c.Address == m.Address {
					if o, exists := s.observations[c.EdgeID]; exists {
						o.failures = s.grant.signed.Grant.Policy.FailureThreshold
						s.observations[c.EdgeID] = o
					}
				}
			}
		}
		return err
	}
	if err := persist(staged.grant); err != nil {
		return err
	}
	s.grant, s.observations, s.lastRound = staged.grant, staged.observations, staged.lastRound
	s.primary, s.primarySince, s.reason = staged.primary, staged.primarySince, staged.reason
	s.challenger, s.betterRounds = staged.challenger, staged.betterRounds
	return nil
}

// Current rechecks expiry, current trust and measured liveness without making
// latency-based switches or counting an old sample as another winning round.
func (s *Selector) Current(keys map[string]TrustKey, now time.Time) (Choice, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.grant.Live(keys, now) {
		return Choice{}, errors.New("no live Agent Edge permission")
	}
	return s.choose(now, false)
}

func (s *Selector) healthy(id string, now time.Time) bool {
	o, exists := s.observations[id]
	p := s.grant.signed.Grant.Policy
	return exists && o.latency > 0 && !o.lastSuccess.IsZero() && !o.lastSuccess.After(now) &&
		now.Sub(o.lastSuccess) < time.Duration(p.FactMaxAgeSeconds)*time.Second && o.failures < p.FailureThreshold
}

func (s *Selector) choose(now time.Time, newRound bool) (Choice, error) {
	g := s.grant.signed.Grant
	ready := []Candidate{}
	var current *Candidate
	for _, c := range g.Candidates {
		if s.healthy(c.EdgeID, now) {
			ready = append(ready, c)
			if c.EdgeID == s.primary {
				copy := c
				current = &copy
			}
		}
	}
	sort.Slice(ready, func(i, j int) bool {
		a, b := s.observations[ready[i].EdgeID].latency, s.observations[ready[j].EdgeID].latency
		if a != b {
			return a < b
		}
		return ready[i].EdgeID < ready[j].EdgeID
	})
	if len(ready) == 0 {
		return Choice{}, errors.New("no authorized Agent Edge has fresh successful measurements")
	}
	// A desired backup count may degrade, but signed hard floors may not.
	// Check the whole measured candidate set before changing the primary.
	readyCells := map[string]bool{}
	readyDomains := map[string]map[string]bool{}
	for dimension := range g.MinDistinctDomains {
		readyDomains[dimension] = map[string]bool{}
	}
	for _, candidate := range ready {
		readyCells[candidate.AuthorityCellID] = true
		for dimension, values := range readyDomains {
			values[candidate.FailureDomains[dimension]] = true
		}
	}
	if len(ready) < g.MinimumCandidates || len(readyCells) < g.MinDistinctCells {
		return Choice{}, errors.New("measured Agent Edge candidates do not meet the signed availability floor")
	}
	for dimension, minimum := range g.MinDistinctDomains {
		if len(readyDomains[dimension]) < minimum {
			return Choice{}, errors.New("measured Agent Edge candidates do not meet signed fault-domain diversity")
		}
	}
	if current == nil {
		s.reason = "primary_unavailable_or_removed"
		if s.primary == "" {
			s.reason = "initial_selection"
		}
		s.primary, s.primarySince, s.challenger, s.betterRounds = ready[0].EdgeID, now, "", 0
		current = &ready[0]
	} else if newRound {
		best := ready[0]
		base, better := s.observations[current.EdgeID], s.observations[best.EdgeID]
		improved := best.EdgeID != current.EdgeID && better.lastSuccess.Equal(s.lastRound) &&
			(base.latency-better.latency)*100 >= base.latency*time.Duration(g.Policy.SwitchImprovementPercent)
		if improved && now.Sub(s.primarySince) >= time.Duration(g.Policy.SwitchCooldownSeconds)*time.Second {
			if s.challenger == best.EdgeID {
				s.betterRounds++
			} else {
				s.challenger, s.betterRounds = best.EdgeID, 1
			}
			if s.betterRounds >= g.Policy.BetterSampleThreshold {
				s.primary, s.primarySince, s.reason = best.EdgeID, now, "sustained_latency_improvement"
				s.challenger, s.betterRounds = "", 0
				current = &ready[0]
			}
		} else {
			s.challenger, s.betterRounds = "", 0
		}
	}
	choice := Choice{GrantDigest: s.grant.Digest(), Origin: g.Origin, Mode: g.Mode, ValidUntil: g.ValidUntil, Primary: cloneCandidate(*current), PrimarySince: s.primarySince, Reason: s.reason}
	usedCells := map[string]bool{current.AuthorityCellID: true}
	usedDomains := map[string]map[string]bool{}
	for dimension := range g.MinDistinctDomains {
		usedDomains[dimension] = map[string]bool{current.FailureDomains[dimension]: true}
	}
	remaining := []Candidate{}
	for _, c := range ready {
		if c.EdgeID != current.EdgeID {
			remaining = append(remaining, c)
		}
	}
	for len(choice.Standbys) < g.Policy.StandbyCount && len(remaining) > 0 {
		best, bestCell, bestDomains := 0, -1, -1
		for i, c := range remaining {
			cellScore, domainScore := 0, 0
			if !usedCells[c.AuthorityCellID] {
				cellScore = 1
			}
			for dimension, values := range usedDomains {
				if !values[c.FailureDomains[dimension]] {
					domainScore++
				}
			}
			// remaining is already ordered by measured latency and stable ID.
			if cellScore > bestCell || cellScore == bestCell && domainScore > bestDomains {
				best, bestCell, bestDomains = i, cellScore, domainScore
			}
		}
		c := remaining[best]
		choice.Standbys = append(choice.Standbys, cloneCandidate(c))
		usedCells[c.AuthorityCellID] = true
		for dimension, values := range usedDomains {
			values[c.FailureDomains[dimension]] = true
		}
		remaining = append(remaining[:best], remaining[best+1:]...)
	}
	choice.Degraded = len(choice.Standbys) < g.Policy.StandbyCount || len(usedCells) < g.Policy.DesiredDistinctCells
	for dimension, minimum := range g.MinDistinctDomains {
		choice.Degraded = choice.Degraded || len(usedDomains[dimension]) < minimum
	}
	return choice, nil
}

func cloneCandidate(c Candidate) Candidate {
	c.RouteDigests = slices.Clone(c.RouteDigests)
	c.FailureDomains = maps.Clone(c.FailureDomains)
	return c
}

// TransportFailure affects subsequent requests only; it never replays the
// request that may already have reached the API. Success requires a new probe.
func (s *Selector) TransportFailure(choice Choice, keys map[string]TrustKey, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.grant.Live(keys, now) || choice.GrantDigest != s.grant.Digest() {
		return
	}
	for _, candidate := range s.grant.signed.Grant.Candidates {
		if candidate.EdgeID == choice.Primary.EdgeID && candidate.Address == choice.Primary.Address {
			o := s.observations[candidate.EdgeID]
			o.failures = min(o.failures+1, s.grant.signed.Grant.Policy.FailureThreshold)
			s.observations[candidate.EdgeID] = o
			return
		}
	}
}
