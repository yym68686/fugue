package entryfailover

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"

	c "fugue/internal/staticedgecontract"
)

type Collector interface {
	Collect(context.Context, Policy, Target, Vantage) (ProbeResult, error)
}

type HealthStreak struct {
	Success int `json:"success"`
	Failure int `json:"failure"`
}

type DecisionState struct {
	Schema       string                  `json:"schema"`
	PolicyDigest string                  `json:"policy_digest"`
	Streaks      map[string]HealthStreak `json:"streaks"`
	LastSwitchAt time.Time               `json:"last_switch_at,omitempty"`
}

type Decision struct {
	At        time.Time               `json:"at"`
	Mode      string                  `json:"mode"`
	Current   string                  `json:"current,omitempty"`
	Selected  string                  `json:"selected,omitempty"`
	Reason    string                  `json:"reason"`
	Health    map[string]string       `json:"health"`
	Streaks   map[string]HealthStreak `json:"streaks"`
	Operation *Operation              `json:"operation,omitempty"`
}

type Engine struct {
	Executor  *Executor
	Collector Collector
	Clock     func() time.Time
}

func (e *Engine) now() time.Time {
	if e.Clock != nil {
		return e.Clock().UTC()
	}
	return time.Now().UTC()
}

func (e *Engine) statePath() string { return filepath.Join(e.Executor.StateDir, "decision-state.json") }
func (e *Engine) loadState() (DecisionState, error) {
	state := DecisionState{Schema: PolicySchema, PolicyDigest: e.Executor.Policy.Digest(), Streaks: map[string]HealthStreak{}}
	raw, err := os.ReadFile(e.statePath())
	if os.IsNotExist(err) {
		return state, nil
	}
	if err != nil {
		return state, err
	}
	var old DecisionState
	if err = c.StrictJSON(raw, &old); err != nil {
		return state, err
	}
	if old.Schema != PolicySchema {
		return state, errors.New("decision state schema mismatch")
	}
	if old.PolicyDigest != state.PolicyDigest {
		return state, nil
	}
	if old.Streaks == nil {
		return state, errors.New("decision streaks missing")
	}
	return old, nil
}

func (e *Engine) saveState(s DecisionState) error {
	raw, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	return writeDurable(e.statePath(), append(raw, '\n'))
}

func summarize(results []ProbeResult, errorsByVantage int, required int) string {
	good, bad := 0, 0
	for _, r := range results {
		if r.Healthy {
			good++
		} else {
			bad++
		}
	}
	if good >= required && bad == 0 {
		return "healthy"
	}
	if bad >= required && good == 0 {
		return "unhealthy"
	}
	if errorsByVantage > 0 || (good < required && bad < required) {
		return "unknown"
	}
	return "split"
}

func (e *Engine) Cycle(ctx context.Context) (Decision, error) {
	if e == nil || e.Executor == nil || e.Collector == nil {
		return Decision{}, errors.New("executor and collector required")
	}
	if err := e.Executor.validate(); err != nil {
		return Decision{}, err
	}
	p := e.Executor.Policy
	state, err := e.loadState()
	if err != nil {
		return Decision{}, err
	}
	decision := Decision{At: e.now(), Mode: p.Mode, Health: map[string]string{}, Streaks: map[string]HealthStreak{}}
	freshHealthy := map[string]bool{}
	required := len(p.Vantages)
	if required > 2 {
		required = 2
	}
	type probeSet struct {
		results []ProbeResult
		unknown int
	}
	collected := make(map[string]*probeSet, len(p.Targets))
	for _, t := range p.Targets {
		collected[t.ID] = &probeSet{}
	}
	probeCtx, cancel := context.WithTimeout(ctx, p.Interval())
	defer cancel()
	var wg sync.WaitGroup
	var mu sync.Mutex
	sem := make(chan struct{}, 4)
	for _, target := range p.Targets {
		for _, vantage := range p.Vantages {
			target, vantage := target, vantage
			wg.Add(1)
			go func() {
				defer wg.Done()
				select {
				case sem <- struct{}{}:
				case <-probeCtx.Done():
					mu.Lock()
					collected[target.ID].unknown++
					mu.Unlock()
					return
				}
				defer func() { <-sem }()
				result, probeErr := e.Collector.Collect(probeCtx, p, target, vantage)
				mu.Lock()
				defer mu.Unlock()
				if probeErr != nil || result.TargetID != target.ID || result.At.IsZero() || decision.At.Sub(result.At) > p.Interval()+p.ProbeTimeout() || result.At.After(decision.At.Add(5*time.Second)) {
					collected[target.ID].unknown++
					return
				}
				collected[target.ID].results = append(collected[target.ID].results, result)
			}()
		}
	}
	wg.Wait()
	for _, target := range p.Targets {
		results := collected[target.ID].results
		unknown := collected[target.ID].unknown
		status := summarize(results, unknown, required)
		decision.Health[target.ID] = status
		streak := state.Streaks[target.ID]
		switch status {
		case "healthy":
			streak.Success++
			streak.Failure = 0
			freshHealthy[target.ID] = true
		case "unhealthy":
			streak.Failure++
			streak.Success = 0
		default:
			streak.Success, streak.Failure = 0, 0
		}
		if streak.Success > p.SuccessThreshold {
			streak.Success = p.SuccessThreshold
		}
		if streak.Failure > p.FailureThreshold {
			streak.Failure = p.FailureThreshold
		}
		state.Streaks[target.ID] = streak
		decision.Streaks[target.ID] = streak
	}
	if err := e.saveState(state); err != nil {
		return decision, err
	}
	_, current, err := e.Executor.snapshot(ctx)
	if err != nil {
		decision.Reason = "dns_outside_policy_or_unavailable"
		return decision, err
	}
	decision.Current, decision.Selected = current.ID, current.ID
	if p.Mode == "off" {
		decision.Reason = "disabled"
		return decision, nil
	}
	if !state.LastSwitchAt.IsZero() && decision.At.Sub(state.LastSwitchAt) < p.Interval()*time.Duration(p.FailureThreshold) {
		decision.Reason = "cooldown"
		return decision, nil
	}
	currentStatus, currentStreak := decision.Health[current.ID], state.Streaks[current.ID]
	if currentStatus == "healthy" && p.Failback == "manual" {
		decision.Reason = "current_healthy_manual_failback"
		return decision, nil
	}
	if currentStatus != "healthy" && (currentStatus != "unhealthy" || currentStreak.Failure < p.FailureThreshold) {
		decision.Reason = "current_failure_unconfirmed"
		return decision, nil
	}
	for _, candidate := range p.SortedTargets() {
		if candidate.ID == current.ID || !freshHealthy[candidate.ID] || state.Streaks[candidate.ID].Success < p.SuccessThreshold {
			continue
		}
		if currentStatus == "healthy" && candidate.Priority >= current.Priority {
			continue
		}
		decision.Selected = candidate.ID
		break
	}
	if decision.Selected == current.ID {
		decision.Reason = "no_ready_alternative"
		return decision, nil
	}
	if p.Mode == "shadow" {
		decision.Reason = "shadow_would_switch"
		return decision, nil
	}
	// The callback is bound to this cycle's fresh, multi-vantage evidence. The
	// executor still verifies the DNS baseline and its writer journal.
	verification := e.Executor.VerifyTarget
	e.Executor.VerifyTarget = func(_ context.Context, target Target) error {
		if target.ID != decision.Selected || !freshHealthy[target.ID] || state.Streaks[target.ID].Success < p.SuccessThreshold {
			return errors.New("target lacks fresh multi-vantage business proof")
		}
		return nil
	}
	op, switchErr := e.Executor.Switch(ctx, decision.Selected, true)
	e.Executor.VerifyTarget = verification
	decision.Operation = &op
	if switchErr != nil {
		decision.Reason = "dns_switch_failed"
		return decision, switchErr
	}
	state.LastSwitchAt = decision.At
	if err := e.saveState(state); err != nil {
		return decision, err
	}
	decision.Reason = "switched"
	return decision, nil
}
