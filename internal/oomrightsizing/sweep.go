// Package oomrightsizing owns runtime OOM evidence and automatic memory-growth
// decisions. It does not know how pods are queried or operations are stored.
package oomrightsizing

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"fugue/internal/model"
)

const (
	initialDelay = 45 * time.Second
	interval     = time.Minute
	lookback     = 15 * time.Minute
	eventTTL     = 24 * time.Hour
)

type Termination struct {
	Pod, Container, State, Reason string
	Restarts                      int
	PodCreatedAt                  time.Time
	FinishedAt                    *time.Time
}

type Target struct {
	Kind, ID  string
	EventKeys []string
}

// Observer is bound to one namespace and discarded after each sweep.
type Observer struct {
	Terminations func(context.Context, model.App, string) ([]Termination, error)
	Close        func()
}

type SweepPorts struct {
	Apps      func() ([]model.App, error)
	Managed   func(model.App) bool
	Namespace func(model.App) string
	Open      func(string) (Observer, error)
	HasActive func(model.App) (bool, error)
	Enqueue   func(model.App, []Target) error
	Logf      func(string, ...any)
}

// Service owns the process-local deduplication window. The durable operation
// writer still serializes and deduplicates writes across API replicas.
type Service struct {
	mu     sync.Mutex
	events map[string]time.Time
}

func (s *Service) Run(ctx context.Context, ports SweepPorts) {
	timer := time.NewTimer(initialDelay)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			s.Sweep(ctx, ports)
			timer.Reset(interval)
		}
	}
}

func (s *Service) Sweep(ctx context.Context, ports SweepPorts) {
	if ctx.Err() != nil {
		return
	}
	logf := ports.Logf
	if logf == nil {
		logf = func(string, ...any) {}
	}
	apps, err := ports.Apps()
	if err != nil {
		logf("list apps for OOM right-sizing failed: %v", err)
		return
	}
	observers := make(map[string]Observer)
	defer func() {
		for _, observer := range observers {
			if observer.Close != nil {
				observer.Close()
			}
		}
	}()
	now := time.Now().UTC()
	for _, app := range apps {
		if ctx.Err() != nil {
			return
		}
		if app.Spec.RightSizing != nil && model.NormalizeAppRightSizingSpec(*app.Spec.RightSizing).Mode != model.AppRightSizingModeAuto {
			continue
		}
		if !ports.Managed(app) {
			continue
		}
		namespace := ports.Namespace(app)
		observer, found := observers[namespace]
		if !found {
			observer, err = ports.Open(namespace)
			if err != nil {
				logf("inspect OOM events for app=%s failed: %v", app.ID, err)
				continue
			}
			observers[namespace] = observer
		}
		targets, err := targetsForApp(ctx, observer, app, now)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			logf("inspect OOM events for app=%s failed: %v", app.ID, err)
			continue
		}
		keys := eventKeys(targets)
		if len(keys) == 0 {
			continue
		}
		if active, err := ports.HasActive(app); err != nil || active {
			if err != nil {
				logf("check active deploy before OOM right-sizing for app=%s failed: %v", app.ID, err)
			}
			continue
		}
		if !s.claim(keys, now) {
			continue
		}
		if err := ports.Enqueue(app, targets); err != nil {
			s.release(keys)
			logf("OOM right-sizing for app=%s failed: %v", app.ID, err)
		}
	}
}

func targetsForApp(ctx context.Context, observer Observer, app model.App, now time.Time) ([]Target, error) {
	var targets []Target
	appendTarget := func(name, kind, id string) error {
		observations, err := observer.Terminations(ctx, app, name)
		if err != nil {
			return err
		}
		if keys := recentOOMEventKeys(observations, now.Add(-lookback), now); len(keys) > 0 {
			targets = append(targets, Target{Kind: kind, ID: id, EventKeys: keys})
		}
		return nil
	}
	if err := appendTarget("app", model.ClusterNodeWorkloadKindApp, app.ID); err != nil {
		return nil, err
	}
	if postgres := firstOwnedPostgresService(app); postgres != nil {
		if err := appendTarget("postgres", model.ClusterNodeWorkloadKindBackingService, postgres.ID); err != nil {
			return nil, err
		}
	}
	return targets, nil
}

func recentOOMEventKeys(terminations []Termination, cutoff, now time.Time) []string {
	var keys []string
	for _, termination := range terminations {
		if !strings.EqualFold(strings.TrimSpace(termination.Reason), "OOMKilled") {
			continue
		}
		observedAt := now
		if termination.FinishedAt != nil {
			observedAt = termination.FinishedAt.UTC()
		} else if !termination.PodCreatedAt.IsZero() {
			observedAt = termination.PodCreatedAt.UTC()
		}
		if observedAt.Before(cutoff) {
			continue
		}
		keys = append(keys, fmt.Sprintf("%s/%s/%s/%d/%s",
			strings.TrimSpace(termination.Pod), strings.TrimSpace(termination.Container),
			termination.State, termination.Restarts, observedAt.Format(time.RFC3339Nano)))
	}
	return keys
}

func (s *Service) claim(keys []string, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.events == nil {
		s.events = make(map[string]time.Time)
	}
	for key, observedAt := range s.events {
		if now.Sub(observedAt) > eventTTL {
			delete(s.events, key)
		}
	}
	claimed := false
	for _, key := range keys {
		if _, exists := s.events[key]; !exists {
			s.events[key] = now
			claimed = true
		}
	}
	return claimed
}

func (s *Service) release(keys []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, key := range keys {
		delete(s.events, key)
	}
}
