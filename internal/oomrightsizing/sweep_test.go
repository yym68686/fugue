package oomrightsizing

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestRecentOOMEvidenceKeepsExistingKeys(t *testing.T) {
	now := time.Date(2026, time.June, 10, 12, 0, 0, 0, time.UTC)
	recent, old := now.Add(-2*time.Minute), now.Add(-time.Hour)
	observations := []Termination{
		{Pod: " pod ", Container: "main", State: "last-state", Reason: " OOMKilled ", Restarts: 1, FinishedAt: &recent},
		{Pod: "pod", Container: "sidecar", State: "last-state", Reason: "Error", FinishedAt: &recent},
		{Pod: "pod", Container: "old", State: "last-state", Reason: "OOMKilled", FinishedAt: &old},
		{Pod: "pod", Container: "init", State: "state", Reason: "OOMKilled", PodCreatedAt: recent},
	}
	want := []string{"pod/main/last-state/1/2026-06-10T11:58:00Z", "pod/init/state/0/2026-06-10T11:58:00Z"}
	if got := recentOOMEventKeys(observations, now.Add(-lookback), now); !slices.Equal(got, want) {
		t.Fatalf("historical event encoding changed: %q", got)
	}
}

func TestSweepReleasesFailedClaimsAndClosesEachObserver(t *testing.T) {
	var service Service
	app := model.App{ID: "app", TenantID: "tenant"}
	opened, closed, attempts := 0, 0, 0
	seen := time.Now().UTC()
	ports := SweepPorts{
		Apps:      func() ([]model.App, error) { return []model.App{app}, nil },
		Managed:   func(model.App) bool { return true },
		Namespace: func(app model.App) string { return app.TenantID },
		Open: func(namespace string) (Observer, error) {
			opened++
			if namespace != app.TenantID {
				t.Fatalf("wrong namespace %q", namespace)
			}
			return Observer{
				Terminations: func(_ context.Context, got model.App, target string) ([]Termination, error) {
					if got.ID != app.ID || target != "app" {
						t.Fatalf("unexpected source %q/%q", got.ID, target)
					}
					return []Termination{{Pod: "pod", State: "state", Reason: "OOMKilled", FinishedAt: &seen}}, nil
				},
				Close: func() { closed++ },
			}, nil
		},
		HasActive: func(model.App) (bool, error) { return false, nil },
		Enqueue: func(got model.App, targets []Target) error {
			attempts++
			if got.ID != app.ID || len(targets) != 1 || targets[0].ID != app.ID {
				t.Fatalf("wrong enqueue: %q %+v", got.ID, targets)
			}
			if attempts == 1 {
				return errors.New("transient write failure")
			}
			return nil
		},
	}
	for range 3 {
		service.Sweep(t.Context(), ports)
	}
	if attempts != 2 || opened != 3 || closed != opened {
		t.Fatalf("attempts/opened/closed=%d/%d/%d, want 2/3/3", attempts, opened, closed)
	}
}

func TestLocalClaimExpiresWithoutExposingState(t *testing.T) {
	var service Service
	start := time.Now().UTC()
	keys := []string{"one"}
	if !service.claim(keys, start) || service.claim(keys, start.Add(eventTTL)) || !service.claim(keys, start.Add(eventTTL+time.Second)) {
		t.Fatal("claim TTL did not preserve the existing inclusive boundary")
	}
}
