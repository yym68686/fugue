package dnsserver

import (
	"context"
	"testing"
	"time"

	"fugue/internal/dnsroutesource"
	"fugue/internal/model"
)

func TestDNSRenewalFactsCannotOutlivePublicationOrHideNewerObservations(t *testing.T) {
	for _, scenario := range []string{"newer", "negative", "configuration", "checkpoint_age", "source_regression", "transition", "newer_main", "equal_clock", "missing_source"} {
		t.Run(scenario, func(t *testing.T) {
			now := time.Now().UTC()
			base := &dnsServingState{checkedAt: now.Add(-time.Minute), record: dnsServingCheckpoint{Positive: true, AppliedAt: now.Add(-time.Hour), Parent: model.PlatformArtifact{ID: "parent", ContentHash: "same-digest"}, Candidate: dnsPlatformCandidate{Assignment: model.PlatformConsumerAssignment{ArtifactID: "dns", FencingToken: 3}}}}
			source := func(version int64) *dnsroutesource.Context {
				return &dnsroutesource.Context{Snapshot: model.PlatformDNSRouteSourceSnapshot{Scopes: []model.PlatformDNSRouteSourceScope{{ScopeKey: "global", Lanes: []model.PlatformDNSRouteSourceLane{{ReleaseChannel: "full", Version: version, FencingToken: version}}}}}}
			}
			base.record.RouteSources = source(4)
			refreshed := *base
			refreshed.checkedAt = now
			refreshed.record.RouteSources = source(5)
			refreshed.facts = []dnsReadinessFact{{Ready: scenario != "negative"}}
			latest := *base
			switch scenario {
			case "configuration":
				latest.record.Candidate.Assignment.FencingToken++
			case "checkpoint_age":
				latest.record.AppliedAt = now.Add(-time.Minute)
			case "source_regression":
				latest.record.RouteSources = source(6)
			case "transition":
				latest.transition = &dnsRecordTransition{}
			case "newer_main":
				latest.checkedAt = now.Add(time.Second)
			case "equal_clock":
				latest.checkedAt = now
			case "missing_source":
				latest.record.RouteSources = nil
			}
			s := &Service{}
			s.platformServing.Store(&latest)
			s.platformRenewal.Store(&dnsServingRenewal{base: base, state: &refreshed})
			want := &latest
			if scenario == "newer" || scenario == "negative" {
				want = &refreshed
			}
			if got := s.currentDNSServingState(); got != want {
				t.Fatal("renewal hid newer or different authority, or ignored actual negative evidence")
			}
			// Clearing serving configuration never falls back to an observation.
			s.platformServing.Store(nil)
			if s.currentDNSServingState() != nil {
				t.Fatal("volatile facts became configuration authority")
			}
		})
	}
}

func TestDNSRenewalWorkerStopsWithParent(t *testing.T) {
	s := &Service{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.runDNSRenewal(ctx, &Service{}); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("renewal loop ignored parent cancellation")
	}
}
