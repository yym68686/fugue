package dnsserver

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"time"

	"fugue/internal/dnsroutesource"
	"fugue/internal/model"
	"fugue/internal/platformconsumer"
	"fugue/internal/platformcontrol"
	"fugue/internal/routeprobe"
)

// A renewal is volatile runtime evidence for an already loaded publication. It
// cannot install a candidate, acknowledge a release, or replace the publication
// checkpoint. Its state preserves the original configuration's AppliedAt.
type dnsServingRenewal struct {
	base  *dnsServingState
	state *dnsServingState
}

func sameDNSRenewalPublication(a, b *dnsServingState) bool {
	return a != nil && b != nil && a.record.Positive && b.record.Positive &&
		a.record.AppliedAt.Equal(b.record.AppliedAt) && a.record.Parent.ID == b.record.Parent.ID &&
		a.record.Parent.ContentHash == b.record.Parent.ContentHash && a.routeID == b.routeID &&
		reflect.DeepEqual(a.record.Candidate.Assignment, b.record.Candidate.Assignment)
}

// Queries, status and exported runtime facts use the same newest completed
// observation. A configuration/transition change, newer main-loop observation,
// or non-monotonic source makes the renewal inapplicable. Negative renewal
// facts are intentionally selected too; old successes cannot mask a failure.
func (s *Service) currentDNSServingState() *dnsServingState {
	base := s.platformServing.Load()
	renewal := s.platformRenewal.Load()
	if renewal == nil || !sameDNSRenewalPublication(base, renewal.base) ||
		base.transition != renewal.base.transition || !renewal.state.checkedAt.After(base.checkedAt) ||
		base.record.RouteSources == nil || renewal.state.record.RouteSources == nil ||
		!dnsroutesource.Monotonic(base.record.RouteSources.Snapshot, renewal.state.record.RouteSources.Snapshot) {
		return base
	}
	return renewal.state
}

// A current independent observation with a full scan interval remaining lets
// the publication loop spend its budget on the candidate. The observer keeps
// renewing concurrently; it never acknowledges that candidate on its behalf.
func (s *Service) freshDNSRenewalCovers(base *dnsServingState) bool {
	current := s.currentDNSServingState()
	if current == base || !sameDNSRenewalPublication(base, current) || current.payload.Policy.DNSReadiness == nil {
		return false
	}
	horizon := time.Now().Add(time.Duration(current.payload.Policy.DNSReadiness.ProbeIntervalSeconds+current.payload.Policy.DNSReadiness.ProbeTimeoutSeconds) * time.Second)
	return dnsServingReady(current, horizon) && probeDNSServingSnapshot(current) == nil
}

func (s *Service) newDNSRenewalWorker() *Service {
	authority := platformcontrol.ConsumerAuthorityID(s.Config.EdgeGroupID)
	if s.PlatformTokenFile == "" || strings.TrimSpace(s.Config.CachePath) == "" || authority == "" || s.Config.PlatformScopeKey != "authority-cell:"+authority {
		return nil
	}
	cfg := s.Config
	cfg.CachePath += ".renewal"
	// Network scheduling and cursors are independent. Only authenticated
	// observation continuity is shared; neither scheduler owns its authority.
	return &Service{Config: cfg, HTTPClient: s.HTTPClient, Logger: s.Logger, PlatformTokenFile: s.PlatformTokenFile, sharedProbeHistories: s.probeHistoryStore()}
}

func (s *Service) probeHistoryStore() *dnsProbeHistoryStore {
	if s.sharedProbeHistories != nil {
		return s.sharedProbeHistories
	}
	return &s.dnsProbeHistories
}

func (s *Service) runDNSRenewal(ctx context.Context, worker *Service) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		if err := s.observeDNSRenewal(ctx, worker, routeprobe.Probe); err != nil && ctx.Err() == nil && s.Logger != nil {
			// Details are recorded by bounded phase diagnostics. Credentials or
			// response bodies never become log text here.
			s.Logger.Print("DNS independent serving renewal unavailable; original deadlines retained")
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Service) observeDNSRenewal(ctx context.Context, worker *Service, probe dnsReadinessProbeFunc) (resultErr error) {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	ctx, finish := startDNSSyncTrace(ctx, s.Logger, "serving_renewal")
	defer func() { finish(resultErr) }()
	base := s.platformServing.Load()
	old := s.currentDNSServingState()
	if worker == nil || !sameDNSRenewalPublication(base, old) || old.record.RouteSources == nil ||
		!old.record.AppliedAt.Add(time.Duration(old.payload.Policy.MaxStaleSeconds)*time.Second).After(time.Now()) {
		return nil
	}
	client := worker.platformConsumerClient()
	id, assignment, err := client.ObserveServingAssignment(ctx, model.PlatformConsumerComponentDNSServer, s.Config.DNSNodeID, platformcontrol.ConfiguredConsumerScope(s.Config.PlatformScopeKey), model.PlatformArtifactKindDNSAnswerBundle)
	if err != nil {
		return err
	}
	var refreshed *dnsServingState
	if reflect.DeepEqual(assignment, old.record.Candidate.Assignment) {
		// Gray may renew only while it remains the exact selected assignment.
		// A selected artifact read never permits applying that artifact here.
		candidate := old.record.Candidate
		payload, _, verifyErr := worker.verifyDNSServingRelease(old.record.Parent, candidate)
		if verifyErr != nil {
			return verifyErr
		}
		var facts []dnsReadinessFact
		payload, facts, err = worker.observeDNSRouteSources(ctx, client, id, candidate, payload, old.record.RouteSources, probe)
		if err != nil {
			return err
		}
		record := old.record
		record.RouteSources = payload.routeSources
		if err := worker.signDNSCheckpoint(&record); err != nil {
			return err
		}
		refreshed, err = buildDNSServingState(record, payload, old.routeID, s.Config.DNSNodeID, s.Config.EdgeGroupID, facts, time.Now().UTC())
		if err != nil {
			return err
		}
		if err := client.CheckServingAssignment(ctx, id, assignment); err != nil {
			return err
		}
	} else {
		// Once deselected, only the authority's positive full LKG has its own
		// independent grant. A historic gray cannot borrow this permission.
		refreshed, err = worker.refreshRetainedDNSRouteSources(ctx, old, probe, "independent_renewal", dnsRetainedRouteSourceReader{client: client, identity: id})
		if err != nil {
			return err
		}
	}
	if !sameDNSRenewalPublication(old, refreshed) || ctx.Err() != nil {
		return errors.New("DNS renewal changed publication or exceeded budget")
	}
	refreshed.transition = old.transition
	refreshed.fallback = old.fallback
	if s.platformServing.Load() != base {
		return platformconsumer.ErrAssignmentChanged
	}
	s.platformRenewal.Store(&dnsServingRenewal{base: base, state: refreshed})
	return nil
}
