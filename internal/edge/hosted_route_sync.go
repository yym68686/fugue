package edge

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
	"fugue/internal/platformconsumer"
	"fugue/internal/platformcontrol"
	"fugue/internal/routeartifact"
)

// A blocked HTTPS probe on A must not delay the ordinary serving loop.
// Startup has already reserved every persisted host before this starts.
func (s *Service) runHostedRoutes(ctx context.Context) {
	s.mu.Lock()
	enabled := s.Config.HostedExecutorAdmission || len(s.hostedRoutes) > 0
	s.mu.Unlock()
	if !enabled || s.PlatformTokenFile == "" {
		return
	}
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		if err := s.syncHostedRoutes(ctx); err != nil && ctx.Err() == nil && !errors.Is(err, platformconsumer.ErrNoServingAssignment) {
			s.Logger.Printf("edge scoped route sync pending: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// This is the executor loop, not the serving observer. Never use a global
// receipt as a scoped receipt. Discovery of zero hosts performs no writes
// and does not load a full route artifact. Per-host failures do not stop B.
func (s *Service) syncHostedRoutes(ctx context.Context) error {
	if !s.Config.HostedExecutorAdmission {
		s.mu.Lock()
		reserved := len(s.hostedRoutes) > 0
		s.mu.Unlock()
		if !reserved {
			return nil
		}
	}
	b, ok := s.Bundle()
	if !ok || b.TrafficRelease == nil || b.TrafficRelease.ReleaseChannel != "full" || b.TrafficRelease.ScopeKey != "global" || s.PlatformTokenFile == "" {
		return nil
	}
	act, e := s.selectRouteBundleSource()
	if e != nil {
		return e
	}
	if act.reporterInactive || act.candidate || act.preparation != nil || act.activeSlot != "" && act.activeSlot != s.Config.EdgeSlot {
		return nil
	}
	cl := platformconsumer.Client{BaseURL: s.Config.APIURL, TokenFile: s.PlatformTokenFile, HTTPClient: s.HTTPClient, AuthorityID: platformcontrol.ConsumerAuthorityID(s.Config.EdgeGroupID)}
	id, a, e := cl.ObserveServingAssignment(ctx, model.PlatformConsumerComponentEdgeWorker, s.Config.EdgeID, "global", model.PlatformArtifactKindEdgeRouteBundle)
	if e != nil {
		return e
	}
	if a.ReleaseChannel != "full" || a.ReleaseSetID != b.TrafficRelease.ReleaseSetID || a.ArtifactID != b.TrafficRelease.RouteArtifactID || a.ContentHash != b.TrafficRelease.RouteArtifactDigest || a.FencingToken != b.TrafficRelease.FencingToken || a.ArtifactReleaseID != b.TrafficRelease.ReleaseID {
		return platformconsumer.ErrAssignmentChanged
	}
	s.mu.Lock()
	after := s.hostedAfter
	s.mu.Unlock()
	page, e := cl.HostedSelections(ctx, id, a, after)
	if e != nil {
		return e
	}
	s.mu.Lock()
	s.hostedAfter = page.Next
	s.mu.Unlock()
	if len(page.Selections) == 0 {
		return nil
	}
	identity, assigned, child, release, e := cl.SyncServing(ctx, id.Component, id.NodeID, id.ScopeKey, a.ArtifactKind)
	if e != nil {
		return e
	}
	if !reflect.DeepEqual(assigned, a) || identity.BoundConsumerID() != id.BoundConsumerID() {
		return platformconsumer.ErrAssignmentChanged
	}
	parent, e := cl.ServingReleaseSet(ctx, identity, a, release)
	if e != nil {
		return e
	}
	keys := bundleauth.NewKeyring(s.Config.BundleSigningKey, s.Config.BundleSigningKeyID, s.Config.BundleSigningPreviousKey, s.Config.BundleSigningPreviousKeyID, s.Config.BundleRevokedKeyIDs)
	snapshot, e := routeartifact.ProjectRelease(parent, child, a, release, keys)
	if e != nil {
		return e
	}
	projection, e := routeartifact.MaterializeSnapshotForGroup(snapshot, s.Config.EdgeGroupID)
	if e != nil {
		return e
	}
	enrollment, e := platformcontrol.ConsumerEnrollmentFromID(id.Component, id.NodeID, id.AuthorityID, id.ScopeKey, id.BoundConsumerID())
	if e != nil {
		return e
	}
	who := platformcontrol.PlatformComponentIdentityClaims{Component: id.Component, NodeID: id.NodeID, AuthorityID: id.AuthorityID, ScopeKey: id.ScopeKey, WorkerEnrollmentID: enrollment, ArtifactKinds: id.ArtifactKinds}
	tid, ta, tlsChild, tr, e := cl.SyncServing(ctx, id.Component, id.NodeID, id.ScopeKey, model.PlatformArtifactKindCaddyRouteConfig)
	if e != nil {
		return e
	}
	if tid.BoundConsumerID() != id.BoundConsumerID() || ta.ReleaseChannel != "full" || ta.ReleaseSetID != a.ReleaseSetID || ta.ArtifactReleaseID != a.ArtifactReleaseID || ta.FencingToken != a.FencingToken {
		return platformconsumer.ErrAssignmentChanged
	}
	tparent, e := cl.ServingReleaseSet(ctx, tid, ta, tr)
	if e != nil {
		return e
	}
	if tparent.ContentHash != parent.ContentHash || tparent.ID != parent.ID {
		return platformconsumer.ErrAssignmentChanged
	}
	member := false
	ids, _ := parent.Content["artifact_ids"].([]any)
	kinds, _ := parent.Content["artifact_kinds"].([]any)
	if len(ids) == len(kinds) {
		for i, k := range kinds {
			if k == ta.ArtifactKind && ids[i] == ta.ArtifactID {
				member = true
			}
		}
	}
	if !member {
		return errors.New("TLS child is not a member of this route release")
	}
	payload, e := s.verifyPlatformTLSCandidate(edgePlatformCandidate{Artifact: tlsChild, Assignment: ta, Release: tr}, edgePlatformCandidate{Artifact: child, Assignment: a, Release: release, ReleaseSet: &parent, TrafficRelease: b.TrafficRelease})
	if e != nil {
		return e
	}
	if payload.Policy.TLSReadiness == nil || pc.ValidateReadinessProbePolicy(payload.Policy.TLSReadiness) != nil {
		return errHostedGroup
	}
	one := func(v routeartifact.HostedHint) error {
		// The hint is not a command: refetch, including the private
		// assignment recheck, even after a disk-only restart.
		cmd, e := cl.HostedCommand(ctx, identity, a, v.Host, v.Fence, v.Decision, keys)
		if e != nil {
			return e
		}
		cert, e := cl.HostedCommand(ctx, tid, ta, v.Host, cmd.Packet.Fence, cmd.Packet.Decision, keys)
		if e != nil {
			return e
		}
		if cert.Packet.Decision != cmd.Packet.Decision || cert.Packet.Fence != cmd.Packet.Fence {
			return platformconsumer.ErrAssignmentChanged
		}
		started := time.Now().UTC()
		// Repeat observations renew evidence, never the configuration grant.
		// Reopen the durable checkpoint on a repeat rather than rewriting it
		// or replacing A's listener pointer while other observers use it.
		applyErr := s.ensureHostedRoute(cmd, v.Host, who, a, projection)
		report := func(id platformconsumer.Identity, sel model.PlatformConsumerAssignment, p routeartifact.HostedSelectedPacket) error {
			o := routeartifact.HostedObservation{Host: v.Host, Member: p.Delivery.Member, Decision: p.Decision, Fence: p.Fence, StartedAt: started}
			var obsErr error
			if applyErr != nil {
				o.Failure = "local_apply"
			} else {
				o.Samples, obsErr = s.witnessHostedWorker(ctx, cmd.Packet, p, payload, nil)
				if obsErr != nil {
					o.Failure = "wire_observation"
				}
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if current, err := s.selectRouteBundleSource(); err != nil || current != act || current.reporterInactive {
				return platformconsumer.ErrAssignmentChanged
			}
			return errors.Join(obsErr, cl.ReportHostedObservation(ctx, id, sel, o))
		}
		return errors.Join(applyErr, report(identity, a, cmd.Packet), report(tid, ta, cert.Packet))
	}
	workers := min(len(page.Selections), 4, payload.Policy.TLSReadiness.MaxConcurrency)
	errs := make([]error, len(page.Selections))
	tasks := make(chan int, len(errs))
	for i := range errs {
		tasks <- i
	}
	close(tasks)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range tasks {
				if ctx.Err() != nil {
					return
				}
				errs[i] = one(page.Selections[i])
			}
		}()
	}
	wg.Wait()
	return errors.Join(ctx.Err(), errors.Join(errs...))
}
