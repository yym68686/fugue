package dnsserver

import (
	"context"
	"errors"
	"os"
	"reflect"
	"sync"
	"time"

	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
	"fugue/internal/platformconsumer"
	"fugue/internal/routeartifact"
)

func dnsHostParts(p routeartifact.HostedSelectedPacket) map[routeartifact.HostedRecipient]routeartifact.HostedRecipientInput {
	live := map[routeartifact.HostedRecipient]routeartifact.HostedRecipientInput{}
	for _, part := range p.Delivery.Parts {
		live[part.Recipient] = part.Content
	}
	return live
}

var errHostedDNSPeerPending = errors.New("shared DNS records await a peer's independently signed membership")

// The network caller has re-read the private selection after the probes.
// Compare it with the independently verified full, actual zone rows and
// durable predecessor; one pointer publishes every zone of this host.
func (s *Service) installHostedDNS(cp platformconsumer.HostedCheckpoint, command model.PlatformArtifact, parent *dnsServingState, base map[routeartifact.HostedRecipient]routeartifact.HostedRecipientInput, facts map[routeartifact.HostedRecipient][]dnsReadinessFact) error {
	s.hostedDNSMu.Lock()
	defer s.hostedDNSMu.Unlock()
	now := time.Now().UTC()
	p, e := routeartifact.VerifyHostedPacket(command, cp.Host, cp.Who, cp.Serving, 0, "", s.platformDNSKeys(), now)
	if e != nil {
		return e
	}
	if parent == nil || s.currentDNSServingState() != parent || p.Delivery.Parts[0].Recipient.Node != s.Config.DNSNodeID || p.Delivery.Parts[0].Recipient.Group != s.Config.EdgeGroupID || !reflect.DeepEqual(parent.record.Candidate.Assignment, cp.Serving) || !hostedDNSParentMatches(parent, base, p) {
		return errors.New("DNS selection is not a complete projection of the loaded parent")
	}
	current := s.hostedDNS.Load()
	if p.MemberBaseline != "" {
		pin, err := routeartifact.HostedDNSBaselinePin(p.Delivery, base)
		if err != nil || pin != p.MemberBaseline {
			return errors.New("DNS host baseline differs from its signed command")
		}
	}
	live := base
	old, e := cp.Reserved()
	if e == nil {
		cross := old.Parent != p.Parent
		repeat := old.Fence == p.Fence && old.Decision == p.Decision
		if cross && !p.ContinuesHostedMembership(old, old.MemberBaseline, p.MemberBaseline) ||
			!cross && (old.ChildDigest != p.ChildDigest || !repeat && p.MembershipRebind != nil) {
			return errors.New("DNS selection requires explicit membership rebind")
		}
		live = dnsHostParts(old)
	} else if !errors.Is(e, os.ErrNotExist) {
		return e
	}
	if current != nil {
		for _, w := range current.keys {
			if local := w.forHost(cp.Host); local != nil && (local.Decision != old.Decision || local.Fence != old.Fence) {
				return errors.New("DNS checkpoint and live host predecessor disagree")
			}
		}
	}
	next, e := current.withPacket(p, parent, facts)
	if e != nil {
		return e
	}
	// Don't exchange the predecessor before the whole new host can really
	// answer. For a repeat, install negative/partial facts promptly instead
	// of extending an old positive observation.
	if p.Fence != old.Fence {
		if next.pending[cp.Host] && (old.Parent == p.Parent || p.MembershipRebind == nil) {
			return errHostedDNSPeerPending // do not stage an unrelated forward or withdrawal
		}
		for k, w := range next.keys {
			if w.forHost(cp.Host) != nil {
				// The old parent is already no longer selected. Persisting
				// this exact continuation reserves the key; it cannot serve,
				// be witnessed or erase a peer's checkpoint.
				if w.awaitingPeer {
					continue
				}
				effective := w.effectiveRows(parent, k)
				if len(effective) != 1 {
					return errors.New("DNS selection awaiting fresh path proofs")
				}
				rr := effective[0]
				rows, err := materializeDNSQueryScope(pc.DNSQueryView{Records: []model.EdgeDNSRecord{rr.record}}, &rr.plan, parent.payload.Policy.DNSReadiness, rr.facts, now, true, rr.hosted)
				if err != nil || len(rows) != 1 || len(rows[0].Values) == 0 {
					return errors.New("DNS selection has no current quorum for " + k.name)
				}
			}
		}
	}
	s.platformReceiptMu.Lock()
	defer s.platformReceiptMu.Unlock()
	if s.currentDNSServingState() != parent {
		return platformconsumer.ErrAssignmentChanged
	}
	if _, e = cp.Commit(command, live, base, now); e != nil {
		// Atomic rename can have succeeded before the directory fsync failed.
		// Never leave a superseded predecessor answering in this ambiguous case.
		if disk, err := cp.Reserved(); err == nil && disk.Fence == p.Fence && disk.Decision == p.Decision {
			if blocked, err := current.withPacket(p, parent, nil); err == nil {
				s.hostedDNS.Store(blocked)
			}
		}
		return e
	}
	s.hostedDNS.Store(next)
	if next.pending[cp.Host] {
		return errHostedDNSPeerPending
	}
	return nil
}

func (s *Service) syncHostedDNS(ctx context.Context) error {
	if !s.Config.HostedExecutorAdmission && s.hostedDNS.Load() == nil {
		return nil
	}
	parent := s.currentDNSServingState()
	if parent == nil || !parent.record.Positive || s.PlatformTokenFile == "" || parent.record.Candidate.Assignment.ScopeKey != "global" || parent.record.Candidate.Assignment.ReleaseChannel != "full" {
		return nil
	}
	cl := s.platformConsumerClient()
	kind := model.PlatformArtifactKindDNSAnswerBundle
	id, a, e := cl.ObserveServingAssignment(ctx, model.PlatformConsumerComponentDNSServer, s.Config.DNSNodeID, "global", kind)
	if e != nil {
		return e
	}
	if !reflect.DeepEqual(a, parent.record.Candidate.Assignment) {
		return platformconsumer.ErrAssignmentChanged
	}
	page, e := cl.HostedSelections(ctx, id, a, s.hostedDNSAfter)
	if e != nil {
		return e
	}
	s.hostedDNSAfter = page.Next // private to this single periodic loop
	if len(page.Selections) == 0 {
		return nil
	}
	id2, a2, child, rel, e := cl.SyncServing(ctx, id.Component, id.NodeID, id.ScopeKey, kind)
	if e != nil {
		return e
	}
	if id.BoundConsumerID() != id2.BoundConsumerID() || !reflect.DeepEqual(a, a2) {
		return platformconsumer.ErrAssignmentChanged
	}
	full, e := cl.ServingReleaseSet(ctx, id2, a2, rel)
	if e != nil {
		return e
	}
	candidate := dnsPlatformCandidate{Artifact: child, Assignment: a2, Release: rel}
	payload, _, e := s.verifyDNSServingRelease(full, candidate)
	if e != nil {
		return e
	}
	if full.ID != parent.record.Parent.ID || full.ContentHash != parent.record.Parent.ContentHash {
		return platformconsumer.ErrAssignmentChanged
	}
	view := func(v dnsServingPayload) pc.DNSArtifactPayload {
		return pc.DNSArtifactPayload{ReadinessPlan: v.Plan, Records: v.Records, QueryViews: v.Queries, Policy: v.Policy}
	}
	parallel := min(4, len(page.Selections), payload.Policy.DNSReadiness.MaxConcurrency)
	if parallel < 1 {
		return errors.New("invalid DNS host probe budget")
	}
	one := func(h routeartifact.HostedHint) error {
		cp, e := s.dnsHostCheckpoint(h.Host)
		if e != nil {
			return e
		}
		cp.Serving = a
		if cp.Who.ConsumerID() != id.BoundConsumerID() {
			return platformconsumer.ErrAssignmentChanged
		}
		base, e := routeartifact.HostedDNSMemberBase(h.Host, id.NodeID, s.Config.EdgeGroupID, view(payload))
		if e != nil {
			return e
		}
		liveFull, e := routeartifact.HostedDNSMemberBase(h.Host, id.NodeID, s.Config.EdgeGroupID, view(parent.payload))
		if e != nil || !reflect.DeepEqual(liveFull, base) {
			return errors.New("DNS host full and live projection differ")
		}
		cmd, e := cl.HostedCommand(ctx, id, a, h.Host, h.Fence, h.Decision, cp.Keys)
		if e != nil {
			return e
		}
		if !hostedDNSParentMatches(parent, base, cmd.Packet) {
			return errors.New("DNS packet's record ownership differs from full")
		}
		observation := routeartifact.HostedObservation{Host: h.Host, Member: id.BoundConsumerID(), Decision: cmd.Packet.Decision, Fence: cmd.Packet.Fence, StartedAt: time.Now().UTC()}
		facts, obsErr := s.observeHostedDNSMember(ctx, cmd.Packet, parent, payload.Policy.DNSReadiness.MaxConcurrency/parallel)
		current, e := cl.HostedCommand(ctx, id, a, h.Host, cmd.Packet.Fence, cmd.Packet.Decision, cp.Keys)
		if e != nil {
			return e
		}
		if current.Artifact.ID != cmd.Artifact.ID {
			return platformconsumer.ErrAssignmentChanged
		}
		s.hostedDNSRecovery(cmd.Packet, parent.payload.Policy.DNSReadiness, facts, time.Now().UTC())
		installErr := s.installHostedDNS(cp, current.Artifact, parent, base, facts)
		if obsErr == nil && errors.Is(installErr, errHostedDNSPeerPending) {
			return nil
		} // no positive or negative wire receipt for staging
		applyErr := errors.Join(obsErr, installErr)
		if applyErr != nil {
			observation.Failure = "local_apply"
		} else {
			observation.Samples, applyErr = s.witnessHostedDNSMember(ctx, observation.Host, observation.Fence, observation.Decision)
			if applyErr != nil {
				observation.Failure = "wire_observation"
			}
		}
		return errors.Join(applyErr, cl.ReportHostedObservation(ctx, id, a, observation))
	}
	// Advance healthy hosts even if A's HTTPS path is stuck. The sum of
	// all hosted probes stays within the parent's physical concurrency bound.
	jobs := make(chan int, len(page.Selections))
	for i := range page.Selections {
		jobs <- i
	}
	close(jobs)
	errs := make([]error, len(page.Selections))
	var wg sync.WaitGroup
	for range parallel {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
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

func (s *Service) runHostedDNS(ctx context.Context) {
	for {
		// Discovery and physical probes have an independent budget. Never hold
		// up the normal global serving/renewal loops or listener startup.
		if err := s.syncHostedDNS(ctx); err != nil && ctx.Err() == nil {
			s.Logger.Printf("scoped DNS reconciliation: %v", err)
		}
		interval := platformDNSConsumerPollInterval
		if st := s.currentDNSServingState(); s.hostedDNS.Load() != nil && st != nil && st.payload.Policy.DNSReadiness != nil {
			interval = time.Duration(st.payload.Policy.DNSReadiness.ProbeIntervalSeconds) * time.Second
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}
