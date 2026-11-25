package dnsserver

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"maps"
	"os"
	"reflect"
	"slices"
	"time"

	"fugue/internal/lkgcache"
	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
	"fugue/internal/platformconsumer"
	"fugue/internal/routeartifact"
	"github.com/miekg/dns"
)

type hostedDNSHandoffInput struct {
	checkpoint platformconsumer.HostedCheckpoint
	command    model.PlatformArtifact
	base       map[routeartifact.HostedRecipient]routeartifact.HostedRecipientInput
	facts      map[routeartifact.HostedRecipient][]dnsReadinessFact
}

// Include all owners of shared keys, even when they are not the last writer.
func (prior *hostedDNSRoutes) heads() (map[string]*routeartifact.HostedSelectedPacket, error) {
	if prior == nil || len(prior.keys) == 0 {
		return nil, errHostedDNSPeerPending
	}
	heads := map[string]*routeartifact.HostedSelectedPacket{}
	add := func(p *routeartifact.HostedSelectedPacket) bool {
		if p == nil || p.Delivery.Host == "" {
			return false
		}
		if v := heads[p.Delivery.Host]; v != nil && (v.Fence != p.Fence || v.Decision != p.Decision || v.Parent != p.Parent) {
			return false
		}
		heads[p.Delivery.Host] = p
		return true
	}
	for _, w := range prior.keys {
		if w == nil || !add(w.packet) {
			return nil, errHostedDNSPeerPending
		}
		for _, m := range w.shared {
			if !add(m.packet) {
				return nil, errHostedDNSPeerPending
			}
		}
	}
	return heads, nil
}

// Build, but do not publish, a closed successor from independent per-host
// commands. In particular, never seed its rows or observations from prior.
func prepareHostedDNSHandoff(prior *hostedDNSRoutes, next *dnsServingState, inputs map[string]hostedDNSHandoffInput, now time.Time) (*hostedDNSRoutes, error) {
	if next == nil || !next.record.Positive || next.transition != nil || len(inputs) == 0 || len(inputs) > 8192 {
		return nil, errHostedDNSPeerPending
	}
	heads, e := prior.heads()
	if e != nil {
		return nil, e
	}
	if len(inputs) != len(heads) {
		return nil, errHostedDNSPeerPending
	}
	var idx *hostedDNSRoutes
	for _, host := range slices.Sorted(maps.Keys(heads)) {
		in, ok := inputs[host]
		cp := in.checkpoint
		if !ok || cp.Host != host || !reflect.DeepEqual(cp.Serving, next.record.Candidate.Assignment) {
			return nil, errHostedDNSPeerPending
		}
		p, err := routeartifact.VerifyHostedPacket(in.command, host, cp.Who, cp.Serving, 0, "", cp.Keys, now)
		if err != nil {
			return nil, err
		}
		pin, err := routeartifact.HostedDNSBaselinePin(p.Delivery, in.base)
		before := heads[host]
		// A crash may already have persisted some of the exact target
		// envelopes. Reopen those byte-equivalent heads, never a sibling's
		// decision, a skipped fence, or their old readiness observations.
		if err != nil || p.MemberBaseline == "" || p.MemberBaseline != pin ||
			(!reflect.DeepEqual(p, *before) && !p.ContinuesHostedMembership(*before, before.MemberBaseline, pin)) || !hostedDNSParentMatches(next, in.base, p) {
			return nil, errors.New("DNS successor does not continue its exact hostname predecessor")
		}
		idx, err = idx.withPacket(p, next, in.facts)
		if err != nil {
			return nil, err
		}
	}
	if len(idx.keys) != len(prior.keys) || len(idx.pending) != 0 {
		return nil, errHostedDNSPeerPending
	}
	for key, old := range prior.keys {
		w := idx.keys[key]
		if w == nil || w.forHost(old.packet.Delivery.Host) == nil || len(w.shared) != len(old.shared) {
			return nil, errHostedDNSPeerPending
		}
		for host := range old.shared {
			if w.forHost(host) == nil {
				return nil, errHostedDNSPeerPending
			}
		}
		if err := idx.checkHostedWire(next, key, now); err != nil {
			return nil, err
		}
	}
	return idx, nil
}

type hostedDNSParentExchange struct {
	prior  *hostedDNSRoutes
	next   *hostedDNSRoutes
	inputs map[string]hostedDNSHandoffInput
	heads  map[string]*routeartifact.HostedSelectedPacket
}

func (s *Service) readHostedDNSExchange(ctx context.Context, c platformconsumer.Client, id platformconsumer.Identity, parent *dnsServingState, prior *hostedDNSRoutes) (*hostedDNSParentExchange, error) {
	heads, err := prior.heads()
	if err != nil || len(heads) > 8192 || parent.payload.Policy.DNSReadiness == nil {
		return nil, errHostedDNSPeerPending
	}
	a := parent.record.Candidate.Assignment
	v := parent.payload
	view := pc.DNSArtifactPayload{ReadinessPlan: v.Plan, Records: v.Records, QueryViews: v.Queries, Policy: v.Policy}
	oldPublication := s.platformServing.publication.Load()
	current := func() bool {
		return ctx.Err() == nil && s.platformServing.publication.Load() == oldPublication && s.hostedDNS.Load() == prior
	}
	x := &hostedDNSParentExchange{prior: prior, heads: heads, inputs: map[string]hostedDNSHandoffInput{}}
	ordered := slices.Sorted(maps.Keys(heads))
	for _, host := range ordered {
		if !current() {
			return nil, platformconsumer.ErrAssignmentChanged
		}
		before := heads[host]
		cp, err := s.dnsHostCheckpoint(host)
		if err != nil {
			return nil, err
		}
		cp.Serving = a
		if id.BoundConsumerID() != cp.Who.ConsumerID() {
			return nil, platformconsumer.ErrAssignmentChanged
		}
		cmd, err := c.HostedCommand(ctx, id, a, host, before.Fence, before.Decision, cp.Keys)
		if err != nil {
			return nil, err
		}
		base, err := routeartifact.HostedDNSMemberBase(host, id.NodeID, s.Config.EdgeGroupID, view)
		if err != nil {
			return nil, err
		}
		if !hostedDNSParentMatches(parent, base, cmd.Packet) {
			return nil, errors.New("candidate DNS rows differ from signed host ownership")
		}
		facts, err := s.probeScopedDNS(ctx, cmd.Packet, parent, v.Policy.DNSReadiness.MaxConcurrency, current)
		if err != nil {
			return nil, err
		}
		x.inputs[host] = hostedDNSHandoffInput{cp, cmd.Artifact, base, facts}
	}
	// A's decision can change while probing B. Do not persist a prefix
	// of the read set, or convert a private candidate into a wire receipt.
	for _, host := range ordered {
		in := x.inputs[host]
		cmd, err := c.HostedCommand(ctx, id, a, host, in.command.GenerationSequence, in.command.Generation, in.checkpoint.Keys)
		if err != nil {
			return nil, err
		}
		if !current() || cmd.Artifact.ID != in.command.ID {
			return nil, platformconsumer.ErrAssignmentChanged
		}
		s.hostedDNSRecovery(cmd.Packet, v.Policy.DNSReadiness, in.facts, time.Now().UTC())
	}
	x.next, err = prepareHostedDNSHandoff(prior, parent, x.inputs, time.Now().UTC())
	return x, err
}

// With the host writer excluded, check *every* durable predecessor before
// advancing the first one. On an ambiguous persistence failure the caller
// restores reservations from disk without treating them as observations.
func (x *hostedDNSParentExchange) persist(now time.Time) error {
	hosts := slices.Sorted(maps.Keys(x.inputs))
	for _, host := range hosts {
		in := x.inputs[host]
		old, err := in.checkpoint.Reserved()
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(old, *x.heads[host]) {
			return platformconsumer.ErrAssignmentChanged
		}
	}
	for _, host := range hosts {
		in := x.inputs[host]
		if _, err := in.checkpoint.Commit(in.command, dnsHostParts(*x.heads[host]), in.base, now); err != nil {
			return err
		}
	}
	return nil
}

// Entry holds platformConsumerMu and hostedDNSMu. Local leases are not
// authority: recheck the selected release after filesystem preparation and
// before exchanging. No whole-parent success is claimed for scoped replies.
func (s *Service) applyHostedDNSExchange(st *dnsServingState, x *hostedDNSParentExchange, check func() error, wireProbe func(*dnsServingState) error) error {
	if s.Config.CachePath == "" || x.prior == nil || x.next == nil || s.hostedDNS.Load() != x.prior {
		return platformconsumer.ErrAssignmentChanged
	}
	heads, err := x.next.heads()
	if err != nil {
		return err
	}
	if old := s.platformServing.Load(); old != nil && !old.record.coversHostedFloor(heads) {
		return errors.New("DNS paired exchange would forget a durable hostname")
	}
	floor := make(map[string]dnsHostedFloor, len(heads))
	for host, p := range heads {
		floor[host] = dnsHostedFloor{p.Fence, p.Decision}
		if x.next == x.prior {
			cp, err := s.dnsHostCheckpoint(host)
			if err != nil {
				return err
			}
			cp.Serving = st.record.Candidate.Assignment
			old, err := cp.Load(time.Now().UTC())
			if err != nil || !reflect.DeepEqual(old, *p) {
				return errors.New("DNS live reservation differs from durable host head")
			}
		}
	}
	st.record.Schema, st.record.HostedFloor = dnsPairedCheckpoint, floor
	if err := s.signDNSCheckpoint(&st.record); err != nil {
		return err
	}
	raw, err := json.Marshal(st.record)
	if err != nil {
		return err
	}
	path := s.Config.CachePath + ".platform-serving.json"
	if err := lkgcache.PreservePrevious(path, s.validPreviousDNSCheckpoint); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := check(); err != nil {
		return err
	}
	checkFresh := func() error {
		return x.next.checkPairedSnapshot(st, time.Now().UTC())
	}
	if err := checkFresh(); err != nil {
		return err
	}
	// Queries only briefly wait for the disk commit, never for an API
	// call, HTTPS readiness pass, or a UDP/TCP loopback self-probe.
	s.hostedPairMu.Lock()
	err = x.persist(time.Now().UTC())
	if err == nil {
		err = lkgcache.AtomicWriteFile(path, raw, 0600)
	}
	if err == nil {
		err = checkFresh()
	}
	if err != nil {
		// We may have renamed any prefix, including every file. Refuse
		// scoped answers until the signed checkpoints are read again.
		blocked := *x.prior
		blocked.pending = map[string]bool{}
		for host := range heads {
			blocked.pending[host] = true
		}
		s.hostedDNS.Store(&blocked)
		recoverErr := s.reserveHostedDNS()
		s.hostedPairMu.Unlock()
		return errors.Join(err, recoverErr)
	}
	s.platformCheckpoint.Store(&dnsPersistedCheckpoint{sha256.Sum256(raw), st.record})
	s.hostedDNS.Store(x.next)
	s.platformServing.publish(st, dnsServingWriterCandidate)
	s.platformServingBound.Store(true)
	s.hostedPairMu.Unlock()
	// The old fences cannot be replayed after durable exchange. A failed
	// listener check is an error, not permission to reinstall either old
	// command. The ordinary host loop independently probes and reports.
	if err := wireProbe(st); err != nil {
		return err
	}
	s.servingDNSProbes.clearQueryBlockers(st.record.Candidate.Assignment)
	return nil
}

// Partition record obligations, not probe IDs. A parent obligation disappears
// only when every local occurrence and every required family have a signed
// scoped wire row. The scoped materializer checks the effective quorum,
// including still-global dependencies of a shared record. Requirements absent
// from this listener's zone inventory are never silently dropped.
func (idx *hostedDNSRoutes) checkPairedSnapshot(st *dnsServingState, now time.Time) error {
	if st == nil || st.transition != nil || st.payload.Plan == nil || st.payload.Policy.DNSReadiness == nil ||
		!st.record.Positive || !st.record.AppliedAt.Add(time.Duration(st.payload.Policy.MaxStaleSeconds)*time.Second).After(now) ||
		idx == nil || len(idx.keys) == 0 || len(idx.pending) != 0 {
		return errHostedDNSPeerPending
	}
	for k := range idx.keys {
		if err := idx.checkHostedWire(st, k, now); err != nil {
			return err
		}
	}
	covered := map[string]map[string]bool{}
	missing := map[string]bool{}
	for zone, z := range st.zones {
		for name, rows := range z.records {
			for _, row := range rows {
				kind := row.record.Type
				if kind != "A" && kind != "AAAA" {
					continue
				}
				if idx.keys[hostedZoneKey{zone, hostedRRKey{name, dns.StringToType[kind]}}] == nil {
					missing[name] = true
				} else {
					if covered[name] == nil {
						covered[name] = map[string]bool{}
					}
					covered[name][kind] = true
				}
			}
		}
	}
	plan := *st.payload.Plan
	plan.Records = nil
	for _, rr := range st.payload.Plan.Records {
		if missing[rr.Hostname] || len(covered[rr.Hostname]) == 0 || len(rr.Targets) == 0 ||
			slices.ContainsFunc(rr.Targets, func(t pc.DNSReadinessTarget) bool { return !covered[rr.Hostname][t.Family] }) {
			plan.Records = append(plan.Records, rr)
		}
	}
	status := summarizeDNSReadiness(&plan, st.payload.Policy.DNSReadiness, st.facts, "", st.checkedAt, now)
	if status.ReadyRecords != status.Records {
		return errors.New("DNS uncovered parent readiness incomplete")
	}
	return checkDNSSelectedSnapshot(st, idx, now, false)
}

// Exercise the actual selection and reply path without changing the listener
// or generating a receipt. A path quorum alone is not a wire answer.
func (idx *hostedDNSRoutes) checkHostedWire(parent *dnsServingState, key hostedZoneKey, now time.Time) error {
	if parent == nil || !parent.record.Positive || idx == nil {
		return errHostedDNSPeerPending
	}
	w := idx.keys[key]
	q := new(dns.Msg).SetQuestion(dns.Fqdn(key.name), key.family)
	st := idx.forQuery(parent, q, now)
	if w == nil || st.hostedWire != w || st.hostedBlocked {
		return errHostedDNSPeerPending
	}
	reply := st.answer(q, "", now)
	if reply.Rcode != dns.RcodeSuccess || len(reply.Answer) == 0 {
		return errors.New("DNS successor cannot answer its scoped record")
	}
	return nil
}
