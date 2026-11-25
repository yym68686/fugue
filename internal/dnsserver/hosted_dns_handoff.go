package dnsserver

import (
	"errors"
	"maps"
	"reflect"
	"slices"
	"time"

	"fugue/internal/model"
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

// Build, but do not publish, a closed successor from independent per-host
// commands. In particular, never seed its rows or observations from prior.
func prepareHostedDNSHandoff(prior *hostedDNSRoutes, next *dnsServingState, inputs map[string]hostedDNSHandoffInput, now time.Time) (*hostedDNSRoutes, error) {
	if prior == nil || next == nil || !next.record.Positive || next.transition != nil || len(inputs) == 0 || len(inputs) > 8192 {
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
		if err != nil || !p.ContinuesHostedMembership(*before, before.MemberBaseline, pin) || !hostedDNSParentMatches(next, in.base, p) {
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
