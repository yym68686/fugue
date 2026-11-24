package dnsserver

import (
	"errors"
	"maps"
	"reflect"
	"slices"
	"strings"
	"time"

	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
	"fugue/internal/routeartifact"
)

// A slice of a shared target contains only the requirements of one hostname
// at each physical endpoint. The wire RR, query policies, target identities
// and quorum always remain the selected parent's.
type sharedDNSProjection struct {
	input routeartifact.HostedRecipientInput
	shape pc.DNSReadinessRecord
	hosts map[string][][]pc.DNSReadinessProbe
}

func readSharedDNSPart(host string, part routeartifact.HostedRecipientInput) (*sharedDNSProjection, error) {
	bad := errors.New("shared DNS selection is not a closed custom-target path edit")
	u := part.DNS
	if u == nil {
		return nil, bad
	}
	if !slices.ContainsFunc(u.Plan.Probes, func(p pc.DNSReadinessProbe) bool { return p.Hostname != host }) {
		return nil, nil
	}
	if len(u.Plan.Records) != 1 || len(u.Views) != 1 || len(u.Plan.Records[0].Targets) == 0 || len(part.Records) == 0 {
		return nil, bad
	}
	rr := u.Plan.Records[0]
	for _, row := range part.Records {
		if row.Hostname != rr.Hostname || row.RecordKind != model.EdgeDNSRecordKindCustomDomainTarget || row.AppID == "" || row.TenantID == "" || row.Type != "A" && row.Type != "AAAA" {
			return nil, bad
		}
	}
	probes := make(map[string]pc.DNSReadinessProbe, len(u.Plan.Probes))
	for _, p := range u.Plan.Probes {
		if p.Hostname == "" || p.Hostname == rr.Hostname || p.ID == "" || probes[p.ID].ID != "" {
			return nil, bad
		}
		probes[p.ID] = p
	}
	v := &sharedDNSProjection{input: part, shape: rr, hosts: map[string][][]pc.DNSReadinessProbe{}}
	v.shape.Targets = slices.Clone(rr.Targets)
	for i, target := range rr.Targets {
		v.shape.Targets[i].ProbeIDs = nil
		if target.RequireSinglePublication {
			return nil, bad
		}
		for _, id := range target.ProbeIDs {
			p, ok := probes[id]
			if !ok || p.EdgeID != target.EdgeID || p.EdgeGroupID != target.EdgeGroupID || p.Address != target.Address {
				return nil, bad
			}
			delete(probes, id)
			if v.hosts[p.Hostname] == nil {
				if len(v.hosts) >= 128 {
					return nil, bad
				}
				v.hosts[p.Hostname] = make([][]pc.DNSReadinessProbe, len(rr.Targets))
			}
			v.hosts[p.Hostname][i] = append(v.hosts[p.Hostname][i], p)
		}
		if v.hosts[host] == nil || len(v.hosts[host][i]) == 0 {
			return nil, bad
		}
	}
	if len(probes) != 0 {
		return nil, bad
	}
	return v, nil
}

func (v *sharedDNSProjection) sameShape(o *sharedDNSProjection) bool {
	return v != nil && o != nil && reflect.DeepEqual(v.input.Records, o.input.Records) && reflect.DeepEqual(v.input.DNS.Views, o.input.DNS.Views) &&
		reflect.DeepEqual(v.input.DNS.Rules, o.input.DNS.Rules) && reflect.DeepEqual(v.shape, o.shape) && len(v.hosts) == len(o.hosts) &&
		slices.Equal(slices.Sorted(maps.Keys(v.hosts)), slices.Sorted(maps.Keys(o.hosts)))
}

func sameSharedPredecessor(host string, base, candidate routeartifact.HostedRecipientInput) bool {
	b, e := readSharedDNSPart(host, base)
	c, f := readSharedDNSPart(host, candidate)
	if e != nil || f != nil {
		return false
	}
	if b == nil || c == nil {
		return b == c
	}
	if !b.sameShape(c) {
		return false
	}
	for name, endpoints := range b.hosts {
		if name != host && !reflect.DeepEqual(endpoints, c.hosts[name]) {
			return false
		}
	}
	return true
}

type sharedDNSMember struct {
	packet   *routeartifact.HostedSelectedPacket
	part     *sharedDNSProjection
	facts    []dnsReadinessFact
	observed bool
}

type hostedDNSLease struct {
	Decision               string
	SelectedAt, ValidUntil time.Time
}

// Each member read its immutable global parent, which may predate its peers'
// newest commands. Never copy its peer requirements (or facts) over a peer
// checkpoint. Rebuild the effective union in stable order, including when
// the checkpoint directory is replayed in a different order.
func (w *hostedDNSWire) mergeShared(old *hostedDNSWire, member sharedDNSMember, record model.EdgeDNSRecord, parent *dnsServingState) error {
	host := member.packet.Delivery.Host
	w.shared = map[string]sharedDNSMember{}
	if old != nil {
		if old.shared == nil {
			return errors.New("exclusive and shared DNS keys overlap")
		}
		maps.Copy(w.shared, old.shared)
	}
	w.shared[host] = member
	v := member.part
	rr := v.shape
	rr.Targets = slices.Clone(rr.Targets)
	plan := pc.DNSReadinessPlan{}
	h := packetDNSDecision(member.packet)
	h.Peers = map[string]hostedDNSLease{}
	h.ParentIDs = map[string]bool{}
	var facts []dnsReadinessFact
	usable := parent != nil
	for _, name := range slices.Sorted(maps.Keys(v.hosts)) {
		chunks := v.hosts[name]
		m, selected := w.shared[name]
		if selected {
			if !m.part.sameShape(v) || m.packet.Parent != member.packet.Parent || m.packet.ChildDigest != member.packet.ChildDigest ||
				m.packet.Child != member.packet.Child || m.packet.Delivery.MembershipSet != member.packet.Delivery.MembershipSet ||
				m.packet.Delivery.MembershipRevision != member.packet.Delivery.MembershipRevision || m.packet.Delivery.MembershipRelease != member.packet.Delivery.MembershipRelease ||
				m.packet.Delivery.MembershipFence != member.packet.Delivery.MembershipFence {
				return errors.New("shared DNS membership or wire record changed")
			}
			chunks = m.part.hosts[name]
			h.Peers[name] = hostedDNSLease{m.packet.Decision, m.packet.SelectedAt, m.packet.ValidUntil}
			usable = usable && m.observed
		}
		ids := map[string]bool{}
		for i, chunk := range chunks {
			for _, p := range chunk {
				rr.Targets[i].ProbeIDs = append(rr.Targets[i].ProbeIDs, p.ID)
				plan.Probes = append(plan.Probes, p)
				ids[p.ID] = true
				if !selected {
					h.ParentIDs[p.ID] = true
				}
			}
		}
		for _, fact := range m.facts {
			if ids[fact.ProbeID] {
				facts = append(facts, fact)
			}
		}
	}
	for name := range w.shared {
		if v.hosts[name] == nil {
			return errors.New("shared DNS member disappeared")
		}
	}
	for i := range rr.Targets {
		slices.Sort(rr.Targets[i].ProbeIDs)
	}
	slices.SortFunc(plan.Probes, func(a, b pc.DNSReadinessProbe) int { return strings.Compare(a.ID, b.ID) })
	plan.Records = []pc.DNSReadinessRecord{rr}
	if parent != nil && pc.ValidateDNSReadinessPlan(&plan, parent.payload.Policy.DNSReadiness) != nil {
		return errors.New("invalid combined DNS requirements")
	}
	if usable {
		w.rows = []dnsServingRecord{{record: record, plan: plan, facts: facts, hosted: &h}}
	}
	return nil
}

func packetDNSDecision(p *routeartifact.HostedSelectedPacket) hostedDNSDecision {
	return hostedDNSDecision{Host: p.Delivery.Host, Decision: p.Decision, SelectedAt: p.SelectedAt, ValidUntil: p.ValidUntil}
}

func (w *hostedDNSWire) forHost(host string) *routeartifact.HostedSelectedPacket {
	if w == nil {
		return nil
	}
	if w.shared != nil {
		return w.shared[host].packet
	}
	if w.packet != nil && w.packet.Delivery.Host == host {
		return w.packet
	}
	return nil
}

// Unselected peers are still served by the global release. Read its current
// observations at query time: a newer negative on that same parent invalidates
// the shared answer immediately. A scoped packet can never supply such facts.
func (w *hostedDNSWire) effectiveRows(parent *dnsServingState, k hostedZoneKey) []dnsServingRecord {
	if len(w.rows) != 1 || w.shared == nil || len(w.rows[0].hosted.ParentIDs) == 0 {
		return w.rows
	}
	out := w.rows[0]
	out.facts = slices.Clone(out.facts)
	for _, entry := range parent.zones[k.zone].records[k.name] {
		if entry.record.Type != out.record.Type {
			continue
		}
		for _, f := range entry.facts {
			if out.hosted.ParentIDs[f.ProbeID] && dnsFactMatchesRelease(f, parent.record.Parent, parent.record.Candidate, parent.routeID, parent.payload) {
				out.facts = append(out.facts, f)
			}
		}
	}
	return []dnsServingRecord{out}
}
