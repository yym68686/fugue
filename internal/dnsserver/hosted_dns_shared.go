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
	return readSharedDNSProjection(host, part, false)
}
func readSharedDNSProjection(host string, part routeartifact.HostedRecipientInput, includeSingle bool) (*sharedDNSProjection, error) {
	bad := errors.New("shared DNS selection is not a closed custom-target path edit")
	u := part.DNS
	if u == nil {
		return nil, bad
	}
	multiHost := slices.ContainsFunc(u.Plan.Probes, func(p pc.DNSReadinessProbe) bool { return p.Hostname != host })
	if !includeSingle && !multiHost {
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
		if multiHost && target.RequireSinglePublication {
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

// Align two valid decisions by physical endpoint identity, never by equal
// slice lengths. A veto of any currently selected member keeps the shared
// target out, even when another member withdraws or replays an older packet.
func (v *sharedDNSProjection) without(deny map[pc.HostedAddressHold]bool) (*sharedDNSProjection, bool) {
	if v == nil {
		return nil, false
	}
	if len(deny) == 0 {
		return v, len(v.input.DNS.Withheld) == 0
	}
	missing := maps.Clone(deny)
	for _, h := range v.input.DNS.Withheld {
		if !missing[h] {
			return nil, false
		}
		delete(missing, h)
	}
	out := *v
	out.shape.Targets = nil
	out.hosts = map[string][][]pc.DNSReadinessProbe{}
	for name := range v.hosts {
		out.hosts[name] = nil
	}
	for i, t := range v.shape.Targets {
		h := pc.HostedAddressHold{EdgeID: t.EdgeID, EdgeGroupID: t.EdgeGroupID, Address: t.Address}
		if deny[h] {
			if !missing[h] {
				return nil, false
			}
			delete(missing, h)
			continue
		}
		out.shape.Targets = append(out.shape.Targets, t)
		for name, parts := range v.hosts {
			out.hosts[name] = append(out.hosts[name], parts[i])
		}
	}
	return &out, len(missing) == 0
}

func (v *sharedDNSProjection) sameShape(o *sharedDNSProjection) bool {
	return v != nil && o != nil && reflect.DeepEqual(v.input.Records, o.input.Records) && reflect.DeepEqual(v.input.DNS.Views, o.input.DNS.Views) &&
		reflect.DeepEqual(v.input.DNS.Rules, o.input.DNS.Rules) && reflect.DeepEqual(v.shape, o.shape) && len(v.hosts) == len(o.hosts) &&
		slices.Equal(slices.Sorted(maps.Keys(v.hosts)), slices.Sorted(maps.Keys(o.hosts)))
}

func sameSharedPredecessor(host string, base, candidate routeartifact.HostedRecipientInput) bool {
	explicit := candidate.DNS != nil && len(candidate.DNS.Withheld) > 0
	b, e := readSharedDNSProjection(host, base, explicit)
	c, f := readSharedDNSProjection(host, candidate, explicit)
	if e != nil || f != nil {
		return false
	}
	if b == nil || c == nil {
		return b == c
	}
	if explicit {
		deny := map[pc.HostedAddressHold]bool{}
		for _, h := range candidate.DNS.Withheld {
			if deny[h] {
				return false
			}
			deny[h] = true
		}
		var ok bool
		if b, ok = b.without(deny); !ok {
			return false
		}
		if c, ok = c.without(deny); !ok {
			return false
		}
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

func sameSharedMembership(a, b *routeartifact.HostedSelectedPacket) bool {
	return a.Parent == b.Parent && a.Child == b.Child && a.ChildDigest == b.ChildDigest &&
		a.Delivery.Member == b.Delivery.Member && a.Delivery.MembershipSet == b.Delivery.MembershipSet &&
		a.Delivery.MembershipRevision == b.Delivery.MembershipRevision && a.Delivery.MembershipRelease == b.Delivery.MembershipRelease &&
		a.Delivery.MembershipFence == b.Delivery.MembershipFence
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
	deny := map[pc.HostedAddressHold]bool{}
	for _, s := range w.shared {
		for _, h := range s.part.input.DNS.Withheld {
			deny[h] = true
		}
	}
	v, ok := member.part.without(deny)
	if !ok {
		return errors.New("shared DNS hold does not match its physical target")
	}
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
			common, ok := m.part.without(deny)
			if !ok || !common.sameShape(v) {
				return errors.New("shared DNS membership or wire record changed")
			}
			if !sameSharedMembership(m.packet, member.packet) {
				// Only an independently checked, later same-host continuation
				// can explain a split. Keep BOTH reservations across restart,
				// never return a wire row or credit mixed-parent evidence.
				later, earlier := member.packet, m.packet
				if later.Delivery.MembershipFence < earlier.Delivery.MembershipFence {
					later, earlier = earlier, later
				}
				if later.MembershipRebind == nil || earlier.MemberBaseline == "" || later.Parent == earlier.Parent ||
					later.Delivery.Member != earlier.Delivery.Member || later.Delivery.MembershipFence <= earlier.Delivery.MembershipFence {
					return errors.New("incompatible shared DNS memberships")
				}
				w.awaitingPeer = true
				usable = false
			}
			chunks = common.hosts[name]
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
	if len(rr.Targets) == 0 {
		return nil
	} // no common eligible endpoint; never fall back to the parent
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
