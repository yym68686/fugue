package dnsserver

import (
	"errors"
	"net/netip"
	"sort"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"github.com/miekg/dns"
)

type dnsServingRecord struct {
	record model.EdgeDNSRecord
	plan   platformconfig.DNSReadinessPlan
	facts  []dnsReadinessFact
}
type dnsServingZone struct {
	authority platformconfig.DNSAuthorityPolicy
	records   map[string][]dnsServingRecord
}
type dnsServingState struct {
	record    dnsServingCheckpoint
	payload   dnsServingPayload
	routeID   string
	facts     []dnsReadinessFact
	checkedAt time.Time
	fallback  string
	zones     map[string]dnsServingZone
	zoneOrder []string
	matcher   platformconfig.DNSClientMatcher
}

func buildDNSServingState(record dnsServingCheckpoint, p dnsServingPayload, routeID, node, group string, facts []dnsReadinessFact, now time.Time) (*dnsServingState, error) {
	st := &dnsServingState{record: record, payload: p, routeID: routeID, facts: facts, checkedAt: now, zones: map[string]dnsServingZone{}}
	clientFound := false
	for _, policy := range p.Policy.DNSClientPolicies {
		if policy.NodeID == node {
			var err error
			st.matcher, err = platformconfig.NewDNSClientMatcher(policy.Rules)
			if err != nil {
				return nil, err
			}
			clientFound = true
		}
	}
	if !clientFound {
		return nil, errors.New("DNS serving client policy missing")
	}
	authorities := map[string]platformconfig.DNSAuthorityPolicy{}
	for _, a := range p.Policy.DNSAuthorities {
		if a.NodeID == node {
			authorities[a.Zone] = a
		}
	}
	probes := map[string]platformconfig.DNSReadinessProbe{}
	for _, p := range p.Plan.Probes {
		probes[p.ID] = p
	}
	factByID := map[string]dnsReadinessFact{}
	for _, f := range facts {
		factByID[f.ProbeID] = f
	}
	readiness := map[string]platformconfig.DNSReadinessRecord{}
	for _, r := range p.Plan.Records {
		readiness[r.Hostname] = r
	}
	for _, view := range p.Queries {
		if view.NodeID != node {
			continue
		}
		if view.EdgeGroupID != group {
			return nil, errors.New("DNS serving group mismatch")
		}
		a, ok := authorities[view.Zone]
		if !ok {
			return nil, errors.New("DNS serving zone authority missing")
		}
		z := dnsServingZone{authority: a, records: map[string][]dnsServingRecord{}}
		for _, r := range view.Records {
			entry := dnsServingRecord{record: r}
			if req, ok := readiness[r.Name]; ok && (r.Type == "A" || r.Type == "AAAA") {
				entry.plan.Records = []platformconfig.DNSReadinessRecord{req}
				seen := map[string]bool{}
				for _, target := range req.Targets {
					for _, id := range target.ProbeIDs {
						if seen[id] {
							continue
						}
						seen[id] = true
						entry.plan.Probes = append(entry.plan.Probes, probes[id])
						if f, ok := factByID[id]; ok {
							entry.facts = append(entry.facts, f)
						}
					}
				}
			}
			z.records[r.Name] = append(z.records[r.Name], entry)
		}
		st.zones[view.Zone] = z
		st.zoneOrder = append(st.zoneOrder, view.Zone)
	}
	if len(st.zones) == 0 || len(st.zones) != len(authorities) {
		return nil, errors.New("DNS serving zone ownership incomplete")
	}
	sort.Slice(st.zoneOrder, func(i, j int) bool { return len(st.zoneOrder[i]) > len(st.zoneOrder[j]) })
	return st, nil
}

func (st *dnsServingState) answer(req *dns.Msg, remote string, now time.Time) *dns.Msg {
	resp := new(dns.Msg)
	resp.SetReply(req)
	resp.Authoritative = true
	if len(req.Question) != 1 {
		resp.Rcode = dns.RcodeFormatError
		return resp
	}
	q := req.Question[0]
	name := normalizeName(q.Name)
	zone := ""
	for _, z := range st.zoneOrder {
		if nameWithinZone(name, z) {
			zone = z
			break
		}
	}
	if zone == "" {
		resp.Rcode = dns.RcodeRefused
		return resp
	}
	if st.payload.Policy.MaxStaleSeconds <= 0 || now.After(st.record.AppliedAt.Add(time.Duration(st.payload.Policy.MaxStaleSeconds)*time.Second)) {
		resp.Rcode = dns.RcodeServerFailure
		return resp
	}
	z := st.zones[zone]
	a := z.authority
	soa := &dns.SOA{Hdr: dns.RR_Header{Name: fqdn(zone), Rrtype: dns.TypeSOA, Class: dns.ClassINET, Ttl: uint32(a.TTLSeconds)}, Ns: fqdn(a.Nameservers[0]), Mbox: fqdn("hostmaster." + zone), Serial: uint32(st.record.Candidate.Artifact.GenerationSequence), Refresh: uint32(a.RefreshSeconds), Retry: uint32(a.RetrySeconds), Expire: uint32(a.ExpireSeconds), Minttl: uint32(a.TTLSeconds)}
	if q.Qclass != dns.ClassINET {
		resp.Rcode = dns.RcodeRefused
		return resp
	}
	if q.Qtype == dns.TypeSOA {
		if name == zone {
			resp.Answer = []dns.RR{soa}
		} else {
			resp.Ns = []dns.RR{soa}
		}
		return resp
	}
	rows, exists := z.records[name]
	if !exists {
		rows, exists = z.records[edgeDNSWildcardName(name)]
	}
	hint := platformDNSHintForQuery(st.matcher, req, remote)
	answer := func(kind string) {
		for _, entry := range rows {
			if entry.record.Type != kind {
				continue
			}
			records, err := materializeDNSQueries(platformconfig.DNSQueryView{Records: []model.EdgeDNSRecord{entry.record}}, &entry.plan, st.payload.Policy.DNSReadiness, entry.facts, now)
			if err != nil || len(records) != 1 {
				resp.Rcode = dns.RcodeServerFailure
				resp.Answer = nil
				return
			}
			rrs, _, _ := rrForEdgeDNSRecordWithGeoAudit(records[0], name, q.Qtype, hint, nil, nil)
			resp.Answer = append(resp.Answer, rrs...)
		}
	}
	answer(dns.TypeToString[q.Qtype])
	if len(resp.Answer) == 0 && (q.Qtype == dns.TypeA || q.Qtype == dns.TypeAAAA) {
		answer("CNAME")
	}
	if len(resp.Answer) == 0 && q.Qtype == dns.TypeNS && name == zone {
		for _, ns := range a.Nameservers {
			resp.Answer = append(resp.Answer, &dns.NS{Hdr: dns.RR_Header{Name: fqdn(zone), Rrtype: dns.TypeNS, Class: dns.ClassINET, Ttl: uint32(a.TTLSeconds)}, Ns: fqdn(ns)})
		}
	}
	if len(resp.Answer) == 0 && resp.Rcode == dns.RcodeSuccess {
		if !exists && name != zone {
			resp.Rcode = dns.RcodeNameError
		}
		resp.Ns = []dns.RR{soa}
	}
	applyPlatformECSScope(resp, req, hint)
	return resp
}

func applyPlatformECSScope(resp, req *dns.Msg, hint dnsGeoHint) {
	opt := req.IsEdns0()
	if opt == nil {
		return
	}
	var subnet *dns.EDNS0_SUBNET
	for _, o := range opt.Option {
		if e, ok := o.(*dns.EDNS0_SUBNET); ok {
			if subnet != nil {
				return
			}
			subnet = e
		}
	}
	if subnet == nil {
		return
	}
	ip, valid := netip.AddrFromSlice(subnet.Address)
	if !valid || subnet.SourceScope != 0 {
		return
	}
	ip = ip.Unmap()
	if !((subnet.Family == 1 && ip.Is4() && subnet.SourceNetmask <= 32) || (subnet.Family == 2 && ip.Is6() && subnet.SourceNetmask <= 128)) {
		return
	}
	resp.SetEdns0(opt.UDPSize(), false)
	scope := uint8(0)
	if hint.Source == "ecs" && (hint.Country != "" || hint.Region != "" || hint.ASN != "" || hint.EdgeGroupID != "") {
		scope = subnet.SourceNetmask
	}
	copy := *subnet
	copy.SourceScope = scope
	resp.IsEdns0().Option = append(resp.IsEdns0().Option, &copy)
}
