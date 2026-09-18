package platformconfig

import (
	"fmt"
	"net/netip"
	"reflect"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
	"github.com/miekg/dns"
)

type DNSConsumerIntent struct {
	NodeID      string   `json:"node_id"`
	EdgeGroupID string   `json:"edge_group_id"`
	Zones       []string `json:"zones"`
	ProbeLabel  string   `json:"probe_label"`
	ProbeTTL    int      `json:"probe_ttl"`
}

type DNSConsumerObservation struct {
	NodeID      string    `json:"node_id"`
	EdgeGroupID string    `json:"edge_group_id"`
	ObservedAt  time.Time `json:"observed_at"`
	A           []string  `json:"a,omitempty"`
	AAAA        []string  `json:"aaaa,omitempty"`
}

// A view adds consumer-local listener records without copying global desired
// application records or granting application readiness.
type DNSConsumerView struct {
	NodeID      string      `json:"node_id"`
	EdgeGroupID string      `json:"edge_group_id"`
	Zone        string      `json:"zone"`
	ProbeLabel  string      `json:"probe_label"`
	Records     []DNSIntent `json:"records"`
}

func normalizeDNSConsumers(in []DNSConsumerIntent) []DNSConsumerIntent {
	out := append([]DNSConsumerIntent(nil), in...)
	for i := range out {
		out[i].Zones = append([]string(nil), in[i].Zones...)
		sort.Strings(out[i].Zones)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out
}
func normalizeDNSConsumerObservations(in []DNSConsumerObservation) []DNSConsumerObservation {
	out := append([]DNSConsumerObservation(nil), in...)
	for i := range out {
		out[i].A = normalizeDNSValues("A", in[i].A)
		out[i].AAAA = normalizeDNSValues("AAAA", in[i].AAAA)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out
}
func validDNSConsumerIdentity(v string) bool {
	return v != "" && v == strings.TrimSpace(v) && len(v) <= 256 && !strings.ContainsAny(v, "\x00\r\n\t")
}
func validDNSConsumerZone(v string) bool {
	_, ok := dns.IsDomainName(dns.Fqdn(v))
	return ok && v != "" && v == normalizedImportHostname(v) && !strings.ContainsAny(v, "* /\t\r\n")
}
func ValidateDNSConsumers(consumers []DNSConsumerIntent) error {
	if len(consumers) > 4096 {
		return fmt.Errorf("too many DNS consumers")
	}
	seen := map[string]bool{}
	totalViews := 0
	for _, c := range consumers {
		totalViews += len(c.Zones)
		if totalViews > 16384 {
			return fmt.Errorf("too many DNS consumer zones")
		}
		if !validDNSConsumerIdentity(c.NodeID) || !validDNSConsumerIdentity(c.EdgeGroupID) || seen[c.NodeID] || len(c.Zones) == 0 || len(c.Zones) > 256 || c.ProbeTTL < 1 || c.ProbeTTL > 3600 || !validDNSConsumerZone(c.ProbeLabel) || strings.Contains(c.ProbeLabel, ".") {
			return fmt.Errorf("DNS consumer declaration is invalid or duplicated")
		}
		seen[c.NodeID] = true
		zones := map[string]bool{}
		for _, zone := range c.Zones {
			if !validDNSConsumerZone(zone) || zones[zone] {
				return fmt.Errorf("DNS consumer zone is invalid or duplicated")
			}
			zones[zone] = true
		}
	}
	return nil
}

func CompileDNSConsumerViews(consumers []DNSConsumerIntent, snapshot RuntimeSnapshot, records []DNSIntent) ([]DNSConsumerView, error) {
	if err := ValidateDNSConsumers(consumers); err != nil {
		return nil, err
	}
	if len(snapshot.DNSConsumers) > 4096 {
		return nil, fmt.Errorf("too many DNS endpoint observations")
	}
	facts := map[string]DNSConsumerObservation{}
	owners := map[string]string{}
	for _, f := range snapshot.DNSConsumers {
		if !validDNSConsumerIdentity(f.NodeID) || !validDNSConsumerIdentity(f.EdgeGroupID) || f.ObservedAt.IsZero() || snapshot.CapturedAt == nil || f.ObservedAt.After(*snapshot.CapturedAt) || len(f.A)+len(f.AAAA) == 0 || len(f.A) > 16 || len(f.AAAA) > 16 {
			return nil, fmt.Errorf("DNS endpoint observation is incomplete")
		}
		if _, ok := facts[f.NodeID]; ok {
			return nil, fmt.Errorf("duplicate DNS endpoint observation")
		}
		for _, family := range []struct {
			v4        bool
			addresses []string
		}{{true, f.A}, {false, f.AAAA}} {
			for _, address := range family.addresses {
				ip, err := netip.ParseAddr(address)
				if err != nil || ip.Is4() != family.v4 || !PublicDNSFlattenIP(ip) || ip.String() != address {
					return nil, fmt.Errorf("DNS endpoint requires canonical public addresses")
				}
				if prior, ok := owners[address]; ok && prior != f.NodeID {
					return nil, fmt.Errorf("DNS endpoint address has conflicting process owners")
				}
				owners[address] = f.NodeID
			}
		}
		facts[f.NodeID] = f
	}
	consumers = normalizeDNSConsumers(consumers)
	views := []DNSConsumerView{}
	for _, c := range consumers {
		f, ok := facts[c.NodeID]
		if !ok || f.EdgeGroupID != c.EdgeGroupID {
			return nil, fmt.Errorf("DNS endpoint observation does not match declared owner")
		}
		delete(facts, c.NodeID)
		for _, zone := range c.Zones {
			view := DNSConsumerView{NodeID: c.NodeID, EdgeGroupID: c.EdgeGroupID, Zone: zone, ProbeLabel: c.ProbeLabel, Records: []DNSIntent{}}
			for _, family := range []struct {
				kind      string
				addresses []string
			}{{"A", f.A}, {"AAAA", f.AAAA}} {
				if len(family.addresses) > 0 {
					view.Records = append(view.Records, DNSIntent{Hostname: c.ProbeLabel + "." + zone, Type: family.kind, Values: normalizeDNSValues(family.kind, family.addresses), TTL: c.ProbeTTL, RecordKind: model.EdgeDNSRecordKindProbe, Status: model.EdgeRouteStatusActive, EdgeGroupID: c.EdgeGroupID})
				}
			}
			views = append(views, view)
		}
	}
	if len(facts) > 0 {
		return nil, fmt.Errorf("DNS endpoint observation lacks desired consumer")
	}
	sort.Slice(views, func(i, j int) bool {
		return views[i].NodeID+"\x00"+views[i].Zone < views[j].NodeID+"\x00"+views[j].Zone
	})
	if err := ValidateDNSConsumerViews(views, records); err != nil {
		return nil, err
	}
	return views, nil
}

func ValidateDNSConsumerViews(views []DNSConsumerView, global []DNSIntent) error {
	if len(views) > 16384 {
		return fmt.Errorf("too many DNS consumer views")
	}
	seen, groups, addresses := map[string]bool{}, map[string]string{}, map[string]string{}
	endpoints := map[string][]DNSIntent{}
	globalHosts := map[string]bool{}
	for _, r := range global {
		globalHosts[r.Hostname] = true
	}
	for _, v := range views {
		key := v.NodeID + "\x00" + v.Zone
		if !validDNSConsumerIdentity(v.NodeID) || !validDNSConsumerIdentity(v.EdgeGroupID) || !validDNSConsumerZone(v.Zone) || !validDNSConsumerZone(v.ProbeLabel) || strings.Contains(v.ProbeLabel, ".") || seen[key] || len(v.Records) == 0 || len(v.Records) > 2 {
			return fmt.Errorf("DNS consumer view identity is invalid")
		}
		seen[key] = true
		if prior, ok := groups[v.NodeID]; ok && prior != v.EdgeGroupID {
			return fmt.Errorf("DNS consumer views disagree on group")
		}
		groups[v.NodeID] = v.EdgeGroupID
		if err := ValidateDNSIntents(v.Records); err != nil {
			return err
		}
		for _, r := range v.Records {
			if r.Hostname != v.ProbeLabel+"."+v.Zone || globalHosts[r.Hostname] || r.RecordKind != model.EdgeDNSRecordKindProbe || r.Status != model.EdgeRouteStatusActive || r.EdgeGroupID != v.EdgeGroupID || r.AppID != "" || r.TenantID != "" || r.FallbackEdgeGroupID != "" || r.StatusReason != "" || r.TTL > 3600 || len(r.Values) > 16 || len(r.ValueExpirations) > 0 || (r.Type != "A" && r.Type != "AAAA") {
				return fmt.Errorf("DNS probe view collides or exceeds process endpoint scope")
			}
			for _, address := range r.Values {
				ip, err := netip.ParseAddr(address)
				if err != nil || !PublicDNSFlattenIP(ip) || ip.String() != address {
					return fmt.Errorf("DNS probe address is not public")
				}
				if owner, ok := addresses[address]; ok && owner != v.NodeID {
					return fmt.Errorf("DNS view address ownership is ambiguous")
				}
				addresses[address] = v.NodeID
			}
		}
		endpoint := append([]DNSIntent(nil), v.Records...)
		for i := range endpoint {
			endpoint[i].Hostname = v.ProbeLabel
			endpoint[i].Values = normalizeDNSValues(endpoint[i].Type, endpoint[i].Values)
		}
		sort.Slice(endpoint, func(i, j int) bool { return endpoint[i].Type < endpoint[j].Type })
		if prior, ok := endpoints[v.NodeID]; ok && !reflect.DeepEqual(prior, endpoint) {
			return fmt.Errorf("DNS consumer zones disagree on listener endpoint")
		}
		endpoints[v.NodeID] = endpoint
	}
	return nil
}

func MaterializeDNSConsumerView(global []DNSIntent, views []DNSConsumerView, nodeID, groupID, zone string, now time.Time) ([]DNSIntent, error) {
	if err := ValidateDNSConsumerViews(views, global); err != nil {
		return nil, err
	}
	var selected *DNSConsumerView
	zones := []string{}
	for i := range views {
		v := &views[i]
		if v.NodeID == nodeID {
			if v.EdgeGroupID != groupID {
				return nil, fmt.Errorf("DNS consumer group differs from signed view")
			}
			zones = append(zones, v.Zone)
			if v.Zone == zone {
				selected = v
			}
		}
	}
	if selected == nil {
		return nil, fmt.Errorf("DNS consumer has no signed zone view")
	}
	records := []DNSIntent{}
	for _, r := range global {
		owner := ""
		for _, candidate := range zones {
			if r.Hostname == candidate || strings.HasSuffix(r.Hostname, "."+candidate) {
				if len(candidate) > len(owner) {
					owner = candidate
				}
			}
		}
		if owner == zone {
			records = append(records, r)
		}
	}
	records = append(records, selected.Records...)
	if err := ValidateDNSIntents(records); err != nil {
		return nil, err
	}
	return DNSRecordsAt(records, now)
}
