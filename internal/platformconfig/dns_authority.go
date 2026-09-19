package platformconfig

import (
	"fmt"
	"sort"
)

type DNSAuthorityPolicy struct {
	NodeID         string   `json:"node_id"`
	Zone           string   `json:"zone"`
	Nameservers    []string `json:"nameservers"`
	TTLSeconds     int      `json:"ttl_seconds"`
	RefreshSeconds int      `json:"refresh_seconds"`
	RetrySeconds   int      `json:"retry_seconds"`
	ExpireSeconds  int      `json:"expire_seconds"`
}

func ValidateDNSAuthorities(in []DNSAuthorityPolicy) error {
	if len(in) > 16384 {
		return fmt.Errorf("too many DNS authority policies")
	}
	seen := map[string]bool{}
	for _, p := range in {
		key := p.NodeID + "\x00" + p.Zone
		if !validDNSConsumerIdentity(p.NodeID) || !validDNSConsumerZone(p.Zone) || seen[key] || len(p.Nameservers) == 0 || len(p.Nameservers) > 16 || p.TTLSeconds < 1 || p.TTLSeconds > 3600 || p.RefreshSeconds < 1 || p.RefreshSeconds > 86400 || p.RetrySeconds < 1 || p.RetrySeconds > 86400 || p.ExpireSeconds < p.RefreshSeconds || p.ExpireSeconds > 604800 {
			return fmt.Errorf("invalid DNS authority policy")
		}
		seen[key] = true
		names := map[string]bool{}
		for _, ns := range p.Nameservers {
			if !validDNSConsumerZone(ns) || names[ns] {
				return fmt.Errorf("invalid DNS authority nameserver")
			}
			names[ns] = true
		}
	}
	return nil
}
func normalizeDNSAuthorities(in []DNSAuthorityPolicy) []DNSAuthorityPolicy {
	out := append([]DNSAuthorityPolicy(nil), in...)
	for i := range out {
		out[i].Nameservers = append([]string(nil), in[i].Nameservers...)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID+"\x00"+out[i].Zone < out[j].NodeID+"\x00"+out[j].Zone })
	return out
}
func ValidateDNSAuthorityOwnership(in []DNSAuthorityPolicy, consumers []DNSConsumerIntent) error {
	if err := ValidateDNSAuthorities(in); err != nil {
		return err
	}
	if len(in) == 0 {
		return nil
	} // legacy artifacts remain readable
	owners := map[string]bool{}
	for _, c := range consumers {
		for _, z := range c.Zones {
			owners[c.NodeID+"\x00"+z] = true
		}
	}
	for _, p := range in {
		key := p.NodeID + "\x00" + p.Zone
		if !owners[key] {
			return fmt.Errorf("DNS authority has no declared owner")
		}
		delete(owners, key)
	}
	if len(owners) != 0 {
		return fmt.Errorf("DNS authority missing for declared zone")
	}
	return nil
}
