package platformconfig

import (
	"fmt"
	"net/netip"
	"sort"
	"strconv"
	"strings"
	"unicode"
)

// Rules preserve first-match precedence. Their order is configuration, not a
// ranking observation or endpoint health assertion.
type DNSClientRule struct {
	CIDR        string `json:"cidr"`
	Country     string `json:"country,omitempty"`
	Region      string `json:"region,omitempty"`
	ASN         string `json:"asn,omitempty"`
	EdgeGroupID string `json:"edge_group_id,omitempty"`
}
type DNSClientPolicy struct {
	NodeID string          `json:"node_id"`
	Rules  []DNSClientRule `json:"rules"`
}

func normalizeDNSClientPolicies(in []DNSClientPolicy) []DNSClientPolicy {
	if len(in) == 0 {
		return nil
	}
	out := append([]DNSClientPolicy(nil), in...)
	for i := range out {
		out[i].Rules = append([]DNSClientRule{}, in[i].Rules...)
		for j := range out[i].Rules {
			r := &out[i].Rules[j]
			r.CIDR = strings.TrimSpace(r.CIDR)
			if prefix, err := netip.ParsePrefix(r.CIDR); err == nil {
				r.CIDR = prefix.Masked().String()
			}
			r.Country = strings.ToLower(strings.TrimSpace(r.Country))
			r.Region = strings.TrimSpace(r.Region)
			r.ASN = strings.TrimSpace(r.ASN)
			r.EdgeGroupID = strings.TrimSpace(r.EdgeGroupID)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out
}

func ValidateDNSClientPolicies(policies []DNSClientPolicy) error {
	if len(policies) > 256 {
		return fmt.Errorf("too many DNS client policies")
	}
	seen := map[string]bool{}
	for _, p := range policies {
		if !validDNSConsumerIdentity(p.NodeID) || seen[p.NodeID] {
			return fmt.Errorf("invalid or duplicate DNS client policy owner")
		}
		seen[p.NodeID] = true
		if _, err := NewDNSClientMatcher(p.Rules); err != nil {
			return err
		}
	}
	return nil
}

func ValidateDNSClientPolicyOwnership(policies []DNSClientPolicy, consumers []DNSConsumerIntent) error {
	if err := ValidateDNSClientPolicies(policies); err != nil {
		return err
	}
	if len(policies) == 0 {
		return nil
	} // older snapshots explicitly have no mappings
	owners := map[string]bool{}
	for _, c := range consumers {
		owners[c.NodeID] = true
	}
	if len(owners) != len(policies) {
		return fmt.Errorf("DNS client policy topology is incomplete")
	}
	for _, p := range policies {
		if !owners[p.NodeID] {
			return fmt.Errorf("DNS client policy has no declared consumer")
		}
	}
	return nil
}

type dnsClientPrefix struct {
	prefix netip.Prefix
	rule   DNSClientRule
}
type DNSClientMatcher struct{ rules []dnsClientPrefix }

func NewDNSClientMatcher(rules []DNSClientRule) (DNSClientMatcher, error) {
	var m DNSClientMatcher
	if len(rules) > 256 {
		return m, fmt.Errorf("too many DNS client rules")
	}
	seen := map[string]bool{}
	for _, r := range rules {
		prefix, err := netip.ParsePrefix(r.CIDR)
		if err != nil || prefix.Addr().Is4In6() || prefix.Masked().String() != r.CIDR || seen[r.CIDR] {
			return DNSClientMatcher{}, fmt.Errorf("invalid or duplicate DNS client CIDR")
		}
		seen[r.CIDR] = true
		fields := []struct {
			s   string
			max int
		}{{r.Country, 16}, {r.Region, 128}, {r.ASN, 64}, {r.EdgeGroupID, 128}}
		for _, f := range fields {
			if len(f.s) > f.max || strings.IndexFunc(f.s, unicode.IsControl) >= 0 || strings.TrimSpace(f.s) != f.s {
				return DNSClientMatcher{}, fmt.Errorf("invalid DNS client mapping metadata")
			}
		}
		if r.Country != strings.ToLower(r.Country) || (r.EdgeGroupID != "" && !platformRouteArtifactGroupID.MatchString(r.EdgeGroupID)) {
			return DNSClientMatcher{}, fmt.Errorf("invalid DNS client geography or group")
		}
		m.rules = append(m.rules, dnsClientPrefix{prefix, r})
	}
	return m, nil
}

func (m DNSClientMatcher) Lookup(raw string) (DNSClientRule, bool) {
	ip, err := netip.ParseAddr(raw)
	if err != nil || ip.Zone() != "" {
		return DNSClientRule{}, false
	}
	ip = ip.Unmap()
	for _, r := range m.rules {
		if r.prefix.Contains(ip) {
			return r.rule, true
		}
	}
	return DNSClientRule{}, false
}

// ClientASN normalizes either notation used in signed client mappings.
// AS_TRANS and the reserved boundary values are not network evidence.
func ClientASN(raw string) uint32 {
	value := strings.TrimPrefix(strings.ToLower(raw), "as")
	asn, err := strconv.ParseUint(value, 10, 32)
	if err != nil || value != strconv.FormatUint(asn, 10) || asn == 0 || asn == 23456 || asn == 65535 || asn == 4294967295 {
		return 0
	}
	return uint32(asn)
}

// LookupPrefixASN requires the entire prefix to have a single signed origin
// under first-match semantics. Reading just the masked address of an ECS or
// privacy prefix can incorrectly apply a narrower override to the whole subnet.
func (m DNSClientMatcher) LookupPrefixASN(prefix netip.Prefix) uint32 {
	if !prefix.IsValid() || prefix.Bits() == 0 || prefix.Addr().Is4In6() ||
		!prefix.Addr().IsGlobalUnicast() || prefix.Addr().IsPrivate() {
		return 0
	}
	for _, rule := range m.rules {
		if !rule.prefix.Overlaps(prefix) {
			continue
		}
		if rule.prefix.Bits() == 0 || rule.prefix.Bits() > prefix.Bits() {
			return 0
		}
		return ClientASN(rule.rule.ASN)
	}
	return 0
}
