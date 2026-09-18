package dnsserver

import (
	"fugue/internal/platformconfig"
	"github.com/miekg/dns"
	"net"
	"net/netip"
)

func platformDNSHintForIP(matcher platformconfig.DNSClientMatcher, raw, source string) dnsGeoHint {
	ip, err := netip.ParseAddr(raw)
	if err != nil || ip.Zone() != "" {
		return dnsGeoHint{}
	}
	ip = ip.Unmap()
	hint := dnsGeoHint{IP: ip.String(), Source: source}
	if r, ok := matcher.Lookup(ip.String()); ok {
		hint.Country = r.Country
		hint.Region = r.Region
		hint.ASN = r.ASN
		hint.EdgeGroupID = r.EdgeGroupID
	}
	return hint
}

// Client hints carry selection preferences only. They do not turn a candidate
// into a healthy route or add an address to signed query authorization.
func platformDNSHintForQuery(matcher platformconfig.DNSClientMatcher, msg *dns.Msg, remote string) dnsGeoHint {
	if msg != nil {
		if opt := msg.IsEdns0(); opt != nil {
			var found *dns.EDNS0_SUBNET
			invalid := false
			for _, o := range opt.Option {
				if e, ok := o.(*dns.EDNS0_SUBNET); ok {
					if found != nil || e == nil {
						invalid = true
					}
					found = e
				}
			}
			if found != nil && !invalid && found.SourceScope == 0 {
				ip, ok := netip.AddrFromSlice(found.Address)
				if ok {
					ip = ip.Unmap()
					bits := int(found.SourceNetmask)
					valid := (found.Family == 1 && ip.Is4() && bits <= 32) || (found.Family == 2 && ip.Is6() && bits <= 128)
					if valid {
						return platformDNSHintForIP(matcher, netip.PrefixFrom(ip, bits).Masked().Addr().String(), "ecs")
					}
				}
			}
		}
	}
	if host, _, err := net.SplitHostPort(remote); err == nil {
		return platformDNSHintForIP(matcher, host, "remote_addr")
	}
	return dnsGeoHint{}
}
