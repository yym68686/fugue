package api

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"time"

	"fugue/internal/model"

	miekgdns "github.com/miekg/dns"
)

const (
	hostedDNSFlattenMaxCNAMEChain = 8
	hostedDNSFlattenQueryTimeout  = 3 * time.Second
)

type hostedDNSFlattenResult struct {
	A    []string
	AAAA []string
	TTL  int
	Err  error
}

func (s *Server) reconcileHostedDNSFlattenRecords(ctx context.Context, now time.Time) (int, error) {
	if s == nil || s.store == nil {
		return 0, nil
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	zones, err := s.store.ListHostedZones("", true)
	if err != nil {
		return 0, err
	}
	resolver := newHostedDNSFlattenResolver()
	cache := map[string]hostedDNSFlattenResult{}
	updatedCount := 0
	for _, zone := range zones {
		if zone.Status == model.HostedZoneStatusDeleted || zone.Status == model.HostedZoneStatusSuspended {
			continue
		}
		records, err := s.store.ListDNSRecords(zone.ID)
		if err != nil {
			return updatedCount, err
		}
		for _, record := range records {
			if !hostedDNSRecordNeedsFlatten(record) || record.Status == model.DNSRecordStatusDisabled {
				continue
			}
			target := hostedDNSFlattenTarget(record)
			if target == "" {
				record.Status = model.DNSRecordStatusDegraded
				record.FlattenStatus = model.DNSRecordFlattenStatusError
				record.ResolveError = "flatten target is empty"
				record.LastMessage = record.ResolveError
				if _, putErr := s.store.PutDNSRecord(zone, record, true); putErr != nil {
					return updatedCount, putErr
				}
				updatedCount++
				continue
			}
			cacheKey := strings.Join([]string{
				target,
				model.NormalizeDNSRecordFlattenIPPolicy(record.FlattenIPv4Policy),
				model.NormalizeDNSRecordFlattenIPPolicy(record.FlattenIPv6Policy),
			}, "\x00")
			result, ok := cache[cacheKey]
			if !ok {
				result = resolver.resolve(ctx, target, record)
				cache[cacheKey] = result
			}
			next := applyHostedDNSFlattenResult(record, result, now)
			if hostedDNSFlattenRecordEqual(record, next) {
				continue
			}
			if _, putErr := s.store.PutDNSRecord(zone, next, true); putErr != nil {
				return updatedCount, putErr
			}
			updatedCount++
		}
	}
	return updatedCount, nil
}

func hostedDNSFlattenTarget(record model.DNSRecord) string {
	if target := normalizeExternalAppDomain(record.FlattenTarget); target != "" {
		return target
	}
	if len(record.Values) == 1 {
		return normalizeExternalAppDomain(record.Values[0])
	}
	return ""
}

func applyHostedDNSFlattenResult(record model.DNSRecord, result hostedDNSFlattenResult, now time.Time) model.DNSRecord {
	record.FlattenIPv4Policy = model.NormalizeDNSRecordFlattenIPPolicy(record.FlattenIPv4Policy)
	record.FlattenIPv6Policy = model.NormalizeDNSRecordFlattenIPPolicy(record.FlattenIPv6Policy)
	record.FlattenFallbackPolicy = model.NormalizeDNSRecordFlattenFallbackPolicy(record.FlattenFallbackPolicy)
	if record.FlattenFallbackPolicy == "" {
		record.FlattenFallbackPolicy = model.DNSRecordFlattenFallbackStaleIfError
	}
	if result.Err != nil {
		record.Status = model.DNSRecordStatusDegraded
		record.ResolveError = result.Err.Error()
		record.LastMessage = record.ResolveError
		switch record.FlattenFallbackPolicy {
		case model.DNSRecordFlattenFallbackFailClosed, model.DNSRecordFlattenFallbackEmptyNoError:
			record.FlattenedA = nil
			record.FlattenedAAAA = nil
			record.FlattenStatus = model.DNSRecordFlattenStatusError
		default:
			if len(record.FlattenedA) > 0 || len(record.FlattenedAAAA) > 0 {
				record.FlattenStatus = model.DNSRecordFlattenStatusStale
			} else {
				record.FlattenStatus = model.DNSRecordFlattenStatusError
			}
		}
		return record
	}
	record.FlattenedA = append([]string(nil), result.A...)
	record.FlattenedAAAA = append([]string(nil), result.AAAA...)
	record.LastResolvedAt = &now
	record.ResolveError = ""
	record.LastMessage = ""
	record.Status = model.DNSRecordStatusActive
	record.FlattenStatus = model.DNSRecordFlattenStatusResolved
	return record
}

func hostedDNSFlattenRecordEqual(left, right model.DNSRecord) bool {
	return strings.EqualFold(left.Status, right.Status) &&
		strings.EqualFold(left.FlattenStatus, right.FlattenStatus) &&
		strings.TrimSpace(left.ResolveError) == strings.TrimSpace(right.ResolveError) &&
		strings.TrimSpace(left.LastMessage) == strings.TrimSpace(right.LastMessage) &&
		hostedDNSStringSlicesEqual(left.FlattenedA, right.FlattenedA) &&
		hostedDNSStringSlicesEqual(left.FlattenedAAAA, right.FlattenedAAAA)
}

type hostedDNSFlattenResolver struct {
	servers []string
	client  *miekgdns.Client
}

func newHostedDNSFlattenResolver() hostedDNSFlattenResolver {
	servers := []string{"1.1.1.1:53", "8.8.8.8:53"}
	if cfg, err := miekgdns.ClientConfigFromFile("/etc/resolv.conf"); err == nil && len(cfg.Servers) > 0 {
		servers = make([]string, 0, len(cfg.Servers))
		port := cfg.Port
		if port == "" {
			port = "53"
		}
		for _, server := range cfg.Servers {
			server = strings.TrimSpace(server)
			if server == "" {
				continue
			}
			servers = append(servers, net.JoinHostPort(server, port))
		}
		if len(servers) == 0 {
			servers = []string{"1.1.1.1:53", "8.8.8.8:53"}
		}
	}
	return hostedDNSFlattenResolver{
		servers: servers,
		client:  &miekgdns.Client{Timeout: hostedDNSFlattenQueryTimeout},
	}
}

func (r hostedDNSFlattenResolver) resolve(ctx context.Context, target string, record model.DNSRecord) hostedDNSFlattenResult {
	target = normalizeExternalAppDomain(target)
	if target == "" {
		return hostedDNSFlattenResult{Err: fmt.Errorf("flatten target is empty")}
	}
	a, aTTL, cnameA, errA := r.query(ctx, target, miekgdns.TypeA)
	aaaa, aaaaTTL, cnameAAAA, errAAAA := r.query(ctx, target, miekgdns.TypeAAAA)
	cname := firstNonEmpty(cnameA, cnameAAAA)
	if len(a) == 0 && len(aaaa) == 0 && cname != "" {
		return r.resolveCNAMEChain(ctx, cname, record, map[string]bool{target: true}, 1)
	}
	if errA != nil && errAAAA != nil && len(a) == 0 && len(aaaa) == 0 {
		return hostedDNSFlattenResult{Err: fmt.Errorf("resolve %s: A=%v AAAA=%v", target, errA, errAAAA)}
	}
	return finalizeHostedDNSFlattenIPs(a, aaaa, minPositiveTTL(aTTL, aaaaTTL), record)
}

func (r hostedDNSFlattenResolver) resolveCNAMEChain(ctx context.Context, name string, record model.DNSRecord, seen map[string]bool, depth int) hostedDNSFlattenResult {
	name = normalizeExternalAppDomain(name)
	if name == "" {
		return hostedDNSFlattenResult{Err: fmt.Errorf("CNAME target is empty")}
	}
	if depth > hostedDNSFlattenMaxCNAMEChain {
		return hostedDNSFlattenResult{Err: fmt.Errorf("CNAME chain exceeds max depth %d", hostedDNSFlattenMaxCNAMEChain)}
	}
	if seen[name] {
		return hostedDNSFlattenResult{Err: fmt.Errorf("CNAME loop detected at %s", name)}
	}
	seen[name] = true
	a, aTTL, cnameA, errA := r.query(ctx, name, miekgdns.TypeA)
	aaaa, aaaaTTL, cnameAAAA, errAAAA := r.query(ctx, name, miekgdns.TypeAAAA)
	cname := firstNonEmpty(cnameA, cnameAAAA)
	if len(a) == 0 && len(aaaa) == 0 && cname != "" {
		return r.resolveCNAMEChain(ctx, cname, record, seen, depth+1)
	}
	if errA != nil && errAAAA != nil && len(a) == 0 && len(aaaa) == 0 {
		return hostedDNSFlattenResult{Err: fmt.Errorf("resolve %s: A=%v AAAA=%v", name, errA, errAAAA)}
	}
	return finalizeHostedDNSFlattenIPs(a, aaaa, minPositiveTTL(aTTL, aaaaTTL), record)
}

func (r hostedDNSFlattenResolver) query(ctx context.Context, name string, qtype uint16) ([]string, int, string, error) {
	msg := new(miekgdns.Msg)
	msg.SetQuestion(miekgdns.Fqdn(name), qtype)
	var lastErr error
	for _, server := range r.servers {
		queryCtx, cancel := context.WithTimeout(ctx, hostedDNSFlattenQueryTimeout)
		resp, _, err := r.client.ExchangeContext(queryCtx, msg, server)
		cancel()
		if err != nil {
			lastErr = err
			continue
		}
		if resp == nil {
			lastErr = fmt.Errorf("empty DNS response")
			continue
		}
		if resp.Rcode != miekgdns.RcodeSuccess {
			lastErr = fmt.Errorf("rcode=%s", miekgdns.RcodeToString[resp.Rcode])
			continue
		}
		values := []string{}
		ttl := 0
		cname := ""
		for _, answer := range resp.Answer {
			switch rr := answer.(type) {
			case *miekgdns.A:
				if qtype == miekgdns.TypeA && rr.A != nil {
					ip := rr.A.String()
					if publicRoutableHostedDNSIP(ip) && !stringSliceContains(values, ip) {
						values = append(values, ip)
						ttl = minPositiveTTL(ttl, int(rr.Hdr.Ttl))
					}
				}
			case *miekgdns.AAAA:
				if qtype == miekgdns.TypeAAAA && rr.AAAA != nil {
					ip := rr.AAAA.String()
					if publicRoutableHostedDNSIP(ip) && !stringSliceContains(values, ip) {
						values = append(values, ip)
						ttl = minPositiveTTL(ttl, int(rr.Hdr.Ttl))
					}
				}
			case *miekgdns.CNAME:
				if cname == "" {
					cname = normalizeExternalAppDomain(rr.Target)
					ttl = minPositiveTTL(ttl, int(rr.Hdr.Ttl))
				}
			}
		}
		return uniqueSortedStrings(values), ttl, cname, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("DNS query failed")
	}
	return nil, 0, "", lastErr
}

func finalizeHostedDNSFlattenIPs(a, aaaa []string, ttl int, record model.DNSRecord) hostedDNSFlattenResult {
	ipv4Policy := model.NormalizeDNSRecordFlattenIPPolicy(record.FlattenIPv4Policy)
	ipv6Policy := model.NormalizeDNSRecordFlattenIPPolicy(record.FlattenIPv6Policy)
	a = uniqueSortedStrings(a)
	aaaa = uniqueSortedStrings(aaaa)
	if ipv4Policy == model.DNSRecordFlattenIPPolicyIPv6Only || ipv6Policy == model.DNSRecordFlattenIPPolicyIPv6Only {
		a = nil
	}
	if ipv4Policy == model.DNSRecordFlattenIPPolicyIPv4Only || ipv6Policy == model.DNSRecordFlattenIPPolicyIPv4Only {
		aaaa = nil
	}
	if ipv4Policy == model.DNSRecordFlattenIPPolicyDualStackRequired || ipv6Policy == model.DNSRecordFlattenIPPolicyDualStackRequired {
		if len(a) == 0 || len(aaaa) == 0 {
			return hostedDNSFlattenResult{Err: fmt.Errorf("dual_stack_required target did not return both A and AAAA")}
		}
	}
	if len(a) == 0 && len(aaaa) == 0 {
		return hostedDNSFlattenResult{Err: fmt.Errorf("flatten target returned no public A/AAAA records")}
	}
	return hostedDNSFlattenResult{A: a, AAAA: aaaa, TTL: ttl}
}

func publicRoutableHostedDNSIP(raw string) bool {
	addr, err := netip.ParseAddr(strings.TrimSpace(raw))
	if err != nil || !addr.IsGlobalUnicast() || addr.IsPrivate() || addr.IsLoopback() || addr.IsLinkLocalUnicast() || addr.IsLinkLocalMulticast() || addr.IsMulticast() || addr.IsUnspecified() {
		return false
	}
	for _, prefix := range hostedDNSReservedIPPrefixes() {
		if prefix.Contains(addr) {
			return false
		}
	}
	return true
}

func hostedDNSReservedIPPrefixes() []netip.Prefix {
	raw := []string{
		"0.0.0.0/8",
		"100.64.0.0/10",
		"127.0.0.0/8",
		"169.254.0.0/16",
		"192.0.0.0/24",
		"192.0.2.0/24",
		"198.18.0.0/15",
		"198.51.100.0/24",
		"203.0.113.0/24",
		"224.0.0.0/4",
		"240.0.0.0/4",
		"::/128",
		"::1/128",
		"64:ff9b:1::/48",
		"100::/64",
		"2001:db8::/32",
		"fc00::/7",
		"fe80::/10",
	}
	out := make([]netip.Prefix, 0, len(raw))
	for _, value := range raw {
		if prefix, err := netip.ParsePrefix(value); err == nil {
			out = append(out, prefix)
		}
	}
	return out
}

func minPositiveTTL(values ...int) int {
	out := 0
	for _, value := range values {
		if value <= 0 {
			continue
		}
		if out == 0 || value < out {
			out = value
		}
	}
	return out
}

func hostedDNSStringSlicesEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if strings.TrimSpace(left[index]) != strings.TrimSpace(right[index]) {
			return false
		}
	}
	return true
}
