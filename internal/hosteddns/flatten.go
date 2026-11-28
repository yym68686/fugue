// Package hosteddns resolves and refreshes hosted DNS flattening results.
package hosteddns

import (
	"cmp"
	"context"
	"fmt"
	"net"
	"net/netip"
	"slices"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"

	miekgdns "github.com/miekg/dns"
)

const (
	hostedDNSFlattenMaxCNAMEChain = 8
	hostedDNSFlattenQueryTimeout  = 3 * time.Second
)

type Result struct {
	A    []string
	AAAA []string
	TTL  int
	Err  error
}

type Store interface {
	ListHostedZones(tenantID string, platformAdmin bool) ([]model.HostedZone, error)
	ListDNSRecords(zoneID string) ([]model.DNSRecord, error)
	CommitHostedDNSFlattenObservation(context.Context, model.HostedZone, model.DNSRecord, model.DNSRecord) (bool, error)
}

func Reconcile(ctx context.Context, db Store, now time.Time) (int, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	zones, err := db.ListHostedZones("", true)
	if err != nil {
		return 0, err
	}
	resolver := NewResolver()
	cache := map[string]Result{}
	updatedCount := 0
	for _, zone := range zones {
		if zone.Status == model.HostedZoneStatusDeleted || zone.Status == model.HostedZoneStatusSuspended || zone.Status == model.HostedZoneStatusPendingOwnership {
			continue
		}
		records, err := db.ListDNSRecords(zone.ID)
		if err != nil {
			return updatedCount, err
		}
		for _, record := range records {
			if err := ctx.Err(); err != nil {
				return updatedCount, err
			}
			if !NeedsFlatten(record) || record.Status == model.DNSRecordStatusDisabled {
				continue
			}
			changed, err := refreshRecord(ctx, db, zone, record, now, resolver.Resolve, cache)
			if err != nil {
				return updatedCount, err
			}
			if changed {
				updatedCount++
			}
		}
	}
	return updatedCount, nil
}

type observationWriter interface {
	CommitHostedDNSFlattenObservation(context.Context, model.HostedZone, model.DNSRecord, model.DNSRecord) (bool, error)
}

func refreshRecord(ctx context.Context, db observationWriter, zone model.HostedZone, record model.DNSRecord, now time.Time, resolve func(context.Context, string, model.DNSRecord) Result, cache map[string]Result) (bool, error) {
	target := Target(record)
	if target == "" {
		next := record
		next.Status = model.DNSRecordStatusDegraded
		next.FlattenStatus = model.DNSRecordFlattenStatusError
		next.ResolveError = "flatten target is empty"
		next.LastMessage = next.ResolveError
		if sameObservation(record, next) {
			return false, nil
		}
		return db.CommitHostedDNSFlattenObservation(ctx, zone, record, next)
	}
	cacheKey := strings.Join([]string{
		target,
		model.NormalizeDNSRecordFlattenIPPolicy(record.FlattenIPv4Policy),
		model.NormalizeDNSRecordFlattenIPPolicy(record.FlattenIPv6Policy),
	}, "\x00")
	result, ok := cache[cacheKey]
	if !ok {
		result = resolve(ctx, target, record)
		cache[cacheKey] = result
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	next := applyResult(record, result, now)
	if sameObservation(record, next) {
		return false, nil
	}
	return db.CommitHostedDNSFlattenObservation(ctx, zone, record, next)
}

func NeedsFlatten(record model.DNSRecord) bool {
	switch model.NormalizeDNSRecordType(record.Type) {
	case model.DNSRecordTypeALIAS, model.DNSRecordTypeANAME:
		return true
	case model.DNSRecordTypeCNAME:
		return model.NormalizeDNSRecordFlattenMode(record.FlattenMode) != model.DNSRecordFlattenModeNone
	default:
		return false
	}
}

func Target(record model.DNSRecord) string {
	if target := normalizeHost(record.FlattenTarget); target != "" {
		return target
	}
	if len(record.Values) == 1 {
		return normalizeHost(record.Values[0])
	}
	return ""
}

func applyResult(record model.DNSRecord, result Result, now time.Time) model.DNSRecord {
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

func sameObservation(left, right model.DNSRecord) bool {
	return strings.EqualFold(left.Status, right.Status) &&
		strings.EqualFold(left.FlattenStatus, right.FlattenStatus) &&
		strings.TrimSpace(left.ResolveError) == strings.TrimSpace(right.ResolveError) &&
		strings.TrimSpace(left.LastMessage) == strings.TrimSpace(right.LastMessage) &&
		timesEqual(left.LastResolvedAt, right.LastResolvedAt) &&
		hostedDNSStringSlicesEqual(left.FlattenedA, right.FlattenedA) &&
		hostedDNSStringSlicesEqual(left.FlattenedAAAA, right.FlattenedAAAA)
}

func timesEqual(left, right *time.Time) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Equal(*right)
}

type Resolver struct {
	servers []string
	client  *miekgdns.Client
}

func SystemResolvers() []string {
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
	return servers
}

func NewResolver() Resolver {
	return Resolver{
		servers: SystemResolvers(),
		client:  &miekgdns.Client{Timeout: hostedDNSFlattenQueryTimeout},
	}
}

func (r Resolver) Resolve(ctx context.Context, target string, record model.DNSRecord) Result {
	target = normalizeHost(target)
	if target == "" {
		return Result{Err: fmt.Errorf("flatten target is empty")}
	}
	a, aTTL, cnameA, errA := r.query(ctx, target, miekgdns.TypeA)
	aaaa, aaaaTTL, cnameAAAA, errAAAA := r.query(ctx, target, miekgdns.TypeAAAA)
	cname := cmp.Or(cnameA, cnameAAAA)
	if len(a) == 0 && len(aaaa) == 0 && cname != "" {
		result := r.resolveCNAMEChain(ctx, cname, record, map[string]bool{target: true}, 1)
		result.TTL = minPositiveTTL(result.TTL, aTTL, aaaaTTL)
		return result
	}
	if errA != nil && errAAAA != nil && len(a) == 0 && len(aaaa) == 0 {
		return Result{Err: fmt.Errorf("resolve %s: A=%v AAAA=%v", target, errA, errAAAA)}
	}
	return finalizeHostedDNSFlattenIPs(a, aaaa, minPositiveTTL(aTTL, aaaaTTL), record)
}

func (r Resolver) resolveCNAMEChain(ctx context.Context, name string, record model.DNSRecord, seen map[string]bool, depth int) Result {
	name = normalizeHost(name)
	if name == "" {
		return Result{Err: fmt.Errorf("CNAME target is empty")}
	}
	if depth > hostedDNSFlattenMaxCNAMEChain {
		return Result{Err: fmt.Errorf("CNAME chain exceeds max depth %d", hostedDNSFlattenMaxCNAMEChain)}
	}
	if seen[name] {
		return Result{Err: fmt.Errorf("CNAME loop detected at %s", name)}
	}
	seen[name] = true
	a, aTTL, cnameA, errA := r.query(ctx, name, miekgdns.TypeA)
	aaaa, aaaaTTL, cnameAAAA, errAAAA := r.query(ctx, name, miekgdns.TypeAAAA)
	cname := cmp.Or(cnameA, cnameAAAA)
	if len(a) == 0 && len(aaaa) == 0 && cname != "" {
		result := r.resolveCNAMEChain(ctx, cname, record, seen, depth+1)
		result.TTL = minPositiveTTL(result.TTL, aTTL, aaaaTTL)
		return result
	}
	if errA != nil && errAAAA != nil && len(a) == 0 && len(aaaa) == 0 {
		return Result{Err: fmt.Errorf("resolve %s: A=%v AAAA=%v", name, errA, errAAAA)}
	}
	return finalizeHostedDNSFlattenIPs(a, aaaa, minPositiveTTL(aTTL, aaaaTTL), record)
}

func (r Resolver) query(ctx context.Context, name string, qtype uint16) ([]string, int, string, error) {
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
					if publicRoutableHostedDNSIP(ip) && !slices.Contains(values, ip) {
						values = append(values, ip)
						ttl = minPositiveTTL(ttl, int(rr.Hdr.Ttl))
					}
				}
			case *miekgdns.AAAA:
				if qtype == miekgdns.TypeAAAA && rr.AAAA != nil {
					ip := rr.AAAA.String()
					if publicRoutableHostedDNSIP(ip) && !slices.Contains(values, ip) {
						values = append(values, ip)
						ttl = minPositiveTTL(ttl, int(rr.Hdr.Ttl))
					}
				}
			case *miekgdns.CNAME:
				if cname == "" {
					cname = normalizeHost(rr.Target)
					ttl = minPositiveTTL(ttl, int(rr.Hdr.Ttl))
				}
			}
		}
		return sortAddresses(values), ttl, cname, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("DNS query failed")
	}
	return nil, 0, "", lastErr
}

func finalizeHostedDNSFlattenIPs(a, aaaa []string, ttl int, record model.DNSRecord) Result {
	ipv4Policy := model.NormalizeDNSRecordFlattenIPPolicy(record.FlattenIPv4Policy)
	ipv6Policy := model.NormalizeDNSRecordFlattenIPPolicy(record.FlattenIPv6Policy)
	a = sortAddresses(a)
	aaaa = sortAddresses(aaaa)
	if ipv4Policy == model.DNSRecordFlattenIPPolicyIPv6Only || ipv6Policy == model.DNSRecordFlattenIPPolicyIPv6Only {
		a = nil
	}
	if ipv4Policy == model.DNSRecordFlattenIPPolicyIPv4Only || ipv6Policy == model.DNSRecordFlattenIPPolicyIPv4Only {
		aaaa = nil
	}
	if ipv4Policy == model.DNSRecordFlattenIPPolicyDualStackRequired || ipv6Policy == model.DNSRecordFlattenIPPolicyDualStackRequired {
		if len(a) == 0 || len(aaaa) == 0 {
			return Result{Err: fmt.Errorf("dual_stack_required target did not return both A and AAAA")}
		}
	}
	if len(a) == 0 && len(aaaa) == 0 {
		return Result{Err: fmt.Errorf("flatten target returned no public A/AAAA records")}
	}
	return Result{A: a, AAAA: aaaa, TTL: ttl}
}

func publicRoutableHostedDNSIP(raw string) bool {
	addr, err := netip.ParseAddr(strings.TrimSpace(raw))
	return err == nil && platformconfig.PublicDNSFlattenIP(addr)
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

func normalizeHost(raw string) string {
	return strings.Trim(strings.TrimSpace(strings.ToLower(raw)), ".")
}

func sortAddresses(values []string) []string {
	slices.Sort(values)
	return slices.Compact(values)
}
