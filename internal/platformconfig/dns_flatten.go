package platformconfig

import (
	"encoding/json"
	"fmt"
	"net/netip"
	"sort"
	"strings"
	"time"

	"github.com/miekg/dns"
)

type DNSFlattenObservation struct {
	InputDigest string    `json:"input_digest"`
	TenantID    string    `json:"tenant_id"`
	CheckedAt   time.Time `json:"checked_at"`
	ObservedAt  time.Time `json:"observed_at"`
	Status      string    `json:"status"`
	A           []string  `json:"a,omitempty"`
	AAAA        []string  `json:"aaaa,omitempty"`
	TargetTTL   int       `json:"target_ttl"`
}

// DNSFlattenInputDigest binds resolver evidence to the complete configuration,
// including tenant, target and policies. A target edit invalidates old answers.
func DNSFlattenInputDigest(record DNSIntent) (string, error) {
	canonical := NormalizePlatformIntent(PlatformIntent{DNS: []DNSIntent{record}})
	data, err := json.Marshal(canonical.DNS[0])
	if err != nil {
		return "", err
	}
	var material map[string]any
	if err := json.Unmarshal(data, &material); err != nil {
		return "", err
	}
	return Digest(material)
}

func normalizeDNSFlattenObservations(in []DNSFlattenObservation) []DNSFlattenObservation {
	out := append([]DNSFlattenObservation(nil), in...)
	for i := range out {
		out[i].A = normalizeDNSValues("A", out[i].A)
		out[i].AAAA = normalizeDNSValues("AAAA", out[i].AAAA)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].InputDigest < out[j].InputDigest })
	return out
}

func validateDNSConfiguration(records []DNSIntent) error {
	if len(records) > 10000 {
		return fmt.Errorf("too many DNS records")
	}
	wire := make([]DNSIntent, 0, len(records))
	seen := make(map[string]bool, len(records))
	for _, record := range records {
		key := record.Hostname + "\x00" + record.Type
		if seen[key] {
			return fmt.Errorf("duplicate DNS input")
		}
		seen[key] = true
		if record.Application != nil || record.Type == "FUGUE_APP" {
			if err := validateDNSApplicationConfiguration(record); err != nil {
				return err
			}
			continue
		}
		f := record.Flatten
		if f == nil {
			wire = append(wire, record)
			continue
		}
		if record.Type != "CNAME" && record.Type != "ALIAS" && record.Type != "ANAME" {
			return fmt.Errorf("flatten requires CNAME, ALIAS or ANAME")
		}
		if f.Mode != "always" && f.Mode != "apex" {
			return fmt.Errorf("flatten mode is invalid")
		}
		if f.Mode == "apex" && f.Zone != record.Hostname {
			return fmt.Errorf("apex flatten requires the zone apex hostname")
		}
		if f.Target == "" || f.Target != normalizedImportHostname(f.Target) || strings.ContainsAny(f.Target, " \r\n\t") || f.Target == record.Hostname {
			return fmt.Errorf("flatten target is invalid")
		}
		if _, ok := dns.IsDomainName(dns.Fqdn(f.Target)); !ok {
			return fmt.Errorf("flatten target is not a DNS name")
		}
		if len(record.Values) != 1 || normalizedImportHostname(record.Values[0]) != f.Target {
			return fmt.Errorf("flatten values must identify the configured target")
		}
		for _, policy := range []string{f.IPv4Policy, f.IPv6Policy} {
			switch policy {
			case "auto", "ipv4_only", "ipv6_only", "dual_stack_required":
			default:
				return fmt.Errorf("flatten IP policy is invalid")
			}
		}
		only4 := f.IPv4Policy == "ipv4_only" || f.IPv6Policy == "ipv4_only"
		only6 := f.IPv4Policy == "ipv6_only" || f.IPv6Policy == "ipv6_only"
		dual := f.IPv4Policy == "dual_stack_required" || f.IPv6Policy == "dual_stack_required"
		if (only4 && only6) || (dual && (only4 || only6)) {
			return fmt.Errorf("flatten IP policies conflict")
		}
		switch f.TTLPolicy {
		case "record", "target", "min", "bounded":
		default:
			return fmt.Errorf("flatten TTL policy is invalid")
		}
		switch f.FallbackPolicy {
		case "fail_closed", "stale_if_error", "empty_noerror":
		default:
			return fmt.Errorf("flatten fallback policy is invalid")
		}
		// Check symbolic input syntax independently: an ALIAS can share an
		// apex with TXT/MX/NS, unlike the CNAME used to validate its target.
		record.Flatten = nil
		record.Type = "CNAME"
		if err := ValidateDNSIntents([]DNSIntent{record}); err != nil {
			return err
		}
	}
	for _, record := range records {
		if record.Application != nil {
			for _, kind := range []string{"A", "AAAA", "CNAME", "ALIAS", "ANAME"} {
				if seen[record.Hostname+"\x00"+kind] {
					return fmt.Errorf("DNS application binding conflicts with another address source")
				}
			}
		}
	}
	return ValidateDNSIntents(wire)
}

// ResolveDNSFlatten performs no DNS queries and never uses the wall clock.
// Old signed input remains replayable at its captured time; a new capture must
// satisfy freshness again. Consumers still apply their artifact/LKG lifetime.
func ResolveDNSFlatten(records []DNSIntent, snapshot RuntimeSnapshot, policy PolicySnapshot) ([]DNSIntent, error) {
	if err := validateDNSConfiguration(records); err != nil {
		return nil, err
	}
	for _, record := range records {
		if record.Application != nil {
			return nil, fmt.Errorf("DNS application bindings require fixed placement resolution before compilation")
		}
	}
	if len(snapshot.DNSFlatten) > 10000 {
		return nil, fmt.Errorf("too many flatten observations")
	}
	byDigest := make(map[string]DNSFlattenObservation, len(snapshot.DNSFlatten))
	for _, fact := range snapshot.DNSFlatten {
		if len(fact.A) > 4096 || len(fact.AAAA) > 4096 {
			return nil, fmt.Errorf("flatten answer count exceeds limit")
		}
		if fact.InputDigest == "" {
			return nil, fmt.Errorf("flatten observation requires input digest")
		}
		if _, exists := byDigest[fact.InputDigest]; exists {
			return nil, fmt.Errorf("duplicate flatten observation")
		}
		byDigest[fact.InputDigest] = fact
	}
	matched := make(map[string]bool, len(byDigest))
	out := make([]DNSIntent, 0, len(records))
	for _, record := range records {
		f := record.Flatten
		if f == nil {
			out = append(out, record)
			continue
		}
		digest, err := DNSFlattenInputDigest(record)
		if err != nil {
			return nil, err
		}
		fact, exists := byDigest[digest]
		if !exists || fact.TenantID != record.TenantID {
			return nil, fmt.Errorf("flatten observation is missing or has mismatched ownership/input digest")
		}
		matched[digest] = true
		if snapshot.CapturedAt == nil || snapshot.CapturedAt.IsZero() || fact.CheckedAt.IsZero() || fact.ObservedAt.IsZero() || fact.CheckedAt.After(*snapshot.CapturedAt) || fact.ObservedAt.After(fact.CheckedAt) {
			return nil, fmt.Errorf("flatten observation time is invalid")
		}
		age := snapshot.CapturedAt.Sub(fact.ObservedAt)
		remaining := float64(policy.MaxStaleSeconds) - age.Seconds()
		if remaining < 1 {
			return nil, fmt.Errorf("flatten observation exceeds policy freshness")
		}
		if fact.Status != "resolved" && fact.Status != "stale" && fact.Status != "error" {
			return nil, fmt.Errorf("flatten observation status is invalid")
		}
		if fact.Status == "resolved" && !fact.CheckedAt.Equal(fact.ObservedAt) {
			return nil, fmt.Errorf("successful flatten check must identify the observed result time")
		}
		if f.FallbackPolicy == "empty_noerror" {
			return nil, fmt.Errorf("empty authoritative answer requires consumer support")
		}
		if fact.Status != "resolved" && f.FallbackPolicy != "stale_if_error" {
			return nil, fmt.Errorf("flatten failure without authorized stale fallback")
		}
		if fact.TargetTTL < 0 || fact.TargetTTL > 2147483647 {
			return nil, fmt.Errorf("flatten target TTL is invalid")
		}
		ttl := min(record.TTL, int(remaining))
		if f.TTLPolicy != "record" {
			targetRemaining := int(float64(fact.TargetTTL) - age.Seconds())
			if fact.Status != "resolved" && f.FallbackPolicy == "stale_if_error" {
				targetRemaining = fact.TargetTTL
			}
			if fact.TargetTTL == 0 || targetRemaining < 1 {
				return nil, fmt.Errorf("flatten TTL policy requires unexpired target TTL evidence")
			}
			if f.TTLPolicy == "target" {
				ttl = min(targetRemaining, int(remaining))
			} else {
				ttl = min(ttl, targetRemaining)
			}
		}
		for _, family := range []struct {
			kind   string
			values []string
		}{{"A", fact.A}, {"AAAA", fact.AAAA}} {
			for _, value := range family.values {
				ip, err := netip.ParseAddr(value)
				if err != nil || (family.kind == "A") != ip.Is4() || !PublicDNSFlattenIP(ip) {
					return nil, fmt.Errorf("flatten answer must contain public addresses of the correct family")
				}
			}
		}
		a, aaaa := fact.A, fact.AAAA
		if f.IPv4Policy == "ipv6_only" || f.IPv6Policy == "ipv6_only" {
			a = nil
		}
		if f.IPv4Policy == "ipv4_only" || f.IPv6Policy == "ipv4_only" {
			aaaa = nil
		}
		if (f.IPv4Policy == "dual_stack_required" || f.IPv6Policy == "dual_stack_required") && (len(a) == 0 || len(aaaa) == 0) {
			return nil, fmt.Errorf("flatten requires both address families")
		}
		if len(a)+len(aaaa) == 0 {
			return nil, fmt.Errorf("flatten has no usable answers")
		}
		for _, family := range []struct {
			kind   string
			values []string
		}{{"A", a}, {"AAAA", aaaa}} {
			if len(family.values) == 0 {
				continue
			}
			compiled := record
			compiled.Flatten, compiled.Type, compiled.Values, compiled.TTL = nil, family.kind, append([]string(nil), family.values...), ttl
			out = append(out, compiled)
		}
	}
	if len(matched) != len(byDigest) {
		return nil, fmt.Errorf("flatten observations reference configuration outside intent")
	}
	out = NormalizePlatformIntent(PlatformIntent{DNS: out}).DNS
	if err := ValidateDNSIntents(out); err != nil {
		return nil, err
	}
	return out, nil
}

var flattenReservedPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"), netip.MustParsePrefix("100.64.0.0/10"), netip.MustParsePrefix("127.0.0.0/8"), netip.MustParsePrefix("169.254.0.0/16"),
	netip.MustParsePrefix("192.0.0.0/24"), netip.MustParsePrefix("192.0.2.0/24"), netip.MustParsePrefix("198.18.0.0/15"), netip.MustParsePrefix("198.51.100.0/24"), netip.MustParsePrefix("203.0.113.0/24"), netip.MustParsePrefix("224.0.0.0/4"), netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("::/128"), netip.MustParsePrefix("::1/128"), netip.MustParsePrefix("64:ff9b:1::/48"), netip.MustParsePrefix("100::/64"), netip.MustParsePrefix("2001:db8::/32"), netip.MustParsePrefix("fc00::/7"), netip.MustParsePrefix("fe80::/10"),
}

// PublicDNSFlattenIP is shared by observation capture and artifact validation.
func PublicDNSFlattenIP(ip netip.Addr) bool {
	if !ip.IsValid() || ip.Is4In6() || !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
		return false
	}
	for _, prefix := range flattenReservedPrefixes {
		if prefix.Contains(ip) {
			return false
		}
	}
	return true
}
