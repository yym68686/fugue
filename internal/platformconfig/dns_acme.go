package platformconfig

import (
	"fmt"
	"slices"
	"strings"
	"time"
)

type ACMEChallengeIntent struct {
	ID        string    `json:"id"`
	Zone      string    `json:"zone"`
	Hostname  string    `json:"hostname"`
	Value     string    `json:"value"`
	TTL       int       `json:"ttl"`
	ExpiresAt time.Time `json:"expires_at"`
}

func ValidateACMEChallenges(challenges []ACMEChallengeIntent) error {
	if len(challenges) > 10000 {
		return fmt.Errorf("too many ACME challenges")
	}
	seen := map[string]bool{}
	for _, c := range challenges {
		if c.ID == "" || c.ID != strings.TrimSpace(c.ID) || seen[c.ID] {
			return fmt.Errorf("ACME challenge identity is invalid")
		}
		seen[c.ID] = true
		if c.Zone == "" || c.Zone != normalizedImportHostname(c.Zone) || !strings.HasPrefix(c.Hostname, "_acme-challenge.") || !strings.HasSuffix(c.Hostname, "."+c.Zone) {
			return fmt.Errorf("ACME challenge hostname is outside its zone")
		}
		if c.TTL < 1 || c.TTL > 3600 || c.ExpiresAt.IsZero() || c.Value != strings.TrimSpace(c.Value) {
			return fmt.Errorf("ACME TTL, expiration or value is invalid")
		}
		if err := ValidateDNSIntents([]DNSIntent{{Hostname: c.Hostname, Type: "TXT", Values: []string{c.Value}, TTL: c.TTL}}); err != nil {
			return err
		}
	}
	return nil
}

func cloneDNSExpirations(in map[string]time.Time) map[string]time.Time {
	out := make(map[string]time.Time, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// DNSRecordsAt enforces absolute value leases both during compilation and at
// runtime. No receiver can extend a lease by repeatedly serving its cached TTL.
func DNSRecordsAt(records []DNSIntent, now time.Time) ([]DNSIntent, error) {
	out := make([]DNSIntent, 0, len(records))
	for _, record := range records {
		if len(record.ValueExpirations) == 0 {
			out = append(out, record)
			continue
		}
		if now.IsZero() {
			return nil, fmt.Errorf("leased DNS values require an explicit time")
		}
		if record.Type != "TXT" {
			return nil, fmt.Errorf("DNS value leases require TXT records")
		}
		if record.TTL < 1 || record.TTL > 2147483647 {
			return nil, fmt.Errorf("leased DNS record requires a valid TTL")
		}
		for value, expires := range record.ValueExpirations {
			if expires.IsZero() || !slices.Contains(record.Values, value) {
				return nil, fmt.Errorf("DNS expiration references an invalid value")
			}
		}
		values := []string{}
		expires := map[string]time.Time{}
		for _, value := range record.Values {
			if until, ok := record.ValueExpirations[value]; ok {
				remaining := int(until.Sub(now).Seconds())
				if remaining < 1 {
					continue
				}
				record.TTL = min(record.TTL, remaining)
				expires[value] = until
			}
			values = append(values, value)
		}
		if len(values) == 0 {
			continue
		}
		record.Values, record.ValueExpirations = values, expires
		out = append(out, record)
	}
	return out, nil
}

func CompileACMEChallenges(records []DNSIntent, challenges []ACMEChallengeIntent, captured *time.Time) ([]DNSIntent, error) {
	if err := ValidateACMEChallenges(challenges); err != nil {
		return nil, err
	}
	now := time.Time{}
	if captured != nil {
		now = *captured
	}
	if len(challenges) > 0 && now.IsZero() {
		return nil, fmt.Errorf("ACME compilation requires a fixed captured_at")
	}
	out := NormalizePlatformIntent(PlatformIntent{DNS: records}).DNS
	byHost := map[string]int{}
	for i, r := range out {
		if r.Type == "TXT" {
			byHost[r.Hostname] = i
		}
	}
	for _, c := range challenges {
		if !c.ExpiresAt.After(now) {
			continue
		}
		i, exists := byHost[c.Hostname]
		if !exists {
			i = len(out)
			byHost[c.Hostname] = i
			out = append(out, DNSIntent{Hostname: c.Hostname, Type: "TXT", TTL: c.TTL, RecordKind: "acme-challenge", Status: "active"})
		}
		r := &out[i]
		r.TTL = min(r.TTL, c.TTL)
		if slices.Contains(r.Values, c.Value) {
			if old, ok := r.ValueExpirations[c.Value]; ok && c.ExpiresAt.After(old) {
				r.ValueExpirations[c.Value] = c.ExpiresAt
			}
			continue // A permanent static value must never acquire an expiry.
		}
		r.Values = append(r.Values, c.Value)
		if r.ValueExpirations == nil {
			r.ValueExpirations = map[string]time.Time{}
		}
		r.ValueExpirations[c.Value] = c.ExpiresAt
	}
	out, err := DNSRecordsAt(out, now)
	if err != nil {
		return nil, err
	}
	out = NormalizePlatformIntent(PlatformIntent{DNS: out}).DNS
	if err := ValidateDNSIntents(out); err != nil {
		return nil, err
	}
	return out, nil
}
