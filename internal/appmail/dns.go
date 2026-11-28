package appmail

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/store"

	"github.com/miekg/dns"
)

// DNSRules preserves administrator authority. Add exact provider requirements
// only when compatible; never silently replace existing MX, SPF or DKIM.
func DNSRules(config model.MailPlatformConfig, binding model.AppMailBinding, provider []model.AppMailDNSRecord) ([]model.AppMailDNSRecord, error) {
	rules := append(append([]model.AppMailDNSRecord(nil), config.DNSRecords...), config.AppDNSRecords[binding.AppID]...)
	for _, record := range provider {
		found := false
		for _, existing := range rules {
			if existing.Name != record.Name || existing.Type != record.Type {
				continue
			}
			if !DNSMatches(record.Type, record.Values, existing.Values) || !DNSMatches(record.Type, existing.Values, record.Values) {
				return nil, fmt.Errorf("mail DNS conflicts with administrator policy")
			}
			found = true
		}
		if !found {
			rules = append(rules, record)
		}
	}
	return rules, ValidateDNSPolicy(rules)
}

func ValidateDNSPolicy(records []model.AppMailDNSRecord) error {
	if len(records) > 12 {
		return store.ErrInvalidInput
	}
	intents := []platformconfig.DNSIntent{}
	for _, r := range records {
		if r.TTL < 60 || r.TTL > 86400 || len(r.Name) > 160 || len(r.Values) > 8 || (r.Type != "MX" && r.Type != "TXT" && r.Type != "CNAME") {
			return store.ErrInvalidInput
		}
		name := "app.apps.example.test"
		if r.Name != "@" {
			if r.Name == "" || r.Name != strings.ToLower(r.Name) || strings.HasSuffix(r.Name, ".") || strings.ContainsAny(r.Name, "/ @\t\r\n") {
				return store.ErrInvalidInput
			}
			name = r.Name + "." + name
		} else if r.Type == "CNAME" {
			return store.ErrInvalidInput
		}
		for _, v := range r.Values {
			if len(v) > 4096 {
				return store.ErrInvalidInput
			}
		}
		intents = append(intents, platformconfig.DNSIntent{Hostname: name, Type: r.Type, Values: r.Values, TTL: r.TTL})
	}
	if err := platformconfig.ValidateDNSIntents(intents); err != nil {
		return fmt.Errorf("invalid mail DNS policy: %w", store.ErrInvalidInput)
	}
	return nil
}

func ValidateDNSConfig(config model.MailPlatformConfig) error {
	if len(config.AppDNSRecords) > 128 {
		return store.ErrInvalidInput
	}
	if err := ValidateDNSPolicy(config.DNSRecords); err != nil {
		return err
	}
	for appID, records := range config.AppDNSRecords {
		if len(appID) < 1 || len(appID) > 160 || strings.ContainsAny(appID, " /\t\r\n") {
			return store.ErrInvalidInput
		}
		combined := append(append([]model.AppMailDNSRecord(nil), config.DNSRecords...), records...)
		if err := ValidateDNSPolicy(combined); err != nil {
			return err
		}
	}
	return nil
}

func normalizeDNSValue(kind, value string) string {
	if kind == "CNAME" {
		return strings.ToLower(dns.Fqdn(strings.TrimSpace(value)))
	}
	if kind == "MX" {
		parts := strings.Fields(value)
		if len(parts) == 2 {
			priority, err := strconv.Atoi(parts[0])
			if err == nil {
				return fmt.Sprintf("%d %s", priority, strings.ToLower(dns.Fqdn(parts[1])))
			}
		}
	}
	return value // TXT content is case-sensitive; never normalize a DKIM key.
}

func DNSMatches(kind string, expected, observed []string) bool {
	normalize := func(values []string) []string {
		out := make([]string, len(values))
		for i, value := range values {
			out[i] = normalizeDNSValue(kind, value)
		}
		slices.Sort(out)
		return slices.Compact(out)
	}
	want, got := normalize(expected), normalize(observed)
	if kind != "TXT" {
		return slices.Equal(want, got)
	}
	for _, value := range want {
		if !slices.Contains(got, value) {
			return false
		}
		for _, prefix := range []string{"v=spf1", "v=dmarc1", "v=dkim1"} {
			if !strings.HasPrefix(strings.ToLower(value), prefix) {
				continue
			}
			for _, actual := range got {
				if actual != value && strings.HasPrefix(strings.ToLower(actual), prefix) {
					return false // Two competing policies must not report healthy.
				}
			}
		}
	}
	return true
}
