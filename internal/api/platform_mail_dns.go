package api

import (
	"fmt"
	"slices"
	"strings"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/store"
)

func validateMailDNSPolicy(records []model.AppMailDNSRecord) error {
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

// Consume only the frozen business snapshot. A provider request, runtime
// observation or a second store read must not change the DNS artifact midway.
func projectAppMailDNS(result *platformIntentProjectionResponse, business store.RouteBusinessSnapshot, base string) error {
	cfg := business.MailPlatformConfig
	if cfg == nil || !cfg.Enabled {
		return nil
	}
	if err := validateMailDNSPolicy(cfg.DNSRecords); err != nil {
		return err
	}
	apps := map[string]model.App{}
	for _, a := range business.Apps {
		apps[a.ID] = a
	}
	for _, b := range business.AppMailBindings {
		app, exists := apps[b.AppID]
		host, err := appMailHostname(app, base)
		if b.State != "active" || !exists || err != nil || host != b.Hostname || app.ID != b.AppID || app.TenantID != b.TenantID || app.ProjectID != b.ProjectID || app.Status.Phase == "deleted" || app.Status.Phase == "deleting" || app.Status.Phase == "disabled" {
			continue
		}
		for _, rule := range cfg.DNSRecords {
			name := host
			if rule.Name != "@" {
				name = rule.Name + "." + host
			}
			record := platformconfig.DNSIntent{Hostname: name, Type: rule.Type, Values: append([]string(nil), rule.Values...), TTL: rule.TTL, AppID: b.AppID, TenantID: b.TenantID, RecordKind: model.EdgeDNSRecordKindPlatform}
			found := -1
			conflict := false
			for i, existing := range result.Intent.DNS {
				if existing.Hostname != name {
					continue
				}
				if existing.TenantID != "" && existing.TenantID != b.TenantID || existing.AppID != "" && existing.AppID != b.AppID {
					conflict = true
					break
				}
				if existing.Type == "CNAME" && rule.Type != "CNAME" || rule.Type == "CNAME" && existing.Type != "CNAME" {
					conflict = true
					break
				}
				if existing.Type == rule.Type {
					found = i
				}
			}
			if !conflict && found >= 0 {
				existing := &result.Intent.DNS[found]
				if rule.Type != "TXT" {
					// An explicit RRset wins. Never silently replace user MX/DKIM.
					for _, value := range rule.Values {
						if !slices.Contains(existing.Values, value) {
							conflict = true
						}
					}
				} else {
					for _, value := range rule.Values {
						if slices.Contains(existing.Values, value) {
							continue
						}
						prefix := ""
						for _, p := range []string{"v=spf1", "v=dmarc1", "v=dkim1"} {
							if strings.HasPrefix(strings.ToLower(value), p) {
								prefix = p
								break
							}
						}
						for _, current := range existing.Values {
							if prefix != "" && strings.HasPrefix(strings.ToLower(current), prefix) {
								conflict = true
							}
						}
						if !conflict {
							existing.Values = append(existing.Values, value)
						}
					}
				}
			} else if !conflict {
				result.Intent.DNS = append(result.Intent.DNS, record)
			}
			if conflict {
				result.Issues = append(result.Issues, platformProjectionIssue{Code: "mail_dns_conflict", Hostname: name, Reason: "Explicit DNS ownership or mail policy conflicts; existing records retained"})
			}
		}
	}
	result.Intent = platformconfig.NormalizePlatformIntent(result.Intent)
	gen, err := platformconfig.PlatformIntentGeneration(result.Intent)
	if err != nil {
		return err
	}
	result.Intent.Generation = gen
	result.RuntimeSnapshot.IntentGeneration = gen
	return nil
}
