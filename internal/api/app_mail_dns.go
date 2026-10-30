package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"

	"github.com/miekg/dns"
)

type appMailDNSRecordObservation struct {
	Hostname string   `json:"hostname"`
	Type     string   `json:"type"`
	Expected []string `json:"expected"`
	Observed []string `json:"observed"`
	Status   string   `json:"status"`
}

func (s *Server) handleGetAppMailDNS(w http.ResponseWriter, r *http.Request) {
	app, ok := s.appMailAuthorized(w, r, false)
	if !ok {
		return
	}
	binding, err := s.store.GetAppMailBinding(app.ID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	host, hostErr := appMailHostname(app, s.appBaseDomain)
	if hostErr != nil || binding.Hostname != host || binding.TenantID != app.TenantID || binding.ProjectID != app.ProjectID {
		httpx.WriteError(w, 409, "mail binding no longer matches application ownership and route")
		return
	}
	cfg, err := s.store.GetMailPlatformConfig()
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if err = validateMailDNSConfig(cfg); err != nil {
		httpx.WriteError(w, 500, "mail DNS policy is invalid")
		return
	}
	status := "inactive"
	records := []appMailDNSRecordObservation{}
	if cfg.Enabled && binding.State == "active" && app.Status.Phase != "deleted" && app.Status.Phase != "deleting" && app.Status.Phase != "disabled" {
		rules := append(append([]model.AppMailDNSRecord(nil), cfg.DNSRecords...), cfg.AppDNSRecords[app.ID]...)
		lookup := s.mailDNSLookup
		if lookup == nil {
			lookup = lookupAppMailDNS
		}
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		records, status = observeAppMailDNS(ctx, host, rules, lookup)
		cancel()
	}
	// Queries are observations, never authority. Discard results when ownership,
	// intent or the DNS policy changed while the resolver was in flight.
	current, currentErr := s.store.GetApp(app.ID)
	saved, savedErr := s.store.GetAppMailBinding(app.ID)
	policy, policyErr := s.store.GetMailPlatformConfig()
	currentHost, routeErr := appMailHostname(current, s.appBaseDomain)
	if currentErr != nil || savedErr != nil || policyErr != nil || routeErr != nil || currentHost != host || current.TenantID != app.TenantID || current.ProjectID != app.ProjectID || current.Status.Phase != app.Status.Phase || saved != binding || policy.Enabled != cfg.Enabled || !reflect.DeepEqual(policy.DNSRecords, cfg.DNSRecords) || !reflect.DeepEqual(policy.AppDNSRecords[app.ID], cfg.AppDNSRecords[app.ID]) {
		httpx.WriteError(w, 409, "mail DNS intent changed during observation; refresh")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	httpx.WriteJSON(w, 200, map[string]any{"app_id": app.ID, "generation": binding.Generation, "hostname": host, "checked_at": time.Now().UTC(), "status": status, "records": records})
}

func observeAppMailDNS(ctx context.Context, host string, rules []model.AppMailDNSRecord, lookup func(context.Context, string, string) ([]string, error)) ([]appMailDNSRecordObservation, string) {
	records := make([]appMailDNSRecordObservation, len(rules))
	var wg sync.WaitGroup
	for i, rule := range rules {
		wg.Add(1)
		go func(i int, rule model.AppMailDNSRecord) {
			defer wg.Done()
			name := host
			if rule.Name != "@" {
				name = rule.Name + "." + host
			}
			observed, err := lookup(ctx, name, rule.Type)
			if observed == nil {
				observed = []string{}
			}
			status := "mismatch"
			if err != nil {
				status, observed = "unknown", []string{}
			} else if appMailDNSMatches(rule.Type, rule.Values, observed) {
				status = "match"
			}
			records[i] = appMailDNSRecordObservation{name, rule.Type, append([]string{}, rule.Values...), observed, status}
		}(i, rule)
	}
	wg.Wait()
	status := "verified"
	if len(records) == 0 {
		status = "unconfigured"
	}
	for _, record := range records {
		if record.Status != "match" {
			status = "pending"
		}
	}
	return records, status
}

func normalizeAppMailDNSValue(kind, value string) string {
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

func appMailDNSMatches(kind string, expected, observed []string) bool {
	normalize := func(values []string) []string {
		out := make([]string, len(values))
		for i, value := range values {
			out[i] = normalizeAppMailDNSValue(kind, value)
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

// Query only the exact RR owner through the system resolver. Do not follow
// CNAMEs or accept unrelated answers as proof of the configured record.
func lookupAppMailDNS(ctx context.Context, host, kind string) ([]string, error) {
	r := newHostedDNSFlattenResolver()
	message := new(dns.Msg)
	message.SetQuestion(dns.Fqdn(host), dns.StringToType[kind])
	for _, server := range r.servers {
		response, _, err := r.client.ExchangeContext(ctx, message, server)
		if err == nil && response != nil && response.Truncated {
			client := &dns.Client{Net: "tcp", Timeout: 3 * time.Second}
			response, _, err = client.ExchangeContext(ctx, message, server)
		}
		if err != nil || response == nil || response.Truncated {
			continue
		}
		if response.Rcode == dns.RcodeNameError {
			return []string{}, nil
		}
		if response.Rcode != dns.RcodeSuccess {
			continue
		}
		return appMailDNSAnswerValues(response, host, kind)
	}
	return nil, errors.New("mail DNS observation unavailable")
}

func appMailDNSAnswerValues(response *dns.Msg, host, kind string) ([]string, error) {
	values := []string{}
	bytes := 0
	for _, answer := range response.Answer {
		if !strings.EqualFold(answer.Header().Name, dns.Fqdn(host)) || answer.Header().Rrtype != dns.StringToType[kind] {
			continue
		}
		var value string
		switch rr := answer.(type) {
		case *dns.MX:
			value = fmt.Sprintf("%d %s", rr.Preference, strings.ToLower(rr.Mx))
		case *dns.TXT:
			value = strings.Join(rr.Txt, "")
		case *dns.CNAME:
			value = strings.ToLower(rr.Target)
		default:
			continue
		}
		bytes += len(value)
		if len(values) >= 32 || len(value) > 4096 || bytes > 65536 {
			return nil, errors.New("mail DNS response exceeds limit")
		}
		values = append(values, value)
	}
	return values, nil
}
