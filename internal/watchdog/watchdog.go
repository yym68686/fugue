package watchdog

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"

	"github.com/miekg/dns"
)

const (
	ProbeStatusPass    = "pass"
	ProbeStatusFail    = "fail"
	ProbeStatusSkipped = "skipped"

	ProviderActionModeDisabled = "disabled"
	ProviderActionModeObserve  = "observe"
)

type Config struct {
	APIURL          string
	KubernetesAPI   string
	DBTCPAddr       string
	EtcdQuorumURL   string
	DNSServer       string
	DNSName         string
	EdgeURLs        []string
	RunnerURL       string
	Timeout         time.Duration
	ProviderMode    string
	ProviderTargets []ProviderPowerTarget
}

type ProbeResult struct {
	Name       string            `json:"name"`
	Status     string            `json:"status"`
	Target     string            `json:"target,omitempty"`
	LatencyMS  int64             `json:"latency_ms,omitempty"`
	Error      string            `json:"error,omitempty"`
	Evidence   map[string]string `json:"evidence,omitempty"`
	ObservedAt time.Time         `json:"observed_at"`
}

type ProviderPowerTarget struct {
	Provider      string `json:"provider"`
	Region        string `json:"region,omitempty"`
	InstanceID    string `json:"instance_id"`
	FailureDomain string `json:"failure_domain,omitempty"`
}

type ProviderActionResult struct {
	Provider   string    `json:"provider"`
	InstanceID string    `json:"instance_id"`
	Action     string    `json:"action"`
	Mode       string    `json:"mode"`
	ActionID   string    `json:"action_id,omitempty"`
	Status     string    `json:"status"`
	Message    string    `json:"message,omitempty"`
	RecordedAt time.Time `json:"recorded_at"`
}

type ProviderPowerClient interface {
	PowerOn(ctx context.Context, target ProviderPowerTarget) (ProviderActionResult, error)
}

type NoopProviderPowerClient struct {
	Mode string
}

func (c NoopProviderPowerClient) PowerOn(ctx context.Context, target ProviderPowerTarget) (ProviderActionResult, error) {
	mode := strings.TrimSpace(c.Mode)
	if mode == "" {
		mode = ProviderActionModeObserve
	}
	return ProviderActionResult{
		Provider:   strings.TrimSpace(target.Provider),
		InstanceID: strings.TrimSpace(target.InstanceID),
		Action:     "power_on",
		Mode:       mode,
		Status:     "not_executed",
		Message:    "provider power action interface is wired; no provider client is configured, so watchdog records evidence only",
		RecordedAt: time.Now().UTC(),
	}, nil
}

type Report struct {
	GeneratedAt     time.Time              `json:"generated_at"`
	Pass            bool                   `json:"pass"`
	ProviderMode    string                 `json:"provider_mode,omitempty"`
	Probes          []ProbeResult          `json:"probes"`
	ProviderActions []ProviderActionResult `json:"provider_actions,omitempty"`
}

func Run(ctx context.Context, cfg Config, provider ProviderPowerClient) Report {
	now := time.Now().UTC()
	if cfg.Timeout <= 0 {
		cfg.Timeout = 5 * time.Second
	}
	if strings.TrimSpace(cfg.ProviderMode) == "" {
		cfg.ProviderMode = ProviderActionModeObserve
	}
	probes := []ProbeResult{
		httpProbe(ctx, "control_plane_api_https", cfg.APIURL, cfg.Timeout, true),
		httpProbe(ctx, "kubernetes_api_https", cfg.KubernetesAPI, cfg.Timeout, true),
		tcpProbe(ctx, "control_plane_db_tcp", cfg.DBTCPAddr, cfg.Timeout),
		httpProbe(ctx, "control_plane_etcd_quorum", cfg.EtcdQuorumURL, cfg.Timeout, true),
		dnsProbe(ctx, "authoritative_dns", cfg.DNSServer, cfg.DNSName, cfg.Timeout),
		httpProbe(ctx, "github_runner", cfg.RunnerURL, cfg.Timeout, false),
	}
	for _, edgeURL := range sortedNonEmpty(cfg.EdgeURLs) {
		probes = append(probes, httpProbe(ctx, "edge_public", edgeURL, cfg.Timeout, true))
	}
	pass := true
	for _, probe := range probes {
		if probe.Status == ProbeStatusFail {
			pass = false
			break
		}
	}
	actions := []ProviderActionResult{}
	if !pass && provider != nil && cfg.ProviderMode != ProviderActionModeDisabled {
		for _, target := range cfg.ProviderTargets {
			if strings.TrimSpace(target.Provider) == "" || strings.TrimSpace(target.InstanceID) == "" {
				continue
			}
			result, err := provider.PowerOn(ctx, target)
			if err != nil {
				result = ProviderActionResult{
					Provider:   strings.TrimSpace(target.Provider),
					InstanceID: strings.TrimSpace(target.InstanceID),
					Action:     "power_on",
					Mode:       cfg.ProviderMode,
					Status:     "failed",
					Message:    err.Error(),
					RecordedAt: time.Now().UTC(),
				}
			}
			actions = append(actions, result)
		}
	}
	return Report{
		GeneratedAt:     now,
		Pass:            pass,
		ProviderMode:    cfg.ProviderMode,
		Probes:          probes,
		ProviderActions: actions,
	}
}

func tcpProbe(ctx context.Context, name, addr string, timeout time.Duration) ProbeResult {
	observedAt := time.Now().UTC()
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return ProbeResult{Name: name, Status: ProbeStatusSkipped, ObservedAt: observedAt}
	}
	if !strings.Contains(addr, ":") {
		return ProbeResult{Name: name, Target: addr, Status: ProbeStatusFail, Error: "host:port address required", ObservedAt: observedAt}
	}
	dialer := &net.Dialer{Timeout: timeout}
	start := time.Now()
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	latency := time.Since(start).Milliseconds()
	if err != nil {
		return ProbeResult{Name: name, Target: addr, Status: ProbeStatusFail, LatencyMS: latency, Error: err.Error(), ObservedAt: observedAt}
	}
	_ = conn.Close()
	return ProbeResult{Name: name, Target: addr, Status: ProbeStatusPass, LatencyMS: latency, Evidence: map[string]string{"tcp_connect": "ok"}, ObservedAt: observedAt}
}

func httpProbe(ctx context.Context, name, rawURL string, timeout time.Duration, requireTLS bool) ProbeResult {
	observedAt := time.Now().UTC()
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return ProbeResult{Name: name, Status: ProbeStatusSkipped, ObservedAt: observedAt}
	}
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return ProbeResult{Name: name, Target: rawURL, Status: ProbeStatusFail, Error: err.Error(), ObservedAt: observedAt}
	}
	if requireTLS && parsed.Scheme != "https" {
		return ProbeResult{Name: name, Target: rawURL, Status: ProbeStatusFail, Error: "https URL required", ObservedAt: observedAt}
	}
	client := &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12},
		},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return ProbeResult{Name: name, Target: rawURL, Status: ProbeStatusFail, Error: err.Error(), ObservedAt: observedAt}
	}
	start := time.Now()
	resp, err := client.Do(req)
	latency := time.Since(start).Milliseconds()
	if err != nil {
		return ProbeResult{Name: name, Target: rawURL, Status: ProbeStatusFail, LatencyMS: latency, Error: err.Error(), ObservedAt: observedAt}
	}
	defer resp.Body.Close()
	status := ProbeStatusPass
	errText := ""
	if resp.StatusCode >= 500 {
		status = ProbeStatusFail
		errText = fmt.Sprintf("http status %d", resp.StatusCode)
	}
	return ProbeResult{
		Name:       name,
		Target:     rawURL,
		Status:     status,
		LatencyMS:  latency,
		Error:      errText,
		Evidence:   map[string]string{"http_status": fmt.Sprintf("%d", resp.StatusCode)},
		ObservedAt: observedAt,
	}
}

func dnsProbe(ctx context.Context, name, server, queryName string, timeout time.Duration) ProbeResult {
	observedAt := time.Now().UTC()
	server = strings.TrimSpace(server)
	queryName = strings.TrimSpace(queryName)
	if server == "" || queryName == "" {
		return ProbeResult{Name: name, Status: ProbeStatusSkipped, ObservedAt: observedAt}
	}
	if !strings.Contains(server, ":") {
		server = net.JoinHostPort(server, "53")
	}
	message := new(dns.Msg)
	message.SetQuestion(dns.Fqdn(queryName), dns.TypeA)
	client := &dns.Client{Net: "udp", Timeout: timeout}
	start := time.Now()
	response, _, err := client.ExchangeContext(ctx, message, server)
	latency := time.Since(start).Milliseconds()
	if err != nil {
		return ProbeResult{Name: name, Target: server + "/" + dns.Fqdn(queryName), Status: ProbeStatusFail, LatencyMS: latency, Error: err.Error(), ObservedAt: observedAt}
	}
	if response == nil || response.Rcode != dns.RcodeSuccess || len(response.Answer) == 0 {
		rcode := "nil"
		if response != nil {
			rcode = dns.RcodeToString[response.Rcode]
		}
		return ProbeResult{Name: name, Target: server + "/" + dns.Fqdn(queryName), Status: ProbeStatusFail, LatencyMS: latency, Error: "no successful A answer", Evidence: map[string]string{"rcode": rcode}, ObservedAt: observedAt}
	}
	return ProbeResult{Name: name, Target: server + "/" + dns.Fqdn(queryName), Status: ProbeStatusPass, LatencyMS: latency, Evidence: map[string]string{"answers": fmt.Sprintf("%d", len(response.Answer))}, ObservedAt: observedAt}
}

func sortedNonEmpty(values []string) []string {
	out := []string{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out = append(out, value)
		}
	}
	sort.Strings(out)
	return out
}

func ParseProviderTargets(raw string) ([]ProviderPowerTarget, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	var targets []ProviderPowerTarget
	if err := json.Unmarshal([]byte(raw), &targets); err != nil {
		return nil, err
	}
	return targets, nil
}

func ClassifyProviderPowerEvent(target ProviderPowerTarget, action ProviderActionResult, providerEventType, providerMessage string, providerObservedAt time.Time) model.ProviderPowerEvent {
	providerEventType = strings.TrimSpace(strings.ToLower(providerEventType))
	providerMessage = strings.TrimSpace(providerMessage)
	eventClass := model.ProviderPowerEventClassUnknownPowerLoss
	switch {
	case strings.TrimSpace(target.Provider) == "" || strings.TrimSpace(target.InstanceID) == "":
		eventClass = model.ProviderPowerEventClassNoProviderEvidence
	case strings.Contains(providerEventType, "maintenance") || strings.Contains(providerEventType, "planned"):
		eventClass = model.ProviderPowerEventClassProviderPlanned
	case strings.Contains(providerEventType, "poweroff") || strings.Contains(providerEventType, "shutdown") || strings.Contains(providerEventType, "stop"):
		if strings.Contains(strings.ToLower(providerMessage), "guest") || strings.Contains(strings.ToLower(providerMessage), "os initiated") {
			eventClass = model.ProviderPowerEventClassGuestInitiated
		} else {
			eventClass = model.ProviderPowerEventClassProviderUnplanned
		}
	case strings.TrimSpace(action.ActionID) != "":
		eventClass = model.ProviderPowerEventClassProviderUnplanned
	case strings.TrimSpace(providerEventType) == "":
		eventClass = model.ProviderPowerEventClassNoProviderEvidence
	}
	if providerObservedAt.IsZero() {
		providerObservedAt = action.RecordedAt
	}
	if providerObservedAt.IsZero() {
		providerObservedAt = time.Now().UTC()
	}
	return model.ProviderPowerEvent{
		ID:         model.NewID("power"),
		Provider:   strings.TrimSpace(target.Provider),
		Region:     strings.TrimSpace(target.Region),
		InstanceID: strings.TrimSpace(target.InstanceID),
		EventType:  providerEventType,
		ActionID:   strings.TrimSpace(action.ActionID),
		EventClass: eventClass,
		Message:    providerMessage,
		Evidence: map[string]string{
			"watchdog_action": strings.TrimSpace(action.Action),
			"watchdog_status": strings.TrimSpace(action.Status),
			"failure_domain":  strings.TrimSpace(target.FailureDomain),
		},
		ProviderAt: providerObservedAt.UTC(),
		ObservedAt: time.Now().UTC(),
	}
}
