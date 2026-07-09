package originhealth

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"fugue/internal/model"
)

const (
	ProbeStatusPass    = "pass"
	ProbeStatusFail    = "fail"
	ProbeStatusSkipped = "skipped"

	DefaultProbeTimeout = 3 * time.Second
)

type RecordInput struct {
	Hostname        string
	PathPrefix      string
	RouteGeneration string
	ServiceIdentity string
	EndpointLKGID   string
	ServiceDNSProbe *model.OriginProbeObservation
	ClusterIPProbe  *model.OriginProbeObservation
	EndpointIPProbe *model.OriginProbeObservation
	HTTPProbe       *model.OriginProbeObservation
	CheckedAt       time.Time
}

func BuildRecord(input RecordInput) model.OriginHealthRecord {
	checkedAt := input.CheckedAt
	if checkedAt.IsZero() {
		checkedAt = time.Now().UTC()
	}
	status := ProbeStatusPass
	lastFailureClass := ""
	for _, probe := range []*model.OriginProbeObservation{
		input.ServiceDNSProbe,
		input.ClusterIPProbe,
		input.EndpointIPProbe,
		input.HTTPProbe,
	} {
		if probe == nil || probe.Status != ProbeStatusFail {
			continue
		}
		status = ProbeStatusFail
		lastFailureClass = readFailureClass(probe)
		break
	}
	return model.OriginHealthRecord{
		Hostname:         strings.TrimSpace(input.Hostname),
		PathPrefix:       strings.TrimSpace(input.PathPrefix),
		RouteGeneration:  strings.TrimSpace(input.RouteGeneration),
		ServiceIdentity:  strings.TrimSpace(input.ServiceIdentity),
		EndpointLKGID:    strings.TrimSpace(input.EndpointLKGID),
		Status:           status,
		LastFailureClass: lastFailureClass,
		ServiceDNSProbe:  cloneObservation(input.ServiceDNSProbe),
		ClusterIPProbe:   cloneObservation(input.ClusterIPProbe),
		EndpointIPProbe:  cloneObservation(input.EndpointIPProbe),
		HTTPProbe:        cloneObservation(input.HTTPProbe),
		CheckedAt:        checkedAt.UTC(),
	}
}

func ProbeEndpointTCP(ctx context.Context, endpoint model.EndpointLKGEndpoint, timeout time.Duration) model.OriginProbeObservation {
	if strings.TrimSpace(endpoint.IP) == "" || endpoint.Port <= 0 {
		return model.OriginProbeObservation{
			Status:    ProbeStatusSkipped,
			Error:     "endpoint ip and port are required",
			CheckedAt: time.Now().UTC(),
		}
	}
	return ProbeTCPAddr(ctx, net.JoinHostPort(strings.TrimSpace(endpoint.IP), fmt.Sprintf("%d", endpoint.Port)), timeout)
}

func ProbeTCPAddr(ctx context.Context, addr string, timeout time.Duration) model.OriginProbeObservation {
	checkedAt := time.Now().UTC()
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return model.OriginProbeObservation{Status: ProbeStatusSkipped, CheckedAt: checkedAt}
	}
	if timeout <= 0 {
		timeout = DefaultProbeTimeout
	}
	if !strings.Contains(addr, ":") {
		return model.OriginProbeObservation{
			Status:    ProbeStatusFail,
			Target:    addr,
			Error:     "host:port address required",
			Evidence:  map[string]string{"failure_class": "endpoint_tcp_invalid_target"},
			CheckedAt: checkedAt,
		}
	}
	dialer := &net.Dialer{Timeout: timeout}
	start := time.Now()
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	latency := time.Since(start).Milliseconds()
	if err != nil {
		return model.OriginProbeObservation{
			Status:    ProbeStatusFail,
			Target:    addr,
			LatencyMS: latency,
			Error:     err.Error(),
			Evidence:  map[string]string{"failure_class": "endpoint_tcp_connect_failed"},
			CheckedAt: checkedAt,
		}
	}
	_ = conn.Close()
	return model.OriginProbeObservation{
		Status:    ProbeStatusPass,
		Target:    addr,
		LatencyMS: latency,
		Evidence:  map[string]string{"tcp_connect": "ok"},
		CheckedAt: checkedAt,
	}
}

func ProbeHTTP(ctx context.Context, rawURL string, timeout time.Duration, requireTLS bool) model.OriginProbeObservation {
	checkedAt := time.Now().UTC()
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return model.OriginProbeObservation{Status: ProbeStatusSkipped, CheckedAt: checkedAt}
	}
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return model.OriginProbeObservation{
			Status:    ProbeStatusFail,
			Target:    rawURL,
			Error:     err.Error(),
			Evidence:  map[string]string{"failure_class": "http_probe_invalid_url"},
			CheckedAt: checkedAt,
		}
	}
	if requireTLS && parsed.Scheme != "https" {
		return model.OriginProbeObservation{
			Status:    ProbeStatusFail,
			Target:    rawURL,
			Error:     "https URL required",
			Evidence:  map[string]string{"failure_class": "http_probe_insecure_scheme"},
			CheckedAt: checkedAt,
		}
	}
	if timeout <= 0 {
		timeout = DefaultProbeTimeout
	}
	client := &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12},
		},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return model.OriginProbeObservation{
			Status:    ProbeStatusFail,
			Target:    rawURL,
			Error:     err.Error(),
			Evidence:  map[string]string{"failure_class": "http_probe_request_build_failed"},
			CheckedAt: checkedAt,
		}
	}
	start := time.Now()
	resp, err := client.Do(req)
	latency := time.Since(start).Milliseconds()
	if err != nil {
		return model.OriginProbeObservation{
			Status:    ProbeStatusFail,
			Target:    rawURL,
			LatencyMS: latency,
			Error:     err.Error(),
			Evidence:  map[string]string{"failure_class": "http_probe_connect_failed"},
			CheckedAt: checkedAt,
		}
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	evidence := map[string]string{"status_code": fmt.Sprintf("%d", resp.StatusCode)}
	if resp.StatusCode >= 500 {
		evidence["failure_class"] = "http_probe_5xx"
		return model.OriginProbeObservation{
			Status:    ProbeStatusFail,
			Target:    rawURL,
			LatencyMS: latency,
			Error:     fmt.Sprintf("http status %d", resp.StatusCode),
			Evidence:  evidence,
			CheckedAt: checkedAt,
		}
	}
	return model.OriginProbeObservation{
		Status:    ProbeStatusPass,
		Target:    rawURL,
		LatencyMS: latency,
		Evidence:  evidence,
		CheckedAt: checkedAt,
	}
}

func readFailureClass(probe *model.OriginProbeObservation) string {
	if probe == nil {
		return ""
	}
	if probe.Evidence != nil && strings.TrimSpace(probe.Evidence["failure_class"]) != "" {
		return strings.TrimSpace(probe.Evidence["failure_class"])
	}
	return strings.TrimSpace(probe.Error)
}

func cloneObservation(observation *model.OriginProbeObservation) *model.OriginProbeObservation {
	if observation == nil {
		return nil
	}
	copied := *observation
	if observation.Evidence != nil {
		copied.Evidence = make(map[string]string, len(observation.Evidence))
		for key, value := range observation.Evidence {
			copied.Evidence[key] = value
		}
	}
	return &copied
}
