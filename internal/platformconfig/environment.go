package platformconfig

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"

	"fugue/internal/model"
)

type EnvironmentImportResult struct {
	Intent       PlatformIntent `json:"intent"`
	SourceDigest string         `json:"source_digest"`
	ImportedKeys []string       `json:"imported_keys"`
}

type environmentRoute struct {
	model.PlatformRoute
	Enabled *bool `json:"enabled,omitempty"`
}

type environmentDNSRecord struct {
	Name                string   `json:"name"`
	Type                string   `json:"type"`
	Values              []string `json:"values"`
	Value               string   `json:"value,omitempty"`
	TTL                 int      `json:"ttl"`
	RecordKind          string   `json:"record_kind,omitempty"`
	Status              string   `json:"status,omitempty"`
	StatusReason        string   `json:"status_reason,omitempty"`
	AppID               string   `json:"app_id,omitempty"`
	TenantID            string   `json:"tenant_id,omitempty"`
	EdgeGroupID         string   `json:"edge_group_id,omitempty"`
	FallbackEdgeGroupID string   `json:"fallback_edge_group_id,omitempty"`
}

// ImportEnvironment reads only the legacy serving inputs. Unsupported or invalid
// rows reject the whole preview: dropping them would silently change traffic.
func ImportEnvironment(env map[string]string, generation string) (EnvironmentImportResult, error) {
	if strings.TrimSpace(generation) == "" {
		return EnvironmentImportResult{}, fmt.Errorf("environment import generation is required")
	}
	keys := []string{"FUGUE_PLATFORM_ROUTES_JSON", "FUGUE_DNS_STATIC_RECORDS_JSON"}
	canonical := map[string]string{}
	for _, key := range keys {
		canonical[key] = strings.TrimSpace(env[key])
	}
	digest, err := Digest(canonical)
	if err != nil {
		return EnvironmentImportResult{}, err
	}
	result := EnvironmentImportResult{
		Intent:       PlatformIntent{SchemaVersion: SchemaVersion, Generation: generation, Scope: GlobalScopeKey},
		SourceDigest: digest, ImportedKeys: []string{},
	}
	if raw := canonical[keys[0]]; raw != "" {
		var routes []environmentRoute
		if err := decodeEnvironmentRows(raw, "routes", &routes); err != nil {
			return EnvironmentImportResult{}, fmt.Errorf("parse platform routes environment: %w", err)
		}
		for index, route := range routes {
			enabled := true
			if route.Enabled != nil {
				enabled = *route.Enabled
			}
			if route.Status == model.EdgeRouteStatusDisabled {
				enabled = false
			}
			intent := RouteIntent{
				Hostname: normalizedImportHostname(route.Hostname), Kind: strings.TrimSpace(route.Kind),
				UpstreamKind: strings.TrimSpace(route.UpstreamKind), UpstreamScope: strings.TrimSpace(route.UpstreamScope),
				UpstreamURL: strings.TrimSpace(route.UpstreamURL), TLSPolicy: strings.TrimSpace(route.TLSPolicy),
				RoutePolicy: strings.TrimSpace(route.RoutePolicy), EdgeGroupMode: strings.TrimSpace(route.EdgeGroupMode),
				EdgeGroupID: strings.TrimSpace(route.EdgeGroupID), Enabled: enabled,
				Status: strings.TrimSpace(route.Status), StatusReason: strings.TrimSpace(route.StatusReason), TTL: route.TTL,
			}
			if intent.TTL <= 0 {
				intent.TTL = 60
			}
			if err := validateImportedRoute(intent); err != nil {
				return EnvironmentImportResult{}, fmt.Errorf("platform route %d: %w", index, err)
			}
			result.Intent.Routes = append(result.Intent.Routes, intent)
		}
		result.ImportedKeys = append(result.ImportedKeys, keys[0])
	}
	if raw := canonical[keys[1]]; raw != "" {
		var records []environmentDNSRecord
		if err := decodeEnvironmentRows(raw, "records", &records); err != nil {
			return EnvironmentImportResult{}, fmt.Errorf("parse static DNS environment: %w", err)
		}
		for index, record := range records {
			intent := DNSIntent{
				Hostname: normalizedImportHostname(record.Name), Type: strings.ToUpper(strings.TrimSpace(record.Type)),
				TTL: record.TTL, RecordKind: strings.TrimSpace(record.RecordKind),
				Status: strings.TrimSpace(record.Status), StatusReason: strings.TrimSpace(record.StatusReason),
				AppID: strings.TrimSpace(record.AppID), TenantID: strings.TrimSpace(record.TenantID),
				EdgeGroupID: strings.TrimSpace(record.EdgeGroupID), FallbackEdgeGroupID: strings.TrimSpace(record.FallbackEdgeGroupID),
			}
			values := append([]string(nil), record.Values...)
			if record.Value != "" {
				values = append(values, record.Value)
			}
			if intent.Hostname == "" || len(values) == 0 {
				return EnvironmentImportResult{}, fmt.Errorf("DNS record %d requires a name and values", index)
			}
			if intent.TTL <= 0 {
				intent.TTL = 60
			}
			if intent.RecordKind == "" {
				intent.RecordKind = model.EdgeDNSRecordKindProtected
			}
			if intent.Status == "" {
				intent.Status = model.EdgeRouteStatusActive
			}
			if !validImportedStatus(intent.Status) {
				return EnvironmentImportResult{}, fmt.Errorf("DNS record %d has unsupported status", index)
			}
			for _, value := range values {
				normalized, err := normalizeImportedDNSValue(intent.Type, value)
				if err != nil {
					return EnvironmentImportResult{}, fmt.Errorf("DNS record %d: %w", index, err)
				}
				intent.Values = append(intent.Values, normalized)
			}
			result.Intent.DNS = append(result.Intent.DNS, intent)
		}
		result.ImportedKeys = append(result.ImportedKeys, keys[1])
	}
	result.Intent = normalizeIntent(result.Intent)
	if err := ValidatePlatformIntent(result.Intent); err != nil {
		return EnvironmentImportResult{}, err
	}
	return result, nil
}

func decodeEnvironmentRows(raw, key string, out any) error {
	data := []byte(raw)
	if strings.HasPrefix(raw, "{") {
		var envelope map[string]json.RawMessage
		if err := json.Unmarshal(data, &envelope); err != nil {
			return err
		}
		var ok bool
		data, ok = envelope[key]
		if !ok || len(envelope) != 1 {
			return fmt.Errorf("expected only the %s envelope", key)
		}
	}
	if !bytes.HasPrefix(bytes.TrimSpace(data), []byte("[")) {
		return fmt.Errorf("%s must be an array", key)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("%s contains trailing JSON", key)
	}
	return nil
}

func validateImportedRoute(route RouteIntent) error {
	parsed, err := url.Parse(route.UpstreamURL)
	if route.Hostname == "" || err != nil || parsed.Hostname() == "" || parsed.User != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return fmt.Errorf("requires a hostname and HTTP(S) upstream")
	}
	if route.RoutePolicy != "" && model.NormalizeEdgeRoutePolicy(route.RoutePolicy) == "" {
		return fmt.Errorf("unsupported route policy")
	}
	if route.Status != "" && !validImportedStatus(route.Status) {
		return fmt.Errorf("unsupported route status")
	}
	switch route.EdgeGroupMode {
	case "", model.PlatformRouteEdgeGroupModeAllHealthy, model.PlatformRouteEdgeGroupModeRegionAware:
	case model.PlatformRouteEdgeGroupModePinned:
		if route.EdgeGroupID == "" {
			return fmt.Errorf("pinned route requires edge group ID")
		}
	default:
		return fmt.Errorf("unsupported edge group mode")
	}
	return nil
}

func validImportedStatus(status string) bool {
	return status == model.EdgeRouteStatusActive || status == model.EdgeRouteStatusDisabled || status == model.EdgeRouteStatusUnavailable
}

func normalizedImportHostname(host string) string {
	return strings.ToLower(strings.TrimSuffix(strings.TrimSpace(host), "."))
}

func normalizeImportedDNSValue(kind, value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("empty DNS value")
	}
	switch kind {
	case "A", "AAAA":
		ip := net.ParseIP(value)
		if ip == nil || (kind == "A") != (ip.To4() != nil) {
			return "", fmt.Errorf("invalid %s address", kind)
		}
		return ip.String(), nil
	case "CNAME", "NS":
		return normalizedImportHostname(value), nil
	case "CAA", "MX", "SRV", "TXT":
		return value, nil
	default:
		return "", fmt.Errorf("unsupported DNS type %q", kind)
	}
}
