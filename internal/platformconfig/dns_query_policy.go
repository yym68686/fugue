package platformconfig

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"slices"

	"fugue/internal/model"
)

// DNSQueryPolicy declares strategy, never current candidates or ranking results.
type DNSQueryPolicy struct {
	OrderedProjection     *DNSOrderedProjection  `json:"ordered_projection,omitempty"`
	PhysicalRoutes        []PhysicalQualityRoute `json:"physical_routes,omitempty"`
	RankingMode           string                 `json:"ranking_mode"`
	PreferenceMode        string                 `json:"preference_mode"`
	ECSEnabled            bool                   `json:"ecs_enabled"`
	ExplorationPercent    int                    `json:"exploration_percent"`
	SwitchCooldownSeconds int                    `json:"switch_cooldown_seconds"`
	MinimumTTLSeconds     int                    `json:"minimum_ttl_seconds"`
	MaximumTTLSeconds     int                    `json:"maximum_ttl_seconds"`
}

type DNSOrderedProjection struct {
	DefaultOrder model.DNSPhysicalOrder `json:"default_order"`
	Overrides    []DNSOrderOverride     `json:"overrides"`
}

type DNSOrderOverride struct {
	NodeID   string                 `json:"node_id"`
	Hostname string                 `json:"hostname"`
	Type     string                 `json:"type"`
	Order    model.DNSPhysicalOrder `json:"order"`
}

func (value *DNSOrderedProjection) UnmarshalJSON(raw []byte) error {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || len(fields) != 2 || fields["default_order"] == nil || fields["overrides"] == nil || bytes.Equal(bytes.TrimSpace(fields["overrides"]), []byte("null")) {
		return fmt.Errorf("ordered projection requires explicit default and overrides")
	}
	type plain DNSOrderedProjection
	var decoded plain
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&decoded); err != nil {
		return err
	}
	*value = DNSOrderedProjection(decoded)
	return nil
}

func CloneDNSOrderedProjection(value *DNSOrderedProjection) *DNSOrderedProjection {
	if value == nil {
		return nil
	}
	cloned := *value
	cloned.DefaultOrder = *model.CloneDNSPhysicalOrder(&value.DefaultOrder)
	cloned.Overrides = make([]DNSOrderOverride, len(value.Overrides))
	copy(cloned.Overrides, value.Overrides)
	for index := range cloned.Overrides {
		cloned.Overrides[index].Order = *model.CloneDNSPhysicalOrder(&value.Overrides[index].Order)
	}
	return &cloned
}

type PhysicalQualityRoute struct {
	Hostname     string                          `json:"hostname"`
	TrafficClass string                          `json:"traffic_class"`
	Policy       model.PhysicalEdgeQualityPolicy `json:"policy"`
}

func (route *PhysicalQualityRoute) UnmarshalJSON(raw []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	if len(fields) != 3 {
		return fmt.Errorf("physical route requires complete explicit fields")
	}
	var policy map[string]json.RawMessage
	if err := json.Unmarshal(fields["policy"], &policy); err != nil {
		return err
	}
	keys := []string{"version", "maximum_node_utilization", "window_seconds", "bucket_seconds", "required_buckets", "minimum_records", "cooldown_seconds", "evidence_max_age_seconds", "advantage_ms", "advantage_ratio", "unknown_cost_ms", "uncertainty_ms", "failure_cost_ms", "capacity_cost_ms", "throughput_cost_ms", "throughput_target_bps", "probe_interval_seconds", "probe_budget_per_interval"}
	if len(policy) != len(keys) {
		return fmt.Errorf("physical route requires a complete explicit network policy")
	}
	for _, key := range keys {
		value, found := policy[key]
		if !found || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return fmt.Errorf("physical route policy requires %s", key)
		}
	}
	type plain PhysicalQualityRoute
	var decoded plain
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&decoded); err != nil {
		return err
	}
	*route = PhysicalQualityRoute(decoded)
	return nil
}

func (p *DNSQueryPolicy) UnmarshalJSON(raw []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	keys := []string{"ranking_mode", "preference_mode", "ecs_enabled", "exploration_percent", "switch_cooldown_seconds", "minimum_ttl_seconds", "maximum_ttl_seconds"}
	optional := 0
	if raw, present := fields["ordered_projection"]; present {
		optional++
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return fmt.Errorf("ordered projection cannot be null")
		}
	}
	if raw, present := fields["physical_routes"]; present {
		optional++
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return fmt.Errorf("physical routes cannot be null")
		}
	}
	if len(fields) != len(keys)+optional {
		return fmt.Errorf("DNS query strategy requires complete explicit fields")
	}
	for _, k := range keys {
		v, ok := fields[k]
		if !ok || bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
			return fmt.Errorf("DNS query strategy requires %s", k)
		}
	}
	type plain DNSQueryPolicy
	var decoded plain
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&decoded); err != nil {
		return err
	}
	if decoder.Decode(new(any)) != io.EOF {
		return fmt.Errorf("DNS query strategy has trailing data")
	}
	*p = DNSQueryPolicy(decoded)
	return ValidateDNSQueryPolicy(p)
}
func ValidateDNSQueryPolicy(p *DNSQueryPolicy) error {
	if p == nil {
		return nil
	}
	if ordered := p.OrderedProjection; ordered != nil {
		if p.RankingMode != "active" || p.ECSEnabled || p.ExplorationPercent != 0 || model.ValidateDNSPhysicalOrder(&ordered.DefaultOrder) != nil || len(ordered.Overrides) > 20000 {
			return fmt.Errorf("ordered projection requires active explicit non-geographic order")
		}
		seen := map[string]bool{}
		for _, entry := range ordered.Overrides {
			key := dnsQueryKey(entry.NodeID, entry.Hostname, entry.Type)
			if !validDNSConsumerIdentity(entry.NodeID) || !validDNSConsumerZone(entry.Hostname) || entry.Type != "A" && entry.Type != "AAAA" || seen[key] || model.ValidateDNSPhysicalOrder(&entry.Order) != nil {
				return fmt.Errorf("invalid or duplicate physical order override")
			}
			seen[key] = true
		}
	}
	if p.RankingMode != "active" && p.RankingMode != "shadow" && p.RankingMode != "disabled" || p.PreferenceMode != "runtime_locality" || p.ExplorationPercent < 0 || p.ExplorationPercent > 50 || p.SwitchCooldownSeconds < 0 || p.SwitchCooldownSeconds > 86400 || p.MinimumTTLSeconds < 1 || p.MaximumTTLSeconds > 3600 || p.MaximumTTLSeconds < p.MinimumTTLSeconds {
		return fmt.Errorf("DNS query strategy outside supported bounds")
	}
	if len(p.PhysicalRoutes) > 64 || len(p.PhysicalRoutes) > 0 && p.RankingMode != "active" {
		return fmt.Errorf("physical route opt-in requires active bounded query policy")
	}
	seen := map[string]bool{}
	for _, route := range p.PhysicalRoutes {
		if !validDNSConsumerZone(route.Hostname) || seen[route.Hostname] || !slices.Contains([]string{"large_body_api", "small_api", "dynamic_api", "static_cacheable", "streaming", "sse", "websocket", "html_dynamic"}, route.TrafficClass) || model.ValidatePhysicalEdgeQualityPolicy(route.Policy) != nil {
			return fmt.Errorf("invalid or duplicate physical route strategy")
		}
		seen[route.Hostname] = true
	}
	return nil
}
