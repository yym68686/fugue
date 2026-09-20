package platformconfig

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// DNSQueryPolicy declares strategy, never current candidates or ranking results.
type DNSQueryPolicy struct {
	RankingMode           string `json:"ranking_mode"`
	PreferenceMode        string `json:"preference_mode"`
	ECSEnabled            bool   `json:"ecs_enabled"`
	ExplorationPercent    int    `json:"exploration_percent"`
	SwitchCooldownSeconds int    `json:"switch_cooldown_seconds"`
	MinimumTTLSeconds     int    `json:"minimum_ttl_seconds"`
	MaximumTTLSeconds     int    `json:"maximum_ttl_seconds"`
}

func (p *DNSQueryPolicy) UnmarshalJSON(raw []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	keys := []string{"ranking_mode", "preference_mode", "ecs_enabled", "exploration_percent", "switch_cooldown_seconds", "minimum_ttl_seconds", "maximum_ttl_seconds"}
	if len(fields) != len(keys) {
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
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return err
	}
	*p = DNSQueryPolicy(decoded)
	return ValidateDNSQueryPolicy(p)
}
func ValidateDNSQueryPolicy(p *DNSQueryPolicy) error {
	if p == nil {
		return nil
	}
	if p.RankingMode != "active" && p.RankingMode != "shadow" && p.RankingMode != "disabled" || p.PreferenceMode != "runtime_locality" || p.ExplorationPercent < 0 || p.ExplorationPercent > 50 || p.SwitchCooldownSeconds < 0 || p.SwitchCooldownSeconds > 86400 || p.MinimumTTLSeconds < 1 || p.MaximumTTLSeconds > 3600 || p.MaximumTTLSeconds < p.MinimumTTLSeconds {
		return fmt.Errorf("DNS query strategy outside supported bounds")
	}
	return nil
}
