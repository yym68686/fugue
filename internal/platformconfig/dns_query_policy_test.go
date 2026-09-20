package platformconfig

import (
	"encoding/json"
	"testing"
)

func TestDNSQueryPolicyRequiresExplicitBoundedStrategy(t *testing.T) {
	valid := map[string]any{"ranking_mode": "active", "preference_mode": "runtime_locality", "ecs_enabled": true, "exploration_percent": 5, "switch_cooldown_seconds": 1800, "minimum_ttl_seconds": 60, "maximum_ttl_seconds": 120}
	for key := range valid {
		for _, null := range []bool{false, true} {
			fields := map[string]any{}
			for k, v := range valid {
				fields[k] = v
			}
			if null {
				fields[key] = nil
			} else {
				delete(fields, key)
			}
			raw, _ := json.Marshal(fields)
			var p DNSQueryPolicy
			if json.Unmarshal(raw, &p) == nil {
				t.Fatal("partial query strategy accepted", key, null)
			}
		}
	}
	for _, test := range []struct {
		key   string
		value any
	}{{"ranking_mode", "shell"}, {"preference_mode", "unknown"}, {"minimum_ttl_seconds", 0}, {"minimum_ttl_seconds", 121}, {"maximum_ttl_seconds", 3601}, {"exploration_percent", 51}, {"switch_cooldown_seconds", 86401}, {"shell", "echo"}} {
		fields := map[string]any{}
		for k, v := range valid {
			fields[k] = v
		}
		fields[test.key] = test.value
		raw, _ := json.Marshal(fields)
		var p DNSQueryPolicy
		if json.Unmarshal(raw, &p) == nil {
			t.Fatal("invalid query strategy accepted", test.key)
		}
	}
	for _, mode := range []string{"active", "shadow", "disabled"} {
		valid["ranking_mode"] = mode
		raw, _ := json.Marshal(valid)
		var p DNSQueryPolicy
		if err := json.Unmarshal(raw, &p); err != nil {
			t.Fatal(err)
		}
	}
}
