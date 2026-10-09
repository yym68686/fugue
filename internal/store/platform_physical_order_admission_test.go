package store

import (
	"testing"

	"fugue/internal/model"
)

func TestPhysicalOrderCapabilityFollowsExactSignedChildPolicy(t *testing.T) {
	for _, test := range []struct {
		name     string
		policy   any
		required bool
		invalid  bool
	}{
		{name: "old", policy: map[string]any{"dns_answer_rules": []any{map[string]any{"selection_mode": "geo"}}}},
		{name: "physical quality", policy: map[string]any{"dns_answer_rules": []any{map[string]any{"selection_mode": "physical_quality"}}}},
		{name: "order", policy: map[string]any{"dns_answer_rules": []any{map[string]any{"selection_mode": "physical_order"}}}, required: true},
		{name: "mixed", policy: map[string]any{"dns_answer_rules": []any{map[string]any{"selection_mode": "geo"}, map[string]any{"selection_mode": "physical_order"}}}, required: true},
		{name: "malformed", policy: map[string]any{"dns_answer_rules": "invalid"}, invalid: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			required, err := physicalOrderCapabilityRequired(model.PlatformArtifact{Content: map[string]any{"policy": test.policy}})
			if required != test.required || (err != nil) != test.invalid {
				t.Fatal(required, err)
			}
		})
	}
}
