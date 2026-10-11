package platformconfig

import "testing"

func TestPolicyFingerprintSeparatesNilTypesAndNestedBoundaries(t *testing.T) {
	values := []any{nil, map[string]any(nil), map[string]any{}, []any(nil), []any{}, []any{"ab", "c"}, []any{"a", "bc"}, map[string]any{"ab": "c"}, map[string]any{"a": "bc"}, true, "true", float64(1), "1"}
	seen := map[[32]byte]bool{}
	for _, value := range values {
		key, _, ok := policyTreeFingerprint(value)
		if !ok || seen[key] {
			t.Fatalf("ambiguous cache identity for %#v", value)
		}
		seen[key] = true
	}
	a, _, _ := policyTreeFingerprint(map[string]any{"a": 1.0, "b": 2.0})
	b, _, _ := policyTreeFingerprint(map[string]any{"b": 2.0, "a": 1.0})
	if a != b {
		t.Fatal("map order changes identity")
	}
}
