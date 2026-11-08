package platformconfig

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"strings"
)

// Keep the complete canonical content hash before any subset is decoded. The
// bytes are local to this one validation, never a cache or a trust witness.
func dnsValidationContentDigest(content map[string]any) (string, []byte, bool, error) {
	// A custom encoder may change its tree while being called. Remember its
	// presence before Marshal, even if it replaces itself with native values.
	remaining := 65536
	native := dnsValidationNativeTree(content, 0, &remaining)
	raw, err := json.Marshal(content)
	if err != nil {
		return "", nil, false, err
	}
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:]), raw, native, nil
}

// Called only after the complete content digest has passed. Encoding that
// content established valid JSON, including fields the typed view ignores.
// Native immutable trees need only re-encode the typed view's fields, keeping
// every case alias in the same sorted order used by encoding/json. Custom or
// small values retain the original second complete Marshal, including its
// observable custom-marshaler calls and errors.
func marshalDNSChildValidationFields(content map[string]any, full []byte, native bool) ([]byte, error) {
	if !native || len(full) < 64<<10 {
		return json.Marshal(content)
	}
	remaining := 65536
	if !dnsValidationNativeTree(content, 0, &remaining) {
		return json.Marshal(content)
	}
	fields := make(map[string]any, 2)
	for key, value := range content {
		if strings.EqualFold(key, "policy") || strings.EqualFold(key, "lineage") {
			fields[key] = value
		}
	}
	return json.Marshal(fields)
}

// Inspect types and structure without scanning string bytes or invoking user
// methods. Work and recursion are bounded; exceeding either limit falls back
// to the original encoder and never changes acceptance.
func dnsValidationNativeTree(value any, depth int, remaining *int) bool {
	if depth > 256 || *remaining == 0 {
		return false
	}
	*remaining -= 1
	switch value := value.(type) {
	case nil, string, bool:
		return true
	case float64:
		return !math.IsNaN(value) && !math.IsInf(value, 0)
	case map[string]any:
		for _, child := range value {
			if !dnsValidationNativeTree(child, depth+1, remaining) {
				return false
			}
		}
		return true
	case []any:
		for _, child := range value {
			if !dnsValidationNativeTree(child, depth+1, remaining) {
				return false
			}
		}
		return true
	default:
		return false
	}
}
