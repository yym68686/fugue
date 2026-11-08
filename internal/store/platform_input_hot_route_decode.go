package store

import (
	"encoding/json"
	"strconv"
	"strings"

	"fugue/internal/model"
)

// Route and TLS dependency extraction reads lineage, policy and metadata. Keep
// the full JSON syntax, depth and float64 checks, without allocating unrelated
// configuration subtrees. Small sources keep the original decoder: the extra
// validation pass is useful only when it avoids a substantial object graph.
func decodeCompilerInputHotArtifactJSON(a *model.PlatformArtifact, content, metadata []byte) error {
	if len(content) < 64<<10 || a.Content != nil ||
		(a.ArtifactKind != model.PlatformArtifactKindEdgeRouteBundle && a.ArtifactKind != model.PlatformArtifactKindCaddyRouteConfig) {
		if json.Unmarshal(content, &a.Content) != nil || json.Unmarshal(metadata, &a.Metadata) != nil {
			return ErrConflict
		}
		return nil
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(content, &fields) != nil || !compilerInputHotJSONNumbersRepresentable(content) || json.Unmarshal(metadata, &a.Metadata) != nil {
		return ErrConflict
	}
	a.Content = map[string]any{}
	for _, key := range []string{"lineage", "policy"} {
		if raw, present := fields[key]; present {
			var value any
			if json.Unmarshal(raw, &value) != nil {
				return ErrConflict
			}
			a.Content[key] = value
		}
	}
	return nil
}

// RawMessage decoding above already checked syntax and maximum JSON depth.
// Match the generic map decoder's float64 overflow refusal in ignored values.
func compilerInputHotJSONNumbersRepresentable(raw []byte) bool {
	for i := 0; i < len(raw); i++ {
		if raw[i] == '"' {
			for i++; i < len(raw); i++ {
				if raw[i] == '\\' {
					i++
				} else if raw[i] == '"' {
					break
				}
			}
		} else if raw[i] == '-' || raw[i] >= '0' && raw[i] <= '9' {
			start := i
			for i < len(raw) && !strings.ContainsRune(" \t\r\n,]}", rune(raw[i])) {
				i++
			}
			if _, err := strconv.ParseFloat(string(raw[start:i]), 64); err != nil {
				return false
			}
			i--
		}
	}
	return true
}
